"""Optional semantic search for memory-mcp (embeddings + vector search).

Deliberately a SEPARATE module: the core server stays stdlib-only. Everything
here activates only when the caller sets `MEMORY_MCP_EMBEDDINGS=1`; the core
imports this file lazily and treats any failure as "semantic search off".

Providers (`MEMORY_MCP_EMBED_PROVIDER`):
  - `ollama`  (default URL http://localhost:11434, model default
    nomic-embed-text; for mixed-language facts prefer bge-m3)
  - `openai`  (any OpenAI-compatible /embeddings endpoint;
    `MEMORY_MCP_EMBED_URL` + optional `MEMORY_MCP_EMBED_KEY`)
  - `fastembed` (offline ONNX; `pip install fastembed`; model default
    intfloat/multilingual-e5-small)
  - `test`    (deterministic char-n-gram vectors — tests/diagnostics only)

Storage: `fact_embeddings(fact_id, vec, model, updated_at)` — one normalized
float32 vector per fact. Search is brute-force cosine over the stored vectors:
for a fact store of thousands of facts this is milliseconds (numpy when
available, pure Python otherwise); swap in sqlite-vec later without changing
the tool API if the store outgrows it.

Env:
  MEMORY_MCP_EMBEDDINGS=1         enable
  MEMORY_MCP_EMBED_PROVIDER       ollama|openai|fastembed|test (default ollama)
  MEMORY_MCP_EMBED_URL            provider base URL (ollama/openai)
  MEMORY_MCP_EMBED_MODEL          model name (provider-specific default)
  MEMORY_MCP_EMBED_KEY            bearer token for openai-compatible endpoints
"""

import array
import hashlib
import json
import math
import os
import urllib.request

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

DEFAULT_URLS = {
    "ollama": "http://localhost:11434",
    "openai": "http://localhost:8000/v1",
}
DEFAULT_MODELS = {
    "ollama": "nomic-embed-text",
    "openai": "text-embedding-3-small",
    "fastembed": "intfloat/multilingual-e5-small",
    "test": "test-n-gram-v1",
}
_TIMEOUT = 10  # seconds per HTTP embedding call (write path waits on this)


def enabled():
    return os.environ.get("MEMORY_MCP_EMBEDDINGS") == "1"


def provider():
    p = (os.environ.get("MEMORY_MCP_EMBED_PROVIDER") or "ollama").strip().lower()
    return p if p in DEFAULT_MODELS else "ollama"


def model_name():
    return os.environ.get("MEMORY_MCP_EMBED_MODEL") or DEFAULT_MODELS[provider()]


def base_url():
    return os.environ.get("MEMORY_MCP_EMBED_URL") or DEFAULT_URLS.get(provider(), "http://localhost:11434")


def api_key():
    return os.environ.get("MEMORY_MCP_EMBED_KEY") or ""


# --------------------------------------------------------------------------
# Schema + storage
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_embeddings (
  fact_id INTEGER PRIMARY KEY REFERENCES facts(id) ON DELETE CASCADE,
  vec BLOB NOT NULL,
  model TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


def ensure_schema(con):
    con.execute(SCHEMA)


def _store(con, fact_id, vec, model):
    ts = _now()
    con.execute(
        "INSERT INTO fact_embeddings (fact_id, vec, model, updated_at) VALUES (?,?,?,?) "
        "ON CONFLICT(fact_id) DO UPDATE SET vec=excluded.vec, model=excluded.model, updated_at=excluded.updated_at",
        (fact_id, _pack(vec), model, ts))
    con.commit()


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# Vector math (numpy when available, pure Python otherwise)
# --------------------------------------------------------------------------

try:  # optional acceleration; core stays dependency-free
    import numpy as _np
except Exception:  # pragma: no cover - exercised only without numpy
    _np = None


def _pack(vec):
    return array.array("f", vec).tobytes()


def _unpack(blob):
    arr = array.array("f")
    arr.frombytes(blob)
    return arr


def _normalize(vec):
    s = 0.0
    for x in vec:
        s += x * x
    if s <= 0:
        return vec
    inv = 1.0 / math.sqrt(s)
    return [x * inv for x in vec]


def _vec_bytes(v):
    """BLOB bytes for storage, or already-bytes — never a raw list."""
    if isinstance(v, (bytes, bytearray)):
        return v
    return _pack(v)


def _dot(a, b):
    ab, bb = _vec_bytes(a), _vec_bytes(b)
    if _np is not None:
        return float(_np.dot(_np.frombuffer(ab, dtype=_np.float32),
                             _np.frombuffer(bb, dtype=_np.float32)))
    aa, bb2 = _unpack(ab), _unpack(bb)
    s = 0.0
    for i in range(len(aa)):
        s += aa[i] * bb2[i]
    return s


# --------------------------------------------------------------------------
# Providers
# --------------------------------------------------------------------------

def _http_json(url, payload, headers=None):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _embed_ollama(texts):
    url = base_url().rstrip("/") + "/api/embed"
    out = _http_json(url, {"model": model_name(), "input": texts})
    return [[float(x) for x in v] for v in out.get("embeddings", [])]


def _embed_openai(texts):
    url = base_url().rstrip("/") + "/embeddings"
    headers = {"Authorization": "Bearer " + api_key()} if api_key() else {}
    out = _http_json(url, {"model": model_name(), "input": texts}, headers)
    rows = out.get("data", [])
    rows.sort(key=lambda r: r.get("index", 0))
    return [[float(x) for x in r["embedding"]] for r in rows]


_fastembed_model = None


def _embed_fastembed(texts):
    global _fastembed_model
    try:
        from fastembed import TextEmbedding
    except ImportError as e:
        raise RuntimeError("fastembed provider needs 'pip install fastembed': %s" % e)
    if _fastembed_model is None:
        _fastembed_model = TextEmbedding(model_name=model_name())
    return [[float(x) for x in v] for v in _fastembed_model.embed(texts)]


def _embed_test(texts):
    """Deterministic char-n-gram vectors: paraphrases share n-grams, so similar
    texts get similar vectors — enough for deterministic tests without a model."""
    dim = 256
    out = []
    for t in texts:
        vec = [0.0] * dim
        low = (" " + t.strip().lower() + " ").encode("utf-8")
        for i in range(len(low) - 2):
            gram = low[i:i + 3]
            idx = int.from_bytes(hashlib.sha256(gram).digest()[:4], "big") % dim
            vec[idx] += 1.0
        out.append(_normalize(vec))
    return out


def embed(texts):
    """Return a list of normalized vectors, one per text. Raises on provider
    failure — callers treat any failure as 'semantic search off for this fact'."""
    texts = [t.strip() for t in texts]
    p = provider()
    if p == "ollama":
        return _embed_ollama(texts)
    if p == "openai":
        return _embed_openai(texts)
    if p == "fastembed":
        return _embed_fastembed(texts)
    return _embed_test(texts)


# --------------------------------------------------------------------------
# Write path + search
# --------------------------------------------------------------------------

def embed_fact(con, fact_id, text):
    """Best-effort: compute and store the fact's vector. Never raises into the
    caller's write path — failures mean the fact simply has no vector yet."""
    try:
        vec = embed([text])[0]
        _store(con, fact_id, vec, model_name())
    except Exception:
        pass


def embed_backfill(con):
    """Compute vectors for facts that have none (or whose model changed)."""
    rows = con.execute(
        "SELECT f.id, f.text FROM facts f WHERE f.archived=0 "
        "AND NOT EXISTS (SELECT 1 FROM fact_embeddings e WHERE e.fact_id=f.id) "
        "ORDER BY f.id LIMIT 500").fetchall()
    processed = failed = 0
    for r in rows:
        try:
            vec = embed([r["text"]])[0]
            _store(con, r["id"], vec, model_name())
            processed += 1
        except Exception:
            failed += 1
    return {"processed": processed, "failed": failed, "remaining": _missing_count(con)}


def _missing_count(con):
    return con.execute(
        "SELECT COUNT(*) FROM facts f WHERE f.archived=0 "
        "AND NOT EXISTS (SELECT 1 FROM fact_embeddings e WHERE e.fact_id=f.id)").fetchone()[0]


def search_semantic(con, query, limit=20, threshold=0.0):
    """Brute-force cosine over stored vectors, best first."""
    qvec = _normalize(embed([query])[0])
    rows = con.execute(
        "SELECT e.fact_id, e.vec, e.model, f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong "
        "FROM fact_embeddings e JOIN facts f ON f.id=e.fact_id WHERE f.archived=0").fetchall()
    scored = []
    for r in rows:
        score = _dot(qvec, r["vec"])
        if score < threshold:
            continue
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    out = []
    for score, r in scored[:limit]:
        d = {k: r[k] for k in ("id", "text", "source", "project", "domain", "trust", "strong")}
        d["score"] = round(float(score), 4)
        out.append(d)
    return {"count": len(out), "model": model_name(), "facts": out}


def hybrid_rerank(con, query, fts_facts, limit=20):
    """RRF merge of FTS BM25 ranks and semantic ranks (k=60)."""
    if not fts_facts:
        return search_semantic(con, query, limit=limit)
    sem = search_semantic(con, query, limit=200, threshold=0.0)
    sem_index = {f["id"]: i for i, f in enumerate(sem["facts"])}
    fts_index = {f["id"]: i for i, f in enumerate(fts_facts)}
    k = 60
    merged = {}
    for fid, i in fts_index.items():
        merged[fid] = 1.0 / (k + i + 1)
    for fid, i in sem_index.items():
        merged[fid] = merged.get(fid, 0.0) + 1.0 / (k + i + 1)
    ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]
    by_id = {f["id"]: f for f in fts_facts}
    by_id.update({f["id"]: f for f in sem["facts"]})
    out = []
    for fid, _score in ranked:
        f = dict(by_id[fid])
        f["semantic_score"] = round(_score, 4)
        out.append(f)
    return {"count": len(out), "model": model_name(), "facts": out}
