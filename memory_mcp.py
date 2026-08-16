#!/usr/bin/env python3
"""memory-mcp — shared fact memory for reasonix/jcode/codex runtimes (Phase 1, 2026-08-15).

Stdio MCP server (JSON-RPC 2.0, newline-delimited), SQLite + FTS5 storage.
Replaces the storage layer of the reasonix memory patches with a shared,
searchable fact store; extraction/gating/injection stay client-side.

Schema: text, sha256 (dedup), source, project, domain, trust (high|medium|low),
strong (bool), created_at, updated_at, archived (soft delete).

Tools:
  remember_fact {text, source?, project?, domain?, trust?, strong?}
  search_facts  {query, limit?, trust_min?, strong_only?, project?, domain?}
  list_facts    {project?, domain?, limit?}
  summarize_index {project?, domain?, trust_min?, strong_only?, limit?, max_chars?}
  forget_fact   {id|sha256}
  stats         {}
  export        {}
  -- v0.3 graph/decisions/provenance --
  remember_entity {name, type?, aliases?}
  remember_relation {subject, predicate, object, source_fact_id?}
  search_graph {entity, depth? (1-2), limit?}
  record_decision {category?, subject?, scenario, reasoning?, outcome?, confidence?,
                   decision_maker?, issue_ref?, parent_decision_id?}
  query_decisions {category?, subject?, outcome?, decision_maker?, issue_ref?, limit?}
  find_precedents {scenario, category?, limit?}
  get_causal_chain {decision_id}
  get_provenance {fact_id | sha256}
  attach_evidence {fact_id, source_ref, source_checksum?, fetched_at?}
  detect_conflicts {text}
"""
import hashlib, json, os, sqlite3, sys
from datetime import datetime, timezone

def default_db_path():
    """Script-relative default: <repo>/data/facts.db — portable across environments.

    Override with MEMORY_MCP_DB (used by all deployment runtimes: host wrapper,
    docker containers via /opt/memory-shared).
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "facts.db")


DB_PATH = os.environ.get("MEMORY_MCP_DB") or default_db_path()
VALID_TRUST = ("high", "medium", "low")

# v0.4 (2026-08-16): bi-temporal validity + importance + human confirmation.
# Additive columns — _migrate() adds them to existing databases.
_FACT_EXTRA_COLUMNS = {
    "importance": "REAL NOT NULL DEFAULT 0.5",
    "invalid_at": "TEXT NOT NULL DEFAULT ''",
    "superseded_by": "INTEGER",
    "confirmed": "INTEGER NOT NULL DEFAULT 0",
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS facts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 TEXT NOT NULL UNIQUE,
  text TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT '',
  project TEXT NOT NULL DEFAULT '',
  domain TEXT NOT NULL DEFAULT '',
  trust TEXT NOT NULL DEFAULT 'medium' CHECK (trust IN ('high','medium','low')),
  strong INTEGER NOT NULL DEFAULT 0,
  importance REAL NOT NULL DEFAULT 0.5,
  invalid_at TEXT NOT NULL DEFAULT '',
  superseded_by INTEGER,
  confirmed INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0
);
CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
  text, content='facts', content_rowid='id'
);
CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
  INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
  INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;
CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN
  INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
  INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
END;

-- v0.3 (2026-08-15): lightweight knowledge graph + decision log + provenance.
CREATE TABLE IF NOT EXISTS entities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL DEFAULT '',
  aliases TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject_id INTEGER NOT NULL REFERENCES entities(id),
  predicate TEXT NOT NULL,
  object_id INTEGER NOT NULL REFERENCES entities(id),
  source_fact_id INTEGER,
  created_at TEXT NOT NULL,
  UNIQUE(subject_id, predicate, object_id)
);
CREATE TABLE IF NOT EXISTS decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  category TEXT NOT NULL DEFAULT '',
  subject TEXT NOT NULL DEFAULT '',
  scenario TEXT NOT NULL,
  reasoning TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL DEFAULT '',
  confidence REAL,
  decision_maker TEXT NOT NULL DEFAULT '',
  issue_ref TEXT NOT NULL DEFAULT '',
  parent_decision_id INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
  scenario, reasoning, category, content='decisions', content_rowid='id'
);
CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
  INSERT INTO decisions_fts(rowid, scenario, reasoning, category)
  VALUES (new.id, new.scenario, new.reasoning, new.category);
END;
CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
  INSERT INTO decisions_fts(decisions_fts, rowid, scenario, reasoning, category)
  VALUES ('delete', old.id, old.scenario, old.reasoning, old.category);
END;
CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
  INSERT INTO decisions_fts(decisions_fts, rowid, scenario, reasoning, category)
  VALUES ('delete', old.id, old.scenario, old.reasoning, old.category);
  INSERT INTO decisions_fts(rowid, scenario, reasoning, category)
  VALUES (new.id, new.scenario, new.reasoning, new.category);
END;
CREATE TABLE IF NOT EXISTS evidence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fact_id INTEGER NOT NULL REFERENCES facts(id),
  source_ref TEXT NOT NULL,
  source_checksum TEXT NOT NULL DEFAULT '',
  fetched_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  UNIQUE(fact_id, source_ref)
);
CREATE INDEX IF NOT EXISTS relations_subject_idx ON relations(subject_id);
CREATE INDEX IF NOT EXISTS relations_object_idx ON relations(object_id);
CREATE INDEX IF NOT EXISTS evidence_fact_idx ON evidence(fact_id);
CREATE INDEX IF NOT EXISTS decisions_parent_idx ON decisions(parent_decision_id);
"""

# Optional semantic search (embeddings.py) — created here so the schema is
# consistent even when the module is off; only filled when embeddings are on.
_EMBED_SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_embeddings (
  fact_id INTEGER PRIMARY KEY REFERENCES facts(id) ON DELETE CASCADE,
  vec BLOB NOT NULL,
  model TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "from", "are", "was", "were",
    "has", "have", "had", "not", "but", "our", "its", "you", "your", "all",
    "can", "will", "would", "should", "about", "into", "than", "then", "there",
    "these", "those", "which", "when", "where", "what", "who", "how", "why",
    "что", "для", "при", "как", "это", "не", "по", "из", "на", "в", "и", "с",
    "о", "к", "у", "же", "бы", "или", "если", "то", "все", "его", "её", "их",
    "нам", "вас", "уже", "ещё", "так", "вот",
}

_TERM_RE = None  # compiled lazily (avoids re module import cost at module load)


def fts_terms(text):
    """Deterministic FTS term list: lowercase alpha-numeric tokens >= 3 chars,
    stopwords removed, deduped, order preserved."""
    global _TERM_RE
    if _TERM_RE is None:
        import re as _re
        _TERM_RE = _re.compile(r"[a-zа-я0-9]{3,}")
    seen, terms = set(), []
    for t in _TERM_RE.findall((text or "").lower()):
        if t not in _STOPWORDS and t not in seen:
            seen.add(t)
            terms.append(t)
    return terms



def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_db():
    dbdir = os.path.dirname(DB_PATH) or "."
    try:
        os.makedirs(dbdir, exist_ok=True)
    except OSError as e:
        # Full detail to stderr only — the client-visible message must not
        # leak the resolved host path (repo rule: no host paths).
        print(f"memory-mcp: cannot create DB directory {dbdir!r}: {e}", file=sys.stderr)
        raise RuntimeError(
            "cannot open the fact store: DB directory is not writable; "
            "set MEMORY_MCP_DB to a writable path (e.g. a rw bind-mount)")
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
    except sqlite3.DatabaseError as e:
        print(f"memory-mcp: cannot open DB {DB_PATH!r}: {e}", file=sys.stderr)
        raise RuntimeError(
            "cannot open the fact store: DB file is not accessible or corrupt; "
            "set MEMORY_MCP_DB to a writable path (e.g. a rw bind-mount)")
    con.row_factory = sqlite3.Row
    # Мульти-райтер: хост + docker-рантаймы пишут в один файл (bind-mount).
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA foreign_keys=ON")
    con.executescript(_SCHEMA)
    con.executescript(_EMBED_SCHEMA)
    _migrate_facts(con)
    return con


def _migrate_facts(con):
    """Additive migration for databases created before v0.4: bring the facts
    table up to the current columns without touching existing rows."""
    existing = {r["name"] for r in con.execute("PRAGMA table_info(facts)")}
    for name, decl in _FACT_EXTRA_COLUMNS.items():
        if name not in existing:
            con.execute("ALTER TABLE facts ADD COLUMN %s %s" % (name, decl))
    con.commit()


# ---------------- tools ----------------

def _emb():
    """Lazy handle to the optional embeddings module, or None when disabled
    (MEMORY_MCP_EMBEDDINGS != 1) or unavailable. The core server never depends
    on it: every call site treats None as 'semantic search off'."""
    if os.environ.get("MEMORY_MCP_EMBEDDINGS") != "1":
        return None
    try:
        import embeddings
        return embeddings
    except ImportError:
        return None


def _graph_expand_facts(con, hit_facts, limit=10):
    """Entity-graph expansion: entities mentioned in the hit facts -> graph
    neighbors -> facts mentioning the neighbors. Returns dict rows (id/text/...).
    Shared by search_facts {graph=true} and compose_recall {graph=true}."""
    rows = con.execute("SELECT name FROM entities WHERE length(name) >= 3").fetchall()
    names = [r["name"] for r in rows]
    mentioned = set()
    for f in hit_facts:
        low = (f.get("text") or "").lower()
        for n in names:
            if n.lower() in low:
                mentioned.add(n)
    neighbors = set()
    for n in list(mentioned)[:8]:
        for r in con.execute(
            "SELECT o.name AS nb FROM relations r JOIN entities s ON s.id=r.subject_id "
            "JOIN entities o ON o.id=r.object_id WHERE s.name=? "
            "UNION SELECT s.name FROM relations r JOIN entities s ON s.id=r.subject_id "
            "JOIN entities o ON o.id=r.object_id WHERE o.name=?", (n, n)):
            neighbors.add(r["nb"])
    out, seen = [], {f["id"] for f in hit_facts}
    for nb in list(neighbors)[:12]:
        esc = nb.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = con.execute(
            "SELECT id, text, source, project, domain, trust, strong, importance, confirmed "
            "FROM facts WHERE text LIKE ? ESCAPE '\\' AND archived=0 AND invalid_at='' LIMIT 5",
            ("%" + esc + "%",)).fetchall()
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                out.append(dict(r))
                if len(out) >= limit:
                    return out
    return out


def _importance(args):
    """Clamp the importance argument to [0,1]; default 0.5."""
    try:
        return max(0.0, min(1.0, float(args.get("importance", 0.5))))
    except (TypeError, ValueError):
        return 0.5


def _mod(name, env):
    """Generic lazy loader for the optional pipeline modules (extract/recall/
    verify). Same contract as _emb: None when the env gate is off."""
    if os.environ.get(env) != "1":
        return None
    try:
        return __import__(name)
    except ImportError:
        return None


def _disabled(env):
    return {"error": "disabled (set %s=1)" % env}


def ingest_turn(args):
    m = _mod("extract", "MEMORY_MCP_EXTRACT")
    if m is None:
        return _disabled("MEMORY_MCP_EXTRACT")
    return m.ingest_turn(args)


def compose_recall(args):
    m = _mod("recall", "MEMORY_MCP_RECALL")
    if m is None:
        return _disabled("MEMORY_MCP_RECALL")
    return m.compose_recall(args)


def sweep_freshness(args):
    m = _mod("recall", "MEMORY_MCP_RECALL")
    if m is None:
        return _disabled("MEMORY_MCP_RECALL")
    return m.sweep_freshness(args)


def consolidate(args):
    m = _mod("verify", "MEMORY_MCP_VERIFY")
    if m is None:
        return _disabled("MEMORY_MCP_VERIFY")
    return m.consolidate(args)


def verify_facts(args):
    m = _mod("verify", "MEMORY_MCP_VERIFY")
    if m is None:
        return _disabled("MEMORY_MCP_VERIFY")
    return m.verify_facts(args)


def remember_fact(args):
    text = (args.get("text") or "").strip()
    if not text:
        return {"error": "text is required"}
    trust = args.get("trust", "medium")
    if trust not in VALID_TRUST:
        return {"error": f"trust must be one of {VALID_TRUST}"}
    importance = _importance(args)
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    ts = now()
    con = get_db()
    try:
        cur = con.execute("SELECT id, created_at FROM facts WHERE sha256=?", (sha,))
        row = cur.fetchone()
        if row:
            # Re-remembering resurrects an archived/invalidated fact and can
            # refresh its importance.
            sets = ["updated_at=?", "archived=0", "invalid_at=''", "superseded_by=NULL"]
            params = [ts]
            if "importance" in args:
                sets.append("importance=?")
                params.append(importance)
            con.execute("UPDATE facts SET %s WHERE id=?" % ", ".join(sets),
                        params + [row["id"]])
            con.commit()
            return {"id": row["id"], "sha256": sha, "dedup": True,
                    "created_at": row["created_at"], "updated_at": ts}
        cur = con.execute(
            "INSERT INTO facts (sha256, text, source, project, domain, trust, strong, importance, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sha, text, args.get("source", ""), args.get("project", ""),
             args.get("domain", ""), trust, 1 if args.get("strong") else 0,
             importance, ts, ts))
        con.commit()
        fid = cur.lastrowid
        emb = _emb()
        if emb is not None:
            emb.embed_fact(con, fid, text)  # best-effort, never raises
        return {"id": fid, "sha256": sha, "dedup": False,
                "created_at": ts, "updated_at": ts}
    finally:
        con.close()


def search_facts(args):
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit = max(1, min(int(args.get("limit", 20)), 100))
    sql = ("SELECT f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong, "
           "f.importance, f.confirmed, f.invalid_at, f.created_at, "
           "bm25(facts_fts) AS rank "
           "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
           "WHERE facts_fts MATCH ? AND f.archived=0")
    params = [query]
    if args.get("valid_at"):
        # bi-temporal: also include facts that were still valid at that time
        sql += " AND (f.invalid_at='' OR f.invalid_at >= ?)"
        params.append(args["valid_at"])
    else:
        sql += " AND f.invalid_at=''"
    if args.get("trust_min"):
        order = {"high": 0, "medium": 1, "low": 2}
        allowed = [t for t in VALID_TRUST if order[t] <= order[args["trust_min"]]]
        sql += f" AND f.trust IN ({','.join('?' * len(allowed))})"
        params += allowed
    if args.get("strong_only"):
        sql += " AND f.strong=1"
    if args.get("project"):
        sql += " AND f.project=?"
        params.append(args["project"])
    if args.get("domain"):
        sql += " AND f.domain=?"
        params.append(args["domain"])
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)
    con = get_db()
    try:
        try:
            rows = [dict(r) for r in con.execute(sql, params)]
        except sqlite3.OperationalError:
            # FTS5 синтаксис (дефисы/операторы) — повтор как литеральная фраза
            phrase = '"' + query.replace('"', '""') + '"'
            rows = [dict(r) for r in con.execute(sql.replace("facts_fts MATCH ?", "facts_fts MATCH ?", 1),
                                                 [phrase] + params[1:])]
        graph = []
        if args.get("graph") and rows:
            graph = _graph_expand_facts(con, rows, limit * 2)
            if graph:
                k = 60
                merged = {f["id"]: 1.0 / (k + i + 1) for i, f in enumerate(rows)}
                for i, f in enumerate(graph):
                    merged[f["id"]] = merged.get(f["id"], 0.0) + 1.0 / (k + i + 1)
                ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]
                by_id = {f["id"]: f for f in rows}
                by_id.update({f["id"]: f for f in graph})
                rows = [dict(by_id[fid], graph_rank=round(score, 4)) for fid, score in ranked]
        emb = _emb()
        if emb is not None and args.get("semantic"):
            if rows:
                return emb.hybrid_rerank(con, query, rows, limit=limit)
            # No lexical hits: fall back to semantic ranking alone.
            return emb.search_semantic(con, query, limit=limit)
        result = {"count": len(rows), "facts": rows}
        if args.get("graph"):
            result["graph"] = len(graph)
        return result
    except sqlite3.OperationalError as e:
        return {"error": f"query failed: {e}", "facts": []}
    finally:
        con.close()


def search_semantic(args):
    """Semantic (embedding) search — enabled only with MEMORY_MCP_EMBEDDINGS=1."""
    emb = _emb()
    if emb is None:
        return {"error": "semantic search is disabled (set MEMORY_MCP_EMBEDDINGS=1)"}
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit = max(1, min(int(args.get("limit", 20)), 100))
    threshold = float(args.get("threshold", 0.0))
    con = get_db()
    try:
        return emb.search_semantic(con, query, limit=limit, threshold=threshold)
    finally:
        con.close()


def embed_backfill(args):
    """Compute vectors for facts that have none (backfill after enabling)."""
    emb = _emb()
    if emb is None:
        return {"error": "semantic search is disabled (set MEMORY_MCP_EMBEDDINGS=1)"}
    con = get_db()
    try:
        return emb.embed_backfill(con)
    finally:
        con.close()


def list_facts(args):
    limit = max(1, min(int(args.get("limit", 50)), 500))
    sql = ("SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
           "created_at, updated_at FROM facts WHERE archived=0 AND invalid_at=''")
    params = []
    if args.get("project"):
        sql += " AND project=?"
        params.append(args["project"])
    if args.get("domain"):
        sql += " AND domain=?"
        params.append(args["domain"])
    sql += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)
    con = get_db()
    try:
        return {"count": len(rows := [dict(r) for r in con.execute(sql, params)]), "facts": rows}
    finally:
        con.close()


def summarize_index(args):
    """Compact one-line-per-fact index, freshest first, capped at max_chars.

    Mirrors the reasonix index-cap behavior (IndexMaxChars=4000, desc clip 120
    chars) for the shared MCP store: lines are `#id trust! [domain] text`,
    ordered by updated_at DESC. Suitable for prompt-injection budgets.
    """
    limit = max(1, min(int(args.get("limit", 200)), 500))
    max_chars = max(int(args.get("max_chars", 4000)), 200)
    sql = ("SELECT id, text, project, domain, trust, strong, updated_at "
           "FROM facts WHERE archived=0 AND invalid_at=''")
    params = []
    if args.get("project"):
        sql += " AND project=?"
        params.append(args["project"])
    if args.get("domain"):
        sql += " AND domain=?"
        params.append(args["domain"])
    if args.get("trust_min"):
        order = {"high": 0, "medium": 1, "low": 2}
        allowed = [t for t in VALID_TRUST if order[t] <= order[args["trust_min"]]]
        sql += f" AND trust IN ({','.join('?' * len(allowed))})"
        params += allowed
    if args.get("strong_only"):
        sql += " AND strong=1"
    sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
    params.append(limit)
    con = get_db()
    try:
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at=''").fetchone()[0]
        rows = [dict(r) for r in con.execute(sql, params)]
    finally:
        con.close()
    lines = []
    for r in rows:
        text = " ".join(r["text"].split())
        if len(text) > 120:
            text = text[:117] + "..."
        tag = r["trust"] + ("!" if r["strong"] else "")
        dom = f" [{r['domain']}]" if r["domain"] else ""
        lines.append(f"#{r['id']} {tag}{dom} {text}")
    joined = "\n".join(lines)
    truncated = len(joined) > max_chars
    if truncated:
        cut = max_chars
        while cut > 0 and joined[cut] != "\n":
            cut -= 1
        joined = joined[:cut]
    return {"count": len(lines), "total": total, "chars": len(joined),
            "truncated": truncated, "index": joined}


def _resolve_entity(con, name, etype="", aliases=""):
    """Get-or-create an entity by name; returns (id, created_flag)."""
    ts = now()
    row = con.execute("SELECT id FROM entities WHERE name=?", (name,)).fetchone()
    if row:
        con.execute("UPDATE entities SET updated_at=?, type=CASE WHEN ?<>'' THEN ? ELSE type END, "
                    "aliases=CASE WHEN ?<>'' THEN ? ELSE aliases END WHERE id=?",
                    (ts, etype, etype, aliases, aliases, row["id"]))
        return row["id"], False
    cur = con.execute("INSERT INTO entities (name, type, aliases, created_at, updated_at) VALUES (?,?,?,?,?)",
                      (name, etype, aliases, ts, ts))
    return cur.lastrowid, True


def remember_entity(args):
    name = (args.get("name") or "").strip()
    if not name:
        return {"error": "name is required"}
    con = get_db()
    try:
        eid, created = _resolve_entity(con, name, args.get("type", ""), args.get("aliases", ""))
        con.commit()
        return {"id": eid, "name": name, "created": created}
    finally:
        con.close()


def remember_relation(args):
    subject = (args.get("subject") or "").strip()
    predicate = (args.get("predicate") or "").strip()
    obj = (args.get("object") or "").strip()
    if not subject or not predicate or not obj:
        return {"error": "subject, predicate and object are required"}
    con = get_db()
    try:
        sid, _ = _resolve_entity(con, subject)
        oid, _ = _resolve_entity(con, obj)
        existing = con.execute(
            "SELECT id FROM relations WHERE subject_id=? AND predicate=? AND object_id=?",
            (sid, predicate, oid)).fetchone()
        if existing:
            return {"id": existing["id"], "dedup": True}
        cur = con.execute(
            "INSERT INTO relations (subject_id, predicate, object_id, source_fact_id, created_at) "
            "VALUES (?,?,?,?,?)",
            (sid, predicate, oid, args.get("source_fact_id"), now()))
        con.commit()
        return {"id": cur.lastrowid, "subject": subject, "predicate": predicate,
                "object": obj, "dedup": False}
    finally:
        con.close()


def search_graph(args):
    name = (args.get("entity") or "").strip()
    if not name:
        return {"error": "entity is required"}
    depth = max(1, min(int(args.get("depth", 1)), 2))
    limit = max(1, min(int(args.get("limit", 50)), 200))
    con = get_db()
    try:
        root = con.execute("SELECT id, name, type FROM entities WHERE name=?", (name,)).fetchone()
        if not root:
            return {"error": f"entity {name!r} not found", "nodes": [], "edges": []}
        nodes, edges, seen = {root["id"]: dict(root)}, [], {root["id"]}
        frontier = [root["id"]]
        for _ in range(depth):
            nxt = []
            for eid in frontier:
                rows = con.execute(
                    "SELECT r.predicate, r.subject_id, r.object_id, s.name AS sn, o.name AS oname "
                    "FROM relations r JOIN entities s ON s.id=r.subject_id "
                    "JOIN entities o ON o.id=r.object_id "
                    "WHERE r.subject_id=? OR r.object_id=?", (eid, eid)).fetchall()
                for r in rows:
                    direction = "out" if r["subject_id"] == eid else "in"
                    other_id = r["object_id"] if direction == "out" else r["subject_id"]
                    other_name = r["oname"] if direction == "out" else r["sn"]
                    edges.append({"subject": r["sn"], "predicate": r["predicate"],
                                  "object": r["oname"], "direction": direction})
                    if other_id not in seen:
                        seen.add(other_id)
                        nxt.append(other_id)
                        nodes[other_id] = {"id": other_id, "name": other_name}
                        if len(nodes) >= limit:
                            break
                if len(nodes) >= limit:
                    break
            frontier = nxt
            if not frontier:
                break
        return {"root": dict(root), "nodes": list(nodes.values()),
                "edges": edges[:limit], "depth": depth}
    finally:
        con.close()


def record_decision(args):
    category = (args.get("category") or "").strip()
    scenario = (args.get("scenario") or "").strip()
    if not scenario:
        return {"error": "scenario is required"}
    ts = now()
    confidence = args.get("confidence")
    if confidence is not None:
        confidence = float(confidence)
    con = get_db()
    try:
        cur = con.execute(
            "INSERT INTO decisions (category, subject, scenario, reasoning, outcome, confidence, "
            "decision_maker, issue_ref, parent_decision_id, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (category, args.get("subject", ""), scenario, args.get("reasoning", ""),
             args.get("outcome", ""), confidence, args.get("decision_maker", ""),
             args.get("issue_ref", ""), args.get("parent_decision_id"), ts, ts))
        con.commit()
        return {"id": cur.lastrowid, "category": category, "scenario": scenario,
                "created_at": ts}
    finally:
        con.close()


def query_decisions(args):
    sql = "SELECT id, category, subject, scenario, reasoning, outcome, confidence, decision_maker, issue_ref, parent_decision_id, created_at FROM decisions WHERE 1=1"
    params = []
    for key in ("category", "subject", "outcome", "decision_maker", "issue_ref"):
        if args.get(key):
            sql += f" AND {key}=?"
            params.append(args[key])
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(max(1, min(int(args.get("limit", 20)), 100)))
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(sql, params)]
        return {"count": len(rows), "decisions": rows}
    finally:
        con.close()


def find_precedents(args):
    scenario = (args.get("scenario") or "").strip()
    if not scenario:
        return {"error": "scenario is required"}
    terms = fts_terms(scenario)
    if not terms:
        return {"error": "scenario has no searchable terms", "count": 0, "precedents": []}
    limit = max(1, min(int(args.get("limit", 10)), 50))
    # OR-joined: precedent search is about similarity, not full-term matching;
    # BM25 ranks the partially-matching decisions.
    query = " OR ".join(terms)
    sql = ("SELECT d.id, d.category, d.subject, d.scenario, d.reasoning, d.outcome, "
           "d.confidence, d.decision_maker, d.issue_ref, d.created_at, "
           "bm25(decisions_fts) AS rank "
           "FROM decisions_fts JOIN decisions d ON d.id = decisions_fts.rowid "
           "WHERE decisions_fts MATCH ?")
    params = [query]
    if args.get("category"):
        sql += " AND d.category=?"
        params.append(args["category"])
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)
    con = get_db()
    try:
        try:
            rows = [dict(r) for r in con.execute(sql, params)]
        except sqlite3.OperationalError:
            phrase = '"' + query.replace('"', '""') + '"'
            rows = [dict(r) for r in con.execute(sql, [phrase] + params[1:])]
        if args.get("semantic"):
            emb = _emb()
            if emb is not None:
                try:
                    sem = emb.search_decision_semantic(con, scenario, limit=limit * 2)
                    sem_rows = sem.get("precedents", [])
                    if sem_rows:
                        k = 60
                        merged = {r["id"]: 1.0 / (k + i + 1) for i, r in enumerate(rows)}
                        for i, r in enumerate(sem_rows):
                            merged[r["id"]] = merged.get(r["id"], 0.0) + 1.0 / (k + i + 1)
                        ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]
                        by_id = {r["id"]: r for r in rows}
                        by_id.update({r["id"]: r for r in sem_rows})
                        rows = [dict(by_id[fid], semantic_score=round(score, 4))
                                for fid, score in ranked]
                except Exception:
                    pass
        return {"count": len(rows), "precedents": rows, "semantic": bool(args.get("semantic"))}
    except sqlite3.OperationalError as e:
        return {"error": f"query failed: {e}", "count": 0, "precedents": []}
    finally:
        con.close()


def get_causal_chain(args):
    did = args.get("decision_id")
    if did is None:
        return {"error": "decision_id is required"}
    con = get_db()
    try:
        chain, cur, guard = [], int(did), 0
        while cur is not None and guard < 50:
            row = con.execute(
                "SELECT id, category, subject, scenario, outcome, decision_maker, issue_ref, "
                "parent_decision_id, created_at FROM decisions WHERE id=?", (cur,)).fetchone()
            if not row:
                break
            chain.append(dict(row))
            cur = row["parent_decision_id"]
            guard += 1
        chain.reverse()
        return {"count": len(chain), "chain": chain}
    finally:
        con.close()


def get_provenance(args):
    con = get_db()
    try:
        fact = None
        if args.get("fact_id"):
            fact = con.execute("SELECT id, sha256, text, source, project, domain, trust, strong, "
                               "created_at, updated_at FROM facts WHERE id=? AND archived=0",
                               (args["fact_id"],)).fetchone()
        elif args.get("sha256"):
            fact = con.execute("SELECT id, sha256, text, source, project, domain, trust, strong, "
                               "created_at, updated_at FROM facts WHERE sha256=? AND archived=0",
                               (args["sha256"],)).fetchone()
        if not fact:
            return {"error": "fact not found (use fact_id or sha256)", "fact": None, "evidence": []}
        evidence = [dict(r) for r in con.execute(
            "SELECT source_ref, source_checksum, fetched_at, created_at FROM evidence "
            "WHERE fact_id=? ORDER BY created_at", (fact["id"],))]
        return {"fact": dict(fact), "evidence": evidence}
    finally:
        con.close()


def attach_evidence(args):
    fact_id = args.get("fact_id")
    source_ref = (args.get("source_ref") or "").strip()
    if not fact_id or not source_ref:
        return {"error": "fact_id and source_ref are required"}
    con = get_db()
    try:
        cur = con.execute(
            "INSERT OR IGNORE INTO evidence (fact_id, source_ref, source_checksum, fetched_at, created_at) "
            "VALUES (?,?,?,?,?)",
            (fact_id, source_ref, args.get("source_checksum", ""),
             args.get("fetched_at", ""), now()))
        con.commit()
        return {"fact_id": fact_id, "source_ref": source_ref,
                "dedup": cur.rowcount == 0}
    finally:
        con.close()


def detect_conflicts(args):
    text = (args.get("text") or "").strip()
    if not text:
        return {"error": "text is required"}
    terms = fts_terms(text)
    text_l = text.lower()
    con = get_db()
    try:
        result = {"text": text, "near_duplicates": [], "decision_conflicts": []}
        if terms:
            query = " OR ".join(terms)
            sql = ("SELECT f.id, f.text, f.source, f.project, f.trust, f.strong, bm25(facts_fts) AS rank "
                   "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
                   "WHERE facts_fts MATCH ? AND f.archived=0 AND f.invalid_at='' ORDER BY rank LIMIT 10")
            try:
                rows = [dict(r) for r in con.execute(sql, [query])]
            except sqlite3.OperationalError:
                rows = []
            # Near-duplicate = most query terms present in the candidate text
            # (coverage >= 0.6). OR-match alone is too loose for reporting.
            text_l = text.lower()
            for r in rows:
                cov = sum(1 for t in terms if t in r["text"].lower()) / len(terms)
                r["coverage"] = round(cov, 2)
            result["near_duplicates"] = [r for r in rows if r["coverage"] >= 0.6][:5]
        # decision conflicts: same subject, >1 distinct outcome
        for row in con.execute(
                "SELECT subject, COUNT(DISTINCT outcome) AS n, "
                "GROUP_CONCAT(DISTINCT outcome) AS outcomes, MAX(created_at) AS last "
                "FROM decisions WHERE subject<>'' GROUP BY subject HAVING n>1 LIMIT 20"):
            # Whole-subject match (case-insensitive): term overlap alone is
            # too loose ("alpha-service" vs text about "beta-service" shares
            # the token "service").
            if row["subject"].lower() in text_l:
                result["decision_conflicts"].append(dict(row))
        return result
    finally:
        con.close()


def forget_fact(args):
    con = get_db()
    try:
        if args.get("id"):
            cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE id=? AND archived=0",
                              (now(), args["id"]))
        elif args.get("sha256"):
            cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE sha256=? AND archived=0",
                              (now(), args["sha256"]))
        else:
            return {"error": "id or sha256 is required"}
        con.commit()
        return {"archived": cur.rowcount}
    finally:
        con.close()


def fact_history(args):
    """Bi-temporal history of one fact: walk the superseded_by chain from the
    given id toward the newest version, oldest first."""
    fid = args.get("id")
    if fid is None:
        return {"error": "id is required"}
    con = get_db()
    try:
        chain, cur, guard = [], int(fid), 0
        while cur is not None and guard < 50:
            row = con.execute(
                "SELECT id, text, source, project, domain, trust, strong, importance, "
                "confirmed, created_at, updated_at, invalid_at, superseded_by "
                "FROM facts WHERE id=?", (cur,)).fetchone()
            if not row:
                break
            chain.append(dict(row))
            cur = row["superseded_by"]
            guard += 1
        return {"count": len(chain), "root_id": int(fid), "chain": chain}
    finally:
        con.close()


def review_pending(args):
    """Human-in-the-loop: unconfirmed facts (confirmed=0, trust != high) that
    are active — ordered by importance, then recency. Confirm with confirm_fact."""
    limit = max(1, min(int(args.get("limit", 20)), 100))
    con = get_db()
    try:
        total = con.execute(
            "SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at='' "
            "AND confirmed=0 AND trust != 'high'").fetchone()[0]
        rows = [dict(r) for r in con.execute(
            "SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
            "updated_at FROM facts WHERE archived=0 AND invalid_at='' "
            "AND confirmed=0 AND trust != 'high' "
            "ORDER BY importance DESC, updated_at DESC LIMIT ?", (limit,))]
        return {"count": len(rows), "total": total, "facts": rows}
    finally:
        con.close()


def confirm_fact(args):
    """Mark a fact as human-confirmed: confirmed=1, trust=high."""
    fid = args.get("id")
    if fid is None:
        return {"error": "id is required"}
    ts = now()
    con = get_db()
    try:
        cur = con.execute(
            "UPDATE facts SET confirmed=1, trust='high', updated_at=? "
            "WHERE id=? AND archived=0", (ts, fid))
        con.commit()
        if cur.rowcount == 0:
            return {"error": "fact not found or archived", "id": fid}
        return {"id": fid, "confirmed": True, "trust": "high", "updated_at": ts}
    finally:
        con.close()


def facts_for_session(args):
    """All active facts recorded from one session (source=session_ref)."""
    session_ref = (args.get("session_ref") or "").strip()
    if not session_ref:
        return {"error": "session_ref is required"}
    limit = max(1, min(int(args.get("limit", 50)), 200))
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(
            "SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
            "created_at, updated_at FROM facts WHERE source=? AND archived=0 AND invalid_at='' "
            "ORDER BY importance DESC, updated_at DESC LIMIT ?", (session_ref, limit))]
        return {"count": len(rows), "session_ref": session_ref, "facts": rows}
    finally:
        con.close()


def list_sessions(args):
    """Session index: distinct sources with active-fact counts, freshest first."""
    limit = max(1, min(int(args.get("limit", 50)), 200))
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(
            "SELECT source, COUNT(*) AS facts, MAX(updated_at) AS last_activity "
            "FROM facts WHERE source != '' AND archived=0 AND invalid_at='' "
            "GROUP BY source ORDER BY last_activity DESC LIMIT ?", (limit,))]
        return {"count": len(rows), "sessions": rows}
    finally:
        con.close()




def stats(_args=None):
    con = get_db()
    try:
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at=''").fetchone()[0]
        by_trust = {r["trust"]: r["n"] for r in con.execute(
            "SELECT trust, COUNT(*) n FROM facts WHERE archived=0 AND invalid_at='' GROUP BY trust")}
        by_domain = {r["domain"] or "(none)": r["n"] for r in con.execute(
            "SELECT domain, COUNT(*) n FROM facts WHERE archived=0 AND invalid_at='' GROUP BY domain")}
        strong = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at='' AND strong=1").fetchone()[0]
        counts = {
            "entities": con.execute("SELECT COUNT(*) FROM entities").fetchone()[0],
            "relations": con.execute("SELECT COUNT(*) FROM relations").fetchone()[0],
            "decisions": con.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
            "evidence": con.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
        }
        return {"total": total, "strong": strong, "by_trust": by_trust, "by_domain": by_domain,
                "counts": counts}
    finally:
        con.close()


def export_facts(_args=None):
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(
            "SELECT id, sha256, text, source, project, domain, trust, strong, created_at, updated_at, archived "
            "FROM facts ORDER BY id")]
        return {"count": len(rows), "facts": rows}
    finally:
        con.close()


TOOLS = {
    "add_fact": {
        "description": "Alias for remember_fact: store a durable fact (upsert, dedup by sha256 of text).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Fact text"},
                "source": {"type": "string", "description": "Origin: session/issue/run"},
                "project": {"type": "string", "description": "Project scope"},
                "domain": {"type": "string", "description": "Category/tag"},
                "trust": {"type": "string", "enum": list(VALID_TRUST), "default": "medium"},
                "strong": {"type": "boolean", "default": False},
            },
            "required": ["text"],
        },
    },
    "remember_fact": {
        "description": "Store a durable fact (upsert, dedup by sha256 of text).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Fact text"},
                "source": {"type": "string", "description": "Origin: session/issue/run"},
                "project": {"type": "string", "description": "Project scope"},
                "domain": {"type": "string", "description": "Category/tag"},
                "trust": {"type": "string", "enum": list(VALID_TRUST), "default": "medium"},
                "strong": {"type": "boolean", "default": False},
                "importance": {"type": "number", "default": 0.5, "description": "0..1 value of the fact for retention"},
            },
            "required": ["text"],
        },
    },
    "search_facts": {
        "description": "Full-text search over stored facts (FTS5, BM25 ranking). With semantic=true and MEMORY_MCP_EMBEDDINGS=1, merges lexical and embedding rankings (RRF).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "trust_min": {"type": "string", "enum": list(VALID_TRUST)},
                "strong_only": {"type": "boolean", "default": False},
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "semantic": {"type": "boolean", "default": False, "description": "Hybrid: RRF-merge FTS BM25 with embedding search (requires MEMORY_MCP_EMBEDDINGS=1)"},
                "valid_at": {"type": "string", "description": "RFC3339: include facts that were still valid at that time (bi-temporal)"},
                "graph": {"type": "boolean", "default": False, "description": "RRF-merge entity-graph expansion"},
            },
            "required": ["query"],
        },
    },
    "search_semantic": {
        "description": "Semantic (embedding) search over stored facts — cosine similarity, best first. Requires MEMORY_MCP_EMBEDDINGS=1 (see embeddings.py).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "threshold": {"type": "number", "default": 0.0, "description": "Minimum cosine similarity"},
            },
            "required": ["query"],
        },
    },
    "embed_backfill": {
        "description": "Compute embeddings for facts that have none (backfill after enabling MEMORY_MCP_EMBEDDINGS=1).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "ingest_turn": {
        "description": "Server-side fact extraction from a conversation transcript (LLM provider, see extract.py). Requires MEMORY_MCP_EXTRACT=1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "transcript": {"type": "string"},
                "session_ref": {"type": "string"},
                "project": {"type": "string"},
                "domain": {"type": "string"},
            },
            "required": ["transcript"],
        },
    },
    "compose_recall": {
        "description": "Build a ready-to-inject <memory-recall> block for a user turn (server-side scoring: lexical + semantic + entity-graph RRF; see recall.py). Requires MEMORY_MCP_RECALL=1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "turn_text": {"type": "string"},
                "limit": {"type": "integer", "default": 8},
                "chars": {"type": "integer", "default": 2400},
                "semantic": {"type": "boolean", "default": False},
                "graph": {"type": "boolean", "default": False, "description": "Expand via the entity graph (third RRF source)"},
                "session_expand": {"type": "integer", "default": 0, "description": "Pull up to N sibling facts from the top hits' sessions (background)"},
            },
            "required": ["turn_text"],
        },
    },
    "sweep_freshness": {
        "description": "Archive facts older than their type's hard window (strong facts kept; see recall.py). Requires MEMORY_MCP_RECALL=1.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "verify_facts": {
        "description": "LLM cross-check of a fact against the store (conflicts/supersessions; see verify.py). Requires MEMORY_MCP_VERIFY=1.",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
    },
    "consolidate": {
        "description": "LLM-merge of paraphrased facts into one fact (inputs invalidated bi-temporally; strong/confirmed never merged). Requires MEMORY_MCP_VERIFY=1.",
        "inputSchema": {
            "type": "object",
            "properties": {"ids": {"type": "array", "items": {"type": "integer"}}},
            "required": ["ids"],
        },
    },
    "fact_history": {
        "description": "Bi-temporal history of one fact: walk the superseded_by chain (oldest first).",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"],
        },
    },
    "review_pending": {
        "description": "Unconfirmed active facts (confirmed=0, trust != high), importance-first — for human review; confirm with confirm_fact.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 20}},
        },
    },
    "confirm_fact": {
        "description": "Mark a fact as human-confirmed (confirmed=1, trust=high).",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"],
        },
    },
    "facts_for_session": {
        "description": "All active facts recorded from one session (source=session_ref), importance-first.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_ref": {"type": "string"},
                "limit": {"type": "integer", "default": 50},
            },
            "required": ["session_ref"],
        },
    },
    "list_sessions": {
        "description": "Session index: distinct sources with active-fact counts, freshest first.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "default": 50}},
        },
    },
    "list_facts": {
        "description": "List recent non-archived facts (optional project/domain filter).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "limit": {"type": "integer", "default": 50},
            },
        },
    },
    "summarize_index": {
        "description": "Compact one-line-per-fact index (freshest first, capped at max_chars) — for prompt-injection budgets.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "trust_min": {"type": "string", "enum": list(VALID_TRUST)},
                "strong_only": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 200},
                "max_chars": {"type": "integer", "default": 4000},
            },
        },
    },
    "remember_entity": {
        "description": "Upsert an entity node (name unique; type/aliases optional).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "type": {"type": "string"},
                "aliases": {"type": "string"},
            },
            "required": ["name"],
        },
    },
    "remember_relation": {
        "description": "Record a subject-predicate-object edge (entities auto-created; dedup by triple).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "subject": {"type": "string"},
                "predicate": {"type": "string"},
                "object": {"type": "string"},
                "source_fact_id": {"type": "integer"},
            },
            "required": ["subject", "predicate", "object"],
        },
    },
    "search_graph": {
        "description": "BFS over relations (depth 1-2): neighbors of an entity in both directions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity": {"type": "string"},
                "depth": {"type": "integer", "default": 1},
                "limit": {"type": "integer", "default": 50},
            },
            "required": ["entity"],
        },
    },
    "record_decision": {
        "description": "Persist a decision node (category, scenario, reasoning, outcome, confidence, maker, issue_ref, optional parent_decision_id for causal chains).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "subject": {"type": "string"},
                "scenario": {"type": "string"},
                "reasoning": {"type": "string"},
                "outcome": {"type": "string"},
                "confidence": {"type": "number"},
                "decision_maker": {"type": "string"},
                "issue_ref": {"type": "string"},
                "parent_decision_id": {"type": "integer"},
            },
            "required": ["scenario"],
        },
    },
    "query_decisions": {
        "description": "List decisions with filters (category/subject/outcome/decision_maker/issue_ref).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "subject": {"type": "string"},
                "outcome": {"type": "string"},
                "decision_maker": {"type": "string"},
                "issue_ref": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
        },
    },
    "find_precedents": {
        "description": "Semantic precedent lookup: FTS BM25 over decision scenario/reasoning (terms OR-joined; optional category filter; semantic=true adds embedding RRF when MEMORY_MCP_EMBEDDINGS=1).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scenario": {"type": "string"},
                "category": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
                "semantic": {"type": "boolean", "default": False},
            },
            "required": ["scenario"],
        },
    },
    "get_causal_chain": {
        "description": "Walk parent_decision_id links from a decision to its root (oldest first).",
        "inputSchema": {
            "type": "object",
            "properties": {"decision_id": {"type": "integer"}},
            "required": ["decision_id"],
        },
    },
    "get_provenance": {
        "description": "Return a fact (by id or sha256) plus its evidence rows (source_ref, checksum).",
        "inputSchema": {
            "type": "object",
            "properties": {"fact_id": {"type": "integer"}, "sha256": {"type": "string"}},
        },
    },
    "attach_evidence": {
        "description": "Link a fact to a source (source_ref + optional checksum); dedup by (fact_id, source_ref).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fact_id": {"type": "integer"},
                "source_ref": {"type": "string"},
                "source_checksum": {"type": "string"},
                "fetched_at": {"type": "string"},
            },
            "required": ["fact_id", "source_ref"],
        },
    },
    "detect_conflicts": {
        "description": "Near-duplicate facts (FTS all-terms AND) + decisions with the same subject but >1 distinct outcome.",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
    },
    "forget_fact": {
        "description": "Soft-delete a fact by id or sha256.",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "sha256": {"type": "string"}},
        },
    },
    "stats": {
        "description": "Store statistics (total, by trust, by domain).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "export": {
        "description": "Export all facts (including archived) as JSON — for migration/backup.",
        "inputSchema": {"type": "object", "properties": {}},
    },
}

HANDLERS = {
    "add_fact": remember_fact,
    "remember_fact": remember_fact,
    "search_facts": search_facts,
    "search_semantic": search_semantic,
    "embed_backfill": embed_backfill,
    "ingest_turn": ingest_turn,
    "compose_recall": compose_recall,
    "sweep_freshness": sweep_freshness,
    "verify_facts": verify_facts,
    "consolidate": consolidate,
    "list_facts": list_facts,
    "summarize_index": summarize_index,
    "remember_entity": remember_entity,
    "remember_relation": remember_relation,
    "search_graph": search_graph,
    "record_decision": record_decision,
    "query_decisions": query_decisions,
    "find_precedents": find_precedents,
    "get_causal_chain": get_causal_chain,
    "get_provenance": get_provenance,
    "attach_evidence": attach_evidence,
    "detect_conflicts": detect_conflicts,
    "forget_fact": forget_fact,
    "fact_history": fact_history,
    "review_pending": review_pending,
    "confirm_fact": confirm_fact,
    "facts_for_session": facts_for_session,
    "list_sessions": list_sessions,
    "stats": stats,
    "export": export_facts,
}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("method") == "initialize":
            reply = {
                "jsonrpc": "2.0", "id": msg.get("id"),
                "result": {
                    "protocolVersion": msg.get("params", {}).get("protocolVersion", "2024-11-05"),
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "memory-mcp", "version": "0.3.0"},
                },
            }
        elif msg.get("method") == "tools/list":
            reply = {"jsonrpc": "2.0", "id": msg.get("id"),
                     "result": {"tools": [{"name": k, "description": v["description"],
                                           "inputSchema": v["inputSchema"]} for k, v in TOOLS.items()]}}
        elif msg.get("method") == "tools/call":
            params = msg.get("params", {})
            name, args = params.get("name"), params.get("arguments", {}) or {}
            try:
                result = HANDLERS[name](args) if name in HANDLERS else {"error": f"unknown tool {name}"}
                reply = {"jsonrpc": "2.0", "id": msg.get("id"),
                         "result": {"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                                    "isError": "error" in result}}
            except Exception as e:  # noqa: BLE001 — surface any tool failure to the client
                reply = {"jsonrpc": "2.0", "id": msg.get("id"),
                         "result": {"content": [{"type": "text", "text": json.dumps({"error": str(e)})}],
                                    "isError": True}}
        elif msg.get("method") == "ping":
            reply = {"jsonrpc": "2.0", "id": msg.get("id"), "result": {}}
        else:
            # notifications (e.g. notifications/initialized) and unknown methods
            if "id" not in msg:
                continue
            reply = {"jsonrpc": "2.0", "id": msg.get("id"),
                     "error": {"code": -32601, "message": f"method not found: {msg.get('method')}"}}
        sys.stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
