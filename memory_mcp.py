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
"""
import hashlib, json, os, sqlite3, sys
from datetime import datetime, timezone

DB_PATH = os.environ.get("MEMORY_MCP_DB", "/home/<user>/shared-store/facts.db")
VALID_TRUST = ("high", "medium", "low")

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
"""


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    # Мульти-райтер: хост + docker-рантаймы пишут в один файл (bind-mount).
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.executescript(_SCHEMA)
    return con


# ---------------- tools ----------------

def remember_fact(args):
    text = (args.get("text") or "").strip()
    if not text:
        return {"error": "text is required"}
    trust = args.get("trust", "medium")
    if trust not in VALID_TRUST:
        return {"error": f"trust must be one of {VALID_TRUST}"}
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    ts = now()
    con = get_db()
    try:
        cur = con.execute("SELECT id, created_at FROM facts WHERE sha256=?", (sha,))
        row = cur.fetchone()
        if row:
            con.execute("UPDATE facts SET updated_at=?, archived=0 WHERE id=?", (ts, row["id"]))
            con.commit()
            return {"id": row["id"], "sha256": sha, "dedup": True,
                    "created_at": row["created_at"], "updated_at": ts}
        cur = con.execute(
            "INSERT INTO facts (sha256, text, source, project, domain, trust, strong, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (sha, text, args.get("source", ""), args.get("project", ""),
             args.get("domain", ""), trust, 1 if args.get("strong") else 0, ts, ts))
        con.commit()
        return {"id": cur.lastrowid, "sha256": sha, "dedup": False,
                "created_at": ts, "updated_at": ts}
    finally:
        con.close()


def search_facts(args):
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit = min(int(args.get("limit", 20)), 100)
    sql = ("SELECT f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong, "
           "f.created_at, bm25(facts_fts) AS rank "
           "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
           "WHERE facts_fts MATCH ? AND f.archived=0")
    params = [query]
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
        rows = [dict(r) for r in con.execute(sql, params)]
        return {"count": len(rows), "facts": rows}
    except sqlite3.OperationalError:
        # FTS5 синтаксис (дефисы/операторы) — повтор как литеральная фраза
        phrase = '"' + query.replace('"', '""') + '"'
        sql2 = sql.replace("facts_fts MATCH ?", "facts_fts MATCH ?", 1)
        try:
            rows = [dict(r) for r in con.execute(sql2, [phrase] + params[1:])]
            return {"count": len(rows), "facts": rows}
        except sqlite3.OperationalError as e:
            return {"error": f"query failed: {e}", "facts": []}
    finally:
        con.close()


def list_facts(args):
    limit = min(int(args.get("limit", 50)), 500)
    sql = "SELECT id, text, source, project, domain, trust, strong, created_at, updated_at FROM facts WHERE archived=0"
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
    limit = min(int(args.get("limit", 200)), 500)
    max_chars = max(int(args.get("max_chars", 4000)), 200)
    sql = ("SELECT id, text, project, domain, trust, strong, updated_at "
           "FROM facts WHERE archived=0")
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
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0").fetchone()[0]
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


def stats(_args=None):
    con = get_db()
    try:
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0").fetchone()[0]
        by_trust = {r["trust"]: r["n"] for r in con.execute(
            "SELECT trust, COUNT(*) n FROM facts WHERE archived=0 GROUP BY trust")}
        by_domain = {r["domain"] or "(none)": r["n"] for r in con.execute(
            "SELECT domain, COUNT(*) n FROM facts WHERE archived=0 GROUP BY domain")}
        strong = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND strong=1").fetchone()[0]
        return {"total": total, "strong": strong, "by_trust": by_trust, "by_domain": by_domain}
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
            },
            "required": ["text"],
        },
    },
    "search_facts": {
        "description": "Full-text search over stored facts (FTS5, BM25 ranking).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "trust_min": {"type": "string", "enum": list(VALID_TRUST)},
                "strong_only": {"type": "boolean", "default": False},
                "project": {"type": "string"},
                "domain": {"type": "string"},
            },
            "required": ["query"],
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
    "remember_fact": remember_fact,
    "search_facts": search_facts,
    "list_facts": list_facts,
    "summarize_index": summarize_index,
    "forget_fact": forget_fact,
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
                    "serverInfo": {"name": "memory-mcp", "version": "0.2.0"},
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
