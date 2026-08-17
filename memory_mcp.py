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
import hashlib, json, os, re, sqlite3, sys
from datetime import datetime, timezone

def default_db_path():
    """Script-relative default: <repo>/data/facts.db — portable across environments.

    Override with MEMORY_MCP_DB (used by all deployment runtimes: host wrapper,
    docker containers via /opt/memory-shared).
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "facts.db")


DB_PATH = os.environ.get("MEMORY_MCP_DB") or default_db_path()
VALID_TRUST = ("high", "medium", "low")
# v0.5 rebuild DDL: same facts shape without the global UNIQUE(sha256).
_FACTS_TABLE_DDL = """
CREATE TABLE facts_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sha256 TEXT NOT NULL,
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
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0,
  last_accessed_at TEXT NOT NULL DEFAULT '',
  access_count INTEGER NOT NULL DEFAULT 0,
  revival_count INTEGER NOT NULL DEFAULT 0,
  lifecycle TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle IN ('active','degraded','forgotten'))
);
"""

_FTS_TRIGGERS_DDL = """
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
  sha256 TEXT NOT NULL,
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
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0,
  last_accessed_at TEXT NOT NULL DEFAULT '',
  access_count INTEGER NOT NULL DEFAULT 0,
  revival_count INTEGER NOT NULL DEFAULT 0,
  lifecycle TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle IN ('active','degraded','forgotten'))
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
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  predicate TEXT NOT NULL,
  object_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
  source_fact_id INTEGER REFERENCES facts(id) ON DELETE SET NULL,
  workspace_id TEXT NOT NULL DEFAULT '',
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
  workspace_id TEXT NOT NULL DEFAULT '',
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
  fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
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

-- v0.6 (2026-08-17): workspace registry — named access scopes with
-- create/reset/archive semantics (soft by default, hard via confirm).
CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','archived','reset')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- v0.7 (2026-08-17): activity days — one row per day with at least one
-- tools/call (proxy for "the system was online and memory was used").
-- Decay counts only these days, so user downtime never ages facts.
CREATE TABLE IF NOT EXISTS activity_days (
  day TEXT PRIMARY KEY
);
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


def _open_db(path):
    dbdir = os.path.dirname(path) or "."
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
        con = sqlite3.connect(path, timeout=10)
    except sqlite3.DatabaseError as e:
        print(f"memory-mcp: cannot open DB {path!r}: {e}", file=sys.stderr)
        raise RuntimeError(
            "cannot open the fact store: DB file is not accessible or corrupt; "
            "set MEMORY_MCP_DB to a writable path (e.g. a rw bind-mount)")
    con.row_factory = sqlite3.Row
    # Мульти-райтер: хост + docker-рантаймы пишут в один файл (bind-mount).
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA foreign_keys=ON")
    preexisting_fts = _existing_fts_tables(con)
    con.executescript(_SCHEMA)
    con.executescript(_EMBED_SCHEMA)
    _migrate_facts(con)
    _migrate_fks(con)
    _migrate_fts(con, preexisting_fts)
    return con


def get_db():
    return _open_db(DB_PATH)


def _migrate_facts(con):
    """Additive migration: v0.4 columns, workspace_id, and the v0.5 facts
    rebuild (global UNIQUE(sha256) -> UNIQUE(sha256, workspace_id)) so the
    same text can exist per workspace. The rebuild copies rows with EXPLICIT
    column lists (column order differs between the ALTERed legacy table and
    facts_new), runs atomically in one executescript, and rebuilds the FTS
    index. Legacy rows land in the shared pool (workspace_id='')."""
    existing = {r["name"] for r in con.execute("PRAGMA table_info(facts)")}
    for name, decl in _FACT_EXTRA_COLUMNS.items():
        if name not in existing:
            con.execute("ALTER TABLE facts ADD COLUMN %s %s" % (name, decl))
    if "workspace_id" not in existing:
        con.execute("ALTER TABLE facts ADD COLUMN workspace_id TEXT NOT NULL DEFAULT ''")
    # v0.7 decay columns (existing databases get plain defaults; the CHECK
    # constraint lives in the DDL for fresh/rebuild paths).
    for name, decl in (
            ("last_accessed_at", "TEXT NOT NULL DEFAULT ''"),
            ("access_count", "INTEGER NOT NULL DEFAULT 0"),
            ("revival_count", "INTEGER NOT NULL DEFAULT 0"),
            ("lifecycle", "TEXT NOT NULL DEFAULT 'active'")):
        if name not in existing:
            con.execute("ALTER TABLE facts ADD COLUMN %s %s" % (name, decl))
    for table in ("decisions", "entities", "relations"):
        try:
            cols = {r["name"] for r in con.execute("PRAGMA table_info(%s)" % table)}
            if "workspace_id" not in cols:
                con.execute("ALTER TABLE %s ADD COLUMN workspace_id TEXT NOT NULL DEFAULT ''" % table)
        except sqlite3.OperationalError:
            pass  # table not created yet (fresh DB)
    indexes = {r["name"] for r in con.execute("PRAGMA index_list(facts)")}
    if "sqlite_autoindex_facts_1" in indexes:
        # legacy global UNIQUE(sha256) present -> rebuild
        cols = ("id, sha256, text, source, project, domain, trust, strong, importance, "
                "invalid_at, superseded_by, confirmed, workspace_id, created_at, updated_at, archived, "
                "last_accessed_at, access_count, revival_count, lifecycle")
        con.executescript(
            "PRAGMA foreign_keys=OFF;\n"
            "BEGIN;\n"
            + _FACTS_TABLE_DDL + "\n"
            "INSERT INTO facts_new (%s) SELECT %s FROM facts;\n" % (cols, cols)
            + "DROP TABLE facts;\n"
            "ALTER TABLE facts_new RENAME TO facts;\n"
            "CREATE UNIQUE INDEX IF NOT EXISTS facts_sha_ws ON facts(sha256, workspace_id);\n"
            "CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5("
            "text, content='facts', content_rowid='id');\n"
            + _FTS_TRIGGERS_DDL + "\n"
            "INSERT INTO facts_fts(facts_fts) VALUES('rebuild');\n"
            "COMMIT;\n"
            "PRAGMA foreign_keys=ON;\n")
    else:
        # fresh or already-rebuilt DB: ensure the composite unique index
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS facts_sha_ws ON facts(sha256, workspace_id)")
    con.commit()


def _migrate_fks(con):
    """v0.8 migration: rebuild `evidence` and `relations` with ON DELETE
    CASCADE (relations.source_fact_id -> SET NULL) so child rows can never
    block a parent delete with a FOREIGN KEY error. Idempotent: skips tables
    whose stored DDL already carries the clause (fresh DBs get it from
    _SCHEMA). Rebuild pattern matches _migrate_facts (atomic executescript,
    foreign_keys toggled off around the swap)."""
    def _has_cascade(table, needle):
        row = con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                          [table]).fetchone()
        return bool(row and row["sql"] and needle in row["sql"])

    if not _has_cascade("evidence", "ON DELETE CASCADE"):
        con.executescript(
            "PRAGMA foreign_keys=OFF;\n"
            "BEGIN;\n"
            "CREATE TABLE evidence_new (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,\n"
            "  source_ref TEXT NOT NULL,\n"
            "  source_checksum TEXT NOT NULL DEFAULT '',\n"
            "  fetched_at TEXT NOT NULL DEFAULT '',\n"
            "  created_at TEXT NOT NULL,\n"
            "  UNIQUE(fact_id, source_ref)\n"
            ");\n"
            "INSERT INTO evidence_new (id, fact_id, source_ref, source_checksum, fetched_at, created_at)\n"
            "  SELECT id, fact_id, source_ref, source_checksum, fetched_at, created_at FROM evidence;\n"
            "DROP TABLE evidence;\n"
            "ALTER TABLE evidence_new RENAME TO evidence;\n"
            "CREATE INDEX IF NOT EXISTS evidence_fact_idx ON evidence(fact_id);\n"
            "COMMIT;\n"
            "PRAGMA foreign_keys=ON;\n")
    if not _has_cascade("relations", "ON DELETE CASCADE"):
        con.executescript(
            "PRAGMA foreign_keys=OFF;\n"
            "BEGIN;\n"
            "CREATE TABLE relations_new (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  subject_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,\n"
            "  predicate TEXT NOT NULL,\n"
            "  object_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,\n"
            "  source_fact_id INTEGER REFERENCES facts(id) ON DELETE SET NULL,\n"
            "  workspace_id TEXT NOT NULL DEFAULT '',\n"
            "  created_at TEXT NOT NULL,\n"
            "  UNIQUE(subject_id, predicate, object_id)\n"
            ");\n"
            "INSERT INTO relations_new (id, subject_id, predicate, object_id, source_fact_id, workspace_id, created_at)\n"
            "  SELECT id, subject_id, predicate, object_id, source_fact_id, workspace_id, created_at FROM relations;\n"
            "DROP TABLE relations;\n"
            "ALTER TABLE relations_new RENAME TO relations;\n"
            "CREATE INDEX IF NOT EXISTS relations_subject_idx ON relations(subject_id);\n"
            "CREATE INDEX IF NOT EXISTS relations_object_idx ON relations(object_id);\n"
            "COMMIT;\n"
            "PRAGMA foreign_keys=ON;\n")
    con.commit()


def _existing_fts_tables(con):
    """FTS5 shadow tables that already exist in the store (before _SCHEMA)."""
    rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' "
                       "AND name IN ('facts_fts', 'decisions_fts')").fetchall()
    return {r["name"] for r in rows}


def _migrate_fts(con, preexisting):
    """Rebuild the FTS index of any content table whose FTS5 shadow was
    created by this open (i.e. the database predates it). A content= FTS
    table created over existing rows starts with an EMPTY index, and the
    AFTER DELETE trigger then fails with SQLITE_CORRUPT ('database disk
    image is malformed') on the first row delete — FTS5's 'delete' command
    requires the entry to be indexed. NOTE: COUNT(*) on a content= FTS table
    counts CONTENT rows, so a count comparison cannot detect the empty
    index; pre-_SCHEMA existence is the reliable signal."""
    for fts in ("facts_fts", "decisions_fts"):
        if fts in preexisting:
            continue
        try:
            con.execute("INSERT INTO %s(%s) VALUES('rebuild')" % (fts, fts))
        except sqlite3.OperationalError as e:
            # Mirrors the _open_db convention: full detail to stderr, and
            # leave the client-visible error to the failing tool (the AFTER
            # DELETE trigger surfaces SQLITE_CORRUPT with a clear message).
            print(f"memory-mcp: FTS rebuild of {fts} failed: {e}", file=sys.stderr)
            continue
    con.commit()


def _graph_expand_facts(con, hit_facts, limit=10, workspace=""):
    """Entity-graph expansion: entities mentioned in the hit facts -> graph
    neighbors -> facts mentioning the neighbors. Returns dict rows (id/text/...).
    Shared by search_facts {graph=true} and compose_recall {graph=true}."""
    if _ws_status(con, workspace) != "active":
        return []
    ent_ws = _ws_check("entities", workspace)
    ent_params = [workspace] if workspace else []
    rows = con.execute("SELECT name FROM entities WHERE length(name) >= 3" + ent_ws,
                       ent_params).fetchall()
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
            "JOIN entities o ON o.id=r.object_id WHERE s.name=?" + _ws_check("r", workspace) +
            " UNION SELECT s.name FROM relations r JOIN entities s ON s.id=r.subject_id "
            "JOIN entities o ON o.id=r.object_id WHERE o.name=?" + _ws_check("r", workspace),
            [n] + ent_params + [n] + ent_params):
            neighbors.add(r["nb"])
    out, seen = [], {f["id"] for f in hit_facts}
    ws_clause = " AND workspace_id IN (?, '')" if workspace else " AND workspace_id = ''"
    for nb in list(neighbors)[:12]:
        esc = nb.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        params = ["%" + esc + "%"]
        if workspace:
            params.append(workspace)
        rows = con.execute(
            "SELECT id, text, source, project, domain, trust, strong, importance, confirmed "
            "FROM facts WHERE text LIKE ? ESCAPE '\\' AND archived=0 AND invalid_at='' "
            "AND lifecycle != 'forgotten'" +
            ws_clause + " LIMIT 5", params).fetchall()
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                out.append(dict(r))
                if len(out) >= limit:
                    return out
    return out


def _workspace(args):
    """Workspace scope: explicit workspace param, or '' (shared pool)."""
    return (args.get("workspace") or "").strip()


def _ws_check(alias, workspace):
    """Ownership predicate for by-id operations: the target must belong to the
    caller's workspace or the shared pool; unscoped callers act on shared only."""
    if workspace:
        return " AND %s.workspace_id IN (?, '')" % alias
    return " AND %s.workspace_id = ''" % alias


def _ws_filter(alias, workspace):
    """SQL fragment: workspace + shared pool when scoped; shared only otherwise."""
    if workspace:
        return " AND %s.workspace_id IN (?, '')" % alias
    return " AND %s.workspace_id = ''" % alias


def _ws_status(con, workspace):
    """Registry status of a workspace. Implicit workspaces (no row in the
    `workspaces` table) and the shared pool ('') count as active."""
    if not workspace:
        return "active"
    row = con.execute("SELECT status FROM workspaces WHERE id=?", [workspace]).fetchone()
    return row["status"] if row else "active"


def _ws_inactive_error(con, workspace):
    """Error dict when a workspace is archived/reset (all its reads and writes
    are refused until it is reactivated); None when active."""
    status = _ws_status(con, workspace)
    if status != "active":
        return {"error": "workspace %r is %s — reactivate it with create_workspace"
                % (workspace, status)}
    return None


def _importance(args):
    """Clamp the importance argument to [0,1]; default 0.5."""
    try:
        return max(0.0, min(1.0, float(args.get("importance", 0.5))))
    except (TypeError, ValueError):
        return 0.5


_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def _validate_name(name, kind):
    """Validate a database/workspace name: 1-64 chars of [A-Za-z0-9._-],
    no path separators, no '..' (repository rule: no host paths)."""
    name = (name or "").strip()
    if not name:
        return None, f"{kind} name is required"
    if ".." in name or not _NAME_RE.match(name):
        return None, f"invalid {kind} name {name!r}: use 1-64 chars of [A-Za-z0-9._-], no '..'"
    return name, ""


def _db_dir():
    """Directory of named databases — sibling of the active DB file."""
    d = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "databases")
    os.makedirs(d, exist_ok=True)
    return d


def _backup_dir():
    """Directory of backups — sibling of the active DB file."""
    d = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "backups")
    os.makedirs(d, exist_ok=True)
    return d


def _db_file(name):
    return os.path.join(_db_dir(), name + ".db")


def _active_db_name():
    return os.path.basename(DB_PATH)


# ---- v0.7 decay support: activity days, search-hit bookkeeping ------------

_LAST_ACTIVITY_DAY = [None]  # module-level cache: one INSERT per day per process


def _register_activity_day():
    """Record "the system was online and memory was used" for today.
    Best-effort, never raises; one row per day (INSERT OR IGNORE)."""
    day = now()[:10]
    if _LAST_ACTIVITY_DAY[0] == day:
        return
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        try:
            con.execute("CREATE TABLE IF NOT EXISTS activity_days (day TEXT PRIMARY KEY)")
            con.execute("INSERT OR IGNORE INTO activity_days (day) VALUES (?)", (day,))
            con.commit()
        finally:
            con.close()
        _LAST_ACTIVITY_DAY[0] = day
    except sqlite3.Error:
        pass


def _decay_param(name, default):
    try:
        return type(default)(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _mark_hits(con, rows):
    """Search hits (facts actually returned by search tools): refresh the
    active ones. Degraded/forgotten facts reached via chains are NOT hits —
    chained access must not keep stale facts alive."""
    ts = now()
    ids = [r["id"] for r in rows if isinstance(r, dict) and r.get("id")]
    if not ids:
        return
    con.executemany(
        "UPDATE facts SET last_accessed_at=?, access_count=access_count+1 "
        "WHERE id=? AND lifecycle='active' AND archived=0",
        [(ts, fid) for fid in ids])
    con.commit()


def _revive_degraded(con, query, ws):
    """Degraded facts matching the query count as "attempts to remember":
    revival_count++ per matching search; at DECAY_REVIVE_HITS they go back
    to active (visible from the next search). Forgotten facts stay out."""
    n = _decay_param("DECAY_REVIVE_HITS", 3)
    params = [query]
    sql = ("SELECT f.id FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
           "WHERE facts_fts MATCH ? AND f.archived=0 AND f.invalid_at='' "
           "AND f.lifecycle='degraded'" + _ws_filter("f", ws))
    if ws:
        params.append(ws)
    try:
        ids = [r["id"] for r in con.execute(sql, params)]
    except sqlite3.OperationalError:
        phrase = '"' + query.replace('"', '""') + '"'
        ids = [r["id"] for r in con.execute(
            sql.replace("facts_fts MATCH ?", "facts_fts MATCH ?", 1), [phrase] + params[1:])]
    if not ids:
        return 0
    con.executemany(
        "UPDATE facts SET revival_count=revival_count+1, updated_at=? WHERE id=?",
        [(now(), fid) for fid in ids])
    revived = 0
    for fid in ids:
        row = con.execute("SELECT revival_count FROM facts WHERE id=?", [fid]).fetchone()
        if row and row["revival_count"] >= n:
            con.execute("UPDATE facts SET lifecycle='active', revival_count=0, updated_at=? WHERE id=?",
                        (now(), fid))
            revived += 1
    con.commit()
    return revived


def _emb():
    """Lazy handle to the optional embeddings module, or None when disabled
    (MEMORY_MCP_EMBEDDINGS != 1) or unavailable."""
    if os.environ.get("MEMORY_MCP_EMBEDDINGS") != "1":
        return None
    try:
        import embeddings
        return embeddings
    except ImportError:
        return None


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
    con = get_db()
    try:
        err = _ws_inactive_error(con, _workspace(args))
        if err:
            return err
    finally:
        con.close()
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


def decay_sweep(args):
    """v0.7: recompute fact lifecycle by active-day decay (no env gate —
    decay is core behavior, parameters via DECAY_* env vars)."""
    try:
        return __import__("decay").decay_sweep(args)
    except ImportError:
        return {"error": "decay module not available"}


def list_forgotten(args):
    try:
        return __import__("decay").list_forgotten(args)
    except ImportError:
        return {"error": "decay module not available"}


def restore_fact(args):
    try:
        return __import__("decay").restore_fact(args)
    except ImportError:
        return {"error": "decay module not available"}


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
    workspace = _workspace(args)
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    ts = now()
    warning = ""
    if not (args.get("source") or "").strip():
        warning = "no source provided; add source=repo@commit/issue/run for provenance"
    if not workspace:
        warning = (warning + "; " if warning else "") + \
            "no workspace provided; add workspace=<project_id> to scope this fact to your project"
    con = get_db()
    try:
        err = _ws_inactive_error(con, workspace)
        if err:
            return err
        # Workspace-scoped dedup: the same text is one fact per workspace.
        # Unscoped callers dedup within the shared pool only.
        cur = con.execute("SELECT id, created_at FROM facts WHERE sha256=?" +
                          _ws_check("facts", workspace),
                          [sha] + ([workspace] if workspace else []))
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
            out = {"id": row["id"], "sha256": sha, "dedup": True,
                   "created_at": row["created_at"], "updated_at": ts}
            if warning:
                out["warning"] = warning
            return out
        cur = con.execute(
            "INSERT INTO facts (sha256, text, source, project, domain, trust, strong, importance, workspace_id, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sha, text, args.get("source", ""), args.get("project", ""),
             args.get("domain", ""), trust, 1 if args.get("strong") else 0,
             importance, workspace, ts, ts))
        con.commit()
        fid = cur.lastrowid
        emb = _emb()
        if emb is not None:
            emb.embed_fact(con, fid, text)  # best-effort, never raises
        out = {"id": fid, "sha256": sha, "dedup": False,
               "created_at": ts, "updated_at": ts}
        if warning:
            out["warning"] = warning
        return out
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
           "WHERE facts_fts MATCH ? AND f.archived=0 AND f.lifecycle='active'")
    ws = _workspace(args)
    params = [query]
    sql += _ws_filter("f", ws)
    if ws:
        params.append(ws)
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
        _revive_degraded(con, query, ws)
        graph = []
        if args.get("graph") and rows:
            graph = _graph_expand_facts(con, rows, limit * 2, ws)
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
                res = emb.hybrid_rerank(con, query, rows, limit=limit, workspace=ws)
            else:
                # No lexical hits: fall back to semantic ranking alone.
                res = emb.search_semantic(con, query, limit=limit, workspace=ws)
            rows = res.get("facts", []) if isinstance(res, dict) else res or []
        _mark_hits(con, rows)
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
    ws = _workspace(args)
    con = get_db()
    try:
        res = emb.search_semantic(con, query, limit=limit, threshold=threshold, workspace=ws)
        rows = res.get("facts", []) if isinstance(res, dict) else res or []
        _revive_degraded(con, query, ws)
        _mark_hits(con, rows)
        return res
    finally:
        con.close()


def embed_backfill(args):
    """Compute vectors for facts that have none (backfill after enabling)."""
    emb = _emb()
    if emb is None:
        return {"error": "semantic search is disabled (set MEMORY_MCP_EMBEDDINGS=1)"}
    con = get_db()
    try:
        return emb.embed_backfill(con, workspace=_workspace(args))
    finally:
        con.close()


def list_facts(args):
    limit = max(1, min(int(args.get("limit", 50)), 500))
    sql = ("SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
           "created_at, updated_at FROM facts WHERE archived=0 AND invalid_at='' AND lifecycle='active'")
    ws = _workspace(args)
    params = []
    sql += _ws_filter("facts", ws)
    if ws:
        params.append(ws)
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
           "FROM facts WHERE archived=0 AND invalid_at='' AND lifecycle='active'")
    ws = _workspace(args)
    params = []
    sql += _ws_filter("facts", ws)
    if ws:
        params.append(ws)
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
        ws = _workspace(args)
        ws_params = [ws] if ws else []
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at='' AND lifecycle='active'" +
                            _ws_check("facts", ws), ws_params).fetchone()[0]
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


def _resolve_entity(con, name, etype="", aliases="", workspace=""):
    """Get-or-create an entity by name; returns (id, created_flag). Scoped to
    the workspace (or the shared pool)."""
    ts = now()
    row = con.execute("SELECT id FROM entities WHERE name=?" + _ws_check("entities", workspace),
                      [name] + ([workspace] if workspace else [])).fetchone()
    if row:
        con.execute("UPDATE entities SET updated_at=?, type=CASE WHEN ?<>'' THEN ? ELSE type END, "
                    "aliases=CASE WHEN ?<>'' THEN ? ELSE aliases END WHERE id=?",
                    (ts, etype, etype, aliases, aliases, row["id"]))
        return row["id"], False
    cur = con.execute("INSERT INTO entities (name, type, aliases, workspace_id, created_at, updated_at) "
                      "VALUES (?,?,?,?,?,?)",
                      (name, etype, aliases, workspace, ts, ts))
    return cur.lastrowid, True


def remember_entity(args):
    name = (args.get("name") or "").strip()
    if not name:
        return {"error": "name is required"}
    con = get_db()
    try:
        ws = _workspace(args)
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        try:
            eid, created = _resolve_entity(con, name, args.get("type", ""), args.get("aliases", ""), ws)
        except sqlite3.IntegrityError:
            return {"error": "entity name exists in another workspace", "name": name}
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
        ws = _workspace(args)
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        sid, _ = _resolve_entity(con, subject, workspace=ws)
        oid, _ = _resolve_entity(con, obj, workspace=ws)
        existing = con.execute(
            "SELECT id FROM relations WHERE subject_id=? AND predicate=? AND object_id=?" +
            _ws_check("relations", ws),
            [sid, predicate, oid] + ([ws] if ws else [])).fetchone()
        if existing:
            return {"id": existing["id"], "dedup": True}
        try:
            cur = con.execute(
                "INSERT INTO relations (subject_id, predicate, object_id, source_fact_id, workspace_id, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (sid, predicate, oid, args.get("source_fact_id"), ws, now()))
        except sqlite3.IntegrityError:
            return {"error": "relation already exists in another workspace",
                    "subject": subject, "predicate": predicate, "object": obj}
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
        ws = _workspace(args)
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        root = con.execute("SELECT id, name, type FROM entities WHERE name=?" + _ws_check("entities", ws),
                           [name] + ([ws] if ws else [])).fetchone()
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
                    "WHERE (r.subject_id=? OR r.object_id=?)" + _ws_check("r", ws),
                    [eid, eid] + ([ws] if ws else [])).fetchall()
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
        workspace = _workspace(args)
        err = _ws_inactive_error(con, workspace)
        if err:
            return err
        cur = con.execute(
            "INSERT INTO decisions (category, subject, scenario, reasoning, outcome, confidence, "
            "decision_maker, issue_ref, parent_decision_id, workspace_id, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (category, args.get("subject", ""), scenario, args.get("reasoning", ""),
             args.get("outcome", ""), confidence, args.get("decision_maker", ""),
             args.get("issue_ref", ""), args.get("parent_decision_id"), workspace, ts, ts))
        con.commit()
        return {"id": cur.lastrowid, "category": category, "scenario": scenario,
                "created_at": ts}
    finally:
        con.close()


def query_decisions(args):
    sql = "SELECT id, category, subject, scenario, reasoning, outcome, confidence, decision_maker, issue_ref, parent_decision_id, created_at FROM decisions WHERE 1=1"
    params = []
    ws = _workspace(args)
    sql += _ws_filter("decisions", ws)
    if ws:
        params.append(ws)
    for key in ("category", "subject", "outcome", "decision_maker", "issue_ref"):
        if args.get(key):
            sql += f" AND {key}=?"
            params.append(args[key])
    sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
    params.append(max(1, min(int(args.get("limit", 20)), 100)))
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
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
    ws = _workspace(args)
    sql += _ws_filter("d", ws)
    if ws:
        params.append(ws)
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        try:
            rows = [dict(r) for r in con.execute(sql, params)]
        except sqlite3.OperationalError:
            phrase = '"' + query.replace('"', '""') + '"'
            rows = [dict(r) for r in con.execute(sql, [phrase] + params[1:])]
        if args.get("semantic"):
            emb = _emb()
            if emb is not None:
                try:
                    sem = emb.search_decision_semantic(con, scenario, limit=limit * 2,
                                                          workspace=ws)
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
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        chain, cur, guard = [], int(did), 0
        while cur is not None and guard < 50:
            row = con.execute(
                "SELECT id, category, subject, scenario, outcome, decision_maker, issue_ref, "
                "parent_decision_id, created_at FROM decisions WHERE id=?" + _ws_check("decisions", ws),
                [cur] + ([ws] if ws else [])).fetchone()
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
        ws = _workspace(args)
        fact = None
        if args.get("fact_id"):
            fact = con.execute("SELECT id, sha256, text, source, project, domain, trust, strong, "
                               "created_at, updated_at FROM facts WHERE id=? AND archived=0" +
                               _ws_check("facts", ws),
                               [args["fact_id"]] + ([ws] if ws else [])).fetchone()
        elif args.get("sha256"):
            fact = con.execute("SELECT id, sha256, text, source, project, domain, trust, strong, "
                               "created_at, updated_at FROM facts WHERE sha256=? AND archived=0" +
                               _ws_check("facts", ws),
                               [args["sha256"]] + ([ws] if ws else [])).fetchone()
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
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        owner = con.execute(
            "SELECT id FROM facts WHERE id=? AND archived=0" + _ws_check("facts", ws),
            [fact_id] + ([ws] if ws else [])).fetchone()
        if not owner:
            return {"error": "fact not found or not in your workspace", "fact_id": fact_id}
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
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        result = {"text": text, "near_duplicates": [], "decision_conflicts": []}
        if terms:
            query = " OR ".join(terms)
            sql = ("SELECT f.id, f.text, f.source, f.project, f.trust, f.strong, bm25(facts_fts) AS rank "
                   "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
                   "WHERE facts_fts MATCH ? AND f.archived=0 AND f.invalid_at='' AND f.lifecycle='active'" +
                   _ws_check("f", ws) +
                   " ORDER BY rank LIMIT 10")
            try:
                rows = [dict(r) for r in con.execute(sql, [query] + ([ws] if ws else []))]
            except sqlite3.OperationalError:
                rows = []
            # Near-duplicate = most query terms present in the candidate text
            # (coverage >= 0.6). OR-match alone is too loose for reporting.
            text_l = text.lower()
            for r in rows:
                cov = sum(1 for t in terms if t in r["text"].lower()) / len(terms)
                r["coverage"] = round(cov, 2)
            result["near_duplicates"] = [r for r in rows if r["coverage"] >= 0.6][:5]
        # decision conflicts: same subject, >1 distinct outcome (workspace-scoped)
        for row in con.execute(
                "SELECT subject, COUNT(DISTINCT outcome) AS n, "
                "GROUP_CONCAT(DISTINCT outcome) AS outcomes, MAX(created_at) AS last "
                "FROM decisions WHERE subject<>''" + _ws_check("decisions", ws) +
                " GROUP BY subject HAVING n>1 LIMIT 20",
                [ws] if ws else []):
            # Whole-subject match (case-insensitive): term overlap alone is
            # too loose ("alpha-service" vs text about "beta-service" shares
            # the token "service").
            if row["subject"].lower() in text_l:
                result["decision_conflicts"].append(dict(row))
        return result
    finally:
        con.close()


def forget_fact(args):
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        if args.get("id"):
            cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE id=? AND archived=0" +
                              _ws_check("facts", ws),
                              [now(), args["id"]] + ([ws] if ws else []))
        elif args.get("sha256"):
            cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE sha256=? AND archived=0" +
                              _ws_check("facts", ws),
                              [now(), args["sha256"]] + ([ws] if ws else []))
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
        ws = _workspace(args)
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        chain, cur, guard = [], int(fid), 0
        while cur is not None and guard < 50:
            row = con.execute(
                "SELECT id, text, source, project, domain, trust, strong, importance, "
                "confirmed, created_at, updated_at, invalid_at, superseded_by "
                "FROM facts WHERE id=?" + _ws_check("facts", ws),
                [cur] + ([ws] if ws else [])).fetchone()
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
        ws = _workspace(args)
        ws_params = [ws] if ws else []
        total = con.execute(
            "SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at='' "
            "AND confirmed=0 AND trust != 'high'" + _ws_filter("facts", ws),
            ws_params).fetchone()[0]
        rows = [dict(r) for r in con.execute(
            "SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
            "updated_at FROM facts WHERE archived=0 AND invalid_at='' "
            "AND confirmed=0 AND trust != 'high'" + _ws_filter("facts", ws) +
            " ORDER BY importance DESC, updated_at DESC LIMIT ?",
            ws_params + [limit])]
        return {"count": len(rows), "total": total, "facts": rows}
    finally:
        con.close()


def confirm_fact(args):
    """Mark a fact as human-confirmed: confirmed=1, trust=high."""
    fid = args.get("id")
    if fid is None:
        return {"error": "id is required"}
    ws = _workspace(args)
    ts = now()
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        cur = con.execute(
            "UPDATE facts SET confirmed=1, trust='high', updated_at=? "
            "WHERE id=? AND archived=0" + _ws_check("facts", ws),
            [ts, fid] + ([ws] if ws else []))
        con.commit()
        if cur.rowcount == 0:
            return {"error": "fact not found or archived", "id": fid}
        return {"id": fid, "confirmed": True, "trust": "high", "updated_at": ts}
    finally:
        con.close()


def fact_references(args):
    """Impact query for one fact: what it supersedes, what supersedes it,
    what it was consolidated into/from, and its evidence."""
    fid = args.get("id")
    if fid is None:
        return {"error": "id is required"}
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        fact = con.execute(
            "SELECT id, text, source, trust, strong, importance, confirmed, "
            "invalid_at, superseded_by, created_at, updated_at FROM facts WHERE id=?" +
            _ws_check("facts", ws),
            [fid] + ([ws] if ws else [])).fetchone()
        if not fact:
            return {"error": "fact not found", "id": fid}
        f = dict(fact)
        ws = _workspace(args)
        ws_params = [ws] if ws else []
        superseded = [r["id"] for r in con.execute(
            "SELECT id FROM facts WHERE superseded_by=? AND id!=?" + _ws_check("facts", ws),
            [fid, fid] + ws_params)]
        supersedes = f.get("superseded_by")
        consolidated_from = [r["source_ref"] for r in con.execute(
            "SELECT source_ref FROM evidence WHERE fact_id=? AND source_ref LIKE 'consolidated:%'",
            (fid,))]
        consolidated_into = [r["fact_id"] for r in con.execute(
            "SELECT fact_id FROM evidence e JOIN facts f ON f.id=e.fact_id "
            "WHERE e.source_ref=? AND e.fact_id!=?" + _ws_check("f", ws),
            ["consolidated:%s" % fid, fid] + ws_params)]
        superseded_evidence = [r["fact_id"] for r in con.execute(
            "SELECT fact_id FROM evidence e JOIN facts f ON f.id=e.fact_id "
            "WHERE e.source_ref=? AND e.fact_id!=?" + _ws_check("f", ws),
            ["supersedes:%s" % fid, fid] + ws_params)]
        evidence = [dict(r) for r in con.execute(
            "SELECT source_ref, source_checksum, created_at FROM evidence WHERE fact_id=?",
            (fid,))]
        return {
            "fact_id": fid, "text": f["text"][:200],
            "incoming": {
                "superseded_by_me": superseded,
                "supersedes_me": supersedes,
                "consolidated_into": consolidated_into,
                "referenced_via_supersedes": superseded_evidence,
            },
            "outgoing": {
                "supersedes": supersedes,
                "consolidated_from": consolidated_from,
            },
            "evidence": evidence,
        }
    finally:
        con.close()


def export_rdf(args):
    """W3C PROV-flavoured Turtle export: facts, entities/relations, decisions,
    evidence, and bi-temporal supersession edges."""
    limit = max(1, min(int(args.get("limit", 5000)), 50000))
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        out = []
        out.append("@prefix prov: <http://www.w3.org/ns/prov#> .")
        out.append("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .")
        out.append("@prefix mem: <https://memory-mcp.dev/vocab#> .")
        n = 0
        def esc(v):
            return (str(v).replace("\\", "\\\\").replace('"', '\\"')
                    .replace("\r", " ").replace("\n", " "))
        def add(line):
            out.append(line)
        for r in con.execute("SELECT id, text, source, trust, strong, importance, confirmed, "
                             "invalid_at, superseded_by, created_at, updated_at FROM facts "
                             "WHERE 1=1" + _ws_check("facts", ws) + " ORDER BY id",
                             [ws] if ws else []):
            f = dict(r)
            add("mem:fact-%d a mem:Fact ;" % f["id"])
            add("    mem:text \"%s\" ;" % esc(f["text"][:400]))
            add("    mem:trust \"%s\" ;" % f["trust"])
            add("    mem:importance \"%s\"^^xsd:decimal ;" % f["importance"])
            if f["source"]:
                add("    prov:wasGeneratedBy [ a prov:Activity ; prov:used \"%s\" ] ;" % esc(f["source"]))
            add("    prov:generatedAtTime \"%s\"^^xsd:dateTime ." % f["created_at"])
            if f["invalid_at"]:
                add("mem:fact-%d prov:invalidatedAtTime \"%s\"^^xsd:dateTime ." % (f["id"], f["invalid_at"]))
            if f["superseded_by"]:
                add("mem:fact-%d mem:supersededBy mem:fact-%d ." % (f["id"], f["superseded_by"]))
        ent_ws = _ws_check("entities", ws)
        ent_params = [ws] if ws else []
        for r in con.execute("SELECT id, name, type FROM entities WHERE 1=1" + ent_ws + " ORDER BY id",
                             ent_params):
            add("mem:entity-%d a mem:Entity ; mem:name \"%s\" ; mem:type \"%s\" ."
                % (r["id"], esc(r["name"]), esc(r["type"] or "")))
        for r in con.execute("SELECT id, subject_id, predicate, object_id FROM relations "
                             "WHERE 1=1" + _ws_check("relations", ws) + " ORDER BY id",
                             [ws] if ws else []):
            add("mem:entity-%d mem:relatedTo mem:entity-%d ; mem:predicate \"%s\" ."
                % (r["subject_id"], r["object_id"], esc(r["predicate"])))
        for r in con.execute("SELECT id, category, subject, scenario, outcome, "
                             "parent_decision_id, created_at FROM decisions "
                             "WHERE 1=1" + _ws_check("decisions", ws) + " ORDER BY id",
                             [ws] if ws else []):
            add("mem:decision-%d a mem:Decision ;" % r["id"])
            add("    mem:scenario \"%s\" ;" % esc(r["scenario"][:300]))
            if r["subject"]:
                add("    mem:subject \"%s\" ;" % esc(r["subject"]))
            if r["outcome"]:
                add("    mem:outcome \"%s\" ;" % esc(r["outcome"]))
            if r["parent_decision_id"]:
                add("    prov:wasDerivedFrom mem:decision-%d ;" % r["parent_decision_id"])
            add("    prov:generatedAtTime \"%s\"^^xsd:dateTime ." % r["created_at"])
        for r in con.execute(
                "SELECT e.fact_id, e.source_ref, e.source_checksum, e.created_at "
                "FROM evidence e JOIN facts f ON f.id=e.fact_id WHERE 1=1" +
                _ws_check("f", ws) + " ORDER BY e.fact_id", [ws] if ws else []):
            add("mem:fact-%d prov:wasDerivedFrom [ a prov:Entity ; "
                "prov:atLocation \"%s\" ; prov:value \"%s\" ] ;"
                % (r["fact_id"], esc(r["source_ref"]), esc(r["source_checksum"])))
            add("    prov:generatedAtTime \"%s\"^^xsd:dateTime ." % r["created_at"])
        # cap and cut at a triple boundary (last line ends with '.')
        if len(out) > limit * 6:
            out = out[:limit * 6]
        while out and not out[-1].rstrip().endswith("."):
            out.pop()
        return {"format": "text/turtle", "triples": len(out), "truncated": len(out) >= limit * 6,
                "rdf": "\n".join(out)}
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
        sql = ("SELECT id, text, source, project, domain, trust, strong, importance, confirmed, "
               "created_at, updated_at FROM facts WHERE source=? AND archived=0 AND invalid_at='' "
               "AND lifecycle != 'forgotten'")
        ws = _workspace(args)
        params = [session_ref]
        sql += _ws_filter("facts", ws)
        if ws:
            params.append(ws)
        sql += " ORDER BY importance DESC, updated_at DESC LIMIT ?"
        params.append(limit)
        rows = [dict(r) for r in con.execute(sql, params)]
        return {"count": len(rows), "session_ref": session_ref, "facts": rows}
    finally:
        con.close()


def list_sessions(args):
    """Session index: distinct sources with active-fact counts, freshest first."""
    limit = max(1, min(int(args.get("limit", 50)), 200))
    con = get_db()
    try:
        ws = _workspace(args)
        rows = [dict(r) for r in con.execute(
            "SELECT source, COUNT(*) AS facts, MAX(updated_at) AS last_activity "
            "FROM facts WHERE source != '' AND archived=0 AND invalid_at=''" +
            _ws_check("facts", ws) +
            " GROUP BY source ORDER BY last_activity DESC LIMIT ?",
            ([limit] if not ws else [ws, limit]))]
        return {"count": len(rows), "sessions": rows}
    finally:
        con.close()




def stats(_args=None):
    con = get_db()
    try:
        ws = _workspace(_args or {})
        ws_clause = _ws_check("facts", ws)
        ws_params = [ws] if ws else []
        total = con.execute("SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at=''" + ws_clause,
                            ws_params).fetchone()[0]
        by_trust = {r["trust"]: r["n"] for r in con.execute(
            "SELECT trust, COUNT(*) n FROM facts WHERE archived=0 AND invalid_at=''" + ws_clause +
            " GROUP BY trust", ws_params)}
        by_domain = {r["domain"] or "(none)": r["n"] for r in con.execute(
            "SELECT domain, COUNT(*) n FROM facts WHERE archived=0 AND invalid_at=''" + ws_clause +
            " GROUP BY domain", ws_params)}
        strong = con.execute(
            "SELECT COUNT(*) FROM facts WHERE archived=0 AND invalid_at='' AND strong=1" + ws_clause,
            ws_params).fetchone()[0]
        counts = {
            "entities": con.execute("SELECT COUNT(*) FROM entities WHERE 1=1" + _ws_check("entities", ws),
                                    ws_params).fetchone()[0],
            "relations": con.execute("SELECT COUNT(*) FROM relations WHERE 1=1" + _ws_check("relations", ws),
                                     ws_params).fetchone()[0],
            "decisions": con.execute("SELECT COUNT(*) FROM decisions WHERE 1=1" + _ws_check("decisions", ws),
                                     ws_params).fetchone()[0],
            "evidence": con.execute("SELECT COUNT(*) FROM evidence e JOIN facts f ON f.id=e.fact_id "
                                    "WHERE 1=1" + _ws_check("f", ws), ws_params).fetchone()[0],
        }
        return {"total": total, "strong": strong, "by_trust": by_trust, "by_domain": by_domain,
                "counts": counts}
    finally:
        con.close()


def export_facts(_args=None):
    # Deliberately includes archived rows and is NOT gated by workspace
    # status: it is the migration/backup dump tool (same policy as
    # backup_workspace), so archived/reset workspaces stay recoverable.
    con = get_db()
    try:
        ws = _workspace(_args or {})
        rows = [dict(r) for r in con.execute(
            "SELECT id, sha256, text, source, project, domain, trust, strong, created_at, updated_at, archived "
            "FROM facts WHERE 1=1" + _ws_check("facts", ws) + " ORDER BY id",
            [ws] if ws else [])]
        return {"count": len(rows), "facts": rows}
    finally:
        con.close()


# ---- database management (v0.6, 2026-08-17) -------------------------------
# Named databases are separate SQLite files under <dbdir>/databases/,
# siblings of the active DB (MEMORY_MCP_DB). The active DB itself can be
# backed up but never archived/deleted through these tools.

def _ts_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def create_database(args):
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if name + ".db" == _active_db_name():
        return {"error": f"database {name} is the active store (MEMORY_MCP_DB)"}
    p = _db_file(name)
    if os.path.exists(p):
        return {"error": f"database {name} already exists"}
    try:
        con = _open_db(p)
        con.close()
    except RuntimeError as e:
        return {"error": str(e)}
    return {"created": name, "file": "databases/" + name + ".db"}


def list_databases(args):
    d = _db_dir()
    dbs = []
    for fn in sorted(os.listdir(d)):
        if fn.endswith(".db"):
            dbs.append({"name": fn[:-3], "active": False, "archived": False})
        elif fn.endswith(".db.archived"):
            dbs.append({"name": fn[:-len(".db.archived")], "active": False, "archived": True})
    active = _active_db_name()
    dbs.insert(0, {"name": active[:-3] if active.endswith(".db") else active,
                   "active": True, "archived": False})
    return {"databases": dbs}


def archive_database(args):
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if name + ".db" == _active_db_name():
        return {"error": f"cannot archive the active database (MEMORY_MCP_DB)"}
    p = _db_file(name)
    if not os.path.exists(p):
        return {"error": f"database {name} not found"}
    if args.get("hard"):
        if args.get("confirm") is not True:
            return {"error": "confirm: true is required for hard archive (permanent delete)"}
        os.remove(p)
        return {"archived": name, "hard": True, "deleted": True}
    if os.path.exists(p + ".archived"):
        return {"error": f"an archived copy of database {name} already exists; "
                         "remove or restore it first, or use hard:true"}
    os.rename(p, p + ".archived")
    return {"archived": name, "hard": False, "deleted": False}


def backup_database(args):
    name = (args.get("name") or "").strip()
    label = _active_db_name()
    src = DB_PATH
    if name:
        name, err = _validate_name(name, "database")
        if err:
            return {"error": err}
        p = _db_file(name)
        if os.path.exists(p):
            src = p
            label = name + ".db"
        elif os.path.exists(p + ".archived"):
            src = p + ".archived"
            label = name + ".db.archived"
        else:
            return {"error": f"database {name} not found"}
    dest = os.path.join(_backup_dir(), label + "." + _ts_stamp() + ".db")
    try:
        src_con = sqlite3.connect(src, timeout=10)
        try:
            dst_con = sqlite3.connect(dest)
            try:
                src_con.backup(dst_con)
            finally:
                dst_con.close()
        finally:
            src_con.close()
    except sqlite3.DatabaseError as e:
        return {"error": f"backup failed: {e}"}
    return {"database": label, "backup": os.path.basename(dest), "size": os.path.getsize(dest)}


def delete_database(args):
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if name + ".db" == _active_db_name():
        return {"error": f"cannot delete the active database (MEMORY_MCP_DB)"}
    if args.get("confirm") is not True:
        return {"error": "confirm: true is required to delete a database"}
    p = _db_file(name)
    if not os.path.exists(p):
        return {"error": f"database {name} not found"}
    os.remove(p)
    return {"deleted": name}


# ---- workspace management (v0.6, 2026-08-17) ------------------------------
# Workspaces are named access scopes registered in the `workspaces` table of
# the active DB. Soft reset/archive mark the workspace's facts archived
# (reversible); hard mode (confirm: true) physically deletes the facts.

def create_workspace(args):
    name, err = _validate_name(args.get("workspace"), "workspace")
    if err:
        return {"error": err}
    con = get_db()
    try:
        ts = now()
        row = con.execute("SELECT status FROM workspaces WHERE id=?", [name]).fetchone()
        if row:
            if row["status"] != "active":
                con.execute("UPDATE workspaces SET status='active', updated_at=? WHERE id=?", [ts, name])
                con.commit()
                return {"workspace": name, "created": False, "reactivated": True}
            return {"workspace": name, "created": False}
        con.execute("INSERT INTO workspaces (id, status, created_at, updated_at) VALUES (?, 'active', ?, ?)",
                    [name, ts, ts])
        con.commit()
        return {"workspace": name, "created": True}
    finally:
        con.close()


def list_workspaces(args):
    con = get_db()
    try:
        status = (args.get("status") or "").strip()
        q = ("SELECT w.id, w.status, w.created_at, w.updated_at, "
             "(SELECT COUNT(*) FROM facts f WHERE f.workspace_id=w.id AND f.archived=0 AND f.invalid_at='') AS active_facts "
             "FROM workspaces w")
        params = []
        if status:
            q += " WHERE w.status=?"
            params.append(status)
        rows = [dict(r) for r in con.execute(q + " ORDER BY w.id", params)]
        return {"count": len(rows), "workspaces": rows}
    finally:
        con.close()


def _purge_workspace_rows(con, name):
    """Physically delete every row owned by a workspace across all tables, in
    FK-safe order (children before parents), within the caller's transaction.
    Returns per-table deleted counts (every key always present). FTS shadow
    tables are updated by the AFTER DELETE triggers on facts/decisions."""
    counts = {}
    cur = con.execute("DELETE FROM evidence WHERE fact_id IN "
                      "(SELECT id FROM facts WHERE workspace_id=?)", [name])
    counts["evidence"] = cur.rowcount
    cur = con.execute("DELETE FROM fact_embeddings WHERE fact_id IN "
                      "(SELECT id FROM facts WHERE workspace_id=?)", [name])
    counts["embeddings"] = cur.rowcount
    cur = con.execute("DELETE FROM relations WHERE workspace_id=?", [name])
    counts["relations"] = cur.rowcount
    cur = con.execute("DELETE FROM entities WHERE workspace_id=?", [name])
    counts["entities"] = cur.rowcount
    # decisions self-link via parent_decision_id (no FK): detach chains that
    # would dangle into the purged set, then delete the decisions.
    con.execute("UPDATE decisions SET parent_decision_id=NULL WHERE parent_decision_id IN "
                "(SELECT id FROM decisions WHERE workspace_id=?)", [name])
    cur = con.execute("DELETE FROM decisions WHERE workspace_id=?", [name])
    counts["decisions"] = cur.rowcount
    cur = con.execute("DELETE FROM facts WHERE workspace_id=?", [name])
    counts["facts"] = cur.rowcount
    return counts


def reset_workspace(args):
    name, err = _validate_name(args.get("workspace"), "workspace")
    if err:
        return {"error": err}
    hard = args.get("hard") is True
    if hard and args.get("confirm") is not True:
        return {"error": "confirm: true is required for hard reset (permanent delete)"}
    con = get_db()
    try:
        ts = now()
        if hard:
            try:
                counts = _purge_workspace_rows(con, name)
                con.execute("DELETE FROM workspaces WHERE id=?", [name])
                con.commit()
            except sqlite3.DatabaseError as e:
                # IntegrityError (FK) and OperationalError (FTS5 SQLITE_CORRUPT,
                # lock contention) both subclass DatabaseError
                con.rollback()
                return {"error": f"hard reset failed: {e}", "workspace": name}
            return {"workspace": name, "hard": True, "deleted": counts,
                    "deleted_total": sum(counts.values()),
                    "deleted_facts": counts["facts"], "reset": True}
        cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE workspace_id=? AND archived=0",
                          [ts, name])
        archived = cur.rowcount
        con.execute("INSERT INTO workspaces (id, status, created_at, updated_at) VALUES (?, 'reset', ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET status='reset', updated_at=excluded.updated_at",
                    [name, ts, ts])
        con.commit()
        return {"workspace": name, "hard": False, "archived_facts": archived, "reset": True}
    finally:
        con.close()


def archive_workspace(args):
    name, err = _validate_name(args.get("workspace"), "workspace")
    if err:
        return {"error": err}
    hard = args.get("hard") is True
    if hard and args.get("confirm") is not True:
        return {"error": "confirm: true is required for hard archive (permanent delete)"}
    con = get_db()
    try:
        ts = now()
        if hard:
            try:
                counts = _purge_workspace_rows(con, name)
                con.execute("INSERT INTO workspaces (id, status, created_at, updated_at) VALUES (?, 'archived', ?, ?) "
                            "ON CONFLICT(id) DO UPDATE SET status='archived', updated_at=excluded.updated_at",
                            [name, ts, ts])
                con.commit()
            except sqlite3.DatabaseError as e:
                # IntegrityError (FK) and OperationalError (FTS5 SQLITE_CORRUPT,
                # lock contention) both subclass DatabaseError
                con.rollback()
                return {"error": f"hard archive failed: {e}", "workspace": name}
            return {"workspace": name, "hard": True, "deleted": counts,
                    "deleted_total": sum(counts.values()),
                    "deleted_facts": counts["facts"], "archived": True}
        cur = con.execute("UPDATE facts SET archived=1, updated_at=? WHERE workspace_id=? AND archived=0",
                          [ts, name])
        archived = cur.rowcount
        con.execute("INSERT INTO workspaces (id, status, created_at, updated_at) VALUES (?, 'archived', ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET status='archived', updated_at=excluded.updated_at",
                    [name, ts, ts])
        con.commit()
        return {"workspace": name, "hard": False, "archived_facts": archived, "archived": True}
    finally:
        con.close()


def backup_workspace(args):
    name, err = _validate_name(args.get("workspace"), "workspace")
    if err:
        return {"error": err}
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(
            "SELECT id, sha256, text, source, project, domain, trust, strong, importance, workspace_id, "
            "created_at, updated_at, archived FROM facts WHERE workspace_id=? ORDER BY id", [name])]
    finally:
        con.close()
    if not rows:
        return {"error": f"workspace {name} has no facts"}
    dest = os.path.join(_backup_dir(), f"workspace-{name}-{_ts_stamp()}.json")
    with open(dest, "w", encoding="utf-8") as f:
        json.dump({"workspace": name, "exported_at": now(), "count": len(rows), "facts": rows},
                  f, ensure_ascii=False, indent=2)
    return {"workspace": name, "backup": os.path.basename(dest), "count": len(rows)}


TOOLS = {
    # NOTE: add_fact exists as a HANDLERS alias for remember_fact (agents
    # guess the name) but is intentionally NOT advertised in the schema —
    # aliases would add tool-choice noise for every client.
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "chars": {"type": "integer", "default": 1400},
                "semantic": {"type": "boolean", "default": False},
                "graph": {"type": "boolean", "default": False, "description": "Expand via the entity graph (third RRF source)"},
                "session_expand": {"type": "integer", "default": 0, "description": "Pull up to N sibling facts from the top hits' sessions (background)"},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
            "properties": {
                "text": {"type": "string"},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
            "required": ["text"],
        },
    },
    "consolidate": {
        "description": "LLM-merge of paraphrased facts into one fact (inputs invalidated bi-temporally; strong/confirmed never merged). Requires MEMORY_MCP_VERIFY=1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ids": {"type": "array", "items": {"type": "integer"}},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "fact_history": {
        "description": "Bi-temporal history of one fact: walk the superseded_by chain (oldest first).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "review_pending": {
        "description": "Unconfirmed active facts (confirmed=0, trust != high), importance-first — for human review; confirm with confirm_fact.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 20},
                "workspace": {"type": "string", "description": "Project scope id; scopes the review to your project + shared pool"},
            },
        },
    },
    "confirm_fact": {
        "description": "Mark a fact as human-confirmed (confirmed=1, trust=high).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "facts_for_session": {
        "description": "All active facts recorded from one session (source=session_ref), importance-first.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_ref": {"type": "string"},
                "limit": {"type": "integer", "default": 50},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
            "required": ["session_ref"],
        },
    },
    "list_sessions": {
        "description": "Session index: distinct sources with active-fact counts, freshest first.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 50},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "fact_references": {
        "description": "Impact query for one fact: supersession chain, consolidation links, evidence.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "export_rdf": {
        "description": "W3C PROV-flavoured Turtle export (facts, entities/relations, decisions, evidence, supersession edges).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 5000},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
        },
    },
    "remember_entity": {
        "description": "Upsert an entity node (name unique; type/aliases optional).",
        "inputSchema": {
            "type": "object",
            "properties": {

                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
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

                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
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
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
            "required": ["scenario"],
        },
    },
    "get_causal_chain": {
        "description": "Walk parent_decision_id links from a decision to its root (oldest first).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "decision_id": {"type": "integer"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
            "required": ["decision_id"],
        },
    },
    "get_provenance": {
        "description": "Return a fact (by id or sha256) plus its evidence rows (source_ref, checksum).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fact_id": {"type": "integer"},
                "sha256": {"type": "string"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
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
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "detect_conflicts": {
        "description": "Near-duplicate facts (term coverage >= 0.6) + decisions with the same subject but >1 distinct outcome.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "forget_fact": {
        "description": "Soft-delete a fact by id or sha256.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "sha256": {"type": "string"},
                "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
            },
        },
    },
    "stats": {
        "description": "Store statistics (total, by trust, by domain).",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string", "description": "Project scope id; scopes the operation to your project + shared pool"},
        }},
    },
    "export": {
        "description": "Export all facts (including archived) as JSON — for migration/backup.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "create_database": {
        "description": "Create a new named database (separate SQLite file under databases/). The active store (MEMORY_MCP_DB) cannot be recreated.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string", "description": "Database name: 1-64 chars of [A-Za-z0-9._-], no '..'"},
        }, "required": ["name"]},
    },
    "list_databases": {
        "description": "List all databases (active + named, including archived ones).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "archive_database": {
        "description": "Archive a named database. Soft (default): rename to <name>.db.archived — data preserved, reversible by renaming back. hard:true deletes the file permanently (requires confirm:true). The active database cannot be archived.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string"},
            "hard": {"type": "boolean", "default": False},
            "confirm": {"type": "boolean", "default": False, "description": "required for hard mode"},
        }, "required": ["name"]},
    },
    "backup_database": {
        "description": "Backup a database (active by default, or a named one incl. archived) to backups/ via SQLite online backup API.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string", "description": "optional; defaults to the active store"},
        }},
    },
    "delete_database": {
        "description": "Permanently delete a named database file (requires confirm:true). The active database cannot be deleted.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string"},
            "confirm": {"type": "boolean", "default": False},
        }, "required": ["name", "confirm"]},
    },
    "create_workspace": {
        "description": "Register a workspace (named access scope) in the active database's workspaces registry. Re-registering reactivates an archived/reset workspace.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string", "description": "Workspace id: 1-64 chars of [A-Za-z0-9._-], no '..'"},
        }, "required": ["workspace"]},
    },
    "list_workspaces": {
        "description": "List registered workspaces with their status (active/archived/reset) and active fact counts.",
        "inputSchema": {"type": "object", "properties": {
            "status": {"type": "string", "enum": ["active", "archived", "reset"]},
        }},
    },
    "reset_workspace": {
        "description": "Reset a workspace. Soft (default): hide all its data — facts are archived (archived=1) and graph/decisions/evidence become unreadable and unwritable — status='reset'. hard:true purges facts, evidence, graph and decisions permanently (requires confirm:true); response reports per-table deleted counts.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string"},
            "hard": {"type": "boolean", "default": False},
            "confirm": {"type": "boolean", "default": False, "description": "required for hard mode"},
        }, "required": ["workspace"]},
    },
    "archive_workspace": {
        "description": "Archive a workspace. Soft (default): hide all its data — facts are archived (archived=1) and graph/decisions/evidence become unreadable and unwritable — status='archived'. hard:true purges facts, evidence, graph and decisions permanently (requires confirm:true); response reports per-table deleted counts.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string"},
            "hard": {"type": "boolean", "default": False},
            "confirm": {"type": "boolean", "default": False, "description": "required for hard mode"},
        }, "required": ["workspace"]},
    },
    "backup_workspace": {
        "description": "Export all facts of a workspace (including archived) as JSON to backups/workspace-<name>-<ts>.json.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string"},
        }, "required": ["workspace"]},
    },
    "decay_sweep": {
        "description": "v0.7: recompute fact lifecycle by active-day decay. Score = importance * 0.95^active_days (days with system activity since last search hit). score < 0.25 -> degraded (hidden from plain search, reachable via chains, revived after N matching searches); score <= 0.1 -> forgotten (visible only via list_forgotten/restore_fact). strong/confirmed never decay. User downtime does not count.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "list_forgotten": {
        "description": "Direct review of forgotten facts (lifecycle=forgotten) — the only way to see them besides restore_fact.",
        "inputSchema": {"type": "object", "properties": {
            "limit": {"type": "integer", "default": 50},
        }},
    },
    "restore_fact": {
        "description": "Manually bring a forgotten/degraded fact back to lifecycle=active (resets revival_count, stamps last_accessed_at).",
        "inputSchema": {"type": "object", "properties": {
            "id": {"type": "integer"},
        }, "required": ["id"]},
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
    "fact_references": fact_references,
    "export_rdf": export_rdf,
    "facts_for_session": facts_for_session,
    "list_sessions": list_sessions,
    "stats": stats,
    "export": export_facts,
    "create_database": create_database,
    "list_databases": list_databases,
    "archive_database": archive_database,
    "backup_database": backup_database,
    "delete_database": delete_database,
    "create_workspace": create_workspace,
    "list_workspaces": list_workspaces,
    "reset_workspace": reset_workspace,
    "archive_workspace": archive_workspace,
    "backup_workspace": backup_workspace,
    "decay_sweep": decay_sweep,
    "list_forgotten": list_forgotten,
    "restore_fact": restore_fact,
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
            _register_activity_day()
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
