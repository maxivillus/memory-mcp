#!/usr/bin/env python3
"""memory-mcp — shared fact memory for reasonix/jcode/codex runtimes (Phase 1, 2026-08-15).

Stdio MCP server (JSON-RPC 2.0, newline-delimited), SQLite + FTS5 storage.
Replaces the storage layer of the reasonix memory patches with a shared,
searchable fact store; extraction/gating/injection stay client-side.

Schema: text, sha256 (workspace-scoped dedup), source, project, domain, trust
(high|medium|low), strong (bool), created_at, updated_at, archived (soft delete).

Tools:
  remember_fact {text, source?, project?, domain?, trust?, strong?}
  search_facts  {query, limit?, trust_min?, strong_only?, project?, domain?}
  list_facts    {project?, domain?, limit?}
  summarize_index {project?, domain?, trust_min?, strong_only?, limit?, max_chars?}
  forget_fact   {id|sha256}
  stats         {}
  export        {}
  -- v0.11 immutable context artifacts --
  put_context {name, content, workspace, schema?, source?, checksum?, ttl_seconds?, parent_refs?}
  list_context {workspace, name?, limit?}
  resolve_context {ref, workspace}
  read_context {ref, workspace, start?, end?, max_chars?}
  search_context {query, workspace, limit?}
  chunk_context {ref, workspace, chunk_chars?, start_chunk?, max_chunks?}
  reduce_context {name, refs, workspace, separator?, schema?, source?, checksum?, ttl_seconds?}
  -- v0.13 bounded lifecycle capture and typed handoffs --
  capture_event {idempotency_key, event_kind, payload, workspace, ...}
  list_events {workspace, session_id?, event_kind?, limit?}
  read_event {event_ref, workspace, max_chars?}
  handoff_begin {content, owner, workspace, source?, checksum?, ttl_seconds?, ...}
  list_handoffs {workspace, owner?, state?, limit?}
  handoff_accept {handoff_ref, actor, workspace, cwd?, max_chars?}
  handoff_cancel {handoff_ref, actor, workspace}
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
  attach_evidence {fact_id, source_ref, source_checksum?, fetched_at?, repo?, ref?, path?, symbol?, line range?}
  chunk_fact {id | sha256, workspace?, chunk_chars?, start_chunk?, max_chunks?, chunk_overlap?}
  absorb {facts, workspace?, dry_run?, commit?, verify?}
  detect_conflicts {text}
"""
import fnmatch, hashlib, json, os, re, sqlite3, sys
from datetime import datetime, timedelta, timezone

def default_db_path():
    """Script-relative default: <repo>/data/facts.db — portable across environments.

    Override with MEMORY_MCP_DB (used by all deployment runtimes: host wrapper,
    docker containers via /opt/memory-shared).
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "facts.db")


DB_PATH = os.environ.get("MEMORY_MCP_DB") or default_db_path()
# v0.9 session-scoped database selection: `select_database` points all
# subsequent tools at a named database (separate SQLite file); None = the
# active store (MEMORY_MCP_DB), which stays protected from delete/archive.
_SELECTED_DB = [None]
VALID_TRUST = ("high", "medium", "low")


def _env_int(name, default, minimum):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


# Contexts are deliberately bounded at both storage and read time. The limits
# are operational guardrails, not a substitute for the caller's workspace ACL.
_CONTEXT_MAX_BYTES = _env_int("MEMORY_MCP_CONTEXT_MAX_BYTES", 16 * 1024 * 1024, 1)
_CONTEXT_DEFAULT_READ_CHARS = _env_int("MEMORY_MCP_CONTEXT_READ_CHARS", 4000, 1)
_CONTEXT_MAX_READ_CHARS = _env_int("MEMORY_MCP_CONTEXT_MAX_READ_CHARS", 16000, 1)
_CONTEXT_MAX_LINEAGE = _env_int("MEMORY_MCP_CONTEXT_MAX_LINEAGE", 100, 1)
_CONTEXT_MAX_CHUNKS = _env_int("MEMORY_MCP_CONTEXT_MAX_CHUNKS", 32, 1)
_CONTEXT_MAX_CHUNK_RESPONSE_CHARS = _env_int(
    "MEMORY_MCP_CONTEXT_MAX_CHUNK_RESPONSE_CHARS", 64 * 1024, 1)
_CONTEXT_MAX_REDUCE_REFS = 64
_CONTEXT_MAX_SEARCH_QUERY = 256

# Fact retrieval uses the same bounded delivery contract as context artifacts,
# but keeps its own knobs so a large fact cannot turn a search response into an
# unbounded prompt payload.
_FACT_DEFAULT_CHUNK_CHARS = _env_int("MEMORY_MCP_FACT_CHUNK_CHARS", 4000, 1)
_FACT_MAX_CHUNK_CHARS = _env_int("MEMORY_MCP_FACT_MAX_CHUNK_CHARS", 16000, 1)
_FACT_MAX_CHUNKS = _env_int("MEMORY_MCP_FACT_MAX_CHUNKS", 32, 1)
_FACT_MAX_CHUNK_RESPONSE_CHARS = _env_int(
    "MEMORY_MCP_FACT_MAX_CHUNK_RESPONSE_CHARS", 64 * 1024, 1)
_ABSORB_MAX_FACTS = _env_int("MEMORY_MCP_ABSORB_MAX_FACTS", 50, 1)
_ABSORB_MAX_TEXT_CHARS = _env_int("MEMORY_MCP_ABSORB_MAX_TEXT_CHARS", 16000, 1)
_EVIDENCE_MAX_FIELD_CHARS = _env_int("MEMORY_MCP_EVIDENCE_MAX_FIELD_CHARS", 2048, 1)

# v0.13 lifecycle capture and typed handoffs. These limits keep the local
# event spool useful between short runtime sessions without turning it into an
# unbounded transcript store.
_LIFECYCLE_MAX_EVENTS = _env_int("MEMORY_MCP_LIFECYCLE_MAX_EVENTS", 1000, 1)
_LIFECYCLE_MAX_PAYLOAD_BYTES = _env_int(
    "MEMORY_MCP_LIFECYCLE_MAX_PAYLOAD_BYTES", 64 * 1024, 1)
_LIFECYCLE_MAX_FIELD_CHARS = _env_int(
    "MEMORY_MCP_LIFECYCLE_MAX_FIELD_CHARS", 256, 1)
_LIFECYCLE_MAX_PATH_CHARS = _env_int(
    "MEMORY_MCP_LIFECYCLE_MAX_PATH_CHARS", 1024, 1)
_HANDOFF_DEFAULT_TTL = _env_int("MEMORY_MCP_HANDOFF_DEFAULT_TTL", 24 * 60 * 60, 0)
_HANDOFF_MAX_TTL = _env_int("MEMORY_MCP_HANDOFF_MAX_TTL", 7 * 24 * 60 * 60, 0)
_HANDOFF_MAX_CONTENT_BYTES = _env_int(
    "MEMORY_MCP_HANDOFF_MAX_CONTENT_BYTES", 256 * 1024, 1)
_HANDOFF_MAX_LIST = 100
_LIFECYCLE_EXCLUDED_GLOBS = (
    ".env", ".env.*", "*.pem", "*.key", "*.p12", "*.pfx",
    "id_rsa*", "credentials*", "secrets*",
)
_LIFECYCLE_KIND_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_LIFECYCLE_SECRET_PATTERNS = (
    re.compile(r"(?is)-----BEGIN [^-]+ PRIVATE KEY-----.*?-----END [^-]+ PRIVATE KEY-----"),
    re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)(\b(?:api[_-]?key|access[_-]?key|secret|password|token|authorization)\s*[=:]\s*)[^\s,;]+"),
    re.compile(r"(?i)([\"']?(?:api[_-]?key|access[_-]?key|secret|password|token|authorization)[\"']?\s*:\s*[\"']?)[^\s,;}\"']+"),
    re.compile(r"\b(?:sk|rk|pk)-[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"\b(?:ghp|gho|ghs|ghr|github_pat|xox[baprs]-)[A-Za-z0-9_-]+\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)
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
  lifecycle TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle IN ('active','degraded','forgotten')),
  category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL
);
"""

# v0.14 rebuild DDL: entity names are unique within a workspace, matching the
# resolver's ownership predicate and the relation workspace boundary.
_ENTITIES_TABLE_DDL = """
CREATE TABLE entities_new (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT '',
  aliases TEXT NOT NULL DEFAULT '',
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(name, workspace_id)
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

# v0.16 structured code-local provenance. These columns are additive so an
# existing evidence table can be upgraded without losing its old source_ref
# rows. The empty resolution status means that an evidence row is a plain
# source link rather than a code anchor.
_EVIDENCE_EXTRA_COLUMNS = {
    "repo": "TEXT NOT NULL DEFAULT ''",
    "ref": "TEXT NOT NULL DEFAULT ''",
    "path": "TEXT NOT NULL DEFAULT ''",
    "symbol": "TEXT NOT NULL DEFAULT ''",
    "start_line": "INTEGER",
    "start_col": "INTEGER",
    "end_line": "INTEGER",
    "end_col": "INTEGER",
    "selected_text_hash": "TEXT NOT NULL DEFAULT ''",
    "resolution_status": "TEXT NOT NULL DEFAULT ''",
}

_SCHEMA = """
-- v0.10 (2026-08-17): topic categories — the "card catalog". Auto-assigned at
-- write time (explicit arg > legacy domain > keyword rules), refined in
-- batches by categorize_pending (LLM). Created before facts so the FK
-- reference resolves.
CREATE TABLE IF NOT EXISTS categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(name, workspace_id)
);
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
  lifecycle TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle IN ('active','degraded','forgotten')),
  category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL
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
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT '',
  aliases TEXT NOT NULL DEFAULT '',
  workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(name, workspace_id)
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
  repo TEXT NOT NULL DEFAULT '',
  ref TEXT NOT NULL DEFAULT '',
  path TEXT NOT NULL DEFAULT '',
  symbol TEXT NOT NULL DEFAULT '',
  start_line INTEGER,
  start_col INTEGER,
  end_line INTEGER,
  end_col INTEGER,
  selected_text_hash TEXT NOT NULL DEFAULT '',
  resolution_status TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  UNIQUE(fact_id, source_ref)
);

-- v0.11: immutable named working contexts. Full content is kept behind a
-- bounded read API; catalog/resolve responses contain metadata only.
CREATE TABLE IF NOT EXISTS contexts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ref TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  content TEXT NOT NULL,
  schema_json TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS context_lineage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_ref TEXT NOT NULL REFERENCES contexts(ref) ON DELETE CASCADE,
  child_ref TEXT NOT NULL REFERENCES contexts(ref) ON DELETE CASCADE,
  relation TEXT NOT NULL DEFAULT 'derived',
  workspace_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(parent_ref, child_ref, relation)
);
CREATE INDEX IF NOT EXISTS contexts_workspace_idx ON contexts(workspace_id);
CREATE INDEX IF NOT EXISTS contexts_name_idx ON contexts(name, workspace_id);
CREATE INDEX IF NOT EXISTS context_lineage_parent_idx ON context_lineage(parent_ref);
CREATE INDEX IF NOT EXISTS context_lineage_child_idx ON context_lineage(child_ref);

-- v0.13: bounded lifecycle event spool. The event payload is stored as an
-- immutable context so existing context reads/ACLs remain the only payload
-- seam; this table is only the idempotency and catalog index.
CREATE TABLE IF NOT EXISTS lifecycle_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  idempotency_key TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  event_id TEXT NOT NULL,
  session_id TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  cwd TEXT NOT NULL DEFAULT '',
  path TEXT NOT NULL DEFAULT '',
  tool_name TEXT NOT NULL DEFAULT '',
  context_ref TEXT NOT NULL UNIQUE REFERENCES contexts(ref) ON DELETE CASCADE,
  workspace_id TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  payload_bytes INTEGER NOT NULL,
  payload_truncated INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  UNIQUE(workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS lifecycle_events_workspace_idx
  ON lifecycle_events(workspace_id, created_at, id);
CREATE INDEX IF NOT EXISTS lifecycle_events_session_idx
  ON lifecycle_events(workspace_id, session_id, created_at, id);

-- v0.13: typed, one-shot handoffs over immutable context artifacts.
CREATE TABLE IF NOT EXISTS handoffs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ref TEXT NOT NULL UNIQUE,
  context_ref TEXT NOT NULL UNIQUE REFERENCES contexts(ref) ON DELETE CASCADE,
  owner TEXT NOT NULL,
  session_id TEXT NOT NULL DEFAULT '',
  cwd TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  shared INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'open'
    CHECK(state IN ('open','accepted','cancelled','expired')),
  idempotency_key TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  accepted_at TEXT NOT NULL DEFAULT '',
  accepted_by TEXT NOT NULL DEFAULT '',
  cancelled_at TEXT NOT NULL DEFAULT '',
  cancelled_by TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS handoffs_idempotency_idx
  ON handoffs(workspace_id, idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS handoffs_workspace_idx
  ON handoffs(workspace_id, state, created_at, id);
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
    _migrate_entities(con)
    _migrate_fks(con)
    _migrate_evidence(con)
    _migrate_fts(con, preexisting_fts)
    _migrate_categories(con)
    return con


def get_db():
    if _SELECTED_DB[0] and not os.path.exists(_db_file(_SELECTED_DB[0])):
        # sqlite3.connect would silently recreate the file as an empty store —
        # fail loudly instead of operating on a brand-new database.
        raise RuntimeError(
            "selected database %r no longer exists — reset_database or "
            "recreate it with create_database" % _SELECTED_DB[0])
    return _open_db(_db_path())


def _migrate_categories(con):
    """v0.10 additive migration: `facts.category_id` column (plain INTEGER on
    migrated stores — the FK + ON DELETE SET NULL live in the fresh/rebuild
    DDL). The `categories` table itself is created by _SCHEMA before this
    runs; UNIQUE(name, workspace_id) comes from its DDL."""
    existing = {r["name"] for r in con.execute("PRAGMA table_info(facts)")}
    if "category_id" not in existing:
        con.execute("ALTER TABLE facts ADD COLUMN category_id INTEGER")
    con.commit()


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


def _migrate_entities(con):
    """v0.14 migration: replace legacy global UNIQUE(name) with the intended
    workspace-scoped UNIQUE(name, workspace_id), preserving ids and relations.

    SQLite cannot drop a table-level UNIQUE constraint in place, so legacy
    entity tables are rebuilt atomically with foreign keys temporarily disabled,
    following the existing facts/FK migration pattern.
    """
    indexes = con.execute("PRAGMA index_list(entities)").fetchall()
    has_workspace_unique = False
    for index in indexes:
        if not index["unique"]:
            continue
        index_name = str(index["name"]).replace('"', '""')
        columns = [row["name"] for row in con.execute(
            'PRAGMA index_info("%s")' % index_name)]
        if columns == ["name", "workspace_id"]:
            has_workspace_unique = True
            break
    if has_workspace_unique:
        return

    con.executescript(
        "PRAGMA foreign_keys=OFF;\n"
        "BEGIN;\n"
        + _ENTITIES_TABLE_DDL + "\n"
        "INSERT INTO entities_new (id, name, type, aliases, workspace_id, created_at, updated_at) "
        "SELECT id, name, type, aliases, workspace_id, created_at, updated_at FROM entities;\n"
        "DROP TABLE entities;\n"
        "ALTER TABLE entities_new RENAME TO entities;\n"
        "COMMIT;\n"
        "PRAGMA foreign_keys=ON;\n")
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
        # Preserve any anchor columns that may already exist on a partially
        # migrated store. Older stores have only the six base columns; the
        # additive migration below fills in the rest after the FK rebuild.
        evidence_columns = ["id", "fact_id", "source_ref", "source_checksum",
                            "fetched_at"] + list(_EVIDENCE_EXTRA_COLUMNS) + ["created_at"]
        existing = {r["name"] for r in con.execute("PRAGMA table_info(evidence)")}
        copied = [column for column in evidence_columns if column in existing]
        copy_sql = ", ".join(copied)
        extra_ddl = "".join("  %s %s,\n" % item
                             for item in _EVIDENCE_EXTRA_COLUMNS.items())
        con.executescript(
            "PRAGMA foreign_keys=OFF;\n"
            "BEGIN;\n"
            "CREATE TABLE evidence_new (\n"
            "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            "  fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,\n"
            "  source_ref TEXT NOT NULL,\n"
            "  source_checksum TEXT NOT NULL DEFAULT '',\n"
            "  fetched_at TEXT NOT NULL DEFAULT '',\n"
            + extra_ddl +
            "  created_at TEXT NOT NULL,\n"
            "  UNIQUE(fact_id, source_ref)\n"
            ");\n"
            "INSERT INTO evidence_new (%s) SELECT %s FROM evidence;\n" %
            (copy_sql, copy_sql) +
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


def _migrate_evidence(con):
    """v0.16 additive migration for structured code-local evidence anchors."""
    existing = {r["name"] for r in con.execute("PRAGMA table_info(evidence)")}
    for name, decl in _EVIDENCE_EXTRA_COLUMNS.items():
        if name not in existing:
            con.execute("ALTER TABLE evidence ADD COLUMN %s %s" % (name, decl))
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


def _bounded_int_arg(args, name, default, minimum, maximum):
    """Parse a bounded integer argument without leaking ValueError/TypeError.

    MCP schema validation normally handles this at the transport boundary, but
    handlers are also called directly by integrations and tests.
    """
    raw = args.get(name, default)
    if isinstance(raw, bool):
        return None, {"error": "%s must be an integer" % name}
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, {"error": "%s must be an integer" % name}
    return max(minimum, min(value, maximum)), None


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


def _context_scope(args):
    """Require an explicit workspace for context data-plane operations."""
    workspace = _workspace(args)
    if not workspace:
        return None, {"error": "workspace is required for context operations"}
    workspace, err = _validate_name(workspace, "workspace")
    if err:
        return None, {"error": err}
    return workspace, None


def _context_ws_check(alias):
    """Context data is private to its explicit workspace, never shared-pool data."""
    return " AND %s.workspace_id = ?" % alias


def _context_json(value):
    """Keep schema metadata stable without accepting arbitrary DB values."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"))
    except (TypeError, ValueError):
        return ""


def _context_metadata(row):
    return {
        "ref": row["ref"],
        "name": row["name"],
        "schema": row["schema_json"],
        "source": row["source"],
        "sha256": row["sha256"],
        "workspace": row["workspace_id"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "size_bytes": row["size_bytes"],
    }


def _context_lineage(con, ref, workspace):
    parents = [dict(r) for r in con.execute(
        "SELECT l.relation, p.ref, p.name FROM context_lineage l "
        "JOIN contexts p ON p.ref=l.parent_ref "
        "WHERE l.child_ref=? AND l.workspace_id=?" +
        _context_ws_check("p") + " ORDER BY l.id LIMIT ?",
        [ref, workspace, workspace, _CONTEXT_MAX_LINEAGE]).fetchall()]
    children = [dict(r) for r in con.execute(
        "SELECT l.relation, c.ref, c.name FROM context_lineage l "
        "JOIN contexts c ON c.ref=l.child_ref "
        "WHERE l.parent_ref=? AND l.workspace_id=?" +
        _context_ws_check("c") + " ORDER BY l.id LIMIT ?",
        [ref, workspace, workspace, _CONTEXT_MAX_LINEAGE]).fetchall()]
    return {"parents": parents, "children": children}


def _context_row(con, ref, workspace):
    row = con.execute(
        "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
        "created_at, expires_at, size_bytes FROM contexts WHERE ref=?" +
        _context_ws_check("contexts"),
        [ref, workspace]).fetchone()
    if not row:
        return None, None
    if row["expires_at"] and row["expires_at"] <= now():
        return row, {"error": "context has expired", "ref": ref}
    return row, None


def _context_limit(value, name, default, maximum):
    if value is None:
        return default, None
    if isinstance(value, bool):
        return None, {"error": f"{name} must be an integer"}
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None, {"error": f"{name} must be an integer"}
    if parsed < 1 or parsed > maximum:
        return None, {"error": f"{name} must be between 1 and {maximum}"}
    return parsed, None


def _bounded_utf8(text, max_bytes):
    """Return text clipped at a UTF-8 byte boundary and a truncation flag."""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, False
    return encoded[:max_bytes].decode("utf-8", "ignore"), True


def _redact_lifecycle_text(text):
    """Remove common credential forms before lifecycle data reaches SQLite."""
    for pattern in _LIFECYCLE_SECRET_PATTERNS:
        def replacement(match):
            prefix = match.group(1) if match.lastindex else ""
            return prefix + "<redacted>"
        text = pattern.sub(replacement, text)
    return text


def _lifecycle_field(value, field, maximum, required=False, redact=True):
    if value is None:
        if required:
            return None, {"error": f"{field} is required"}
        return "", None
    if not isinstance(value, str):
        return None, {"error": f"{field} must be a string"}
    value = value.strip()
    if required and not value:
        return None, {"error": f"{field} is required"}
    if len(value) > maximum:
        return None, {"error": f"{field} must be at most {maximum} characters"}
    return (_redact_lifecycle_text(value) if redact else value), None


def _lifecycle_payload(payload):
    if payload is None:
        return None, None, None, {"error": "payload is required"}
    if isinstance(payload, str):
        payload_text, payload_format = payload, "text"
    else:
        try:
            payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError):
            return None, None, None, {"error": "payload must be JSON-serializable"}
        payload_format = "json"
    payload_text = _redact_lifecycle_text(payload_text)
    payload_text, truncated = _bounded_utf8(
        payload_text, _LIFECYCLE_MAX_PAYLOAD_BYTES)
    return payload_text, payload_format, truncated, None


def _lifecycle_path_excluded(path, patterns):
    if not path:
        return False
    normalized = path.replace("\\", "/")
    candidates = (normalized, os.path.basename(normalized))
    return any(fnmatch.fnmatchcase(candidate, pattern)
               for candidate in candidates for pattern in patterns)


def _lifecycle_exclusion_patterns(args):
    patterns = args.get("exclude_paths", args.get("capture_exclusions", []))
    if patterns is None:
        return [], None
    if not isinstance(patterns, list):
        return None, {"error": "exclude_paths must be an array of strings"}
    if len(patterns) > 32:
        return None, {"error": "exclude_paths may contain at most 32 patterns"}
    clean = []
    for pattern in patterns:
        if not isinstance(pattern, str) or not pattern.strip():
            return None, {"error": "exclude_paths must contain non-empty strings"}
        pattern = pattern.strip()
        if len(pattern) > _LIFECYCLE_MAX_PATH_CHARS:
            return None, {"error": "exclude path patterns are too long"}
        clean.append(pattern)
    return clean, None


def _context_expiry(ttl_seconds, default=None, maximum=None):
    ttl = default if ttl_seconds is None else ttl_seconds
    if isinstance(ttl, bool):
        return None, {"error": "ttl_seconds must be a non-negative integer"}
    try:
        ttl = int(ttl)
    except (TypeError, ValueError):
        return None, {"error": "ttl_seconds must be a non-negative integer"}
    if ttl < 0:
        return None, {"error": "ttl_seconds must be a non-negative integer"}
    if maximum is not None and ttl > maximum:
        return None, {"error": f"ttl_seconds must be at most {maximum}"}
    return (datetime.now(timezone.utc) + timedelta(seconds=ttl)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), None


def _new_context_ref(name, created_at, prefix="ctx"):
    ref_seed = (name + "\0" + created_at + "\0" + os.urandom(16).hex()).encode("utf-8")
    return prefix + "_" + hashlib.sha256(ref_seed).hexdigest()


def _insert_context_row(con, *, name, content, workspace, schema, source,
                        checksum, created_at, expires_at, ref_prefix="ctx"):
    """Insert a context inside the caller's transaction and return its row."""
    size_bytes = len(content.encode("utf-8"))
    ref = _new_context_ref(name, created_at, ref_prefix)
    con.execute(
        "INSERT INTO contexts (ref, name, content, schema_json, source, sha256, "
        "workspace_id, created_at, expires_at, size_bytes) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (ref, name, content, _context_json(schema), source, checksum, workspace,
         created_at, expires_at, size_bytes))
    return con.execute(
        "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
        "created_at, expires_at, size_bytes FROM contexts WHERE ref=?", [ref]).fetchone()


def _event_row(con, workspace, idempotency_key):
    return con.execute(
        "SELECT id, idempotency_key, event_kind, event_id, session_id, source, cwd, "
        "path, tool_name, context_ref, workspace_id, sha256, payload_bytes, "
        "payload_truncated, created_at FROM lifecycle_events "
        "WHERE workspace_id=? AND idempotency_key=?",
        [workspace, idempotency_key]).fetchone()


def _event_metadata(row):
    return {
        "event_ref": row["context_ref"],
        "context_ref": row["context_ref"],
        "idempotency_key": row["idempotency_key"],
        "event_id": row["event_id"],
        "event_kind": row["event_kind"],
        "session_id": row["session_id"],
        "source": row["source"],
        "tool_name": row["tool_name"],
        "sha256": row["sha256"],
        "payload_bytes": row["payload_bytes"],
        "payload_truncated": bool(row["payload_truncated"]),
        "workspace": row["workspace_id"],
        "created_at": row["created_at"],
    }


def _trim_event_spool(con, workspace):
    """Keep only the newest bounded number of event contexts per workspace."""
    limit = max(1, int(_LIFECYCLE_MAX_EVENTS))
    count = con.execute(
        "SELECT COUNT(*) FROM lifecycle_events WHERE workspace_id=?", [workspace]
    ).fetchone()[0]
    excess = count - limit
    if excess <= 0:
        return 0
    old_rows = con.execute(
        "SELECT context_ref FROM lifecycle_events WHERE workspace_id=? "
        "ORDER BY id LIMIT ?", [workspace, excess]).fetchall()
    for old in old_rows:
        # Context deletion cascades to its event index row. Event contexts have
        # no parent lineage and are never shared with a handoff.
        con.execute("DELETE FROM contexts WHERE ref=?", [old["context_ref"]])
    return len(old_rows)


def capture_event(args):
    """Capture one sanitized lifecycle envelope into the bounded local spool."""
    workspace, err = _context_scope(args)
    if err:
        return err
    idempotency_key, err = _lifecycle_field(
        args.get("idempotency_key", args.get("event_id")),
        "idempotency_key", _LIFECYCLE_MAX_FIELD_CHARS, required=True, redact=False)
    if err:
        return err
    event_id, err = _lifecycle_field(
        args.get("event_id", idempotency_key), "event_id",
        _LIFECYCLE_MAX_FIELD_CHARS, required=True, redact=False)
    if err:
        return err
    kind, err = _lifecycle_field(args.get("event_kind", args.get("kind")),
                                 "event_kind", _LIFECYCLE_MAX_FIELD_CHARS,
                                 required=True, redact=False)
    if err:
        return err
    kind = kind.lower().replace("_", "-")
    if not _LIFECYCLE_KIND_RE.match(kind):
        return {"error": "event_kind must use lowercase letters, digits, '.', '_' or '-'"}
    session_id, err = _lifecycle_field(args.get("session_id", args.get("session_ref")),
                                       "session_id", _LIFECYCLE_MAX_FIELD_CHARS,
                                       redact=False)
    if err:
        return err
    source, err = _lifecycle_field(args.get("source"), "source",
                                   _LIFECYCLE_MAX_FIELD_CHARS)
    if err:
        return err
    cwd, err = _lifecycle_field(args.get("cwd"), "cwd", _LIFECYCLE_MAX_PATH_CHARS)
    if err:
        return err
    path, err = _lifecycle_field(args.get("path"), "path", _LIFECYCLE_MAX_PATH_CHARS)
    if err:
        return err
    tool_name, err = _lifecycle_field(args.get("tool_name"), "tool_name",
                                      _LIFECYCLE_MAX_FIELD_CHARS)
    if err:
        return err
    patterns, err = _lifecycle_exclusion_patterns(args)
    if err:
        return err
    if args.get("capture", True) is False:
        return {"accepted": False, "status": "excluded", "reason": "capture_disabled"}
    if _lifecycle_path_excluded(path, _LIFECYCLE_EXCLUDED_GLOBS + tuple(patterns)):
        return {"accepted": False, "status": "excluded", "reason": "path_excluded"}
    payload, payload_format, truncated, err = _lifecycle_payload(
        args.get("payload", args.get("content")))
    if err:
        return err

    created_at = now()
    envelope = {
        "version": 1,
        "event_id": event_id,
        "event_kind": kind,
        "session_id": session_id,
        "source": source,
        "tool_name": tool_name,
        "payload_format": payload_format,
        "payload": payload,
        "truncated": truncated,
    }
    content = json.dumps(envelope, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"))
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    schema = {"kind": "lifecycle_event", "version": 1, "event_kind": kind,
              "payload_format": payload_format}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        con.execute("BEGIN IMMEDIATE")
        existing = _event_row(con, workspace, idempotency_key)
        if existing:
            con.rollback()
            if existing["sha256"] != checksum:
                return {"error": "idempotency key already used for a different event"}
            return {"accepted": True, "duplicate": True,
                    "event": _event_metadata(existing)}
        context = _insert_context_row(
            con, name="event-" + hashlib.sha256(idempotency_key.encode()).hexdigest()[:32],
            content=content, workspace=workspace, schema=schema, source=source,
            checksum=checksum, created_at=created_at, expires_at="")
        con.execute(
            "INSERT INTO lifecycle_events (idempotency_key, event_kind, event_id, "
            "session_id, source, cwd, path, tool_name, context_ref, workspace_id, "
            "sha256, payload_bytes, payload_truncated, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (idempotency_key, kind, event_id, session_id, source, cwd, path, tool_name,
             context["ref"], workspace, checksum, len(payload.encode("utf-8")),
             int(truncated), created_at))
        pruned = _trim_event_spool(con, workspace)
        con.commit()
        row = _event_row(con, workspace, idempotency_key)
        return {"accepted": True, "duplicate": False, "event": _event_metadata(row),
                "context": _context_metadata(context), "pruned": pruned}
    except sqlite3.IntegrityError:
        con.rollback()
        existing = _event_row(con, workspace, idempotency_key)
        if existing and existing["sha256"] == checksum:
            return {"accepted": True, "duplicate": True,
                    "event": _event_metadata(existing)}
        return {"error": "lifecycle event write conflicted with another event"}
    except sqlite3.DatabaseError as e:
        con.rollback()
        return {"error": f"lifecycle event write failed: {e}"}
    finally:
        con.close()


def list_events(args):
    """List lifecycle metadata without returning captured payloads."""
    workspace, err = _context_scope(args)
    if err:
        return err
    limit, err = _context_limit(args.get("limit"), "limit", 50, _HANDOFF_MAX_LIST)
    if err:
        return err
    session_id, err = _lifecycle_field(args.get("session_id", args.get("session_ref")),
                                       "session_id", _LIFECYCLE_MAX_FIELD_CHARS,
                                       redact=False)
    if err:
        return err
    kind = (args.get("event_kind", args.get("kind")) or "").strip().lower().replace("_", "-")
    if kind and not _LIFECYCLE_KIND_RE.match(kind):
        return {"error": "event_kind must use lowercase letters, digits, '.', '_' or '-'"}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        query = "SELECT id, idempotency_key, event_kind, event_id, session_id, source, cwd, " \
                "path, tool_name, context_ref, workspace_id, sha256, payload_bytes, " \
                "payload_truncated, created_at FROM lifecycle_events WHERE workspace_id=?"
        params = [workspace]
        if session_id:
            query += " AND session_id=?"
            params.append(session_id)
        if kind:
            query += " AND event_kind=?"
            params.append(kind)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(query, params).fetchall()
        return {"count": len(rows), "events": [_event_metadata(row) for row in rows]}
    finally:
        con.close()


def read_event(args):
    """Read a bounded lifecycle envelope by event/context ref."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("event_ref", args.get("ref")) or "").strip()
    if not ref:
        return {"error": "event_ref is required"}
    max_chars, err = _context_limit(args.get("max_chars"), "max_chars",
                                     _CONTEXT_DEFAULT_READ_CHARS,
                                     _CONTEXT_MAX_READ_CHARS)
    if err:
        return err
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        row = con.execute(
            "SELECT e.id, e.idempotency_key, e.event_kind, e.event_id, e.session_id, "
            "e.source, e.cwd, e.path, e.tool_name, e.context_ref, e.workspace_id, "
            "e.sha256, e.payload_bytes, e.payload_truncated, e.created_at "
            "FROM lifecycle_events e WHERE e.workspace_id=? AND "
            "(e.context_ref=? OR e.idempotency_key=?)", [workspace, ref, ref]).fetchone()
        if not row:
            return {"error": "event not found or not in your workspace", "event_ref": ref}
        context_row, row_err = _context_row(con, row["context_ref"], workspace)
        if row_err:
            return row_err
        if not context_row:
            return {"error": "event context not found or not in your workspace",
                    "event_ref": ref}
        content = context_row["content"][:max_chars]
        context = _context_metadata(context_row)
        context.update({"content": content, "start": 0, "end": len(content),
                        "total_chars": len(context_row["content"]),
                        "truncated": len(content) < len(context_row["content"]),
                        "next_start": len(content) if len(content) < len(context_row["content"])
                        else None})
        return {"event": _event_metadata(row), "context": context,
                "lineage": _context_lineage(con, row["context_ref"], workspace)}
    finally:
        con.close()


def _handoff_row(con, workspace, ref):
    return con.execute(
        "SELECT id, ref, context_ref, owner, session_id, cwd, source, sha256, "
        "workspace_id, shared, state, idempotency_key, created_at, expires_at, "
        "accepted_at, accepted_by, cancelled_at, cancelled_by FROM handoffs "
        "WHERE workspace_id=? AND ref=?", [workspace, ref]).fetchone()


def _handoff_metadata(row):
    return {
        "ref": row["ref"],
        "context_ref": row["context_ref"],
        "owner": row["owner"],
        "session_id": row["session_id"],
        "source": row["source"],
        "sha256": row["sha256"],
        "workspace": row["workspace_id"],
        "shared": bool(row["shared"]),
        "state": row["state"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "accepted_at": row["accepted_at"],
        "accepted_by": row["accepted_by"],
        "cancelled_at": row["cancelled_at"],
        "cancelled_by": row["cancelled_by"],
    }


def _expire_handoffs(con, workspace):
    cur = con.execute(
        "UPDATE handoffs SET state='expired' WHERE workspace_id=? AND state='open' "
        "AND expires_at<>'' AND expires_at<=?", [workspace, now()])
    return cur.rowcount


def handoff_begin(args):
    """Create a typed, expiring handoff backed by one immutable context ref."""
    workspace, err = _context_scope(args)
    if err:
        return err
    owner, err = _lifecycle_field(args.get("owner"), "owner",
                                  _LIFECYCLE_MAX_FIELD_CHARS, required=True,
                                  redact=False)
    if err:
        return err
    session_id, err = _lifecycle_field(args.get("session_id", args.get("session_ref")),
                                       "session_id", _LIFECYCLE_MAX_FIELD_CHARS,
                                       redact=False)
    if err:
        return err
    cwd, err = _lifecycle_field(args.get("cwd"), "cwd", _LIFECYCLE_MAX_PATH_CHARS,
                                redact=False)
    if err:
        return err
    source, err = _lifecycle_field(args.get("source"), "source",
                                   _LIFECYCLE_MAX_FIELD_CHARS)
    if err:
        return err
    idempotency_key, err = _lifecycle_field(
        args.get("idempotency_key"), "idempotency_key",
        _LIFECYCLE_MAX_FIELD_CHARS, redact=False)
    if err:
        return err
    content = args.get("content")
    if not isinstance(content, str) or not content:
        return {"error": "content is required and must be a non-empty string"}
    if len(content.encode("utf-8")) > _HANDOFF_MAX_CONTENT_BYTES:
        return {"error": "content exceeds the handoff storage limit"}
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    supplied_checksum = (args.get("checksum") or "").strip().lower()
    if supplied_checksum and supplied_checksum != checksum:
        return {"error": "checksum does not match content", "sha256": checksum}
    expires_at, err = _context_expiry(args.get("ttl_seconds"),
                                      _HANDOFF_DEFAULT_TTL, _HANDOFF_MAX_TTL)
    if err:
        return err
    shared = args.get("shared", False)
    if not isinstance(shared, bool):
        return {"error": "shared must be a boolean"}
    name = (args.get("name") or "").strip()
    if not name:
        name = "handoff-" + checksum[:32]
    if len(name) > 128:
        return {"error": "name must be at most 128 characters"}

    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        con.execute("BEGIN IMMEDIATE")
        if idempotency_key:
            existing = con.execute(
                "SELECT id, ref, context_ref, owner, session_id, cwd, source, sha256, "
                "workspace_id, shared, state, idempotency_key, created_at, expires_at, "
                "accepted_at, accepted_by, cancelled_at, cancelled_by FROM handoffs "
                "WHERE workspace_id=? AND idempotency_key=?", [workspace, idempotency_key]
            ).fetchone()
            if existing:
                con.rollback()
                if existing["sha256"] != checksum or existing["owner"] != owner:
                    return {"error": "idempotency key already used for a different handoff"}
                return {"created": True, "duplicate": True,
                        "handoff": _handoff_metadata(existing)}
        created_at = now()
        schema = {"kind": "typed_handoff", "version": 1, "owner": owner,
                  "session_id": session_id, "shared": shared}
        context = _insert_context_row(
            con, name=name, content=content, workspace=workspace, schema=schema,
            source=source, checksum=checksum, created_at=created_at,
            expires_at=expires_at)
        handoff_ref = _new_context_ref(name, created_at, "hnd")
        con.execute(
            "INSERT INTO handoffs (ref, context_ref, owner, session_id, cwd, source, "
            "sha256, workspace_id, shared, state, idempotency_key, created_at, "
            "expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (handoff_ref, context["ref"], owner, session_id, cwd, source, checksum,
             workspace, int(shared), "open", idempotency_key, created_at, expires_at))
        con.commit()
        row = _handoff_row(con, workspace, handoff_ref)
        return {"created": True, "duplicate": False,
                "handoff": _handoff_metadata(row),
                "context": _context_metadata(context)}
    except sqlite3.IntegrityError:
        con.rollback()
        if idempotency_key:
            existing = con.execute(
                "SELECT id, ref, context_ref, owner, session_id, cwd, source, sha256, "
                "workspace_id, shared, state, idempotency_key, created_at, expires_at, "
                "accepted_at, accepted_by, cancelled_at, cancelled_by FROM handoffs "
                "WHERE workspace_id=? AND idempotency_key=?", [workspace, idempotency_key]
            ).fetchone()
            if existing and existing["sha256"] == checksum and existing["owner"] == owner:
                return {"created": True, "duplicate": True,
                        "handoff": _handoff_metadata(existing)}
        return {"error": "handoff write conflicted with another handoff"}
    except sqlite3.DatabaseError as e:
        con.rollback()
        return {"error": f"handoff write failed: {e}"}
    finally:
        con.close()


def list_handoffs(args):
    """List handoff metadata and transition open expired rows safely."""
    workspace, err = _context_scope(args)
    if err:
        return err
    limit, err = _context_limit(args.get("limit"), "limit", 50, _HANDOFF_MAX_LIST)
    if err:
        return err
    owner, err = _lifecycle_field(args.get("owner"), "owner",
                                  _LIFECYCLE_MAX_FIELD_CHARS, redact=False)
    if err:
        return err
    state = (args.get("state") or "").strip().lower()
    if state and state not in ("open", "accepted", "cancelled", "expired"):
        return {"error": "state must be open, accepted, cancelled, or expired"}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        _expire_handoffs(con, workspace)
        con.commit()
        query = (
            "SELECT id, ref, context_ref, owner, session_id, cwd, source, sha256, "
            "workspace_id, shared, state, idempotency_key, created_at, expires_at, "
            "accepted_at, accepted_by, cancelled_at, cancelled_by FROM handoffs "
            "WHERE workspace_id=?")
        params = [workspace]
        if owner:
            query += " AND owner=?"
            params.append(owner)
        if state:
            query += " AND state=?"
            params.append(state)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(query, params).fetchall()
        return {"count": len(rows), "handoffs": [_handoff_metadata(row) for row in rows]}
    finally:
        con.close()


def handoff_accept(args):
    """Atomically claim one open handoff and return a bounded payload once."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("handoff_ref", args.get("ref")) or "").strip()
    if not ref:
        return {"error": "handoff_ref is required"}
    actor, err = _lifecycle_field(
        args.get("actor", args.get("accepted_by")), "actor",
        _LIFECYCLE_MAX_FIELD_CHARS, required=True, redact=False)
    if err:
        return err
    cwd, err = _lifecycle_field(args.get("cwd"), "cwd", _LIFECYCLE_MAX_PATH_CHARS,
                                redact=False)
    if err:
        return err
    max_chars, err = _context_limit(args.get("max_chars"), "max_chars",
                                     _CONTEXT_DEFAULT_READ_CHARS,
                                     _CONTEXT_MAX_READ_CHARS)
    if err:
        return err
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        con.execute("BEGIN IMMEDIATE")
        _expire_handoffs(con, workspace)
        row = _handoff_row(con, workspace, ref)
        if not row:
            con.rollback()
            return {"error": "handoff not found or not in your workspace", "handoff_ref": ref}
        if row["state"] != "open":
            con.rollback()
            return {"error": "handoff is not open", "state": row["state"],
                    "handoff": _handoff_metadata(row)}
        if not row["shared"] and row["owner"] != actor:
            con.rollback()
            return {"error": "handoff owner does not match actor"}
        if row["cwd"] and (not cwd or cwd != row["cwd"]):
            con.rollback()
            return {"error": "handoff cwd does not match"}
        context_row, row_err = _context_row(con, row["context_ref"], workspace)
        if row_err:
            con.execute("UPDATE handoffs SET state='expired' WHERE id=?", [row["id"]])
            con.commit()
            return {"error": "handoff has expired", "handoff_ref": ref}
        if not context_row:
            con.rollback()
            return {"error": "handoff context not found or not in your workspace"}
        accepted_at = now()
        con.execute(
            "UPDATE handoffs SET state='accepted', accepted_at=?, accepted_by=? WHERE id=?",
            [accepted_at, actor, row["id"]])
        con.commit()
        row = _handoff_row(con, workspace, ref)
        content = context_row["content"][:max_chars]
        context = _context_metadata(context_row)
        context.update({"content": content, "start": 0, "end": len(content),
                        "total_chars": len(context_row["content"]),
                        "truncated": len(content) < len(context_row["content"]),
                        "next_start": len(content) if len(content) < len(context_row["content"])
                        else None})
        return {"accepted": True, "handoff": _handoff_metadata(row),
                "context": context,
                "lineage": _context_lineage(con, row["context_ref"], workspace)}
    except sqlite3.DatabaseError as e:
        con.rollback()
        return {"error": f"handoff accept failed: {e}"}
    finally:
        con.close()


def handoff_cancel(args):
    """Cancel one open handoff; only its owner can consume this transition."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("handoff_ref", args.get("ref")) or "").strip()
    if not ref:
        return {"error": "handoff_ref is required"}
    actor, err = _lifecycle_field(
        args.get("actor", args.get("cancelled_by")), "actor",
        _LIFECYCLE_MAX_FIELD_CHARS, required=True, redact=False)
    if err:
        return err
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        con.execute("BEGIN IMMEDIATE")
        _expire_handoffs(con, workspace)
        row = _handoff_row(con, workspace, ref)
        if not row:
            con.rollback()
            return {"error": "handoff not found or not in your workspace", "handoff_ref": ref}
        if row["state"] != "open":
            con.rollback()
            return {"error": "handoff is not open", "state": row["state"],
                    "handoff": _handoff_metadata(row)}
        if row["owner"] != actor:
            con.rollback()
            return {"error": "only the handoff owner may cancel it"}
        cancelled_at = now()
        con.execute(
            "UPDATE handoffs SET state='cancelled', cancelled_at=?, cancelled_by=? WHERE id=?",
            [cancelled_at, actor, row["id"]])
        con.commit()
        row = _handoff_row(con, workspace, ref)
        return {"cancelled": True, "handoff": _handoff_metadata(row)}
    except sqlite3.DatabaseError as e:
        con.rollback()
        return {"error": f"handoff cancel failed: {e}"}
    finally:
        con.close()


def put_context(args):
    """Store an immutable, named context artifact and optional parent refs."""
    workspace, err = _context_scope(args)
    if err:
        return err
    name = (args.get("name") or "").strip()
    content = args.get("content")
    if not name:
        return {"error": "name is required"}
    if len(name) > 128:
        return {"error": "name must be at most 128 characters"}
    if not isinstance(content, str) or not content:
        return {"error": "content is required and must be a non-empty string"}
    size_bytes = len(content.encode("utf-8"))
    if size_bytes > _CONTEXT_MAX_BYTES:
        return {"error": "content exceeds the context storage limit"}
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    supplied_checksum = (args.get("checksum") or "").strip().lower()
    if supplied_checksum and supplied_checksum != checksum:
        return {"error": "checksum does not match content", "sha256": checksum}
    parent_refs = args.get("parent_refs", [])
    if parent_refs is None:
        parent_refs = []
    if not isinstance(parent_refs, list) or any(not isinstance(ref, str) or not ref.strip()
                                                for ref in parent_refs):
        return {"error": "parent_refs must be an array of non-empty refs"}
    parent_refs = list(dict.fromkeys(ref.strip() for ref in parent_refs))
    if len(parent_refs) > 64:
        return {"error": "parent_refs may contain at most 64 refs"}
    ttl = args.get("ttl_seconds")
    expires_at = ""
    if ttl is not None:
        if isinstance(ttl, bool):
            return {"error": "ttl_seconds must be a non-negative integer"}
        try:
            ttl = int(ttl)
        except (TypeError, ValueError):
            return {"error": "ttl_seconds must be a non-negative integer"}
        if ttl < 0:
            return {"error": "ttl_seconds must be a non-negative integer"}
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).strftime(
            "%Y-%m-%dT%H:%M:%SZ")
    created_at = now()
    ref_seed = (name + "\0" + created_at + "\0" + os.urandom(16).hex()).encode("utf-8")
    ref = "ctx_" + hashlib.sha256(ref_seed).hexdigest()
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        for parent_ref in parent_refs:
            parent = con.execute(
                "SELECT ref FROM contexts WHERE ref=?" + _context_ws_check("contexts"),
                [parent_ref, workspace]).fetchone()
            if not parent:
                return {"error": "parent context not found or not in your workspace",
                        "parent_ref": parent_ref}
        con.execute(
            "INSERT INTO contexts (ref, name, content, schema_json, source, sha256, "
            "workspace_id, created_at, expires_at, size_bytes) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (ref, name, content, _context_json(args.get("schema")),
             (args.get("source") or "").strip(), checksum, workspace, created_at,
             expires_at, size_bytes))
        for parent_ref in parent_refs:
            con.execute(
                "INSERT INTO context_lineage (parent_ref, child_ref, relation, "
                "workspace_id, created_at) VALUES (?,?,?,?,?)",
                (parent_ref, ref, "derived", workspace, created_at))
        con.commit()
        row = con.execute(
            "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
            "created_at, expires_at, size_bytes FROM contexts WHERE ref=?", [ref]).fetchone()
        return {"context": _context_metadata(row),
                "lineage": _context_lineage(con, ref, workspace)}
    except sqlite3.DatabaseError as e:
        con.rollback()
        return {"error": f"context write failed: {e}"}
    finally:
        con.close()


def list_context(args):
    """List context metadata only; content is never returned by the catalog."""
    workspace, err = _context_scope(args)
    if err:
        return err
    limit, err = _context_limit(args.get("limit"), "limit", 50, 100)
    if err:
        return err
    name = (args.get("name") or "").strip()
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        query = (
            "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
            "created_at, expires_at, size_bytes FROM contexts "
            "WHERE (expires_at='' OR expires_at>?)")
        params = [now()]
        if name:
            query += " AND name=?"
            params.append(name)
        query += _context_ws_check("contexts") + " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.extend([workspace, limit])
        rows = con.execute(query, params).fetchall()
        return {"count": len(rows), "contexts": [_context_metadata(row) for row in rows]}
    finally:
        con.close()


def resolve_context(args):
    """Resolve one ref to catalog metadata and bounded lineage, never content."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("ref") or "").strip()
    if not ref:
        return {"error": "ref is required"}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        row, row_err = _context_row(con, ref, workspace)
        if row_err:
            return row_err
        if not row:
            return {"error": "context not found or not in your workspace", "ref": ref}
        return {"context": _context_metadata(row),
                "lineage": _context_lineage(con, ref, workspace)}
    finally:
        con.close()


def read_context(args):
    """Read one bounded character slice from a context ref."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("ref") or "").strip()
    if not ref:
        return {"error": "ref is required"}
    start = args.get("start", 0)
    if isinstance(start, bool):
        return {"error": "start must be a non-negative integer"}
    try:
        start = int(start)
    except (TypeError, ValueError):
        return {"error": "start must be a non-negative integer"}
    if start < 0:
        return {"error": "start must be a non-negative integer"}
    max_chars, err = _context_limit(args.get("max_chars"), "max_chars",
                                     _CONTEXT_DEFAULT_READ_CHARS,
                                     _CONTEXT_MAX_READ_CHARS)
    if err:
        return err
    end = args.get("end")
    if end is not None:
        if isinstance(end, bool):
            return {"error": "end must be a non-negative integer"}
        try:
            end = int(end)
        except (TypeError, ValueError):
            return {"error": "end must be a non-negative integer"}
        if end < 0 or end < start:
            return {"error": "end must be greater than or equal to start"}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        row, row_err = _context_row(con, ref, workspace)
        if row_err:
            return row_err
        if not row:
            return {"error": "context not found or not in your workspace", "ref": ref}
        total_chars = len(row["content"])
        bounded_start = min(start, total_chars)
        requested_end = total_chars if end is None else min(end, total_chars)
        slice_end = min(requested_end, bounded_start + max_chars)
        content = row["content"][bounded_start:slice_end]
        context = _context_metadata(row)
        context.update({
            "content": content,
            "start": bounded_start,
            "end": slice_end,
            "total_chars": total_chars,
            "truncated": slice_end < total_chars,
            "next_start": slice_end if slice_end < total_chars else None,
        })
        return {"context": context,
                "lineage": _context_lineage(con, ref, workspace)}
    finally:
        con.close()


def search_context(args):
    """Search context names, metadata, and payloads without returning payloads."""
    workspace, err = _context_scope(args)
    if err:
        return err
    query = args.get("query")
    if not isinstance(query, str) or not query.strip():
        return {"error": "query is required and must be a non-empty string"}
    query = query.strip()
    if len(query) > _CONTEXT_MAX_SEARCH_QUERY:
        return {"error": f"query must be at most {_CONTEXT_MAX_SEARCH_QUERY} characters"}
    limit, err = _context_limit(args.get("limit"), "limit", 20, 100)
    if err:
        return err
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        rows = con.execute(
            "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
            "created_at, expires_at, size_bytes FROM contexts "
            "WHERE (expires_at='' OR expires_at>?) "
            "AND instr(lower(name || char(10) || source || char(10) || "
            "schema_json || char(10) || content), lower(?)) > 0" +
            _context_ws_check("contexts") +
            " ORDER BY created_at DESC, id DESC LIMIT ?",
            [now(), query, workspace, limit]).fetchall()
        return {"query": query, "count": len(rows),
                "contexts": [_context_metadata(row) for row in rows]}
    finally:
        con.close()


def chunk_context(args):
    """Return a bounded sequence of chunks from one context ref."""
    workspace, err = _context_scope(args)
    if err:
        return err
    ref = (args.get("ref") or "").strip()
    if not ref:
        return {"error": "ref is required"}
    chunk_chars, err = _context_limit(
        args.get("chunk_chars"), "chunk_chars", _CONTEXT_DEFAULT_READ_CHARS,
        _CONTEXT_MAX_READ_CHARS)
    if err:
        return err
    start_chunk = args.get("start_chunk", 0)
    if isinstance(start_chunk, bool):
        return {"error": "start_chunk must be a non-negative integer"}
    try:
        start_chunk = int(start_chunk)
    except (TypeError, ValueError):
        return {"error": "start_chunk must be a non-negative integer"}
    if start_chunk < 0:
        return {"error": "start_chunk must be a non-negative integer"}
    max_chunks, err = _context_limit(
        args.get("max_chunks"), "max_chunks", 8, _CONTEXT_MAX_CHUNKS)
    if err:
        return err
    # Keep the aggregate response bounded even when callers request the
    # largest legal chunk and chunk count.
    chunk_chars = min(chunk_chars, _CONTEXT_MAX_CHUNK_RESPONSE_CHARS)
    max_response_chunks = max(1, _CONTEXT_MAX_CHUNK_RESPONSE_CHARS // chunk_chars)
    max_chunks = min(max_chunks, max_response_chunks)
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        row, row_err = _context_row(con, ref, workspace)
        if row_err:
            return row_err
        if not row:
            return {"error": "context not found or not in your workspace", "ref": ref}
        total_chars = len(row["content"])
        total_chunks = (total_chars + chunk_chars - 1) // chunk_chars
        bounded_start = min(start_chunk, total_chunks)
        end_chunk = min(total_chunks, bounded_start + max_chunks)
        chunks = []
        for index in range(bounded_start, end_chunk):
            start = index * chunk_chars
            end = min(total_chars, start + chunk_chars)
            chunks.append({"index": index, "start": start, "end": end,
                           "content": row["content"][start:end]})
        return {
            "context": _context_metadata(row),
            "chunks": chunks,
            "start_chunk": bounded_start,
            "next_chunk": end_chunk if end_chunk < total_chunks else None,
            "total_chunks": total_chunks,
            "chunk_chars": chunk_chars,
        }
    finally:
        con.close()


def reduce_context(args):
    """Create a bounded immutable context by deterministically joining refs."""
    workspace, err = _context_scope(args)
    if err:
        return err
    name = (args.get("name") or "").strip()
    if not name:
        return {"error": "name is required"}
    if len(name) > 128:
        return {"error": "name must be at most 128 characters"}
    refs = args.get("refs")
    if not isinstance(refs, list) or not refs:
        return {"error": "refs must be a non-empty array"}
    if any(not isinstance(ref, str) or not ref.strip() for ref in refs):
        return {"error": "refs must be an array of non-empty refs"}
    refs = list(dict.fromkeys(ref.strip() for ref in refs))
    if len(refs) > _CONTEXT_MAX_REDUCE_REFS:
        return {"error": f"refs may contain at most {_CONTEXT_MAX_REDUCE_REFS} refs"}
    separator = args.get("separator", "\n\n")
    if not isinstance(separator, str):
        return {"error": "separator must be a string"}
    if len(separator) > 1024:
        return {"error": "separator must be at most 1024 characters"}
    separator_bytes = len(separator.encode("utf-8"))
    contents = []
    total_bytes = 0
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        for ref in refs:
            row, row_err = _context_row(con, ref, workspace)
            if row_err:
                return row_err
            if not row:
                return {"error": "context not found or not in your workspace", "ref": ref}
            total_bytes += len(row["content"].encode("utf-8"))
            if contents:
                total_bytes += separator_bytes
            if total_bytes > _CONTEXT_MAX_BYTES:
                return {"error": "reduced content exceeds the context storage limit",
                        "size_bytes": total_bytes}
            contents.append(row["content"])
    finally:
        con.close()
    result = put_context({
        "name": name,
        "content": separator.join(contents),
        "workspace": workspace,
        "schema": args.get("schema"),
        "source": args.get("source"),
        "checksum": args.get("checksum"),
        "ttl_seconds": args.get("ttl_seconds"),
        "parent_refs": refs,
    })
    if "error" in result:
        return result
    result["reduced_from"] = refs
    result["reduction"] = "deterministic-concat"
    return result


def _split_fact_text(text, chunk_chars, overlap=0):
    """Split fact text into deterministic, offset-addressable chunks."""
    if not text:
        return []
    step = chunk_chars - overlap
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_chars)
        chunks.append({"index": len(chunks), "start": start, "end": end,
                       "content": text[start:end]})
        if end >= len(text):
            break
        start += step
    return chunks


def _fact_chunk_params(args):
    """Validate bounded fact chunk pagination and return its parameters."""
    chunk_chars, err = _context_limit(
        args.get("chunk_chars"), "chunk_chars", _FACT_DEFAULT_CHUNK_CHARS,
        _FACT_MAX_CHUNK_CHARS)
    if err:
        return None, err
    start_chunk = args.get("start_chunk", 0)
    if isinstance(start_chunk, bool):
        return None, {"error": "start_chunk must be a non-negative integer"}
    try:
        start_chunk = int(start_chunk)
    except (TypeError, ValueError):
        return None, {"error": "start_chunk must be a non-negative integer"}
    if start_chunk < 0:
        return None, {"error": "start_chunk must be a non-negative integer"}
    max_chunks, err = _context_limit(
        args.get("max_chunks"), "max_chunks", 8, _FACT_MAX_CHUNKS)
    if err:
        return None, err
    overlap = args.get("chunk_overlap", 0)
    if isinstance(overlap, bool):
        return None, {"error": "chunk_overlap must be a non-negative integer"}
    try:
        overlap = int(overlap)
    except (TypeError, ValueError):
        return None, {"error": "chunk_overlap must be a non-negative integer"}
    if overlap < 0 or overlap >= chunk_chars:
        return None, {"error": "chunk_overlap must be less than chunk_chars"}
    # Keep the aggregate response bounded even at the largest legal page.
    max_response_chunks = max(1, _FACT_MAX_CHUNK_RESPONSE_CHARS // chunk_chars)
    return (chunk_chars, overlap, start_chunk, min(max_chunks, max_response_chunks)), None


def _add_fact_chunks(rows, chunk_chars, overlap):
    """Add a bounded chunk page to search rows without changing their rank."""
    budget = _FACT_MAX_CHUNK_RESPONSE_CHARS
    for row in rows:
        all_chunks = _split_fact_text(row.get("text") or "", chunk_chars, overlap)
        row["total_chunks"] = len(all_chunks)
        row["chunks"] = []
        for chunk in all_chunks:
            chunk_size = len(chunk["content"].encode("utf-8"))
            if chunk_size > budget:
                row["chunks_truncated"] = True
                break
            row["chunks"].append(chunk)
            budget -= chunk_size
        if len(row["chunks"]) < len(all_chunks):
            row["chunks_truncated"] = True
    return rows


def chunk_fact(args):
    """Read a bounded, paginated chunk sequence from one active fact."""
    if args.get("id") is None and args.get("fact_id") is None and not args.get("sha256"):
        return {"error": "id, fact_id, or sha256 is required"}
    params, err = _fact_chunk_params(args)
    if err:
        return err
    chunk_chars, overlap, start_chunk, max_chunks = params
    ws = _workspace(args)
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, ws)
        if inactive:
            return inactive
        if args.get("id") is not None or args.get("fact_id") is not None:
            fid = args.get("id") if args.get("id") is not None else args.get("fact_id")
            if isinstance(fid, bool):
                return {"error": "id must be an integer"}
            try:
                fid = int(fid)
            except (TypeError, ValueError):
                return {"error": "id must be an integer"}
            row = con.execute(
                "SELECT id, sha256, text, source, project, domain, trust, strong, "
                "importance, confirmed, invalid_at, created_at, updated_at "
                "FROM facts WHERE id=? AND archived=0 AND invalid_at='' "
                "AND lifecycle='active'" + _ws_check("facts", ws),
                [fid] + ([ws] if ws else [])).fetchone()
        else:
            row = con.execute(
                "SELECT id, sha256, text, source, project, domain, trust, strong, "
                "importance, confirmed, invalid_at, created_at, updated_at "
                "FROM facts WHERE sha256=? AND archived=0 AND invalid_at='' "
                "AND lifecycle='active'" + _ws_check("facts", ws),
                [args["sha256"]] + ([ws] if ws else [])).fetchone()
        if not row:
            return {"error": "fact not found or not in your workspace"}
        all_chunks = _split_fact_text(row["text"], chunk_chars, overlap)
        total_chunks = len(all_chunks)
        bounded_start = min(start_chunk, total_chunks)
        end_chunk = min(total_chunks, bounded_start + max_chunks)
        chunks = all_chunks[bounded_start:end_chunk]
        fact = dict(row)
        fact.pop("text", None)
        fact["text_length"] = len(row["text"])
        return {
            "fact": fact,
            "chunks": chunks,
            "start_chunk": bounded_start,
            "next_chunk": end_chunk if end_chunk < total_chunks else None,
            "total_chunks": total_chunks,
            "chunk_chars": chunk_chars,
            "chunk_overlap": overlap,
        }
    finally:
        con.close()


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


def _active_db_label():
    """Display label of the active store (extension stripped when .db)."""
    n = _active_db_name()
    return n[:-3] if n.endswith(".db") else n


def _db_path():
    """Path of the session-selected database, or the active store."""
    if _SELECTED_DB[0]:
        return _db_file(_SELECTED_DB[0])
    return DB_PATH


def _is_active_name(name):
    """True when `name` refers to the active store (with or without .db)."""
    return name + ".db" == _active_db_name() or name == _active_db_label()


# ---- v0.7 decay support: activity days, search-hit bookkeeping ------------

_LAST_ACTIVITY_DAY = [None]  # (day, db_path) of the last stamp, per database


def _register_activity_day():
    """Record "the system was online and memory was used" for today.
    Best-effort, never raises; one row per day (INSERT OR IGNORE)."""
    day = now()[:10]
    path = _db_path()
    if _LAST_ACTIVITY_DAY[0] == (day, path):
        return
    try:
        con = sqlite3.connect(path, timeout=10)
        try:
            con.execute("CREATE TABLE IF NOT EXISTS activity_days (day TEXT PRIMARY KEY)")
            con.execute("INSERT OR IGNORE INTO activity_days (day) VALUES (?)", (day,))
            con.commit()
        finally:
            con.close()
        _LAST_ACTIVITY_DAY[0] = (day, path)
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


def _advisory_only_error(args, operation):
    """Reject explicit attempts to use memory as an authorization source."""
    purpose = str(args.get("purpose") or "advisory").strip().lower()
    if purpose not in ("advisory", "safety_critical"):
        return {"error": "purpose must be advisory or safety_critical",
                "code": "invalid_memory_purpose", "fail_closed": True,
                "operation": operation}
    if purpose == "safety_critical" or args.get("safety_critical") is True:
        return {
            "error": "memory-mcp is advisory-only and cannot authorize safety-critical operations",
            "code": "advisory_only",
            "fail_closed": True,
            "operation": operation,
            "memory_policy": "advisory_only",
            "safety_critical_allowed": False,
            "source_of_truth": [
                "live_multica_state",
                "current_registry_reads",
                "lock_and_hash_checks",
            ],
        }
    return None


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
    policy_error = _advisory_only_error(args, "compose_recall")
    if policy_error:
        return policy_error
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


# v0.10 rule-based categorization: ordered (regex, category) pairs evaluated
# on the lowercased fact text. First match wins; no match leaves the fact
# uncategorized (NULL) until categorize_pending (LLM batch) refines it.
# Explicit `category` / legacy `domain` args override rules entirely.
_CATEGORY_RULES = [
    (r"memory-mcp|memory_mcp|facts\.db|summarize_index|search_facts|compose_recall|recall\b", "memory-mcp"),
    (r"правило|директива|обязательн", "rules"),
    (r"skill|скил", "skills"),
    (r"card|карточк|issue|ntl-\d", "issues"),
    (r"docker|compose|контейнер|container|dockerfile|образ", "docker"),
    (r"reasonix|jcode|codex|claude|multica|daemon|runtime", "runtimes"),
    (r"git|gitea|commit|push|репозитор", "git"),
    (r"vpn|tardis|proxy|socks|privoxy|tunnel", "network"),
    (r"ollama|llm\b|model|модел|embed|token", "llm"),
    (r"test|unittest|qa\b|тест", "testing"),
    (r"sqlite|fts5?|database|баз", "database"),
]


def _categorize_by_rules(text):
    """First matching rule on the lowercased text, or '' (uncategorized)."""
    low = text.lower()
    for pattern, cat in _CATEGORY_RULES:
        if re.search(pattern, low):
            return cat
    return ""


def _resolve_category(con, name, workspace):
    """Idempotent get-or-create of a workspace-scoped category; returns id.
    Category names are capped at 64 chars (they are interpolated into the LLM
    prompt by categorize_pending — treat as untrusted data)."""
    name = (name or "").strip()[:64]
    if not name:
        return None
    ts = now()
    row = con.execute("SELECT id FROM categories WHERE name=? AND workspace_id=?",
                      [name, workspace]).fetchone()
    if row:
        return row["id"]
    con.execute("INSERT OR IGNORE INTO categories (name, workspace_id, created_at, updated_at) "
                "VALUES (?,?,?,?)", [name, workspace, ts, ts])
    return con.execute("SELECT id FROM categories WHERE name=? AND workspace_id=?",
                       [name, workspace]).fetchone()["id"]


def _categorize_fact(con, args, text, workspace):
    """Category for a new fact: explicit `category` arg > legacy `domain` arg
    > keyword rules; '' (uncategorized) when nothing matches — refined later
    by categorize_pending."""
    cat = (args.get("category") or "").strip()
    if not cat:
        cat = (args.get("domain") or "").strip()
    if not cat:
        cat = _categorize_by_rules(text)
    if not cat:
        return None
    return _resolve_category(con, cat, workspace)


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
        # v0.10: resolve the topic category once, before the dedup branch, so
        # re-remembering with an explicit category also refreshes it.
        cat_id = _categorize_fact(con, args, text, workspace)
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
            # Refresh the category only when the caller asked for one
            # explicitly — rules must not overwrite a stored explicit choice.
            if cat_id is not None and (
                    (args.get("category") or "").strip() or (args.get("domain") or "").strip()):
                sets.append("category_id=?")
                params.append(cat_id)
            con.execute("UPDATE facts SET %s WHERE id=?" % ", ".join(sets),
                        params + [row["id"]])
            con.commit()
            out = {"id": row["id"], "sha256": sha, "dedup": True,
                   "created_at": row["created_at"], "updated_at": ts}
            if warning:
                out["warning"] = warning
            return out
        cur = con.execute(
            "INSERT INTO facts (sha256, text, source, project, domain, trust, strong, importance, workspace_id, created_at, updated_at, category_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (sha, text, args.get("source", ""), args.get("project", ""),
             args.get("domain", ""), trust, 1 if args.get("strong") else 0,
             importance, workspace, ts, ts, cat_id))
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


def _absorb_candidates(con, text, workspace):
    """Return bounded lexical candidates for absorb classification."""
    terms = fts_terms(text)
    if not terms:
        return []
    query = " OR ".join(terms)
    sql = ("SELECT f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong, "
           "f.importance, f.confirmed, f.invalid_at, f.archived, "
           "bm25(facts_fts) AS rank FROM facts_fts "
           "JOIN facts f ON f.id=facts_fts.rowid "
           "WHERE facts_fts MATCH ? AND f.lifecycle != 'forgotten'" +
           _ws_filter("f", workspace) + " ORDER BY rank LIMIT 10")
    params = [query] + ([workspace] if workspace else [])
    try:
        rows = [dict(row) for row in con.execute(sql, params)]
    except sqlite3.OperationalError:
        return []
    for row in rows:
        row["coverage"] = round(
            sum(1 for term in terms if term in (row["text"] or "").lower()) /
            len(terms), 2)
    return [row for row in rows if row["coverage"] >= 0.6][:5]


def _absorb_evidence_items(item, fallback_source=""):
    """Normalize one candidate's evidence list without storing raw snippets."""
    raw = item.get("evidence", [])
    if raw is None:
        raw = []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list) or len(raw) > 8:
        return None, {"error": "evidence must be an object or an array of at most 8 objects"}
    evidence = []
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict):
            return None, {"error": "evidence[%d] must be an object" % index}
        normalized = dict(entry)
        source_ref = normalized.get("source_ref") or ""
        if not isinstance(source_ref, str):
            return None, {"error": "evidence[%d].source_ref must be a string" % index}
        source_ref = source_ref.strip()
        if not source_ref:
            parts = []
            values = {}
            for name in ("repo", "ref", "path"):
                value = normalized.get(name) or ""
                if not isinstance(value, str):
                    return None, {"error": "evidence[%d].%s must be a string" %
                                   (index, name)}
                values[name] = value.strip()
            repo, ref, path = values["repo"], values["ref"], values["path"]
            if repo:
                parts.append(repo + (("@" + ref) if ref else ""))
            if path:
                parts.append(":" + path)
            source_ref = "".join(parts)
        if not source_ref:
            return None, {"error": "evidence[%d] requires source_ref or repo/path" % index}
        if len(source_ref) > _EVIDENCE_MAX_FIELD_CHARS:
            return None, {"error": "evidence[%d].source_ref is too long" % index}
        normalized["source_ref"] = source_ref
        _, anchor_err = _evidence_anchor_fields(normalized)
        if anchor_err:
            return None, {"error": "evidence[%d]: %s" %
                          (index, anchor_err["error"])}
        evidence.append(normalized)
    if fallback_source and not any(e["source_ref"] == fallback_source for e in evidence):
        evidence.insert(0, {"source_ref": fallback_source})
    return evidence, None


def absorb(args):
    """Preview or explicitly commit a batch of candidate facts.

    Classification is deliberately conservative: exact duplicates are no-ops,
    lexical near-duplicates are review items, and only candidates classified as
    new are committed. Optional LLM verification can turn a related candidate
    into a new/update/contradiction classification, but update and contradiction
    still require a separate explicit operation.
    """
    raw_facts = args.get("facts")
    if raw_facts is None and args.get("text") is not None:
        raw_facts = [args.get("text")]
    if not isinstance(raw_facts, list) or not raw_facts:
        return {"error": "facts must be a non-empty array"}
    if len(raw_facts) > _ABSORB_MAX_FACTS:
        return {"error": "facts may contain at most %d items" % _ABSORB_MAX_FACTS}
    dry_run_provided = "dry_run" in args
    dry_run = args.get("dry_run", True)
    commit = args.get("commit", False)
    if not isinstance(dry_run, bool) or not isinstance(commit, bool):
        return {"error": "dry_run and commit must be booleans"}
    if dry_run_provided and dry_run and commit:
        return {"error": "dry_run and commit cannot both be true"}
    if commit:
        dry_run = False
    elif not dry_run:
        commit = True
    verify_requested = args.get("verify", False)
    if not isinstance(verify_requested, bool):
        return {"error": "verify must be a boolean"}
    if verify_requested and os.environ.get("MEMORY_MCP_VERIFY") != "1":
        return {"error": "verification is disabled (set MEMORY_MCP_VERIFY=1)"}

    workspace = _workspace(args)
    defaults = {key: args[key] for key in (
        "source", "project", "domain", "category", "trust", "strong", "importance")
        if key in args}
    normalized = []
    for index, raw in enumerate(raw_facts):
        if isinstance(raw, str):
            item = {"text": raw}
        elif isinstance(raw, dict):
            item = dict(raw)
        else:
            return {"error": "facts[%d] must be a string or object" % index}
        item_workspace = item.get("workspace") or ""
        if not isinstance(item_workspace, str):
            return {"error": "facts[%d].workspace must be a string" % index}
        item_workspace = item_workspace.strip()
        if item_workspace and item_workspace != workspace:
            return {"error": "facts[%d] workspace must match the batch workspace" % index}
        text = item.get("text") or ""
        if not isinstance(text, str):
            return {"error": "facts[%d].text must be a string" % index}
        text = text.strip()
        if not text:
            return {"error": "facts[%d].text is required" % index}
        if len(text) > _ABSORB_MAX_TEXT_CHARS:
            return {"error": "facts[%d].text is too long (max %d characters)" %
                    (index, _ABSORB_MAX_TEXT_CHARS)}
        fact_args = dict(defaults)
        fact_args.update({key: item[key] for key in (
            "source", "project", "domain", "category", "trust", "strong", "importance")
            if key in item})
        fact_args["text"] = text
        for name in ("source", "project", "domain", "category"):
            if name in fact_args and fact_args[name] is not None and not isinstance(fact_args[name], str):
                return {"error": "facts[%d].%s must be a string" % (index, name)}
        if workspace:
            fact_args["workspace"] = workspace
        trust = fact_args.get("trust", "medium")
        if trust not in VALID_TRUST:
            return {"error": "facts[%d].trust must be one of %s" % (index, VALID_TRUST)}
        fallback_source = fact_args.get("source") or ""
        if not isinstance(fallback_source, str):
            return {"error": "facts[%d].source must be a string" % index}
        evidence, evidence_err = _absorb_evidence_items(item, fallback_source=fallback_source.strip())
        if evidence_err:
            return {"error": "facts[%d]: %s" % (index, evidence_err["error"])}
        normalized.append({"args": fact_args, "text": text, "evidence": evidence})

    con = get_db()
    planned = []
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        for index, item in enumerate(normalized):
            text = item["text"]
            sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
            exact = con.execute(
                "SELECT id, archived, invalid_at, lifecycle FROM facts WHERE sha256=?" +
                _ws_check("facts", workspace),
                [sha] + ([workspace] if workspace else [])).fetchone()
            candidates = [] if exact else _absorb_candidates(con, text, workspace)
            classification = "duplicate" if exact else ("related" if candidates else "new")
            action = "noop" if exact else ("review" if candidates else "create")
            verdict = None
            verification_error = ""
            if verify_requested and candidates and not exact:
                try:
                    from verify import verify_facts
                    check_args = {"text": text}
                    if workspace:
                        check_args["workspace"] = workspace
                    check = verify_facts(check_args)
                    if "error" in check:
                        verification_error = check["error"]
                    else:
                        verdict = check.get("verdict") or {}
                        verdict_action = verdict.get("action")
                        reason = (verdict.get("reason") or "").lower()
                        if verdict_action == "add":
                            classification, action = "new", "create"
                        elif verdict_action in ("update", "supersedes"):
                            classification, action = "update", "review"
                        elif verdict_action == "delete" or (
                                verdict_action == "noop" and reason == "conflict"):
                            classification, action = "contradiction", "review"
                except Exception:
                    verification_error = "verification failed (provider error)"
            out = {
                "index": index,
                "sha256": sha,
                "text_preview": text[:300],
                "classification": classification,
                "action": action,
                "candidate_ids": [row["id"] for row in candidates],
                "evidence_count": len(item["evidence"]),
            }
            if exact:
                out["existing_id"] = exact["id"]
            if candidates:
                out["candidates"] = [
                    {"id": row["id"], "text": row["text"][:300],
                     "coverage": row["coverage"], "source": row["source"]}
                    for row in candidates]
            if verdict is not None:
                out["verdict"] = verdict
            if verification_error:
                out["verification_error"] = verification_error
            planned.append({"out": out, "item": item, "existing_id":
                            exact["id"] if exact else None})
    finally:
        con.close()

    result = {
        "dry_run": not commit,
        "committed": False,
        "count": len(planned),
        "created": 0,
        "deduped": 0,
        "pending_review": sum(1 for entry in planned
                               if entry["out"]["action"] == "review"),
        "evidence_attached": 0,
        "items": [entry["out"] for entry in planned],
    }
    if not commit:
        return result

    evidence_errors = []
    for entry in planned:
        out = entry["out"]
        item = entry["item"]
        fid = entry["existing_id"]
        if out["classification"] == "new":
            stored = remember_fact(item["args"])
            if "error" in stored:
                out["error"] = stored["error"]
                continue
            fid = stored["id"]
            out["id"] = fid
            if stored.get("dedup"):
                result["deduped"] += 1
            else:
                result["created"] += 1
        elif out["classification"] == "duplicate":
            result["deduped"] += 1
        else:
            continue
        for evidence in item["evidence"]:
            attach_args = dict(evidence)
            attach_args["fact_id"] = fid
            if workspace:
                attach_args["workspace"] = workspace
            attached = attach_evidence(attach_args)
            if "error" in attached:
                evidence_errors.append({"index": out["index"],
                                        "error": attached["error"]})
            elif not attached.get("dedup"):
                result["evidence_attached"] += 1
    result["committed"] = True
    if evidence_errors:
        result["evidence_errors"] = evidence_errors
    return result


def search_facts(args):
    policy_error = _advisory_only_error(args, "search_facts")
    if policy_error:
        return policy_error
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit, err = _bounded_int_arg(args, "limit", 20, 1, 100)
    if err:
        return err
    chunk_params = None
    if "chunk_chars" in args or "chunk_overlap" in args:
        chunk_params, err = _fact_chunk_params(args)
        if err:
            return err
        # Search pagination is controlled by the result limit; only the
        # chunk-size and overlap portion of the fact paging contract applies.
        chunk_params = chunk_params[:2]
    sql = ("SELECT f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong, "
           "f.importance, f.confirmed, f.invalid_at, f.created_at, "
           "c.name AS category, bm25(facts_fts) AS rank "
           "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
           "LEFT JOIN categories c ON c.id = f.category_id "
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
        if args["trust_min"] not in VALID_TRUST:
            return {"error": "trust_min must be one of %s" % (VALID_TRUST,)}
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
    if args.get("category"):
        sql += " AND c.name=?"
        params.append(args["category"])
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
        if chunk_params:
            rows = _add_fact_chunks(rows, *chunk_params)
        _mark_hits(con, rows)
        result = {"count": len(rows), "facts": rows,
                  "memory_policy": "advisory_only",
                  "safety_critical_allowed": False}
        if args.get("graph"):
            result["graph"] = len(graph)
        return result
    except sqlite3.OperationalError as e:
        return {"error": f"query failed: {e}", "facts": []}
    finally:
        con.close()


def search_semantic(args):
    """Semantic (embedding) search — enabled only with MEMORY_MCP_EMBEDDINGS=1."""
    policy_error = _advisory_only_error(args, "search_semantic")
    if policy_error:
        return policy_error
    emb = _emb()
    if emb is None:
        return {"error": "semantic search is disabled (set MEMORY_MCP_EMBEDDINGS=1)"}
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit, err = _bounded_int_arg(args, "limit", 20, 1, 100)
    if err:
        return err
    try:
        threshold = float(args.get("threshold", 0.0))
    except (TypeError, ValueError):
        return {"error": "threshold must be a number"}
    ws = _workspace(args)
    con = get_db()
    try:
        res = emb.search_semantic(con, query, limit=limit, threshold=threshold, workspace=ws)
        rows = res.get("facts", []) if isinstance(res, dict) else res or []
        _revive_degraded(con, query, ws)
        _mark_hits(con, rows)
        if isinstance(res, dict):
            res["memory_policy"] = "advisory_only"
            res["safety_critical_allowed"] = False
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
    limit, err = _bounded_int_arg(args, "limit", 50, 1, 500)
    if err:
        return err
    sql = ("SELECT f.id, f.text, f.source, f.project, f.domain, f.trust, f.strong, f.importance, f.confirmed, "
           "f.created_at, f.updated_at, c.name AS category "
           "FROM facts f LEFT JOIN categories c ON c.id = f.category_id "
           "WHERE f.archived=0 AND f.invalid_at='' AND f.lifecycle='active'")
    ws = _workspace(args)
    params = []
    sql += _ws_filter("f", ws)
    if ws:
        params.append(ws)
    if args.get("project"):
        sql += " AND f.project=?"
        params.append(args["project"])
    if args.get("domain"):
        sql += " AND f.domain=?"
        params.append(args["domain"])
    if args.get("category"):
        sql += " AND c.name=?"
        params.append(args["category"])
    sql += " ORDER BY f.updated_at DESC LIMIT ?"
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
    limit, err = _bounded_int_arg(args, "limit", 200, 1, 500)
    if err:
        return err
    max_chars, err = _bounded_int_arg(args, "max_chars", 4000, 200, 1_000_000)
    if err:
        return err
    sql = ("SELECT f.id, f.text, f.project, f.domain, f.trust, f.strong, f.updated_at, "
           "c.name AS category "
           "FROM facts f LEFT JOIN categories c ON c.id = f.category_id "
           "WHERE f.archived=0 AND f.invalid_at='' AND f.lifecycle='active'")
    ws = _workspace(args)
    params = []
    sql += _ws_filter("f", ws)
    if ws:
        params.append(ws)
    if args.get("project"):
        sql += " AND project=?"
        params.append(args["project"])
    if args.get("domain"):
        sql += " AND domain=?"
        params.append(args["domain"])
    if args.get("trust_min"):
        if args["trust_min"] not in VALID_TRUST:
            return {"error": "trust_min must be one of %s" % (VALID_TRUST,)}
        order = {"high": 0, "medium": 1, "low": 2}
        allowed = [t for t in VALID_TRUST if order[t] <= order[args["trust_min"]]]
        sql += f" AND trust IN ({','.join('?' * len(allowed))})"
        params += allowed
    if args.get("strong_only"):
        sql += " AND strong=1"
    if args.get("category"):
        sql += " AND c.name=?"
        params.append(args["category"])
    sql += " ORDER BY f.updated_at DESC, f.id DESC LIMIT ?"
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
        cat = f" [{r['category']}]" if r["category"] else ""
        lines.append(f"#{r['id']} {tag}{cat}{dom} {text}")
    joined = "\n".join(lines)
    truncated = len(joined) > max_chars
    if truncated:
        cut = max_chars
        while cut > 0 and joined[cut] != "\n":
            cut -= 1
        joined = joined[:cut]
    return {"count": len(lines), "total": total, "chars": len(joined),
            "truncated": truncated, "index": joined}


def _snippet(text, limit=120):
    """Short reference: whitespace-collapsed text, trimmed at a word boundary."""
    t = " ".join(text.split())
    if len(t) <= limit:
        return t
    cut = t[:limit]
    i = cut.rfind(" ")
    return (cut[:i] if i > 60 else cut) + "…"


def list_categories(args):
    """v0.10: the card catalog — topic categories with active/total fact
    counts, most-used first. `query` filters category names."""
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        q = (args.get("query") or "").strip()
        sql = ("SELECT c.id, c.name, c.workspace_id, "
               "(SELECT COUNT(*) FROM facts f WHERE f.category_id=c.id AND f.archived=0 "
               " AND f.invalid_at='' AND f.lifecycle='active'"
               + _ws_filter("f", ws) + ") AS active_facts, "
               "(SELECT COUNT(*) FROM facts f WHERE f.category_id=c.id) AS facts "
               "FROM categories c WHERE 1=1" + _ws_filter("c", ws))
        params = []
        if ws:
            params += [ws, ws]
        if q:
            sql += " AND c.name LIKE ?"
            params.append(f"%{q}%")
        sql += " ORDER BY active_facts DESC, c.name LIMIT 200"
        rows = [dict(r) for r in con.execute(sql, params)]
        return {"count": len(rows), "categories": rows}
    finally:
        con.close()


def search_index(args):
    """v0.10: short reference by search vector — one-line snippets of matching
    facts grouped by category, capped at max_chars. The library shelf lookup:
    list_categories (catalog) -> search_index (shelf) -> get_provenance (book).
    Full texts are NOT returned here."""
    query = (args.get("query") or "").strip()
    if not query:
        return {"error": "query is required"}
    limit, err = _bounded_int_arg(args, "limit", 30, 1, 100)
    if err:
        return err
    max_chars, err = _bounded_int_arg(args, "max_chars", 2000, 200, 1_000_000)
    if err:
        return err
    sql = ("SELECT f.id, f.text, f.source, f.trust, f.strong, f.importance, f.updated_at, "
           "c.name AS category, bm25(facts_fts) AS rank "
           "FROM facts_fts JOIN facts f ON f.id = facts_fts.rowid "
           "LEFT JOIN categories c ON c.id = f.category_id "
           "WHERE facts_fts MATCH ? AND f.archived=0 AND f.lifecycle='active' AND f.invalid_at=''")
    ws = _workspace(args)
    params = [query]
    sql += _ws_filter("f", ws)
    if ws:
        params.append(ws)
    if args.get("category"):
        sql += " AND c.name=?"
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
        emb = _emb()
        if emb is not None and args.get("semantic"):
            if rows:
                res = emb.hybrid_rerank(con, query, rows, limit=limit, workspace=ws)
            else:
                res = emb.search_semantic(con, query, limit=limit, workspace=ws)
            rows = res.get("facts", []) if isinstance(res, dict) else res or []
            if rows:
                # Semantic rerank rows carry only id/text/score — rehydrate the
                # category and display fields from the store (FTS rows already
                # have them; this also protects against key loss in the merge).
                ids = [r["id"] for r in rows]
                ph = ",".join("?" * len(ids))
                meta = {r["id"]: dict(r) for r in con.execute(
                    "SELECT f.id, c.name AS category, f.importance, f.updated_at, "
                    "f.trust, f.strong FROM facts f LEFT JOIN categories c ON c.id=f.category_id "
                    "WHERE f.id IN (%s)" % ph, ids)}
                for r in rows:
                    m2 = meta.get(r["id"], {})
                    r["category"] = m2.get("category")
                    r["importance"] = m2.get("importance", 0.5)
                    r["updated_at"] = m2.get("updated_at", "")
                    r["trust"] = m2.get("trust", "medium")
                    r["strong"] = m2.get("strong", 0)
        # group snippets by category, preserving rank order; respect max_chars
        groups, order, used = {}, [], 0
        shown, truncated = 0, False
        for r in rows:
            cat = r.get("category") or "(uncategorized)"
            cost = len(cat) + 24 + min(len(r["text"]), 120)
            if groups and used + cost > max_chars:
                truncated = True
                break
            used += cost
            if cat not in groups:
                groups[cat] = []
                order.append(cat)
            groups[cat].append({
                "id": r["id"], "category": cat, "snippet": _snippet(r["text"]),
                "trust": r["trust"], "strong": r["strong"], "importance": r["importance"],
                "updated_at": r["updated_at"]})
            shown += 1
        return {"count": len(rows), "shown": shown, "truncated": truncated,
                "groups": [{"category": c, "facts": groups[c]} for c in order]}
    except sqlite3.OperationalError as e:
        # No host paths in client-visible errors (repo rule); detail to stderr.
        print(f"memory-mcp: search_index query failed: {e}", file=sys.stderr)
        return {"error": "search_index query failed", "groups": []}
    finally:
        con.close()


def categorize_pending(args):
    """v0.10: LLM batch refinement (the background half of hybrid
    categorization) — assigns categories to facts with none, reusing existing
    category names when they fit. Enabled with MEMORY_MCP_CATEGORIZE=1;
    provider comes from llm.py (MEMORY_MCP_LLM_*). Rule-based categories from
    remember_fact stay as the instant fallback."""
    if not os.environ.get("MEMORY_MCP_CATEGORIZE"):
        return {"error": "categorize_pending is disabled (set MEMORY_MCP_CATEGORIZE=1)"}
    limit, err = _bounded_int_arg(args, "limit", 20, 1, 100)
    if err:
        return err
    ws = _workspace(args)
    con = get_db()
    try:
        err = _ws_inactive_error(con, ws)
        if err:
            return err
        sql = ("SELECT f.id, f.text, f.source FROM facts f "
               "WHERE f.category_id IS NULL AND f.archived=0 AND f.invalid_at='' "
               "AND f.lifecycle='active'" + _ws_filter("f", ws))
        params = [ws] if ws else []
        sql += " ORDER BY f.updated_at DESC LIMIT ?"
        params.append(limit)
        rows = [dict(r) for r in con.execute(sql, params)]
        if not rows:
            return {"count": 0, "categorized": 0, "errors": 0}
        existing = [r["name"] for r in con.execute(
            "SELECT name FROM categories c WHERE 1=1" + _ws_filter("c", ws),
            [ws] if ws else [])]
        try:
            import llm
        except ImportError:
            return {"error": "llm module not available"}
        categorized, errors = 0, 0
        for r in rows:
            try:
                label = (llm.category_for(r["text"], existing) or "").strip()
            except Exception:
                errors += 1
                continue
            if not label:
                errors += 1
                continue
            cid = _resolve_category(con, label, ws)
            if cid is None:
                errors += 1
                continue
            con.execute("UPDATE facts SET category_id=?, updated_at=? WHERE id=?",
                        [cid, now(), r["id"]])
            # Commit per fact: LLM calls can take tens of seconds and must not
            # hold the SQLite write lock for the whole batch.
            con.commit()
            categorized += 1
        return {"count": len(rows), "categorized": categorized, "errors": errors}
    finally:
        con.close()


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
    depth, err = _bounded_int_arg(args, "depth", 1, 1, 2)
    if err:
        return err
    limit, err = _bounded_int_arg(args, "limit", 50, 1, 200)
    if err:
        return err
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
    limit, err = _bounded_int_arg(args, "limit", 20, 1, 100)
    if err:
        return err
    params.append(limit)
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
    policy_error = _advisory_only_error(args, "find_precedents")
    if policy_error:
        return policy_error
    scenario = (args.get("scenario") or "").strip()
    if not scenario:
        return {"error": "scenario is required"}
    terms = fts_terms(scenario)
    if not terms:
        return {"error": "scenario has no searchable terms", "count": 0, "precedents": []}
    limit, err = _bounded_int_arg(args, "limit", 10, 1, 50)
    if err:
        return err
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
        return {"count": len(rows), "precedents": rows,
                "semantic": bool(args.get("semantic")),
                "memory_policy": "advisory_only",
                "safety_critical_allowed": False}
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


def _evidence_anchor_fields(args):
    """Validate and normalize optional code-local evidence anchor fields."""
    fields = {}
    for name in ("repo", "ref", "path", "symbol"):
        value = args.get(name, "")
        if value is None:
            value = ""
        if not isinstance(value, str):
            return None, {"error": "%s must be a string" % name}
        value = value.strip()
        if len(value) > _EVIDENCE_MAX_FIELD_CHARS:
            return None, {"error": "%s is too long" % name}
        fields[name] = value
    for name in ("start_line", "start_col", "end_line", "end_col"):
        value = args.get(name)
        if value in (None, ""):
            fields[name] = None
            continue
        if isinstance(value, bool):
            return None, {"error": "%s must be a non-negative integer" % name}
        try:
            value = int(value)
        except (TypeError, ValueError):
            return None, {"error": "%s must be a non-negative integer" % name}
        if value < 0 or value > 10_000_000:
            return None, {"error": "%s is outside the supported range" % name}
        if name.endswith("line") and value == 0:
            return None, {"error": "%s must start at line 1" % name}
        fields[name] = value
    if (fields["start_line"] is not None and fields["end_line"] is not None and
            fields["end_line"] < fields["start_line"]):
        return None, {"error": "end_line must be greater than or equal to start_line"}
    if (fields["start_line"] is not None and fields["end_line"] is not None and
            fields["end_line"] == fields["start_line"] and
            fields["start_col"] is not None and fields["end_col"] is not None and
            fields["end_col"] < fields["start_col"]):
        return None, {"error": "end_col must be greater than or equal to start_col"}

    selected_text = args.get("selected_text")
    if selected_text is not None:
        if not isinstance(selected_text, str):
            return None, {"error": "selected_text must be a string"}
        if len(selected_text.encode("utf-8")) > 128 * 1024:
            return None, {"error": "selected_text is too long"}
    selected_hash = args.get("selected_text_hash", "") or ""
    if not isinstance(selected_hash, str):
        return None, {"error": "selected_text_hash must be a SHA-256 string"}
    selected_hash = selected_hash.strip().lower()
    if selected_text is not None:
        computed = hashlib.sha256(selected_text.encode("utf-8")).hexdigest()
        if selected_hash and selected_hash != computed:
            return None, {"error": "selected_text_hash does not match selected_text"}
        selected_hash = computed
    if selected_hash and not re.fullmatch(r"[0-9a-f]{64}", selected_hash):
        return None, {"error": "selected_text_hash must be a SHA-256 string"}
    fields["selected_text_hash"] = selected_hash

    status = args.get("resolution_status", "") or ""
    if not isinstance(status, str) or status not in ("", "resolved", "stale", "unresolved"):
        return None, {"error": "resolution_status must be resolved, stale, or unresolved"}
    has_anchor = any(fields[name] not in ("", None) for name in (
        "repo", "ref", "path", "symbol", "start_line", "start_col",
        "end_line", "end_col", "selected_text_hash"))
    fields["resolution_status"] = status or ("unresolved" if has_anchor else "")
    return fields, None


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
            "SELECT source_ref, source_checksum, fetched_at, repo, ref, path, symbol, "
            "start_line, start_col, end_line, end_col, selected_text_hash, "
            "resolution_status, created_at FROM evidence "
            "WHERE fact_id=? ORDER BY created_at", (fact["id"],))]
        return {"fact": dict(fact), "evidence": evidence}
    finally:
        con.close()


def attach_evidence(args):
    fact_id = args.get("fact_id")
    source_ref = args.get("source_ref") or ""
    if not isinstance(source_ref, str):
        return {"error": "source_ref must be a string"}
    source_ref = source_ref.strip()
    if not fact_id or not source_ref:
        return {"error": "fact_id and source_ref are required"}
    if len(source_ref) > _EVIDENCE_MAX_FIELD_CHARS:
        return {"error": "source_ref is too long"}
    source_checksum = args.get("source_checksum", "") or ""
    fetched_at = args.get("fetched_at", "") or ""
    if not isinstance(source_checksum, str) or not isinstance(fetched_at, str):
        return {"error": "source_checksum and fetched_at must be strings"}
    if len(source_checksum) > _EVIDENCE_MAX_FIELD_CHARS:
        return {"error": "source_checksum is too long"}
    if len(fetched_at) > _EVIDENCE_MAX_FIELD_CHARS:
        return {"error": "fetched_at is too long"}
    anchor, err = _evidence_anchor_fields(args)
    if err:
        return err
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
            "INSERT OR IGNORE INTO evidence (fact_id, source_ref, source_checksum, fetched_at, "
            "repo, ref, path, symbol, start_line, start_col, end_line, end_col, "
            "selected_text_hash, resolution_status, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (fact_id, source_ref, source_checksum, fetched_at, anchor["repo"], anchor["ref"],
             anchor["path"], anchor["symbol"], anchor["start_line"],
             anchor["start_col"], anchor["end_line"], anchor["end_col"],
             anchor["selected_text_hash"], anchor["resolution_status"], now()))
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
    limit, err = _bounded_int_arg(args, "limit", 20, 1, 100)
    if err:
        return err
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
            "SELECT source_ref, source_checksum, repo, ref, path, symbol, start_line, "
            "start_col, end_line, end_col, selected_text_hash, resolution_status, "
            "created_at FROM evidence WHERE fact_id=?",
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
    limit, err = _bounded_int_arg(args, "limit", 5000, 1, 50000)
    if err:
        return err
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
                "SELECT e.id, e.fact_id, e.source_ref, e.source_checksum, e.repo, e.ref, e.path, "
                "e.symbol, e.start_line, e.start_col, e.end_line, e.end_col, "
                "e.selected_text_hash, e.resolution_status, e.created_at "
                "FROM evidence e JOIN facts f ON f.id=e.fact_id WHERE 1=1" +
                _ws_check("f", ws) + " ORDER BY e.fact_id", [ws] if ws else []):
            add("mem:fact-%d prov:wasDerivedFrom [ a prov:Entity ; "
                "prov:atLocation \"%s\" ; prov:value \"%s\" ] ;"
                % (r["fact_id"], esc(r["source_ref"]), esc(r["source_checksum"])))
            add("    prov:generatedAtTime \"%s\"^^xsd:dateTime ." % r["created_at"])
            anchor = []
            for key in ("repo", "ref", "path", "symbol", "resolution_status"):
                if r[key]:
                    anchor.append("mem:%s \"%s\"" % (key, esc(r[key])))
            for key in ("start_line", "start_col", "end_line", "end_col"):
                if r[key] is not None:
                    anchor.append("mem:%s \"%s\"^^xsd:integer" % (key, r[key]))
            if r["selected_text_hash"]:
                anchor.append("mem:selectedTextHash \"%s\"" % esc(r["selected_text_hash"]))
            if anchor:
                add("mem:evidence-%s a prov:Entity ; %s ." %
                    (r["id"], " ; ".join(anchor)))
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
    limit, err = _bounded_int_arg(args, "limit", 50, 1, 200)
    if err:
        return err
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
    limit, err = _bounded_int_arg(args, "limit", 50, 1, 200)
    if err:
        return err
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


# ---- v0.9 session-scoped database selection -------------------------------
# A stdio MCP server lives for one session, so selection is process-scoped:
# select_database points ALL subsequent tools at a named database. The active
# store (MEMORY_MCP_DB) stays protected — it can be selected back at any
# time, but never archived/deleted through these tools.

def select_database(args):
    """Session-level: point all subsequent tool calls at a named database.
    Selecting the active store (its name) returns to the default."""
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if _is_active_name(name):
        _SELECTED_DB[0] = None
        return {"database": _active_db_label(), "selected": True, "active": True}
    p = _db_file(name)
    if not os.path.exists(p):
        return {"error": f"database {name} not found — create it with create_database first"}
    _SELECTED_DB[0] = name
    return {"database": name, "selected": True, "active": False}


def current_database(_args=None):
    """Name of the database all tools currently operate on."""
    sel = _SELECTED_DB[0]
    if sel:
        return {"database": sel, "active": False}
    return {"database": _active_db_label(), "active": True}


def reset_database(_args=None):
    """Return to the active store (MEMORY_MCP_DB) for all tools."""
    _SELECTED_DB[0] = None
    return {"database": _active_db_label(), "selected": True, "active": True}


def create_database(args):
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if _is_active_name(name):
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
    sel = _SELECTED_DB[0]
    dbs = []
    for fn in sorted(os.listdir(d)):
        if fn.endswith(".db"):
            dbs.append({"name": fn[:-3], "active": False, "archived": False,
                        "selected": sel == fn[:-3]})
        elif fn.endswith(".db.archived"):
            dbs.append({"name": fn[:-len(".db.archived")], "active": False,
                        "archived": True, "selected": False})
    dbs.insert(0, {"name": _active_db_label(), "active": True,
                   "archived": False, "selected": sel is None})
    return {"databases": dbs}


def archive_database(args):
    name, err = _validate_name(args.get("name"), "database")
    if err:
        return {"error": err}
    if _is_active_name(name):
        return {"error": f"cannot archive the active database (MEMORY_MCP_DB)"}
    if _SELECTED_DB[0] == name:
        return {"error": f"cannot archive database {name}: it is currently selected "
                         "(reset_database or select the active store first)"}
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
    sel = _SELECTED_DB[0]
    label = (sel + ".db") if sel else _active_db_name()
    src = _db_path()
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
    if _is_active_name(name):
        return {"error": f"cannot delete the active database (MEMORY_MCP_DB)"}
    if _SELECTED_DB[0] == name:
        return {"error": f"cannot delete database {name}: it is currently selected "
                         "(reset_database or select the active store first)"}
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
             "(SELECT COUNT(*) FROM facts f WHERE f.workspace_id=w.id AND f.archived=0 AND f.invalid_at='') AS active_facts, "
             "(SELECT COUNT(*) FROM facts f WHERE f.workspace_id=w.id) AS facts, "
             "(SELECT COUNT(*) FROM entities e WHERE e.workspace_id=w.id) AS entities, "
             "(SELECT COUNT(*) FROM relations r WHERE r.workspace_id=w.id) AS relations, "
             "(SELECT COUNT(*) FROM decisions d WHERE d.workspace_id=w.id) AS decisions, "
             "(SELECT COUNT(*) FROM evidence e JOIN facts f ON f.id=e.fact_id WHERE f.workspace_id=w.id) AS evidence, "
             "(SELECT COUNT(*) FROM contexts c WHERE c.workspace_id=w.id) AS contexts, "
             "(SELECT COUNT(*) FROM lifecycle_events le WHERE le.workspace_id=w.id) AS lifecycle_events, "
             "(SELECT COUNT(*) FROM handoffs h WHERE h.workspace_id=w.id) AS handoffs "
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
    cur = con.execute("DELETE FROM categories WHERE workspace_id=?", [name])
    counts["categories"] = cur.rowcount
    cur = con.execute("DELETE FROM handoffs WHERE workspace_id=?", [name])
    counts["handoffs"] = cur.rowcount
    cur = con.execute("DELETE FROM lifecycle_events WHERE workspace_id=?", [name])
    counts["lifecycle_events"] = cur.rowcount
    cur = con.execute("DELETE FROM context_lineage WHERE workspace_id=?", [name])
    counts["context_lineage"] = cur.rowcount
    cur = con.execute("DELETE FROM contexts WHERE workspace_id=?", [name])
    counts["contexts"] = cur.rowcount
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
        facts = [dict(r) for r in con.execute(
            "SELECT id, sha256, text, source, project, domain, trust, strong, importance, workspace_id, "
            "created_at, updated_at, archived FROM facts WHERE workspace_id=? ORDER BY id", [name])]
        entities = [dict(r) for r in con.execute(
            "SELECT id, name, type, aliases, workspace_id, created_at, updated_at "
            "FROM entities WHERE workspace_id=? ORDER BY id", [name])]
        relations = [dict(r) for r in con.execute(
            "SELECT id, subject_id, predicate, object_id, source_fact_id, workspace_id, created_at "
            "FROM relations WHERE workspace_id=? ORDER BY id", [name])]
        decisions = [dict(r) for r in con.execute(
            "SELECT id, category, subject, scenario, reasoning, outcome, confidence, decision_maker, "
            "issue_ref, parent_decision_id, workspace_id, created_at, updated_at "
            "FROM decisions WHERE workspace_id=? ORDER BY id", [name])]
        evidence = [dict(r) for r in con.execute(
            "SELECT e.id, e.fact_id, e.source_ref, e.source_checksum, e.fetched_at, "
            "e.repo, e.ref, e.path, e.symbol, e.start_line, e.start_col, e.end_line, "
            "e.end_col, e.selected_text_hash, e.resolution_status, e.created_at "
            "FROM evidence e JOIN facts f ON f.id=e.fact_id WHERE f.workspace_id=? ORDER BY e.id", [name])]
        contexts = [dict(r) for r in con.execute(
            "SELECT ref, name, content, schema_json, source, sha256, workspace_id, "
            "created_at, expires_at, size_bytes FROM contexts WHERE workspace_id=? ORDER BY id", [name])]
        context_lineage = [dict(r) for r in con.execute(
            "SELECT parent_ref, child_ref, relation, workspace_id, created_at "
            "FROM context_lineage WHERE workspace_id=? ORDER BY id", [name])]
        lifecycle_events = [dict(r) for r in con.execute(
            "SELECT idempotency_key, event_kind, event_id, session_id, source, cwd, path, "
            "tool_name, context_ref, workspace_id, sha256, payload_bytes, "
            "payload_truncated, created_at FROM lifecycle_events "
            "WHERE workspace_id=? ORDER BY id", [name])]
        handoffs = [dict(r) for r in con.execute(
            "SELECT ref, context_ref, owner, session_id, cwd, source, sha256, workspace_id, "
            "shared, state, idempotency_key, created_at, expires_at, accepted_at, "
            "accepted_by, cancelled_at, cancelled_by FROM handoffs "
            "WHERE workspace_id=? ORDER BY id", [name])]
    finally:
        con.close()
    counts = {"facts": len(facts), "entities": len(entities), "relations": len(relations),
              "decisions": len(decisions), "evidence": len(evidence),
              "contexts": len(contexts), "context_lineage": len(context_lineage),
              "lifecycle_events": len(lifecycle_events), "handoffs": len(handoffs)}
    if sum(counts.values()) == 0:
        return {"error": f"workspace {name} has no data"}
    dest = os.path.join(_backup_dir(), f"workspace-{name}-{_ts_stamp()}.json")
    try:
        with open(dest, "w", encoding="utf-8") as f:
            json.dump({"workspace": name, "exported_at": now(), "counts": counts,
                       "facts": facts, "entities": entities, "relations": relations,
                       "decisions": decisions, "evidence": evidence,
                       "contexts": contexts, "context_lineage": context_lineage,
                       "lifecycle_events": lifecycle_events, "handoffs": handoffs},
                      f, ensure_ascii=False, indent=2)
    except OSError as e:
        # No host paths in client-visible errors (repo rule).
        print(f"memory-mcp: workspace backup write failed: {e}", file=sys.stderr)
        return {"error": "workspace backup failed: backups/ is not writable or disk is full"}
    return {"workspace": name, "backup": os.path.basename(dest), "counts": counts}


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
                "domain": {"type": "string", "description": "Legacy free tag; used as category when `category` is absent"},
                "category": {"type": "string", "description": "Topic category; auto-assigned by keyword rules when absent (explicit arg > domain > rules > uncategorized)"},
                "trust": {"type": "string", "enum": list(VALID_TRUST), "default": "medium"},
                "strong": {"type": "boolean", "default": False},
                "importance": {"type": "number", "default": 0.5, "description": "0..1 value of the fact for retention"},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
            "required": ["text"],
        },
    },
    "absorb": {
        "description": "Preview or explicitly commit a bounded batch of candidate facts. Exact duplicates are no-ops; related, update, and contradiction candidates remain review-only. dry_run defaults to true.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "facts": {
                    "type": "array", "minItems": 1, "maxItems": 50,
                    "items": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "object", "properties": {
                                "text": {"type": "string"},
                                "source": {"type": "string"},
                                "project": {"type": "string"},
                                "domain": {"type": "string"},
                                "category": {"type": "string"},
                                "trust": {"type": "string", "enum": list(VALID_TRUST)},
                                "strong": {"type": "boolean"},
                                "importance": {"type": "number"},
                                "workspace": {"type": "string"},
                                "evidence": {"type": ["object", "array"]},
                            }, "required": ["text"]},
                        ],
                    },
                },
                "text": {"type": "string", "description": "Single-item alias for facts"},
                "source": {"type": "string"},
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "category": {"type": "string"},
                "trust": {"type": "string", "enum": list(VALID_TRUST)},
                "strong": {"type": "boolean"},
                "importance": {"type": "number"},
                "workspace": {"type": "string", "description": "Project scope id; the whole batch stays in one scope"},
                "dry_run": {"type": "boolean", "default": True},
                "commit": {"type": "boolean", "default": False, "description": "Explicitly apply only items classified as new"},
                "verify": {"type": "boolean", "default": False, "description": "Use the optional LLM verifier to classify related candidates"},
            },
        },
    },
    "chunk_fact": {
        "description": "Read one active fact as bounded, offset-addressable chunks without returning the full text in one payload.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "fact_id": {"type": "integer", "description": "Alias for id"},
                "sha256": {"type": "string"},
                "chunk_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "default": 4000},
                "chunk_overlap": {"type": "integer", "minimum": 0, "maximum": 15999, "default": 0},
                "start_chunk": {"type": "integer", "minimum": 0, "default": 0},
                "max_chunks": {"type": "integer", "minimum": 1, "maximum": 32, "default": 8},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads to your project + shared pool"},
            },
        },
    },
    "put_context": {
        "description": "Store an immutable named context artifact. Returns a ref, checksum, metadata, and lineage; reads require an explicit workspace.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "content": {"type": "string", "description": "Context payload; bounded by MEMORY_MCP_CONTEXT_MAX_BYTES"},
                "schema": {"description": "Optional schema metadata (string or JSON value)"},
                "source": {"type": "string", "description": "Origin: issue/run/artifact ref"},
                "checksum": {"type": "string", "description": "Optional SHA-256 checksum to verify against content"},
                "ttl_seconds": {"type": "integer", "minimum": 0},
                "parent_refs": {"type": "array", "items": {"type": "string"}, "maxItems": 64},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["name", "content", "workspace"],
        },
    },
    "list_context": {
        "description": "List context metadata only. Payloads are never returned by the catalog; expired and out-of-scope refs are hidden.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 100},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["workspace"],
        },
    },
    "resolve_context": {
        "description": "Resolve one context ref to metadata and bounded lineage without returning its payload.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ref": {"type": "string"},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["ref", "workspace"],
        },
    },
    "read_context": {
        "description": "Read a bounded character slice from one context ref. max_chars is capped by the server.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ref": {"type": "string"},
                "start": {"type": "integer", "minimum": 0, "default": 0},
                "end": {"type": "integer", "minimum": 0},
                "max_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "default": 4000},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["ref", "workspace"],
        },
    },
    "search_context": {
        "description": "Search context names, metadata, and payloads in one workspace. Returns metadata only; payloads require read_context.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "maxLength": 256},
                "limit": {"type": "integer", "default": 20, "minimum": 1, "maximum": 100},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["query", "workspace"],
        },
    },
    "chunk_context": {
        "description": "Read a bounded sequence of chunks from one context ref. The response cap and workspace ACL are enforced by the server.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ref": {"type": "string"},
                "chunk_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "default": 4000},
                "start_chunk": {"type": "integer", "minimum": 0, "default": 0},
                "max_chunks": {"type": "integer", "minimum": 1, "maximum": 32, "default": 8},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["ref", "workspace"],
        },
    },
    "reduce_context": {
        "description": "Create a new immutable context by deterministically joining existing refs. This is concatenation, not semantic model summarization.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "refs": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 64},
                "separator": {"type": "string", "maxLength": 1024, "default": "\\n\\n"},
                "schema": {"description": "Optional schema metadata (string or JSON value)"},
                "source": {"type": "string", "description": "Origin: issue/run/artifact ref"},
                "checksum": {"type": "string", "description": "Optional SHA-256 checksum for the reduced content"},
                "ttl_seconds": {"type": "integer", "minimum": 0},
                "workspace": {"type": "string", "description": "Required project/run access scope"},
            },
            "required": ["name", "refs", "workspace"],
        },
    },
    "capture_event": {
        "description": "Capture one sanitized, bounded lifecycle envelope in the exact workspace. Idempotency is required; payloads are stored behind a context ref.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "idempotency_key": {"type": "string", "maxLength": 256},
                "event_id": {"type": "string", "maxLength": 256},
                "event_kind": {"type": "string", "maxLength": 64},
                "session_id": {"type": "string", "maxLength": 256},
                "source": {"type": "string", "maxLength": 256},
                "cwd": {"type": "string", "maxLength": 1024},
                "path": {"type": "string", "maxLength": 1024},
                "tool_name": {"type": "string", "maxLength": 256},
                "payload": {"description": "JSON value or text; secrets are redacted and the payload is byte-bounded"},
                "content": {"type": "string", "description": "Alias for a text payload"},
                "exclude_paths": {"type": "array", "items": {"type": "string"}, "maxItems": 32},
                "capture": {"type": "boolean", "default": True},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["idempotency_key", "event_kind", "workspace"],
            "anyOf": [{"required": ["payload"]}, {"required": ["content"]}],
        },
    },
    "list_events": {
        "description": "List lifecycle event metadata only. Payloads require read_event and are bounded by the server.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {"type": "string", "maxLength": 256},
                "event_kind": {"type": "string", "maxLength": 64},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["workspace"],
        },
    },
    "read_event": {
        "description": "Read one bounded sanitized lifecycle envelope by event_ref or idempotency key.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "event_ref": {"type": "string"},
                "max_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "default": 4000},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["event_ref", "workspace"],
        },
    },
    "handoff_begin": {
        "description": "Create an expiring typed handoff over one immutable context. Owner and exact workspace are mandatory; checksum and optional idempotency are retained.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "content": {"type": "string", "description": "Bounded handoff payload; treated as data"},
                "owner": {"type": "string", "maxLength": 256},
                "session_id": {"type": "string", "maxLength": 256},
                "cwd": {"type": "string", "maxLength": 1024},
                "source": {"type": "string", "maxLength": 256},
                "checksum": {"type": "string", "description": "Optional SHA-256 checksum for content"},
                "ttl_seconds": {"type": "integer", "minimum": 0, "default": 86400},
                "shared": {"type": "boolean", "default": False},
                "idempotency_key": {"type": "string", "maxLength": 256},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["content", "owner", "workspace"],
        },
    },
    "list_handoffs": {
        "description": "List typed handoff metadata in one exact workspace; expired open rows are transitioned to expired before readback.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "owner": {"type": "string", "maxLength": 256},
                "state": {"type": "string", "enum": ["open", "accepted", "cancelled", "expired"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["workspace"],
        },
    },
    "handoff_accept": {
        "description": "Atomically accept one open handoff once and return one bounded payload slice. Owner/shared, exact workspace, optional cwd, and safe expiry are enforced.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "handoff_ref": {"type": "string"},
                "actor": {"type": "string", "maxLength": 256},
                "cwd": {"type": "string", "maxLength": 1024},
                "max_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "default": 4000},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["handoff_ref", "actor", "workspace"],
        },
    },
    "handoff_cancel": {
        "description": "Cancel one open handoff exactly once. Only the owner may cancel it; accepted/cancelled/expired rows remain auditable.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "handoff_ref": {"type": "string"},
                "actor": {"type": "string", "maxLength": 256},
                "workspace": {"type": "string", "description": "Required exact project/run access scope"},
            },
            "required": ["handoff_ref", "actor", "workspace"],
        },
    },
    "search_facts": {
        "description": "Advisory full-text search over stored facts. It cannot authorize safety-critical operations. With semantic=true and MEMORY_MCP_EMBEDDINGS=1, merges lexical and embedding rankings (RRF).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "trust_min": {"type": "string", "enum": list(VALID_TRUST)},
                "strong_only": {"type": "boolean", "default": False},
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "category": {"type": "string", "description": "Topic category filter (see list_categories)"},
                "semantic": {"type": "boolean", "default": False, "description": "Hybrid: RRF-merge FTS BM25 with embedding search (requires MEMORY_MCP_EMBEDDINGS=1)"},
                "valid_at": {"type": "string", "description": "RFC3339: include facts that were still valid at that time (bi-temporal)"},
                "graph": {"type": "boolean", "default": False, "description": "RRF-merge entity-graph expansion"},
                "chunk_chars": {"type": "integer", "minimum": 1, "maximum": 16000, "description": "Optionally add bounded chunks to each hit"},
                "chunk_overlap": {"type": "integer", "minimum": 0, "maximum": 15999, "default": 0},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
                "purpose": {"type": "string", "enum": ["advisory", "safety_critical"], "default": "advisory", "description": "safety_critical is rejected fail-closed; live state and lock/hash checks remain authoritative"},
            },
            "required": ["query"],
        },
    },
    "search_semantic": {
        "description": "Advisory semantic search over stored facts. It cannot authorize safety-critical operations. Requires MEMORY_MCP_EMBEDDINGS=1 (see embeddings.py).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
                "threshold": {"type": "number", "default": 0.0, "description": "Minimum cosine similarity"},
                "purpose": {"type": "string", "enum": ["advisory", "safety_critical"], "default": "advisory", "description": "safety_critical is rejected fail-closed"},
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
        "description": "Build an advisory ready-to-inject <memory-recall> block. The server focuses transcript input on the latest user intent and rejects purpose=safety_critical. Requires MEMORY_MCP_RECALL=1.",
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
                "purpose": {"type": "string", "enum": ["advisory", "safety_critical"], "default": "advisory", "description": "safety_critical is rejected fail-closed; memory never authorizes writes, locks, routes, or hashes"},
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
        "description": "List recent non-archived facts (optional project/domain/category filter).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "category": {"type": "string"},
                "limit": {"type": "integer", "default": 50},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
        },
    },
    "summarize_index": {
        "description": "Compact one-line-per-fact index (freshest first, capped at max_chars, with [category] tags) — for prompt-injection budgets.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project": {"type": "string"},
                "domain": {"type": "string"},
                "category": {"type": "string"},
                "trust_min": {"type": "string", "enum": list(VALID_TRUST)},
                "strong_only": {"type": "boolean", "default": False},
                "limit": {"type": "integer", "default": 200},
                "max_chars": {"type": "integer", "default": 4000},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
        },
    },
    "list_categories": {
        "description": "Card catalog: topic categories with active/total fact counts (most-used first). Optional query filters category names.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
        },
    },
    "search_index": {
        "description": "Short reference by search vector: one-line snippets of matching facts grouped by category, capped at max_chars. Library shelf lookup — full texts via get_provenance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "category": {"type": "string"},
                "limit": {"type": "integer", "default": 30},
                "max_chars": {"type": "integer", "default": 2000},
                "semantic": {"type": "boolean", "default": False, "description": "Hybrid: RRF-merge FTS BM25 with embedding search (requires MEMORY_MCP_EMBEDDINGS=1)"},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
            "required": ["query"],
        },
    },
    "categorize_pending": {
        "description": "LLM batch refinement: assigns topic categories to uncategorized facts (requires MEMORY_MCP_CATEGORIZE=1; provider via MEMORY_MCP_LLM_*).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 20},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
            },
        },
    },
    "remember_entity": {
        "description": "Upsert an entity node (name unique within the workspace; type/aliases optional).",
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
        "description": "Advisory precedent lookup: FTS BM25 over decision scenario/reasoning. It cannot authorize safety-critical operations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "scenario": {"type": "string"},
                "category": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
                "semantic": {"type": "boolean", "default": False},
                "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project + shared pool"},
                "purpose": {"type": "string", "enum": ["advisory", "safety_critical"], "default": "advisory", "description": "safety_critical is rejected fail-closed"},
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
        "description": "Return a fact plus evidence rows, including optional repository/ref/path/symbol/line-range anchors.",
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
        "description": "Link a fact to a source and optional code-local anchor; dedup by (fact_id, source_ref).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "fact_id": {"type": "integer"},
                "source_ref": {"type": "string"},
                "source_checksum": {"type": "string"},
                "fetched_at": {"type": "string"},
                "repo": {"type": "string", "description": "Repository URL or stable repository id"},
                "ref": {"type": "string", "description": "Commit, tag, or branch"},
                "path": {"type": "string"},
                "symbol": {"type": "string"},
                "start_line": {"type": "integer", "minimum": 1},
                "start_col": {"type": "integer", "minimum": 0},
                "end_line": {"type": "integer", "minimum": 1},
                "end_col": {"type": "integer", "minimum": 0},
                "selected_text": {"type": "string", "description": "Optional local snippet; only its SHA-256 is retained"},
                "selected_text_hash": {"type": "string"},
                "resolution_status": {"type": "string", "enum": ["resolved", "stale", "unresolved"]},
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
        "description": "Backup a database (the selected or active store by default, or a named one incl. archived) to backups/ via SQLite online backup API.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string", "description": "optional; defaults to the selected/active store"},
        }},
    },
    "delete_database": {
        "description": "Permanently delete a named database file (requires confirm:true). The active database and a currently selected one cannot be deleted.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string"},
            "confirm": {"type": "boolean", "default": False},
        }, "required": ["name", "confirm"]},
    },
    "select_database": {
        "description": "Session-level: point all subsequent tools at a named database (create it with create_database first). Selecting the active store's name returns to the default. The active store (MEMORY_MCP_DB) stays protected.",
        "inputSchema": {"type": "object", "properties": {
            "name": {"type": "string"},
        }, "required": ["name"]},
    },
    "current_database": {
        "description": "Name of the database all tools currently operate on (the active store or a selected named database).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "reset_database": {
        "description": "Return to the active store (MEMORY_MCP_DB) for all tools.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "create_workspace": {
        "description": "Register a workspace (named access scope) in the active database's workspaces registry. Re-registering reactivates an archived/reset workspace.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string", "description": "Workspace id: 1-64 chars of [A-Za-z0-9._-], no '..'"},
        }, "required": ["workspace"]},
    },
    "list_workspaces": {
        "description": "List registered workspaces with their status (active/archived/reset) and full data counts: active_facts, facts, entities, relations, decisions, evidence.",
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
        "description": "Export ALL workspace data (facts incl. archived, entities, relations, decisions, evidence) as JSON with per-table counts to backups/workspace-<name>-<ts>.json.",
        "inputSchema": {"type": "object", "properties": {
            "workspace": {"type": "string"},
        }, "required": ["workspace"]},
    },
    "decay_sweep": {
        "description": "v0.7: recompute fact lifecycle by active-day decay. Score = importance * 0.95^active_days (days with system activity since last search hit). score < 0.25 -> degraded (hidden from plain search, reachable via chains, revived after N matching searches); score <= 0.1 -> forgotten (visible only via list_forgotten/restore_fact). strong/confirmed never decay. User downtime does not count.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "list_forgotten": {
        "description": "Direct review of forgotten facts in the requested workspace (lifecycle=forgotten) — the only way to see them besides restore_fact.",
        "inputSchema": {"type": "object", "properties": {
            "limit": {"type": "integer", "default": 50, "minimum": 1, "maximum": 200},
            "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project plus the shared pool"},
        }},
    },
    "restore_fact": {
        "description": "Manually bring a forgotten/degraded fact in the requested workspace back to lifecycle=active (resets revival_count, stamps last_accessed_at).",
        "inputSchema": {"type": "object", "properties": {
            "id": {"type": "integer"},
            "workspace": {"type": "string", "description": "Project scope id; scopes reads/writes to your project plus the shared pool"},
        }, "required": ["id"]},
    },
}

HANDLERS = {
    "add_fact": remember_fact,
    "remember_fact": remember_fact,
    "absorb": absorb,
    "chunk_fact": chunk_fact,
    "put_context": put_context,
    "list_context": list_context,
    "resolve_context": resolve_context,
    "read_context": read_context,
    "search_context": search_context,
    "chunk_context": chunk_context,
    "reduce_context": reduce_context,
    "capture_event": capture_event,
    "list_events": list_events,
    "read_event": read_event,
    "handoff_begin": handoff_begin,
    "list_handoffs": list_handoffs,
    "handoff_accept": handoff_accept,
    "handoff_cancel": handoff_cancel,
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
    "list_categories": list_categories,
    "search_index": search_index,
    "categorize_pending": categorize_pending,
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
    "select_database": select_database,
    "current_database": current_database,
    "reset_database": reset_database,
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
                    "serverInfo": {"name": "memory-mcp", "version": "0.16.0"},
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
