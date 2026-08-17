# memory-mcp

Shared fact-memory MCP server (stdio, JSON-RPC 2.0, newline-delimited) for
reasonix / jcode / codex runtimes. SQLite + FTS5 storage; one fact store
shared across the host and docker runtimes via bind-mount.

Replaces the per-runtime memory storage with a single searchable store.
Extraction/gating/injection are client-side by default (reasonix memory
patches); optional server-side modules (extract/recall/verify) move the same
pipeline into the server for runtimes that have no client patches.

## Tools

- `remember_fact {text, source?, project?, domain?, trust?, strong?}` — upsert
  (dedup by sha256 of text). `add_fact` is an alias for the same operation.
- `search_facts {query, limit?, trust_min?, strong_only?, project?, domain?}` —
  FTS5 full-text, BM25 ranking; falls back to literal phrase on FTS syntax errors
- `list_facts {project?, domain?, limit?}` — recent non-archived facts
- `summarize_index {project?, domain?, trust_min?, strong_only?, limit?, max_chars?}` —
  compact one-line-per-fact index (`#id trust! [domain] text`), freshest first,
  description clipped at 120 chars, total capped at `max_chars` (default 4000,
  cut at line boundary) — mirrors the reasonix index-cap for prompt budgets
- `forget_fact {id | sha256}` — soft delete (archived=1)
- `stats {}` — totals by trust/domain plus v0.3 counts (entities/relations/decisions/evidence)
- `export {}` — all facts incl. archived (migration/backup)

### v0.6 — database & workspace management (2026-08-17)

Named databases are separate SQLite files under `databases/` (sibling of the
active DB); backups land in `backups/`. The active store (`MEMORY_MCP_DB`) can
be backed up but never archived/deleted through these tools. Workspaces are
named access scopes registered in the `workspaces` table of the active DB —
`workspace` in fact tools remains an access-scope filter; the registry gives
it create/reset/archive/backup semantics.

- `create_database {name}` — new SQLite database (full schema); rejects the
  active store's name, duplicates, and invalid names
  (1-64 chars of `[A-Za-z0-9._-]`, no `..`)
- `list_databases {}` — active + named databases, archived flag
- `archive_database {name, hard?, confirm?}` — soft (default): rename to
  `<name>.db.archived`, data preserved, reversible by renaming back; refuses
  to clobber an existing archive (returns an error);
  `hard:true` deletes the file permanently (requires `confirm:true`)
- `backup_database {name?}` — SQLite online backup to `backups/` (default:
  active store; named incl. archived databases)
- `delete_database {name, confirm:true}` — permanent file delete; active
  store protected
- `create_workspace {workspace}` — register a workspace (idempotent;
  re-registering reactivates an archived/reset workspace)
- `list_workspaces {status?}` — registry rows with active fact counts
- `reset_workspace {workspace, hard?, confirm?}` — soft (default): archive
  all its facts (`archived=1`, reversible), status='reset'; `hard:true`
  deletes the facts permanently (requires `confirm:true`)
- `archive_workspace {workspace, hard?, confirm?}` — soft (default): archive
  all its facts, status='archived'; `hard:true` deletes permanently
  (requires `confirm:true`)
- `backup_workspace {workspace}` — JSON export of all its facts (incl.
  archived) to `backups/workspace-<name>-<ts>.json`

### v0.7 — automatic decay (2026-08-17)

Facts age only on **active days** — days with at least one memory-mcp call
(`activity_days` table) — so user downtime never ages them. Score =
`importance * 0.95^active_days` since the last search hit (or creation).

- `active` (score ≥ 0.25): normal search/recall participant.
- `degraded` (score < 0.25): hidden from plain search; reachable via
  entity-graph/session chains; revived after `DECAY_REVIVE_HITS` (default 3)
  matching searches.
- `forgotten` (score ≤ 0.1): excluded everywhere; visible only via
  `list_forgotten {limit?}` and `restore_fact {id}` (manual return to active).
- `decay_sweep {}` — full lifecycle recompute + report; run manually or via
  cron (the stdio server does not live between sessions). `strong` and
  `confirmed` facts never decay.
- Params: `DECAY_RATE` (0.95), `DECAY_ARCHIVE` (0.25), `DECAY_FORGET` (0.1),
  `DECAY_REVIVE_HITS` (3). Search hits refresh `last_accessed_at` /
  `access_count` on active facts only (chained access does not keep stale
  facts alive).

### v0.3 — knowledge graph, decision log, provenance (2026-08-15)

Covers the agent needs that motivated the Semantica evaluation (decision
rationale, precedent search, evidence lineage) with zero new dependencies —
just SQLite + FTS5.

- `remember_entity {name, type?, aliases?}` — upsert entity node
- `remember_relation {subject, predicate, object, source_fact_id?}` — edge
  (entities auto-created; dedup by triple)
- `search_graph {entity, depth? (1-2), limit?}` — BFS neighbors, both directions
- `record_decision {category?, subject?, scenario, reasoning?, outcome?,
  confidence?, decision_maker?, issue_ref?, parent_decision_id?}` — decision node;
  `parent_decision_id` builds causal chains
- `query_decisions {category?, subject?, outcome?, decision_maker?, issue_ref?, limit?}`
- `find_precedents {scenario, category?, limit?}` — similar decisions via FTS BM25
  (OR-joined terms, ranked)
- `get_causal_chain {decision_id}` — walk parent links to the root
- `get_provenance {fact_id | sha256}` — fact + evidence rows
- `attach_evidence {fact_id, source_ref, source_checksum?, fetched_at?}` — link a
  fact to a source (dedup by fact_id+source_ref)
- `detect_conflicts {text}` — near-duplicate facts (term coverage ≥ 0.6) +
  decisions with the same subject but >1 distinct outcome

## Schema

`facts(id, sha256 UNIQUE, text, source, project, domain,
trust CHECK IN ('high','medium','low'), strong, created_at, updated_at, archived)`
+ `facts_fts` FTS5 virtual table with insert/delete/update triggers.

v0.3 additions (all additive — `CREATE TABLE IF NOT EXISTS`, existing DBs
migrate in place):

- `entities(id, name UNIQUE, type, aliases, created_at, updated_at)`
- `relations(id, subject_id → entities, predicate, object_id → entities,
  source_fact_id?, created_at, UNIQUE(subject_id, predicate, object_id))`
- `decisions(id, category, subject, scenario, reasoning, outcome, confidence,
  decision_maker, issue_ref, parent_decision_id?, created_at, updated_at)`
  + `decisions_fts` FTS5 (scenario/reasoning/category) with triggers
- `evidence(id, fact_id → facts, source_ref, source_checksum, fetched_at,
  created_at, UNIQUE(fact_id, source_ref))`

## Environment

- `MEMORY_MCP_DB` — SQLite path. Default is **script-relative**: `<repo>/data/facts.db`
  (portable — clone the repo anywhere and it works out of the box; `data/` is gitignored).
  The deployment stack always sets it explicitly: host wrapper
  (`~/.local/bin/memory-mcp`) → the shared host store,
  docker runtimes → `/opt/memory-shared/facts.db`.
- Journal mode WAL, busy_timeout 5000 (multi-writer: host + containers).

## Integration

- Host: `~/.local/bin/memory-mcp` wrapper → this script; registered in
  `~/.reasonix/config.toml` (`[[plugins]]`), `~/.jcode/mcp.json`,
  `~/.codex/config.toml` (`[mcp_servers.memory-mcp]`).
- Docker runtimes: script bind-mounted read-only to `/opt/memory-mcp`,
  DB dir to `/opt/memory-shared` (rw); env `MEMORY_MCP_DB=/opt/memory-shared/facts.db`.
  The shared dir must be writable by the container uid (e.g. 1001 for codex-daemon).
- reasonix dual-write: `REASONIX_MEMORY_MCP=1` (+ `MEMORY_MCP_CMD`, `MEMORY_MCP_DB`)
  makes the memory-extract child process sync extracted facts into this store
  (see reasonix `internal/memory/mcp_sync.go`, best-effort).
- reasonix read (step a, 2026-08-16): the same env flag turns on reading the
  shared store in reasonix (see `internal/memory/mcp_read.go`):
  - **prefix index** — `Load()` swaps the native (uncapped) index for the
    capped `summarize_index` (4000 chars, freshest first); native index stays
    as fallback on any server error;
  - **per-turn recall** — `AutoRecall` additionally runs `search_facts` over
    the shared store and scores the matches with the same BM25/freshness/trust
    pipeline as native facts (dual-read; native wins on duplicate text, so
    dual-write overlap costs nothing). One `search_facts` round-trip ≈ 0.1 s;
    failures degrade silently to native-only.

`migrate_memory.py` — one-time migration of native reasonix memory facts
(frontmatter + body) into the shared store (Phase 3). All paths are
env-overridable and default to portable values: `MEMORY_MIGRATE_SRC`
(auto-discovered first `<project>/memory` under `~/.reasonix/projects/`,
symlinked project dirs skipped), `MEMORY_MIGRATE_PROJECT` (derived from the
project dir name), `MEMORY_MCP_CMD` (`memory-mcp` via PATH), `MEMORY_MCP_DB`
(XDG-style `~/.local/share/memory-mcp/facts.db` — propagated to the server
only when explicitly set, so a host wrapper pin is never overridden;
resolved source/server/target are printed before writing).

## Agent skill

- `skills/memory-mcp/SKILL.md` — agent-facing playbook for the 44 MCP tools:
  when to search facts before researching, how to record decisions with
  rationale for precedent lookup, graph/provenance/conflict usage, semantic
  search, and shared-store conventions. Compatible with the `SKILL.md` format
  used by agent skill collections; copy it into your agent's skill directory
  (or point discovery at this repo).

## Semantic search (optional module)

`embeddings.py` adds embedding-based search without touching the stdlib-only
core — it activates only when the server runs with `MEMORY_MCP_EMBEDDINGS=1`
and every failure degrades to lexical-only.

- **Providers** (`MEMORY_MCP_EMBED_PROVIDER`):
  - `ollama` (default) — URL `MEMORY_MCP_EMBED_URL` (default
    `http://localhost:11434`; in docker runtimes point at the ollama service),
    model `MEMORY_MCP_EMBED_MODEL` (default `nomic-embed-text`; for mixed
    RU/EN facts prefer `bge-m3`);
  - `openai` — any OpenAI-compatible `/embeddings` endpoint
    (`MEMORY_MCP_EMBED_URL` + optional `MEMORY_MCP_EMBED_KEY`);
  - `fastembed` — offline ONNX (`pip install fastembed`; default model
    `intfloat/multilingual-e5-small`);
  - `test` — deterministic char-n-gram vectors (tests/diagnostics only).
- **Storage**: one normalized float32 vector per fact in `fact_embeddings`;
  computed on write (best-effort, synchronous — the write path waits at most
  10 s for the provider; a down provider degrades to lexical-only) and
  backfillable via `embed_backfill`.
- **Privacy**: the configured provider receives fact texts to embed — use a
  local/trusted provider (e.g. `ollama`) for private stores.
- **Tools**: `search_semantic {query, limit?, threshold?}` (cosine, brute
  force — milliseconds for a fact store of thousands of entries), and
  `search_facts` with `semantic=true` for an RRF-merged hybrid ranking.
- **Env**: `MEMORY_MCP_EMBEDDINGS=1` (+ `_PROVIDER`, `_URL`, `_MODEL`, `_KEY`).

## Design boundaries

memory-mcp is a shared fact **store + search**, deliberately not a full memory
system. The following stay client-side by default (e.g. the reasonix memory
patches: extraction, recall tiers, fact gate); the optional server-side
pipeline modules below move each of them into the server when enabled:

- **Fact extraction from conversations** and deciding what is worth
  remembering — client-side by default, or `ingest_turn` (extract.py).
- **Prompt injection / recall assembly** — the store is read on demand
  (`search_facts`, `summarize_index`) or via the client's own recall pipeline;
  `compose_recall` (recall.py) returns a ready-to-inject block, so the client
  only inserts it.
- **Truth verification** — `trust`/`strong` are client-set metadata and query
  filters, not a verification or protection mechanism.
- **Semantic search** — optional module (`embeddings.py`, gated by
  `MEMORY_MCP_EMBEDDINGS=1`): `search_semantic` + hybrid `search_facts
  semantic=true` (RRF merge). The core stays lexical (FTS5 + BM25) with zero
  dependencies; embeddings add a provider dependency only when enabled.
  `find_precedents` OR-joins terms on purpose: precedent lookup is about
  similarity, and BM25 ranks partially matching decisions.
- **Near-duplicate handling** — writes dedup on exact text (sha256).
  Paraphrased facts stay separate records; `detect_conflicts` surfaces
  near-duplicates (term coverage ≥ 0.6) on demand.


## Server-side pipeline (optional modules)

Same env-gate pattern as embeddings — the core stays stdlib-only; these
activate only when set, and every failure degrades to store-only.

- **Extraction** (`MEMORY_MCP_EXTRACT=1`): `ingest_turn {transcript,
  session_ref?, project?, domain?}` sends the transcript to the LLM provider
  (see llm.py; ollama/openai/test) and stores extracted facts with provenance
  (`attach_evidence`). Minimum transcript length:
  `MEMORY_MCP_EXTRACT_MIN_CHARS` (default 800). When `MEMORY_MCP_VERIFY=1`,
  new facts are cross-checked and superseded ones archived.
- **Recall assembly** (`MEMORY_MCP_RECALL=1`): `compose_recall {turn_text,
  limit?, chars?, semantic?}` returns a ready-to-inject `<memory-recall>`
  block (authoritative + background tiers, reasonix-compatible format);
  `sweep_freshness {}` archives facts past their type's hard window (strong
  facts kept): reference 45d, user/feedback 365d, project 180d.
- **Verification** (`MEMORY_MCP_VERIFY=1`): `verify_facts {text}` LLM
  cross-checks a candidate against the store (conflicts/supersessions).
  `check_new_facts` (ingestion hook) archives superseded old facts
  (graphiti-style invalidation) only on high-confidence verdicts
  (`MEMORY_MCP_VERIFY_MIN_CONFIDENCE`, default 0.8) and attaches
  `supersedes:<old_id>` evidence to the new one.
- **LLM provider** (shared by extract/verify): `MEMORY_MCP_LLM_PROVIDER`
  (ollama|openai|test), `_URL`, `_MODEL` (ollama default qwen2.5:14b), `_KEY`,
  `_TIMEOUT` (default 60s).

## Memory quality (v0.4): bi-temporal validity, importance, confirmation

- **Bi-temporal validity** — superseded facts are not deleted or archived:
  `invalid_at` + `superseded_by` keep them queryable with
  `search_facts {valid_at}` (what was true at time T) and `fact_history {id}`
  (the version chain, oldest first). Default searches exclude invalidated
  facts.
- **Importance** — `remember_fact {importance 0..1}` (extraction assigns it
  too). Retention (`sweep_freshness`) archives facts past their type window
  only when they are low-importance; strong and human-confirmed facts are
  never auto-archived.
- **Human confirmation** — `review_pending` lists active unconfirmed facts
  (importance-first); `confirm_fact` marks one as human-verified
  (confirmed=1, trust=high). Confirmed facts ride the authoritative tier of
  `compose_recall`.
- **Update taxonomy** — verification (`verify.py`) decides
  add/update/supersedes/delete/noop per ingested fact; supersedes/update
  invalidate the old fact bi-temporally (never archive, never touch strong
  facts, ids whitelisted to the verification context).
- **Consolidation** — `consolidate {ids}` LLM-merges paraphrased facts into
  one (inputs invalidated bi-temporally with `consolidated:<id>` evidence);
  strong/confirmed facts are never merged.
- **Sessions first-class** — `facts_for_session {session_ref}` and
  `list_sessions` (source index); `compose_recall {session_expand}` pulls
  sibling facts from the top hits' sessions as background context.
- **Entity-graph in recall** — `compose_recall {graph=true}` adds a third RRF
  source: entities mentioned in hits -> graph neighbors -> facts mentioning
  them (facts unreachable by lexical/semantic search surface via the graph).

## Deploying in a docker runtime

Step-by-step guide with a codex-runtime compose example, verification
commands, and MCP registration: see **DEPLOYMENT.md**.
