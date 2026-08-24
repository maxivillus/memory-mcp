# memory-mcp

Shared fact-memory MCP server (stdio, JSON-RPC 2.0, newline-delimited) for
reasonix / jcode / codex runtimes. SQLite + FTS5 storage; one fact store
shared across the host and docker runtimes via bind-mount.

Replaces the per-runtime memory storage with a single searchable store.
Extraction/gating/injection are client-side by default (reasonix memory
patches); optional server-side modules (extract/recall/verify) move the same
pipeline into the server for runtimes that have no client patches.

## Tools

- `remember_fact {text, source?, project?, domain?, category?, trust?, strong?}` —
  upsert (dedup by sha256 of text). Category auto-assigned at write time
  (explicit `category` > legacy `domain` > keyword rules > uncategorized).
  `add_fact` is an alias for the same operation.
- `absorb {facts, workspace?, dry_run?, commit?, verify?}` — bounded batch
  ingestion with `new` / `duplicate` / `related` classification. Preview is
  the default; `commit:true` only writes `new` items and leaves related,
  update, and contradiction candidates for review.
- `search_facts {query, limit?, trust_min?, strong_only?, project?, domain?, category?, valid_at?, workspace?, chunk_chars?, purpose?}` —
  advisory FTS5 full-text, BM25 ranking; falls back to literal phrase on FTS syntax errors.
  `chunk_chars?` optionally adds bounded, offset-addressable chunks to hits.
- `search_semantic {query, limit?, threshold?, workspace?, valid_at?, trust_min?, strong_only?, project?, domain?, category?, purpose?}` — advisory embedding
  search when `MEMORY_MCP_EMBEDDINGS=1`; its eligibility filters match
  `search_facts`, and it cannot authorize safety-critical operations.
- `chunk_fact {id | fact_id | sha256, workspace?, chunk_chars?, chunk_overlap?, start_chunk?, max_chunks?}` — read one active fact through a bounded page API
- `list_facts {project?, domain?, category?, limit?}` — recent non-archived facts
- `summarize_index {project?, domain?, category?, trust_min?, strong_only?, limit?, max_chars?}` —
  compact one-line-per-fact index (`#id trust! [category] [domain] text`), freshest first,
  description clipped at 120 chars, total capped at `max_chars` (default 4000,
  cut at line boundary) — mirrors the reasonix index-cap for prompt budgets
- `forget_fact {id | sha256}` — soft delete (archived=1)
- `stats {}` — totals by trust/domain plus v0.3 counts (entities/relations/decisions/evidence), v0.18 run counts, and the memory-access log (kept events, per-site counts, last access, pull hit-rate)
- `export {}` — all facts incl. archived (migration/backup)
- `put_context {name, content, workspace, schema?, source?, checksum?, ttl_seconds?, parent_refs?}` — store an immutable named context artifact and return its `ctx_...` ref
- `list_context {workspace, name?, limit?}` — catalog metadata only; payloads are never returned
- `resolve_context {ref, workspace}` — resolve metadata and parent/child lineage without payload
- `read_context {ref, workspace, start?, end?, max_chars?}` — read one bounded slice after ACL and TTL checks
- `search_context {query, workspace, limit?}` — search context metadata and payloads, returning metadata only
- `chunk_context {ref, workspace, chunk_chars?, start_chunk?, max_chunks?}` — read a bounded page of numbered chunks
- `reduce_context {name, refs, workspace, separator?, schema?, source?, checksum?, ttl_seconds?}` — create a derived ref by deterministic concatenation
- `capture_event {idempotency_key, event_kind, payload, workspace, session_id?, source?, path?, exclude_paths?}` — sanitize and capture one bounded lifecycle envelope; returns stable refs and deduplicates retries
- `list_events {workspace, session_id?, event_kind?, limit?}` / `read_event {event_ref, workspace, max_chars?}` — inspect event metadata and read one bounded payload slice
- `handoff_begin {content, owner, workspace, source?, checksum?, ttl_seconds?, cwd?, shared?, idempotency_key?}` — create an expiring typed handoff over immutable context
- `list_handoffs {workspace, owner?, state?, limit?}` — list open and terminal handoff metadata; expired rows are marked safely
- `handoff_accept {handoff_ref, actor, workspace, cwd?, max_chars?}` / `handoff_cancel {handoff_ref, actor, workspace}` — one-shot owner-scoped accept or cancel transitions
- `run_begin {run_id, workspace?, issue_ref?, pr_ref?, session_id?, cwd?, source?}` / `run_end {run_id, workspace?, base_sha?, head_sha?, files_changed?, diff?, issue_ref?, pr_ref?}` — open/close a run record with bounded client-supplied git facts (the server never shells out to git)
- `link_run {run_id, workspace?, issue_ref?, pr_ref?}` — bind a run to issue/PR references (at least one required)
- `query_run {run_id?, workspace?, state?, issue_ref?, limit?}` — one run record or a filtered list; diffs are clipped to bounded slices
- `prepare_summary {run_id, workspace?, max_decisions?}` — assemble a ready-to-post markdown summary from a run's own records (decisions in its window or bound to its issue_ref, event catalog); posts nothing
- `query_anchored {path?, symbol?, repo?, repo_root?, workspace?, limit?, purpose?}` — advisory lookup of facts (via evidence code anchors) and decisions (via their own path/symbol anchors) bound to a code location; an explicit local `repo_root` adds read-only filesystem verdicts
- `auto_orient {turn_text, session_id?, workspace?}` — one first-input, six-hit, 2.5-second capped recall orientation with silent degradation
- `search_guard {session_id, action, threshold?, workspace?}` — non-blocking warning after repeated external searches without a memory action

Retrieval is advisory only. `compose_recall`, `search_facts`, semantic search,
`find_precedents`, and `query_anchored` reject `purpose: "safety_critical"`
fail-closed. Memory never authorizes registry writes, route selection, lock
validity, or hash acceptance. Those decisions must use current Multica state
and local lock/hash checks.

`attach_evidence` accepts optional code-local fields (`repo`, `ref`, `path`,
`symbol`, line/column range, `selected_text` or its SHA-256, and
`resolution_status`). `get_provenance` and `backup_workspace` retain these
anchors. Raw `selected_text` is never stored; only its SHA-256 is kept. The
complete v0.16 ingestion and provenance flow is documented in
[`docs/ingestion-and-provenance.md`](docs/ingestion-and-provenance.md).

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
  the selected or active store; named incl. archived databases)
- `delete_database {name, confirm:true}` — permanent file delete; active
  store and a currently selected database are protected
- `select_database {name}` — session-level: point ALL subsequent tools at a
  named database (create it with `create_database` first); selecting the
  active store's name returns to the default. The active store is never
  archived/deleted through these tools
- `current_database {}` — name of the database all tools currently operate
  on; `reset_database {}` returns to the active store
- `create_workspace {workspace}` — register a workspace (idempotent;
  re-registering reactivates an archived/reset workspace)
- `list_workspaces {status?}` — registry rows with full data counts
  (active_facts, facts, entities, relations, decisions, evidence, contexts,
  lifecycle_events, handoffs)
- `reset_workspace {workspace, hard?, confirm?}` — soft (default): hide all
  its data (facts get `archived=1`; graph/decisions/evidence become
  unreadable and unwritable), status='reset'; `hard:true` purges facts,
  evidence, graph, decisions, lifecycle events, handoffs, and contexts
  permanently (requires `confirm:true`;
  response reports per-table deleted counts)
- `archive_workspace {workspace, hard?, confirm?}` — soft (default): hide all
  its data, status='archived'; `hard:true` purges facts, evidence, graph and
  decisions, lifecycle events, handoffs, and contexts permanently (requires
  `confirm:true`; per-table deleted counts)
- `backup_workspace {workspace}` — versioned, schema-complete JSON export of
  all workspace data (facts incl. temporal/decay/confirmation fields,
  categories, embeddings, graph, decisions, evidence, contexts, lifecycle
  events, handoffs, the workspace registry row, and activity-day metadata) with
  per-table counts to `backups/workspace-<name>-<ts>.json`. Embedding BLOBs are
  base64 encoded; the backup directory is `0700`, files are `0600`, and writes
  are atomic.

### v0.7 — automatic decay (2026-08-17)

Facts age only on **active days** — days with at least one memory-mcp call
(`activity_days` table) — so user downtime never ages them. Score =
`importance * 0.95^active_days` since the last search hit (or creation).

- `active` (score ≥ 0.25): normal search/recall participant.
- `degraded` (score < 0.25): hidden from plain search; reachable via
  entity-graph/session chains; revived after `DECAY_REVIVE_HITS` (default 3)
  matching searches.
- `forgotten` (score ≤ 0.1): excluded everywhere — plain search, semantic
  search and graph/session chains; visible only via
  `list_forgotten {limit?, workspace?}` and `restore_fact {id, workspace?}`
  (manual return to active).
- `decay_sweep {}` — full lifecycle recompute + report; run manually or via
  cron (the stdio server does not live between sessions). `strong` and
  `confirmed` facts never decay.
- Params: `DECAY_RATE` (0.95), `DECAY_ARCHIVE` (0.25), `DECAY_FORGET` (0.1),
  `DECAY_REVIVE_HITS` (3). Search hits refresh `last_accessed_at` /
  `access_count` on active facts only (chained access does not keep stale
  facts alive).

### v0.8 — cascade-safe workspace cleanup (2026-08-17)

External audit follow-up: hard reset/archive previously deleted only `facts`
rows and failed with `FOREIGN KEY constraint failed` because `evidence`
(child of facts) and `relations` (child of entities) were not deleted
cascading and no delete tool existed for them. Now:

- `hard:true` on `reset_workspace` / `archive_workspace` purges **all**
  workspace rows in FK-safe order — `evidence`, `fact_embeddings`,
  `relations`, `entities`, `decisions` (parent chains detached first),
  `facts` — atomically in one transaction; the response reports per-table
  deleted counts. `reset` also removes the workspace registry row; `archive`
  keeps it with status `archived`.
- Soft reset/archive now hides the whole workspace, not just facts: reads of
  decisions/graph (`query_decisions`, `find_precedents`, `get_causal_chain`,
  `search_graph`, `export_rdf`, graph expansion) return an error and all
  writes (`remember_fact`, `ingest_turn`, `remember_entity`,
  `remember_relation`, `record_decision`, `attach_evidence`) are refused
  until the workspace is reactivated with `create_workspace`.
- Schema hardening: `evidence.fact_id` and `relations.subject_id/object_id`
  now carry `ON DELETE CASCADE` (`relations.source_fact_id` → `SET NULL`);
  existing databases are migrated by `_migrate_fks` (idempotent table
  rebuild, same pattern as the v0.5 facts rebuild). Stores that predate the
  FTS tables get their index rebuilt by `_migrate_fts` on open — otherwise
  the FTS5 `delete` trigger fails with SQLITE_CORRUPT on the first row
  delete (FTS5's `delete` command requires the entry to be indexed).

### v0.9 — session-scoped database isolation (2026-08-17)

Database isolation: `workspace` is an access scope, not physical isolation —
scoped reads include the shared pool. For real isolation use a named
database:

- `select_database {name}` — session-level switch: ALL subsequent tools
  (facts, graph, decisions, evidence, backup, export, workspaces) operate on
  the named database file until `reset_database {}` (or selecting the active
  store's name). The active store (`MEMORY_MCP_DB`) is never archived or
  deleted through these tools, and a currently selected database cannot be
  archived/deleted either.
- `current_database {}` — reports the active selection; `list_databases`
  marks the selected one.

Full workspace read-back: `backup_workspace` exports a versioned,
schema-complete JSON manifest with every workspace table, full fact state,
optional embeddings (base64 encoded), the registry row, and activity-day
metadata. Counts cover every table. The backup directory is private (`0700`),
files are private (`0600`), and publication is atomic. `list_workspaces`
continues to report the scoped data counts used to verify hard cleanup.

### v0.10 — topic categories: the library flow (2026-08-17)

Accessing memory like a library: never dump everything — ask the librarian
first, then go to the shelf, then take the book. Three tiers:

- `list_categories {query?}` — the card catalog: topic categories with
  active/total fact counts, most-used first.
- `search_index {query, category?, limit?, max_chars?, semantic?}` — the
  shelf lookup: one-line snippets (≤120 chars) of matching facts **grouped by
  category**, capped at `max_chars` (default 2000). Full texts are never
  returned here.
- `get_provenance {fact_id | sha256}` — the book: full fact + evidence,
  loaded only for facts chosen from the index.

Categorization is hybrid (A4): at write time `remember_fact` assigns a
category instantly — explicit `category` arg > legacy `domain` arg > keyword
rules (`_CATEGORY_RULES`) > uncategorized (NULL). The background half is
`categorize_pending {limit?}` — an LLM batch that assigns categories to
uncategorized facts, reusing existing category names when they fit (enabled
with `MEMORY_MCP_CATEGORIZE=1`; provider via `MEMORY_MCP_LLM_*`, see
Environment). Categories are workspace-scoped (`categories` table,
`facts.category_id` with `ON DELETE SET NULL` on fresh stores; additive
`_migrate_categories` for existing ones). Every read (`search_facts`,
`list_facts`, `summarize_index`) now carries the `category` of each fact and
accepts a `category` filter; `summarize_index` lines include `[category]`
tags.

### v0.11 — immutable context artifacts

Context artifacts provide a small, auditable handoff surface for runtime
orchestration. The memory server stores the payload, but callers exchange a
stable `ctx_...` ref instead of copying an unbounded transcript or injecting
executable content:

- `put_context` requires an explicit workspace, computes a SHA-256 checksum,
  and returns metadata plus optional parent lineage. A ref is immutable; a new
  version is a new ref.
- `list_context` and `resolve_context` return metadata only. `read_context`
  is the only payload endpoint and enforces `start`/`end`/`max_chars` bounds.
- `parent_refs` must resolve inside the same workspace. Context operations never
  fall back to the shared fact pool: catalog, reads, lineage, search, chunking,
  reduction, and expiry checks enforce the explicit workspace; an archived/reset
  workspace is rejected.
- `ttl_seconds` expires a ref without deleting its audit row. `checksum` can
  be supplied by the caller for integrity verification.

The default storage cap is 16 MiB per context. Reads default to 4000
characters and are capped at 16000; override these operational limits with
`MEMORY_MCP_CONTEXT_MAX_BYTES`, `MEMORY_MCP_CONTEXT_READ_CHARS`, and
`MEMORY_MCP_CONTEXT_MAX_READ_CHARS`.

### v0.12 — context search, chunking, and deterministic reduction

- `search_context` searches context names, metadata, and payloads inside one
  explicit workspace but returns metadata only; callers use `read_context` for
  the bounded payload read.
- `chunk_context` returns a bounded page of numbered chunks with a
  `next_chunk` cursor. The response has its own character cap so a large
  request cannot recreate an unbounded prompt.
- `reduce_context` joins up to 64 existing refs into a new immutable ref,
  records all source refs as lineage, and enforces the normal storage cap. It
  is deterministic concatenation, not semantic model summarization.

### v0.13 — bounded lifecycle capture and typed handoffs

Lifecycle capture and handoffs are additive SQLite seams over the immutable
context store. They do not import transcripts, call a remote server, or use an
LLM to rewrite memory.

- `capture_event` requires an opaque `idempotency_key`, an `event_kind`, an
  exact `workspace`, and a text/JSON `payload`. The server stores a versioned
  envelope behind a `ctx_...` ref, redacts common bearer/API-key/private-key
  forms, and caps the sanitized payload at 64 KiB. A retry with the same key
  and identical envelope is a no-op; a different envelope under that key is
  rejected.
- Capture can be disabled per event (`capture:false`) or excluded by path with
  `exclude_paths`. `.env`, credential/key/certificate files, and SSH private
  key names are excluded by default. `list_events` is metadata-only; use
  `read_event` for a bounded payload slice. The per-workspace spool keeps the
  newest `MEMORY_MCP_LIFECYCLE_MAX_EVENTS` rows (default 1000).
- `handoff_begin` records owner, exact workspace, source, checksum, optional
  session/cwd, and a bounded TTL (default 24 hours, maximum 7 days). The
  payload remains immutable context data. `handoff_accept` is an atomic
  one-shot transition: private handoffs require the exact owner (shared ones
  accept any actor), and a stored cwd must match. `handoff_cancel` is owner-only.
  Expiry transitions open rows to `expired`; terminal rows remain auditable.
- Workspace isolation applies to every event and handoff operation. Hard
  workspace cleanup removes their rows and backup JSON includes both tables.
  No new runtime dependency is required: the implementation uses Python's
  standard library, SQLite, and the existing context APIs.

### v0.16 — absorb, bounded fact retrieval, and code-local provenance (2026-08-22)

The core now supports a safe ingestion boundary for clients that have already
extracted candidate memories:

- `absorb` is a dry-run planner by default. Exact SHA-256 matches are
  `duplicate`; lexical term coverage of at least 0.6 is `related`; remaining
  candidates are `new`. Optional `verify:true` (only with
  `MEMORY_MCP_VERIFY=1`) adds the existing verifier's `update`,
  `supersedes`, and `conflict` signals. No LLM result automatically rewrites
  or invalidates an existing fact.
- `commit:true` is explicit and idempotent at the fact layer. It writes only
  `new` candidates, reuses normal workspace/category/dedup rules, and attaches
  supplied evidence. Duplicate evidence rows are ignored by
  `(fact_id, source_ref)`.
- `chunk_fact` and `search_facts {chunk_chars}` provide bounded chunks with
  character offsets, optional overlap, pagination, and a 64 KiB aggregate
  chunk budget. This keeps retrieval usable for long facts without changing
  BM25 or semantic ranking.
- Evidence anchors are additive and migration-safe. A code-local record can
  point to a repository, immutable ref, path, symbol, line/column range, and
  selected-text hash, with `resolved`, `stale`, or `unresolved` status.

The repository deliberately does not bundle a UI, cloud sync, or a separate
code graph into this stdlib core. No external product is required for the
v0.16 local path; existing provider-backed modules remain opt-in.

The full request/response contract and threat notes are in
[`docs/ingestion-and-provenance.md`](docs/ingestion-and-provenance.md).
Lifecycle events and typed handoffs remain documented in
[`docs/lifecycle-and-handoffs.md`](docs/lifecycle-and-handoffs.md). The
architecture records are
[`docs/decisions/ADR-0001-lifecycle-capture-and-typed-handoffs.md`](docs/decisions/ADR-0001-lifecycle-capture-and-typed-handoffs.md)
and
[`docs/decisions/ADR-0002-local-ingestion-and-code-provenance.md`](docs/decisions/ADR-0002-local-ingestion-and-code-provenance.md).

### v0.17 — focused advisory retrieval and safety boundary (2026-08-23)

Retrieval now has an explicit safety boundary for runtimes that use the
optional server-side recall path:

- `compose_recall`, `search_facts`, `search_semantic`, and `find_precedents`
  accept `purpose: "advisory" | "safety_critical"`. The default is
  `advisory`; `safety_critical` is rejected fail-closed.
- `compose_recall` focuses a complete transcript on the latest user turn and
  removes system reminders, tool/result markers, and older assistant turns
  before selecting bounded candidates. A noise-only transcript produces no
  searchable query.
- Retrieval results are context only. They never authorize registry writes,
  route selection, lock validity, or hash acceptance. Current runtime state
  and local lock/hash checks remain authoritative.
- The public MCP server reports `serverInfo.version = 0.17.0`.

### v0.18 — runs, issue/PR links, anchored queries, and access telemetry (2026-08-23)

Lightweight, additive execution context for runtimes that work issue/task
shaped turns — no new dependencies, no optional modules, no git access from
the server:

- `run_begin {run_id, workspace?, issue_ref?, pr_ref?, session_id?, cwd?,
  source?}` / `run_end {run_id, workspace?, base_sha?, head_sha?,
  files_changed?, diff?, issue_ref?, pr_ref?}` — open/close a run record.
  Git facts are **client-supplied** (the server never shells out to git);
  the diff is capped at 64 KiB with a `diff_truncated` flag and changed
  paths are deduplicated (max 200).
- `link_run {run_id, workspace?, issue_ref?, pr_ref?}` binds an issue/PR
  reference after the fact (at least one ref required; empty keeps the
  existing value). `query_run {run_id?, state?, issue_ref?, limit?}` reads
  one record or a filtered list with clipped diffs.
- `prepare_summary {run_id, workspace?, max_decisions?}` — assembles a
  ready-to-post markdown summary from the run's own records: decisions
  recorded inside its window or bound to its issue_ref, plus the window's
  event catalog. It posts nothing (same boundary as a prepare-comment
  helper).
- `record_decision` and `query_decisions` now carry optional `path`/`symbol`
  code anchors (additive migration), and `query_anchored {path?, symbol?,
  repo?, workspace?, limit?, purpose?}` returns facts (via evidence anchors)
  and decisions bound to a code location. Advisory-only like all retrieval;
  fact texts are clipped to bounded slices.
- **Memory access telemetry**: the main retrieval sites (`search_facts`,
  `search_semantic`, `find_precedents`, `get_provenance`, `query_anchored`)
  and `compose_recall` record one bounded row per pull/push in
  `memory_access_events` (channel, site, query hash, result count, latency —
  never payloads). Retention is capped per workspace
  (`MEMORY_MCP_ACCESS_MAX_EVENTS`, default 5000); `stats` reports the
  log. Recording is best-effort and never breaks retrieval.
- **Post-compaction re-grounding**: `capture_event` accepts
  `event_kind: "post_compact"` — the documented pattern for long sessions is
  to re-run `compose_recall` after a compaction so the window is re-filled
  from the store instead of only the summarizer's output.
- The public MCP server reports `serverInfo.version = 0.18.0`.

The operational contract is documented in
[`docs/ingestion-and-provenance.md`](docs/ingestion-and-provenance.md), and
deployment smoke checks are documented in `DEPLOYMENT.md`.

### v0.19 — live anchor verification, session orientation, and search guard (2026-08-24)

The v0.19 additions remain local, advisory, and dependency-free:

- **Query-time anchor verdicts**: `query_anchored` accepts an optional
  `repo_root`. With a repository-relative path and a stored selection hash it
  returns `STRONG` when the live selection matches, `STALE` when it changed,
  `REBUILT` when the same content is found after a bounded move, and `REMOVED`
  when no replacement is found. Path-only or metadata-only checks are
  `WEAK`. Stored `resolution_status` is preserved; verification is read-only.
- **CI drift gate**: `python3 verify.py --health --root . --repo <repo-id>`
  checks active fact and decision anchors. It exits `1` for `STALE`,
  `REBUILT`, or `REMOVED`, emits bounded JSON with `--json`, and never stores
  source snippets or reads outside the supplied root.
- **First-input orientation**: `auto_orient` invokes `compose_recall` once
  per `session_id`, with at most six hits and a 2.5-second deadline. Timeout,
  disabled recall, and provider failures return an empty advisory block rather
  than blocking the runtime.
- **Search-loop hint**: `search_guard` tracks explicit external `search`
  actions until a `memory` action resets the counter. The default threshold is
  three and the response is always non-blocking.
- **Hit-rate telemetry**: `stats.access` reports pull events, hits, misses,
  overall `hit_rate`, and per-site hit rates. Telemetry remains bounded and
  best-effort.

The public MCP server reports `serverInfo.version = 0.19.0`.

### v0.3 — knowledge graph, decision log, provenance (2026-08-15)

Covers decision rationale, precedent search and evidence lineage with zero
new dependencies — just SQLite + FTS5.

- `remember_entity {name, type?, aliases?}` — upsert entity node
- `remember_relation {subject, predicate, object, source_fact_id?}` — edge
  (entities auto-created; dedup by triple)
- `search_graph {entity, depth? (1-2), limit?}` — BFS neighbors, both directions
- `record_decision {category?, subject?, scenario, reasoning?, outcome?,
  confidence?, decision_maker?, issue_ref?, parent_decision_id?}` — decision node;
  `parent_decision_id` builds causal chains
- `query_decisions {category?, subject?, outcome?, decision_maker?, issue_ref?, limit?}`
- `find_precedents {scenario, category?, limit?, semantic?, purpose?}` — advisory
  similar-decision lookup via FTS BM25 (OR-joined terms, ranked)
- `get_causal_chain {decision_id}` — walk parent links to the root
- `get_provenance {fact_id | sha256}` — fact + evidence rows
- `attach_evidence {fact_id, source_ref, source_checksum?, fetched_at?}` — link a
  fact to a source (dedup by fact_id+source_ref)
- `detect_conflicts {text}` — near-duplicate facts (term coverage ≥ 0.6) +
  decisions with the same subject but >1 distinct outcome

## Schema

`facts(id, sha256, text, source, project, domain,
trust CHECK IN ('high','medium','low'), strong, importance, invalid_at,
superseded_by, confirmed, workspace_id, created_at, updated_at, archived,
last_accessed_at, access_count, revival_count, lifecycle, category_id)`
+ `facts_fts` FTS5 virtual table with insert/delete/update triggers.

v0.3 additions (all additive — `CREATE TABLE IF NOT EXISTS`, existing DBs
migrate in place):

- `entities(id, name, type, aliases, workspace_id, created_at, updated_at,
  UNIQUE(name, workspace_id))`
- `relations(id, subject_id → entities, predicate, object_id → entities,
  source_fact_id?, created_at, UNIQUE(subject_id, predicate, object_id))`
- `decisions(id, category, subject, scenario, reasoning, outcome, confidence,
  decision_maker, issue_ref, parent_decision_id?, created_at, updated_at)`
  + `decisions_fts` FTS5 (scenario/reasoning/category) with triggers
- `evidence(id, fact_id → facts, source_ref, source_checksum, fetched_at,
  created_at, UNIQUE(fact_id, source_ref))`

v0.10 additions (additive — `_migrate_categories` adds `facts.category_id`
to existing DBs):

- `categories(id, name, workspace_id, created_at, updated_at,
  UNIQUE(name, workspace_id))`
- `facts.category_id → categories(id) ON DELETE SET NULL` (fresh/rebuild DDL;
  plain column on migrated stores)

v0.11 additions (additive — existing stores create these tables on open):

- `contexts(ref, name, content, schema_json, source, sha256, workspace_id,
  created_at, expires_at, size_bytes)`
- `context_lineage(parent_ref → contexts, child_ref → contexts, relation,
  workspace_id, created_at)`

v0.13 additions (additive — existing stores create these tables on open):

- `lifecycle_events(workspace_id, idempotency_key, event_kind, event_id,
  context_ref → contexts, sha256, payload_bytes, created_at)` with a unique
  `(workspace_id, idempotency_key)` and bounded retention.
- `handoffs(ref, context_ref → contexts, owner, session_id, cwd, source,
  sha256, workspace_id, state, expires_at, accepted_at, cancelled_at)` with
  one-shot state transitions and workspace-scoped optional idempotency.

v0.18 additions (additive — existing stores migrate in place):

- `decisions.path` / `decisions.symbol` — optional code anchors
  (`_migrate_decisions_anchors` adds the columns to existing stores)
- `runs(run_id, issue_ref, pr_ref, session_id, cwd, source, base_sha, head_sha,
  files_changed, diff, diff_truncated, state, workspace_id, created_at,
  ended_at)` with `UNIQUE(workspace_id, run_id)`
- `memory_access_events(workspace_id, channel, site, query_hash, result_count,
  latency_ms, created_at)` — bounded per-workspace retrieval log

v0.19 additions are schema-free: query-time anchor verdicts, first-input
orientation, search-guard counters, and hit-rate fields use the existing
evidence and access-log rows plus bounded in-process runtime state.

## Environment

- `MEMORY_MCP_DB` — SQLite path. Default is **script-relative**: `<repo>/data/facts.db`
  (portable — clone the repo anywhere and it works out of the box; `data/` is gitignored).
  The deployment stack always sets it explicitly: host wrapper
  (`~/.local/bin/memory-mcp`) → the shared host store,
  docker runtimes → `/opt/memory-shared/facts.db`.
- `MEMORY_MCP_CONTEXT_MAX_BYTES` — maximum UTF-8 payload size per context
  (default 16 MiB).
- `MEMORY_MCP_CONTEXT_READ_CHARS` — default `read_context` slice size
  (default 4000).
- `MEMORY_MCP_CONTEXT_MAX_READ_CHARS` — hard maximum `read_context` slice
  size (default 16000).
- `MEMORY_MCP_CONTEXT_MAX_LINEAGE` — maximum parent or child refs returned by
  one metadata/read response (default 100).
- `MEMORY_MCP_CONTEXT_MAX_CHUNKS` — maximum chunks returned by one
  `chunk_context` request (default 32).
- `MEMORY_MCP_CONTEXT_MAX_CHUNK_RESPONSE_CHARS` — aggregate character cap for
  one `chunk_context` response (default 65536).
- `MEMORY_MCP_LIFECYCLE_MAX_EVENTS` — newest lifecycle events retained per
  workspace (default 1000).
- `MEMORY_MCP_LIFECYCLE_MAX_PAYLOAD_BYTES` — sanitized event payload cap
  (default 65536).
- `MEMORY_MCP_LIFECYCLE_MAX_FIELD_CHARS` / `_MAX_PATH_CHARS` — metadata field
  caps (defaults 256 / 1024).
- `MEMORY_MCP_HANDOFF_DEFAULT_TTL` / `_MAX_TTL` — handoff TTL defaults and
  hard maximum in seconds (defaults 86400 / 604800).
- `MEMORY_MCP_HANDOFF_MAX_CONTENT_BYTES` — handoff payload cap (default 262144).
- `MEMORY_MCP_RUN_MAX_FIELD_CHARS` / `_MAX_FILES` / `_MAX_DIFF_BYTES` — run
  field caps and the client-supplied diff cap (defaults 256 / 200 / 65536).
- `MEMORY_MCP_ACCESS_MAX_EVENTS` — newest memory-access telemetry rows kept
  per workspace (default 5000).
- `MEMORY_MCP_ANCHOR_MAX_FILES` / `_MAX_BYTES` — bounds for moved-anchor
  verification scans (defaults 2000 files / 32 MiB).
- `MEMORY_MCP_AUTO_ORIENT_MAX_CHARS` — maximum orientation block budget
  (default 1400; timeout is fixed at 2.5 seconds and hits at 6).
- `MEMORY_MCP_SEARCH_GUARD_THRESHOLD` — default warning threshold (3).
- `MEMORY_MCP_RUNTIME_STATE_MAX_SESSIONS` — in-process orientation/guard
  session-state cap (default 1024).
- `MEMORY_MCP_LLM_KEY` / `MEMORY_MCP_EMBED_KEY` — bearer tokens for
  OpenAI-compatible providers. Credential-bearing plaintext HTTP is rejected
  unless `MEMORY_MCP_ALLOW_INSECURE_HTTP=1` is explicitly set.
- `MEMORY_MCP_ALLOW_INSECURE_HTTP` — explicit opt-in for plaintext HTTP when a
  provider requires a bearer token; prefer HTTPS.
- Journal mode WAL, busy_timeout 5000 (multi-writer: host + containers).

## Integration

- Host: wrapper script → this script; register it as an MCP server in your
  runtime's config (e.g. `[mcp_servers.memory-mcp]` in a Codex-style
  `config.toml`).
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
(frontmatter + body) into the shared store (Phase 3). Facts are scoped to
the project workspace (`workspace = project slug`, not the shared pool).
All paths are
env-overridable and default to portable values: `MEMORY_MIGRATE_SRC`
(auto-discovered first `<project>/memory` under `~/.reasonix/projects/`,
symlinked project dirs skipped), `MEMORY_MIGRATE_PROJECT` (derived from the
project dir name), `MEMORY_MCP_CMD` (`memory-mcp` via PATH), `MEMORY_MCP_DB`
(XDG-style `~/.local/share/memory-mcp/facts.db` — propagated to the server
only when explicitly set, so a host wrapper pin is never overridden;
resolved source/server/target are printed before writing).

## Agent skill

- `skills/memory-mcp/SKILL.md` — agent-facing playbook for the MCP tools:
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
  `compose_recall` (recall.py) returns a ready-to-inject advisory block, so the
  client only inserts it. It sanitizes transcript input and leads retrieval
  with the latest user intent instead of system/tool noise.
- **Truth verification** — `trust`/`strong` are client-set metadata and query
  filters, not a verification or protection mechanism. Server-side LLM
  extraction treats model authority claims as unconfirmed: `trust=high` is
  stored as `medium`, `strong=true` is stored as `false`, and `confirm_fact`
  is required before a fact can become high-trust. They never turn memory into
  an authorization source.
- **Semantic search** — optional module (`embeddings.py`, gated by
  `MEMORY_MCP_EMBEDDINGS=1`): `search_semantic` + hybrid `search_facts
  semantic=true` (RRF merge). The core stays lexical (FTS5 + BM25) with zero
  dependencies; embeddings add a provider dependency only when enabled.
  `find_precedents` OR-joins terms on purpose: precedent lookup is about
  similarity, and BM25 ranks partially matching decisions.
- **Near-duplicate handling** — writes dedup on exact text (sha256).
  Paraphrased facts stay separate records; `detect_conflicts` surfaces
  near-duplicates (term coverage ≥ 0.6) on demand, while `absorb` exposes the
  same signal during a dry-run and never auto-merges it.


## Server-side pipeline (optional modules)

Same env-gate pattern as embeddings — the core stays stdlib-only; these
activate only when set, and every failure degrades to store-only.

- **Extraction** (`MEMORY_MCP_EXTRACT=1`): `ingest_turn {transcript,
  session_ref?, project?, domain?}` sends the transcript to the LLM provider
  (see llm.py; ollama/openai/test) and stores extracted facts with provenance
  (`attach_evidence`). Model output is always an unconfirmed candidate:
  `trust=high` is downgraded to `medium` and `strong=true` to `false` until
  `confirm_fact` is called after human review. Minimum transcript length:
  `MEMORY_MCP_EXTRACT_MIN_CHARS` (default 800). When `MEMORY_MCP_VERIFY=1`,
  new facts are cross-checked and superseded ones archived.
- **Recall assembly** (`MEMORY_MCP_RECALL=1`): `compose_recall {turn_text,
  limit?, chars?, semantic?, purpose?}` returns a ready-to-inject advisory
  `<memory-recall>` block (authoritative + background tiers,
  reasonix-compatible format); transcript noise is removed before retrieval and
  `purpose: "safety_critical"` is rejected;
  `sweep_freshness {workspace?}` archives facts past their type's hard window
  (strong facts kept): reference 45d, user/feedback 365d, project 180d.
  Archived/reset workspaces are rejected before any shared-pool update.
- **Verification** (`MEMORY_MCP_VERIFY=1`): `verify_facts {text}` LLM
  cross-checks a candidate against the store (conflicts/supersessions).
  `check_new_facts` (ingestion hook) archives superseded old facts
  (graphiti-style invalidation) only on high-confidence verdicts
  (`MEMORY_MCP_VERIFY_MIN_CONFIDENCE`, default 0.8) and attaches
  `supersedes:<old_id>` evidence to the new one.
- **LLM provider** (shared by extract/verify/categorize): `MEMORY_MCP_LLM_PROVIDER`
  (ollama|openai|test), `_URL`, `_MODEL` (ollama default qwen2.5:14b), `_KEY`,
  `_TIMEOUT` (default 60s). `MEMORY_MCP_CATEGORIZE=1` enables
  `categorize_pending` — the v0.10 LLM batch that assigns topic categories to
  uncategorized facts (rule-based categories from `remember_fact` are the
  instant fallback and need no env).

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
