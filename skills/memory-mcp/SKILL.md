---
name: memory-mcp
description: >-
  Use for durable cross-session agent memory: store and retrieve facts, record
  decisions with rationale for precedent lookup, link evidence to facts,
  search the entity graph, and detect conflicting outcomes — via the
  memory-mcp MCP tools (shared SQLite+FTS5 store).
metadata:
  author: reasonix
  version: "1.0"
---

# Shared Agent Memory (memory-mcp)

Available to runtime agents as the `mcp__memory-mcp__*` tools (44 tools).
One shared store across all runtimes: a fact or decision written in one
session is visible to every later session.

## Facts — remember_fact / add_fact / search_facts / search_semantic / list_facts / summarize_index / forget_fact

- `ingest_turn {transcript, session_ref?}` — server-side extraction: send a
  conversation transcript, the server's LLM provider extracts durable facts
  and stores them with provenance (when enabled).
- `compose_recall {turn_text}` — returns a ready-to-inject `<memory-recall>`
  block (server-side scoring); `sweep_freshness` archives stale facts.
- `verify_facts {text}` — LLM cross-check for contradictions/supersessions
  before writing; superseded facts are invalidated (bi-temporal) on
  high-confidence verdicts — history stays via `fact_history {id}`.
- `review_pending` / `confirm_fact` — human-in-the-loop: list unconfirmed
  facts and mark them verified (confirmed=1, trust=high).
- `remember_fact {importance}` — 0..1 value for retention; low-importance
  stale facts are archived by `sweep_freshness`, strong/confirmed never.
- `consolidate {ids}` — LLM-merge of paraphrased facts into one (old versions
  stay in `fact_history`); strong/confirmed never merged.
- `facts_for_session {session_ref}` / `list_sessions` — session-scoped views;
  `compose_recall {session_expand}` adds same-session context.
- `compose_recall {graph=true}` — entity-graph as a third recall source.

- `remember_fact {text, source?, project?, domain?, trust?, strong?}` — store a
  durable fact (upsert, dedup by sha256). Use `strong=true` for
  user-confirmed facts, `trust=high` for verified facts, default `medium`.
- Before researching something, `search_facts` the store first — a fresh
  distinctive fact can skip heuristic research (fact gate).
- `search_facts` with `semantic=true` merges lexical (FTS5/BM25) and embedding
  rankings (RRF); `search_semantic` is pure embedding search — use it for
  paraphrased or cross-language recall when embeddings are enabled.
- `summarize_index` gives a compact freshest-first index for prompt budgets.
- `forget_fact` soft-deletes (archives) an obsolete fact.

## Decisions — record_decision / query_decisions / find_precedents / get_causal_chain

- `record_decision {category?, subject?, scenario, reasoning?, outcome?,
  confidence?, decision_maker?, issue_ref?, parent_decision_id?}` — record a
  consequential choice: scenario = the situation, reasoning = why, outcome =
  what was chosen. Pass `parent_decision_id` when this decision follows from
  an earlier one (builds causal chains).
- `find_precedents {scenario, category?}` — before deciding, look up similar
  past scenarios; ranked precedents come back via BM25. Treat them as
  evidence, not authority — verify against the current card.
- `query_decisions` filters by category/subject/outcome/maker/issue_ref;
  `get_causal_chain` walks parent links from a decision to its root.

## Graph — remember_entity / remember_relation / search_graph

- Link entities (services, components, people, issues) with
  subject-predicate-object edges via `remember_relation` (entities
  auto-create; triples dedup).
- `search_graph {entity, depth 1-2}` returns neighbors in both directions.

## Provenance — attach_evidence / get_provenance

- `attach_evidence {fact_id, source_ref, source_checksum?}` links a fact to
  its source (card/comment/run reference + checksum); `get_provenance`
  returns the fact with its evidence. Use when a conclusion must be
  traceable back to a source.

## Conflicts — detect_conflicts

- Before overwriting a conclusion, run `detect_conflicts {text}`: it returns
  near-duplicate facts (term coverage >= 0.6) and same-subject decisions with
  divergent outcomes. Flag conflicts instead of silently overwriting.

## Workspace scoping

- One shared store, per-project isolation: pass `workspace=<project_id>` on
  every read/write tool (remember_fact, search_facts, list_facts,
  summarize_index, facts_for_session, review_pending, compose_recall,
  find_precedents, record_decision, query_decisions, ingest_turn,
  verify_facts, forget_fact, confirm_fact, fact_history,
  get_provenance, fact_references, attach_evidence, detect_conflicts,
  consolidate, export_facts, export_rdf, stats, list_sessions). Resolve it
  from your task context (the project id of the card/issue you work on).
- A scoped query sees YOUR project + the shared pool; an unscoped query sees
  only the shared pool (legacy facts). `remember_fact` warns when
  `workspace` is missing — always pass it.

## Database & workspace management (v0.6)

Separate databases are extra SQLite stores for isolation or migration;
workspaces are named access scopes in the active store. The active store
(MEMORY_MCP_DB) can be backed up but never archived or deleted through
these tools. Soft operations are reversible (facts get archived=1, data is
kept); hard mode physically deletes and requires confirm: true.

- create_database {name} — new named database (separate SQLite file under
  databases/); list_databases shows active + named + archived.
- backup_database {name?} — online backup to backups/ (default: the active
  store; a named or archived database can be backed up too).
- archive_database {name, hard?, confirm?} — soft: rename to
  <name>.db.archived (reversible); hard: true deletes the file (requires
  confirm: true). Refuses to clobber an existing archive.
- delete_database {name, confirm: true} — permanent file delete.
- create_workspace {workspace} — register a workspace (idempotent;
  re-registering reactivates an archived/reset one); list_workspaces shows
  status + active fact counts.
- reset_workspace {workspace, hard?, confirm?} / archive_workspace
  {workspace, hard?, confirm?} — soft: hide the whole workspace (facts get
  archived=1; graph/decisions/evidence become unreadable and unwritable);
  hard: true purges facts, evidence, graph and decisions in one transaction
  (per-table counts in the response; requires confirm: true). Reactivate an
  archived/reset workspace with create_workspace before writing again.
- backup_workspace {workspace} — JSON export of all its facts (incl.
  archived) to backups/workspace-<name>-<ts>.json.
- Names are validated: 1-64 chars of [A-Za-z0-9._-], no '..' — never pass
  unvalidated input to the file-touching tools.

## Automatic decay (v0.7)

Facts age only on ACTIVE days — days with at least one memory-mcp call
(activity_days table) — so user downtime (no sessions, no calls) never ages
them. Score = importance x 0.95^active_days since the last search hit.

- active (score >= 0.25): normal participant in search/recall.
- degraded (score < 0.25): hidden from plain search results; still reachable
  through entity-graph/session chains; returns to active after 3 matching
  searches (attempts to remember). Do not expect it in search until revived.
- forgotten (score <= 0.1): excluded everywhere — plain search AND
  graph/session chains; see it only via list_forgotten, bring it back
  with restore_fact.
- strong and confirmed facts never decay.
- decay_sweep runs the lifecycle recompute (manually or by cron); search
  hits refresh last_accessed_at on active facts only.

## Conventions

- Use consistent `project`/`domain` scopes so queries and the index stay
  clean; never pollute the shared store with test data (use a temp DB for
  experiments).
- The store is a shared read-model: agents may write facts/decisions/evidence,
  but must not delete or mutate records owned by another agent without a
  strong reason.




