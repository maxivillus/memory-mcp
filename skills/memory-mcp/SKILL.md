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

Available to runtime agents as the `mcp__memory-mcp__*` tools (24 tools).
One shared store across all runtimes: a fact or decision written in one
session is visible to every later session.

## Facts — remember_fact / add_fact / search_facts / search_semantic / list_facts / summarize_index / forget_fact

- `ingest_turn {transcript, session_ref?}` — server-side extraction: send a
  conversation transcript, the server's LLM provider extracts durable facts
  and stores them with provenance (when enabled).
- `compose_recall {turn_text}` — returns a ready-to-inject `<memory-recall>`
  block (server-side scoring); `sweep_freshness` archives stale facts.
- `verify_facts {text}` — LLM cross-check for contradictions/supersessions
  before writing; superseded facts are archived on high-confidence verdicts.

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

## Conventions

- Use consistent `project`/`domain` scopes so queries and the index stay
  clean; never pollute the shared store with test data (use a temp DB for
  experiments).
- The store is a shared read-model: agents may write facts/decisions/evidence,
  but must not delete or mutate records owned by another agent without a
  strong reason.

