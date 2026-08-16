# memory-mcp

Shared fact-memory MCP server (stdio, JSON-RPC 2.0, newline-delimited) for
reasonix / jcode / codex runtimes. SQLite + FTS5 storage; one fact store
shared across the host and docker runtimes via bind-mount.

Replaces the per-runtime memory storage with a single searchable store:
extraction/gating/injection stay client-side (reasonix memory patches).

## Tools

- `remember_fact {text, source?, project?, domain?, trust?, strong?}` — upsert
  (dedup by sha256 of text)
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

- `skills/memory-mcp/SKILL.md` — agent-facing playbook for the 17 MCP tools:
  when to search facts before researching, how to record decisions with
  rationale for precedent lookup, graph/provenance/conflict usage, and shared-
  store conventions. Compatible with the `SKILL.md` format used by agent skill
  collections; copy it into your agent's skill directory (or point discovery
  at this repo).
