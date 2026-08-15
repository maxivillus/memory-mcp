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
- `stats {}` — totals by trust/domain
- `export {}` — all facts incl. archived (migration/backup)

## Schema

`facts(id, sha256 UNIQUE, text, source, project, domain,
trust CHECK IN ('high','medium','low'), strong, created_at, updated_at, archived)`
+ `facts_fts` FTS5 virtual table with insert/delete/update triggers.

## Environment

- `MEMORY_MCP_DB` — SQLite path (default `/home/<user>/shared-store/facts.db`)
- Journal mode WAL, busy_timeout 5000 (multi-writer: host + containers)

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
(frontmatter + body) into the shared store (Phase 3).
