# Deploying memory-mcp in a docker runtime

memory-mcp is a stdio MCP server: the agent runtime spawns it as a child
process and talks newline-delimited JSON-RPC over stdin/stdout. In a docker
stack the server files are bind-mounted read-only and one shared SQLite DB is
bind-mounted read-write, so every runtime (host CLI, daemons, workers) reads
and writes the same fact store.

This guide uses a **codex-style runtime container** as the example; the same
pattern applies to any runtime that can spawn a process.

The v0.16 ingestion, bounded fact retrieval, and code-local provenance paths,
plus the v0.17 focused advisory retrieval boundary, the v0.18 runs/telemetry,
the v0.19 live anchor checks and runtime policy helpers, and the v0.22 bounded
profiles, local document adapter, entity normalization, and feedback paths,
run inside the existing local
SQLite-backed server. They do not require a UI, cloud service, separate code
graph, or another external product. The detailed request flow is in
`docs/ingestion-and-provenance.md`; the optional runtime adapters below
describe pre-existing deployment choices and are not required for the
local-only path.

## 1. Compose service

```yaml
services:
  codex-daemon:
    image: codex-daemon:local
    volumes:
      # server files (read-only) — clone of this repo, e.g. ./memory-mcp
      - ./memory-mcp:/opt/memory-mcp:ro
      # shared DB dir (read-write, WAL needs write access even for reads)
      # v0.6: named databases/ and backups/ are created here automatically
      - ./memory-shared:/opt/memory-shared
    environment:
      MEMORY_MCP_CMD: /opt/memory-mcp/memory_mcp.py
      MEMORY_MCP_DB: /opt/memory-shared/facts.db
      # optional semantic search: enable + point at an embeddings provider
      MEMORY_MCP_EMBEDDINGS: "1"
      MEMORY_MCP_EMBED_PROVIDER: ollama
      MEMORY_MCP_EMBED_URL: http://ollama:11434
      # optional server-side pipeline: extraction / recall / verification
      MEMORY_MCP_EXTRACT: "1"
      MEMORY_MCP_RECALL: "1"
      MEMORY_MCP_VERIFY: "1"
      # v0.10: LLM batch category refinement for uncategorized facts
      # (categorize_pending; provider shared with extract/verify)
      MEMORY_MCP_CATEGORIZE: "1"
      MEMORY_MCP_LLM_PROVIDER: ollama
      MEMORY_MCP_LLM_URL: http://ollama:11434
      MEMORY_MCP_LLM_MODEL: qwen2.5:14b
      # v0.13: bounded local lifecycle spool and typed handoff limits
      MEMORY_MCP_LIFECYCLE_MAX_EVENTS: "1000"
      MEMORY_MCP_LIFECYCLE_MAX_PAYLOAD_BYTES: "65536"
      MEMORY_MCP_HANDOFF_DEFAULT_TTL: "86400"
      MEMORY_MCP_HANDOFF_MAX_TTL: "604800"
      # reasonix runtimes: read+write the shared store (dual-write/dual-read)
      REASONIX_MEMORY_MCP: "1"
```

Notes:

- The DB dir must be writable by the container uid (e.g. `1001` for a
  codex-style user). Multiple containers can share the same DB — SQLite WAL +
  `busy_timeout 5000` handle multi-writer.
- The server creates `backups/` with mode `0700`, writes database/JSON backup
  files with mode `0600`, and publishes them atomically. Keep the mounted DB
  directory private even when backups are not enabled.
- `MEMORY_MCP_DB` is mandatory in containers: the script's default is
  script-relative and would point at the read-only mount.

## 2. Verify from inside the container

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"summarize_index","arguments":{"limit":5,"max_chars":2000}}}' \
  | docker exec -i \
      -e MEMORY_MCP_DB=/opt/memory-shared/facts.db \
      codex-daemon /opt/memory-mcp/memory_mcp.py
```

Expect `{"result":{"content":[{"type":"text","text":"{...\"index\":...}"}]}}`
with one `#<id> trust [category] [domain] text` line per fact (v0.10:
`[category]` tags are included when a fact has one).

When `MEMORY_MCP_RECALL=1`, also verify the advisory retrieval boundary with
`compose_recall`. A normal request may use `purpose: "advisory"`; a request
with `purpose: "safety_critical"` must return an error payload with
`code: "advisory_only"` and `fail_closed: true`. A complete transcript is
focused on its latest user turn before candidate retrieval. System reminders,
tool/result markers, and older assistant turns must not expand the bounded
candidate query. Do not use any memory result as authority for writes, routes,
locks, or hashes.

For a checkout-mounted CI or release gate, verify code-local anchors against
the mounted repository without starting an MCP session:

```sh
python3 /opt/memory-mcp/verify.py --health \
  --root /opt/memory-mcp --repo <repo-id> --json
```

The command returns exit `1` for `STALE`, `REBUILT`, or `REMOVED` anchors and
exit `0` when no drift is found. `WEAK` path-only anchors are reported but do
not fail the gate. The scan is bounded and stays under the supplied root.

Runtime clients may call `auto_orient` once with their stable `session_id` at
the first user input. It is capped at six hits and 2.5 seconds, and provider
or recall failures degrade to an empty advisory block. If the client exposes
external grep/search actions, call `search_guard` with `action: "search"` and
call it with `action: "memory"` after a memory lookup; the warning is advisory
and never blocks a tool call.

For v0.22 retrieval, clients may pass `profile: "orientation"`,
`"implementation"`, `"review"`, or `"incident"` to the retrieval tools. The
profiles only select bounded defaults; `balanced` remains the default and all
retrieval stays advisory. A profile cannot authorize writes, routes, locks,
hashes, or safety-critical operations.

For v0.23 admission and retrieval handling, clients may pass
`admission: "strict"` to `remember_fact` or `absorb`. Strict candidates need
bounded `evidence[].selected_text`; the snippet is checked transiently and
only its hash/metadata is stored. Rejected candidates are not written. Empty
retrieval responses keep `result_status: "empty"` and add typed
`retrieval_outcome: "abstained"`, `abstention_reason`, and `remedy` fields.
Fact writes are capped by `MEMORY_MCP_FACT_MAX_TEXT_CHARS` (default 16000).
Normal fact search also bounds legacy rows and marks clipped results with
`text_truncated` and `text_length`; use `chunk_fact` for explicit pagination.
`export_rdf.limit` counts complete source records and returns `truncated` when
additional records remain.

For a local document, first preview it with `commit:false` (or omit `commit`):

```json
{"root":"/opt/project","path":"docs/guide.md","workspace":"smoke","commit":false}
```

Then explicitly repeat with `commit:true` only after reviewing the returned
path, byte count, document SHA-256, and chunk count. The adapter reads one
UTF-8 relative file, rejects traversal and symlink escapes, excludes common
secret/binary paths, stores chunks in the existing context table, and never
stores the supplied root. Use a disposable workspace for smoke checks; do not
point it at credentials, production databases, or private key material.

Clients that display or consume retrieval results may send aggregate feedback
with `record_feedback` and inspect it with `query_feedback`. Supply only an
opaque item reference, a fixed signal, and optionally a SHA-256 query hash;
raw queries, notes, and payloads are rejected. Feedback is bounded evidence,
not an authorization or ranking signal.

For the v0.13 seams, use a disposable workspace and send an event with a
stable idempotency key, then accept a short handoff as the same owner:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"capture_event","arguments":{"workspace":"smoke","idempotency_key":"smoke-event-1","event_kind":"session_start","payload":{"status":"ok"}}}}' \
  '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"handoff_begin","arguments":{"workspace":"smoke","owner":"smoke-agent","content":"bounded smoke handoff","ttl_seconds":60}}}' \
  | docker exec -i -e MEMORY_MCP_DB=/opt/memory-shared/facts.db \
      codex-daemon /opt/memory-mcp/memory_mcp.py
```

The event response contains metadata only. Record the returned handoff ref and
call `handoff_accept {handoff_ref, actor: "smoke-agent", workspace: "smoke"}`
in the client that owns the session. Do not use real credentials or production
payloads in this smoke check. The server adds no Python package dependency;
the existing stdlib/SQLite runtime is sufficient.

## 3. Register as an MCP server (MCP-native runtimes)

For runtimes that consume MCP servers from config (codex, jcode, claude):

```toml
# ~/.codex/config.toml
[mcp_servers.memory-mcp]
command = "/opt/memory-mcp/memory_mcp.py"
env = { MEMORY_MCP_DB = "/opt/memory-shared/facts.db" }
```

## 4. reasonix runtimes (optional integration)

`REASONIX_MEMORY_MCP=1` (+ `MEMORY_MCP_CMD`, `MEMORY_MCP_DB`) makes reasonix:

- **write** — sync auto-extracted facts into the shared store (best-effort);
- **read** — swap the prompt index for the capped `summarize_index` and merge
  per-turn `search_facts` results into automatic recall (dual-read; native
  facts win on duplicate text). These results are advisory context only and
  must not choose routes or authorize writes, locks, or hashes. All failures
  degrade silently to native-only.

## 5. Run the tests

```sh
cd <repo>
MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -v
python3 -m unittest -v test_memory_mcp.py
```

Zero external dependencies (stdlib only).
