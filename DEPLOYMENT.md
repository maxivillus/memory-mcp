# Deploying memory-mcp in a docker runtime

memory-mcp is a stdio MCP server: the agent runtime spawns it as a child
process and talks newline-delimited JSON-RPC over stdin/stdout. In a docker
stack the server files are bind-mounted read-only and one shared SQLite DB is
bind-mounted read-write, so every runtime (host CLI, daemons, workers) reads
and writes the same fact store.

This guide uses a **codex-style runtime container** as the example; the same
pattern applies to any runtime that can spawn a process.

## 1. Compose service

```yaml
services:
  codex-daemon:
    image: codex-daemon:local
    volumes:
      # server files (read-only) — clone of this repo, e.g. ./memory-mcp
      - ./memory-mcp:/opt/memory-mcp:ro
      # shared DB dir (read-write, WAL needs write access even for reads)
      - ./memory-shared:/opt/memory-shared
    environment:
      MEMORY_MCP_CMD: /opt/memory-mcp/memory_mcp.py
      MEMORY_MCP_DB: /opt/memory-shared/facts.db
      # optional semantic search: enable + point at an embeddings provider
      MEMORY_MCP_EMBEDDINGS: "1"
      MEMORY_MCP_EMBED_PROVIDER: ollama
      MEMORY_MCP_EMBED_URL: http://ollama:11434
      # reasonix runtimes: read+write the shared store (dual-write/dual-read)
      REASONIX_MEMORY_MCP: "1"
```

Notes:

- The DB dir must be writable by the container uid (e.g. `1001` for a
  codex-style user). Multiple containers can share the same DB — SQLite WAL +
  `busy_timeout 5000` handle multi-writer.
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
with one `#<id> trust [domain] text` line per fact.

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
  facts win on duplicate text). All failures degrade silently to native-only.

## 5. Run the tests

```sh
cd <repo>
python3 -m unittest discover -s tests -v
```

Zero external dependencies (stdlib only).
