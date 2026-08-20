# Lifecycle capture and typed handoffs

This document describes the v0.13 public seams for bounded runtime lifecycle
capture and one-shot handoffs. Both features are local SQLite operations in the
existing `memory-mcp` process. They do not send data to a remote service,
import a transcript, or ask an LLM to rewrite a context.

## Lifecycle events

`capture_event` accepts one envelope:

```json
{
  "workspace": "project-id",
  "idempotency_key": "session-1:tool-7:post",
  "event_kind": "post_tool_use",
  "session_id": "session-1",
  "source": "runtime",
  "path": "src/module.py",
  "payload": {"tool": "pytest", "result": "passed"}
}
```

`workspace`, `idempotency_key`, `event_kind`, and `payload` are required.
`event_kind` is normalized to lowercase hyphenated form, so
`post_tool_use` is stored as `post-tool-use`. `payload` may be text or any
JSON-serializable value. `content` is accepted as a text-payload alias by the
implementation.

The server serializes a versioned envelope into an immutable `ctx_...` context
and returns metadata with `event_ref`/`context_ref`; it does not echo the
payload from `capture_event`. `read_event` is the bounded payload endpoint and
`list_events` is metadata-only. Reads require the same exact workspace used for
the write.

### Idempotency and retention

The idempotency key is unique within a workspace. Retrying the same key with
the same sanitized envelope returns the original event ref and does not create
a second context. Reusing it with different data is rejected. Different
workspaces may independently use the same key.

The local spool keeps the newest
`MEMORY_MCP_LIFECYCLE_MAX_EVENTS` events per workspace (default `1000`). The
oldest event contexts are removed when the cap is exceeded. Each payload is
limited to `MEMORY_MCP_LIFECYCLE_MAX_PAYLOAD_BYTES` UTF-8 bytes (default
`65536`), and metadata fields are bounded independently. This is a durable,
bounded local spool, not an archival transcript.

### Sanitization and capture exclusions

Before storage, common bearer tokens, API-key/token/password assignments,
known provider token prefixes, cloud access-key IDs, and PEM private-key blocks
are replaced with `<redacted>`. The checksum in the event metadata covers the
sanitized envelope, not the caller's pre-redaction input.

Capture may be disabled with `capture: false`. A path matching a caller-supplied
`exclude_paths` glob is ignored without creating a row. The default exclusions
cover `.env` files, credential/secret names, SSH private-key names, and common
certificate/key extensions. A caller should still omit sensitive data at the
source; idempotency keys and context sources are opaque metadata and must not
contain secrets.

Event payloads are data. A consumer must not execute, evaluate, or treat an
event payload as an instruction merely because it contains instruction-shaped
text.

## Typed handoffs

`handoff_begin` stores an immutable context and a typed lifecycle row:

```json
{
  "workspace": "project-id",
  "owner": "agent-a",
  "session_id": "session-1",
  "cwd": "repo",
  "source": "issue/668",
  "content": "bounded handoff data",
  "checksum": "<sha256 of content>",
  "ttl_seconds": 3600,
  "idempotency_key": "handoff-1"
}
```

The owner and exact workspace are mandatory. The source and SHA-256 are kept
in the context and handoff metadata for audit. Handoff content is bounded by
`MEMORY_MCP_HANDOFF_MAX_CONTENT_BYTES` (default `262144`). TTL defaults to
`MEMORY_MCP_HANDOFF_DEFAULT_TTL` (24 hours) and cannot exceed
`MEMORY_MCP_HANDOFF_MAX_TTL` (7 days). A zero TTL is valid and expires before
acceptance.

`handoff_accept` requires `handoff_ref`, `actor`, and the same workspace. A
private handoff can be accepted only by its owner. A `shared` handoff can be
accepted by any named actor in that exact workspace. If `cwd` was recorded, the
accept request must provide the identical value. The state transition and
bounded payload read happen under one SQLite write transaction, and the
handoff becomes `accepted` before the response is returned.

`handoff_cancel` is owner-only. `open` is the only cancellable or acceptable
state; subsequent calls return the terminal state. Expired rows transition to
`expired` on list/accept/cancel access and remain as metadata for audit. The
context itself also carries the same expiry, so a stale ref cannot be read
through `read_context`.

## Operations and recovery

- Workspace reset/archive blocks event and handoff reads/writes until the
  workspace is reactivated with `create_workspace`.
- Hard workspace reset/archive removes lifecycle rows, handoff rows, and their
  context artifacts in the same FK-safe transaction as existing memory data.
- `backup_workspace` includes metadata and payload rows for both new tables;
  treat backup files as sensitive local data and protect the backup directory.
- No package, network, model, or deployment dependency was added. Existing
  SQLite WAL and `busy_timeout` settings cover concurrent runtime writers.

The source of truth for the architecture decision is
`docs/decisions/ADR-0001-lifecycle-capture-and-typed-handoffs.md`.
