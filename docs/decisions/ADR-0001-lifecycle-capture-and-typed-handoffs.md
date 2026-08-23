# ADR-0001: Lifecycle capture and typed handoffs over context artifacts

- Status: Proposed (PM ratification required)
- Date: 2026-08-20
- Scope: `memory-mcp` product repository, v0.13

## Context

The existing server already provides immutable, workspace-scoped context
artifacts with bounded reads, checksums, TTL, and lineage. Runtime lifecycle
signals still had no durable local seam, and handoffs had no typed ownership or
one-shot state transition. Adding a second store or a remote coordinator would
duplicate the existing ACL and retention behavior.

## Decision

1. Store lifecycle envelopes as immutable context artifacts and add a small
   `lifecycle_events` index for event metadata and workspace-scoped idempotency.
2. Keep lifecycle capture bounded by payload/field limits and a per-workspace
   newest-row cap. Redact common credential forms and support caller/default
   path exclusions before writing.
3. Store typed handoffs as immutable contexts plus a `handoffs` state row. Keep
   owner, exact workspace, source, checksum, optional session/cwd, and bounded
   expiry in the row. Accept and cancel are atomic one-shot transitions.
4. Reuse the existing SQLite connection, WAL mode, context ACL checks, hard
   cleanup, and backup surfaces. No new runtime dependency or remote service is
   introduced.

## Security and privacy constraints

- Every new data-plane operation requires an exact workspace and rejects
  archived/reset workspaces.
- Lifecycle payloads are sanitized and bounded before persistence. Metadata-only
  catalog calls never return payload content.
- Handoff content remains data, not executable instructions. Handoff consumers
  must use the bounded response and preserve the checksum/source metadata.
- Backups contain payloads and must be treated as sensitive local artifacts.

## Deliberately excluded scope

This decision does not add a remote memory server, LLM-driven auto-improvement,
automatic transcript/workstream import, or a replacement for the existing
fact/context store. Those are separate decisions.

## Consequences

Positive:

- Runtime events and handoffs have stable, auditable refs without a new
  coordination service.
- Retries are safe, payload growth is bounded, workspace isolation is reused,
  and terminal handoff states remain inspectable.
- Existing deployments remain stdlib-only and additive-schema compatible.

Trade-offs:

- The local event spool intentionally drops the oldest event contexts after its
  configured cap; it is not an archive.
- Redaction changes the event checksum relative to the caller's raw payload.
- App consumers must supply stable idempotency keys and must not put secrets in
  opaque metadata fields.

## Verification

- Public behavior tests cover sanitization, default/custom exclusions,
  idempotency, bounded retention, exact workspace isolation, checksum/source
  readback, owner/cwd enforcement, one-shot accept/cancel, safe expiry, and
  hard workspace cleanup.
- The full stdlib test suite passes with the documented migration source
  precondition set: `MEMORY_MIGRATE_SRC=/tmp`.
