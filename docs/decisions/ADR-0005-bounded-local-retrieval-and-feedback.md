# ADR-0005: keep role-aware retrieval and local document capture bounded

- Status: proposed for PM acceptance
- Date: 2026-08-25
- Scope: local `memory-mcp` SQLite store and its agent skill

## Context

The existing store already has lexical/semantic retrieval, immutable context,
code anchors, graph lookup, and bounded telemetry. The next useful seams are
portable across runtimes, but they must not turn memory into an authority
source, a repository crawler, or a transcript/feedback archive. A client also
needs a safe way to supply one document from a checkout without granting the
server ambient filesystem access.

## Decision

- Add five named retrieval profiles: `balanced`, `orientation`,
  `implementation`, `review`, and `incident`. A profile only supplies bounded
  defaults for result count, graph expansion, and recall characters. Explicit
  values remain capped, and successful responses expose typed `profile` and
  `result_status` metadata.
- Add `ingest_document` as a preview-first, single-file adapter. The caller
  supplies an explicit root, a relative path, and an exact workspace. The
  server rejects traversal, symlink escape, non-UTF-8 content, oversized files,
  and common secret/binary paths. `commit:true` writes immutable context chunks
  with a relative source path and document/chunk hashes; the root is transient.
- Add `record_feedback` and `query_feedback` for fixed, aggregate usage
  signals. Feedback IDs are workspace-scoped and idempotent; conflicting
  retries fail. Raw queries, notes, prompts, and arbitrary payloads are not
  accepted or stored.
- Add a separate Unicode-normalized entity lookup key. Display names remain
  intact while NFKC, whitespace folding, and case-folding stabilize graph
  resolution. Existing databases migrate additively.
- Keep all of the above in the current Python standard library and SQLite
  process. No cloud service, UI, LLM, repository checkout, or workflow/registry
  authority is added. Retrieval and feedback remain advisory evidence.

## Security and privacy boundary

The document adapter reads only the requested file under the canonicalized
caller root and applies size, chunk, encoding, and path exclusions. It does not
return file content in a preview, store the root, crawl sibling files, or
follow an escaping symlink. Context ACLs and TTL rules remain in force after a
commit. Feedback stores only bounded identifiers, fixed enums, and an optional
query hash. Retention is capped per workspace.

## Consequences

Clients can select a bounded retrieval shape, import a reviewed local document,
and measure coarse usefulness without a new service or dependency. Existing
callers that omit `profile` retain the balanced behavior. The server still
cannot decide routes, locks, registry mutations, acceptance, or safety-critical
actions; callers must validate changing details against current authoritative
state.

## Verification

The implementation is covered by bounded unit tests for profile caps and typed
results, feedback idempotency and aggregate counts, Unicode entity lookup, and
document preview/commit/path safety. Migration tests cover fresh and existing
SQLite stores. Project skill and deployment documentation must describe the
same limits before release.
