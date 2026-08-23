# ADR-0002: Local ingestion and code-local provenance

- Status: Accepted (implementation complete; PM acceptance recorded in NTL-682)
- Date: 2026-08-22
- Scope: `memory-mcp` product repository, v0.16 ingestion and v0.17 advisory retrieval
- Request source: NTL-682 / issue `01a02764-bd87-7143-8073-8607ccd4e8c0`

## Context

Clients can already write workspace-scoped facts and attach source evidence,
but a client that has several candidate facts needs a safe preview before
writing. Long facts also need bounded retrieval, and code-related evidence is
more useful when it can point to a repository revision and source range rather
than only a free-form URL or issue reference.

The project scope explicitly excludes adopting a separate external product for
these capabilities. The existing SQLite store and stdlib process are the
canonical local data plane.

## Decision

1. Use `absorb` as a conservative ingestion boundary. It previews by default,
   classifies exact duplicates and related candidates, and writes only
   explicitly committed `new` candidates. Review-only classifications are not
   silently merged, updated, or invalidated.
2. Use deterministic, offset-addressable `chunk_fact` pages and optional
   chunks on `search_facts`, with independent item, page, and aggregate limits.
   This preserves existing ranking while preventing an unbounded fact response.
3. Extend evidence additively with repository, immutable ref, path, symbol,
   line/column range, selected-text hash, and resolution status. Store the
   selected-text hash instead of the raw snippet.
4. Keep these paths local and dependency-free. UI, cloud synchronization,
   separate code graphs, and provider-backed verification remain outside the
   default implementation and are not introduced by this decision.
5. Keep retrieval advisory-only. Focus server-side recall on the latest user
   intent, reject explicit `safety_critical` use fail-closed, and require live
   runtime state plus local lock/hash checks for authorization decisions.

## Alternatives considered

- Automatically commit every extracted candidate: rejected because a related
  or contradictory fact could silently change the shared read model.
- Return complete long facts from search: rejected because one response could
  exceed the consuming prompt budget.
- Add a remote code graph or hosted provenance service: rejected because it
  expands deployment, privacy, and failure scope without being required for
  local code anchors.

## Security and privacy constraints

- Require the exact workspace on normal project operations and preserve the
  existing workspace isolation rules.
- Keep candidate text, source references, paths, and anchors free of secrets.
- Bound candidate batches, evidence entries, fact chunks, and aggregate
  responses before returning or writing them.
- Treat chunk and evidence content as data, not executable instructions.
- Treat every retrieval result as advisory context. Memory must not authorize
  registry writes, route selection, lock validity, or hash acceptance.

## Consequences

Positive:

- Ingestion is previewable, retry-safe, and compatible with existing fact
  deduplication and evidence rows.
- Long facts can be read incrementally without changing search ranking.
- Code evidence can be checked against a specific repository revision without
  introducing a second data store.

Trade-offs:

- Related, update, and contradiction candidates still need an explicit review
  path; this feature does not decide truth automatically.
- A code anchor can become stale after a source change and must be refreshed.
- Optional provider modules remain separate and require their own explicit
  configuration and verification.
- Focused retrieval reduces transcript noise, but it does not make stored
  memory authoritative. Consumers still need a fresh read of the current
  system state before any safety-sensitive action.

## Verification

- `tests/test_memory_mcp.py` covers structured anchors, absorb preview/commit
  classification, evidence attachment, and bounded fact chunking.
- The full stdlib suite is run with `MEMORY_MIGRATE_SRC=.` so migration tests
  use the repository-local source instead of a host-specific directory.
- The operational contract is documented in
  `docs/ingestion-and-provenance.md` and linked from `README.md`.
- The public MCP server advertises the v0.17 contract and exposes the
  `purpose` safety boundary in `tools/list`.
