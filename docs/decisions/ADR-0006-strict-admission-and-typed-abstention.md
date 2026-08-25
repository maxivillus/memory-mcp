# ADR-0006: keep evidence admission strict and retrieval uncertainty explicit

- Status: proposed for PM acceptance
- Date: 2026-08-25
- Scope: local `memory-mcp` SQLite store, retrieval handlers, and agent skill

## Context

Candidate fact text can be useful before it is trusted, but a write boundary
should make missing or unrelated evidence visible. Likewise, an empty search
is not enough information to conclude that a fact does not exist. Both cases
need small, deterministic response signals that clients can act on without
changing the advisory authority of memory.

## Decision

- Add `admission: "strict"` to `remember_fact` and `absorb` as an opt-in mode.
  It requires bounded `selected_text` evidence and checks that the claim's
  non-stopword terms occur in order. The snippet is transient; only its hash
  and structured evidence metadata are stored.
- Return a typed rejection (`result_status: "rejected"`, stable code, and
  remediation) before any write when strict evidence is missing or unrelated.
  Strict admission does not raise trust, set `strong`, confirm a fact, or grant
  workflow authority. New strict facts attach accepted evidence in one SQLite
  transaction.
- Add `retrieval_outcome: "matched" | "abstained"` to retrieval responses.
  Empty responses retain `result_status: "empty"` for compatibility and add a
  reason code plus bounded remedy. Clients must not convert abstention into an
  absence claim.

## Privacy and safety boundary

The strict check does not persist snippets, raw queries, or hidden execution
details. Evidence fields remain bounded and callers must keep credentials and
other sensitive data out of fact text and metadata. Retrieval remains advisory;
live state, locks, hashes, routes, and acceptance decisions stay authoritative
outside the memory store.

## Consequences

Clients can choose a stronger admission check for claims that need local
evidence while preserving the existing default path. Empty search results now
provide an actionable next step without overstating what the store knows.
Older clients continue to receive the existing `result_status` values and can
ignore the additive fields.

## Verification

Unit tests cover accepted and rejected strict writes, transient snippet
handling, preview-first strict batches, same-transaction evidence attachment,
and typed empty-search responses. Documentation and the project skill carry
the same field names, refusal codes, and privacy boundary.
