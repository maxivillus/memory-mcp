# ADR-0003: Verify code anchors at read time and keep runtime hints advisory

- Status: Accepted
- Date: 2026-08-24
- Scope: local `memory-mcp` retrieval and runtime helpers

## Context

Code-local evidence stored a client-supplied `resolution_status`, but a later
read could not tell whether the referenced path or selected content still
existed. Runtime integrations also needed a bounded first-input orientation
and a way to notice repeated external searches without turning memory into an
authorization gate.

## Decision

- Keep stored provenance immutable and accept an explicit `repo_root` only for
  read-time filesystem verification. Report `STRONG`, `WEAK`, `STALE`,
  `REBUILT`, or `REMOVED`; never rewrite `resolution_status` implicitly.
- Rebuild searches are bounded by file and byte budgets, stay inside the
  supplied root, and return `REBUILT` as a refresh finding rather than a
  silent provenance update.
- Provide `verify.py --health` as a dependency-free CI command. It fails only
  on content/path drift (`STALE`, `REBUILT`, `REMOVED`); path-only evidence is
  visible as `WEAK` for review.
- Provide `auto_orient` as a once-per-session, six-hit, 2.5-second wrapper
  around advisory recall. Failures return an empty degraded result.
- Provide `search_guard` as bounded in-process state with a non-blocking
  warning, and derive hit-rate fields from the existing access telemetry.

## Consequences

The core remains stdlib-only and existing databases require no migration.
Callers must provide the correct checkout root when they want filesystem
confidence; without it, an anchor is intentionally `WEAK`. Runtime clients
must explicitly report external search and memory actions to use the guard.
Telemetry and guard state remain bounded and carry no source payloads.

## Alternatives rejected

- Mutating stored anchor status during a read would make provenance depend on
  the reader's checkout and would hide the original client claim.
- A daemon or full repository index would add deployment and ownership costs
  that are outside the local memory store's boundary.
- Blocking or authorizing actions from the guard would violate the advisory
  memory contract.
