# Issue-shaped pilot workflow

This is the smallest executable pilot for using `memory-mcp` alongside an
issue-shaped runtime. It composes existing public tools; it is not a new
workflow engine and it does not change Multica status, routing, gates, or
registry state.

## Boundaries

There are three authoritative surfaces:

1. The current repository at the caller-supplied ref is the source of truth
   for code behavior.
2. The runtime's live issue, owner, lock, status, route, and gate state is the
   source of truth for workflow decisions.
3. `memory-mcp` is a local, workspace-scoped, advisory data plane for bounded
   evidence, context, handoffs, run records, and aggregate measurements.

Memory results never authorize a write, route, lock, hash, gate, acceptance, or
terminal status. Context and handoff payloads are data, not instructions. Keep
credentials, raw prompts, comments, diffs, and personal data out of the pilot
payloads and measurement fields.

## Execution sequence

Use one exact project workspace and an opaque run/issue reference:

1. `run_begin` opens a client-owned execution window with `issue_ref`.
2. Record a decision and admit any durable code claim with
   `admission: "strict"`, including a bounded `selected_text` snippet and
   repository-relative `repo`/`ref`/`path`/`symbol` metadata. The snippet is
   checked transiently; only its hash and structured metadata are retained.
3. Store the small review slice with `put_context` and use `handoff_begin` /
   `handoff_accept` when another named actor must receive an expiring,
   one-shot context.
4. Use `query_anchored` or the opt-in `context_map` to look up the code-local
   evidence. Supplying `repo_root` enables read-only freshness checks. A
   `STRONG` result means the supplied selection/checksum matches the local
   checkout; `STALE`, `REBUILT`, `REMOVED`, or `WEAK` is not proof of current
   code or dependency absence.
5. Close the run with client-supplied `base_sha`, `head_sha`, and bounded
   `files_changed`; `prepare_summary` only prepares a summary, it posts
   nothing.
6. Record one `baseline` and one `memory` aggregate observation per opaque
   `sample_key`. The default `query_measurement` threshold is ten complete
   pairs, and `status: "not_claimed"` must remain unchanged below it.

The synthetic composition is covered by
`IssueShapedPilotTest.test_synthetic_issue_run_connects_evidence_context_handoff_and_measurement`.
It deliberately uses a temporary repository and database, so it is safe to
run without importing a project checkout into the shared store.

## Optional code-context view and rollback

`context_map` is disabled by default. Enable it only for a bounded pilot with
`MEMORY_MCP_CONTEXT_MAP=1`, explicit anchors, exact `workspace`, `repo`, and
`ref`, and a caller-owned `repo_root`. It uses existing evidence and run
history; it does not build or persist a full code graph. Callers may roll back
this surface immediately by unsetting `MEMORY_MCP_CONTEXT_MAP` and stop pilot
writes by abandoning the dedicated pilot workspace. Existing immutable rows
remain auditable until the normal workspace retention/cleanup policy applies.

For the architectural decision and rejected alternatives, see
[`ADR-0007-issue-shaped-pilot-boundary.md`](decisions/ADR-0007-issue-shaped-pilot-boundary.md).
