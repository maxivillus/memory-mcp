# ADR-0007: issue-shaped pilot stays advisory and repository-local

- Status: accepted for pilot implementation
- Date: 2026-08-25
- Scope: local `memory-mcp` integration contract, tests, and documentation

## Context

The repository comparison identified useful patterns for code-grounded
context, typed handoffs, execution records, and baseline-versus-memory
measurement. The current `memory-mcp` core already provides bounded local
seams for those capabilities. The missing implementation boundary is a
repeatable pilot that composes them without making memory a second workflow
authority or importing a separate code graph.

## Decision

Implement the pilot as a client-composed, issue-shaped sequence over existing
public tools:

1. `run_begin` binds an opaque execution window to an exact workspace and
   issue reference.
2. Strict evidence admission, `record_decision`, and code-local evidence
   anchors tie durable claims to a repository/ref/path/symbol. The repository
   at the supplied ref remains the code source of truth.
3. `put_context` and typed handoffs carry bounded data. Handoffs remain
   owner-scoped, expiring, one-shot records; context content is never an
   instruction channel.
4. `query_anchored` and opt-in `context_map` provide bounded, read-only
   orientation from existing evidence and run history. Freshness verdicts are
   evidence signals, not authority.
5. `run_end` and `prepare_summary` close and summarize the client-owned run;
   they do not post comments or change issue state.
6. `record_measurement` collects only aggregate baseline/memory observations.
   The default ten-pair threshold and `not_claimed` state remain in force
   until a human/PM reviews a complete slice with quality and safety evidence.

The pilot uses synthetic data in a temporary database and repository. The
end-to-end contract is locked by
`IssueShapedPilotTest.test_synthetic_issue_run_connects_evidence_context_handoff_and_measurement`.

## Authority and rollback

Multica's live issue, owner, status, route, locks, and gates remain the
workflow source of truth. `memory-mcp` is advisory and cannot authorize
registry writes, routing, lock/hash acceptance, gates, or terminal status.
The `context_map` surface is opt-in and can be disabled by unsetting
`MEMORY_MCP_CONTEXT_MAP`; a pilot can be stopped by abandoning its dedicated
workspace. No foreign repository, cloud service, full code graph, or runtime
workflow mutation is part of this decision.

## Alternatives considered

- Importing a full external code graph would add a second code truth and
  exceed the local stdlib/SQLite boundary; rejected for this pilot.
- Generating or silently updating `AGENTS.md` or workflow registry state would
  create shadow authority; rejected.
- Treating a ready measurement slice as proof of savings or quality would
  overstate one bounded observation; rejected until the paired threshold and
  human review are complete.

## Consequences

The pilot is runnable and testable with synthetic data while preserving the
existing privacy, workspace, advisory, and rollback boundaries. A future
integration may add a client adapter only when it has an explicit target,
source-of-truth mapping, and acceptance checks; this ADR does not authorize
changes to other repositories or the Multica workflow registry.
