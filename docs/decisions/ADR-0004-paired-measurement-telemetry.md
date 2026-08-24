## ADR-0004: aggregate-only paired measurement telemetry

- Status: accepted for implementation
- Date: 2026-08-24
- Scope: local memory-mcp SQLite store

### Context

The existing run and memory-access records show activity, but they do not
support a reproducible comparison of similar work with memory disabled and
trigger-enabled memory. The available evidence does not establish token
savings, adoption, latency benefit, or quality impact. A first product change
must therefore improve observability without collecting conversation content.

### Decision

Add a workspace-scoped `measurement_observations` table and the
`record_measurement` / `query_measurement` tools. Each observation is tied to
one opaque `measurement_id`, one paired `sample_key`, one `baseline` or
`memory` variant, and at least one run or issue reference. The server accepts
only bounded numeric counters, durations, rates, a normalized quality score,
and a safety-regression flag.

The unique key `(workspace, measurement_id, sample_key, variant)` makes
retries idempotent. A conflicting retry is rejected. The summary uses only
complete pairs and returns per-variant median and p95 values. It returns
`not_claimed` until the configured minimum number of pairs exists in both
variants; `ready_for_review` means only that the slice is complete enough for
human/PM review.

### Security and privacy boundary

No prompt, retrieved fact, comment, diff, secret, credential, free-text metric,
or arbitrary JSON is accepted or stored. Unknown input fields are rejected.
Workspace isolation is exact, supplied run links must exist in that workspace,
and retention is capped per workspace. The measurement layer does not grant
workflow authority, alter gates, or make an acceptance decision.

### Consequences

Clients can collect a repeatable baseline/memory slice and inspect median/p95
metrics without a separate telemetry service. The server intentionally does
not compute savings or infer causality; the paired slice and quality/safety
threshold must be reviewed before any product claim. Measurement rows are
aggregate evidence and remain bounded operational data, not a transcript
archive.
