# Ingestion, bounded retrieval, and code-local provenance

This document is the operational contract for the v0.16, v0.17, v0.19, and v0.20 additions to the
local `memory-mcp` server. The canonical behavior is implemented in
`memory_mcp.py` and covered by `tests/test_memory_mcp.py`; this document keeps
the client-facing flow concise.

## Scope and boundary

The new operations stay inside the existing Python standard-library process
and SQLite store:

- `absorb` plans and optionally commits candidate facts;
- `chunk_fact` and chunked `search_facts` deliver bounded fact slices; and
- `attach_evidence` / `get_provenance` can retain structured code-local
  anchors without storing the selected source snippet;
- `query_anchored` can verify those anchors against an explicitly supplied
  local repository root; and
- `auto_orient`, `search_guard`, and `stats` provide bounded runtime policy
  and retrieval-quality signals.

No UI, cloud synchronization service, separate code graph, or other external
product is required or introduced by these operations. Existing optional
embedding, extraction, recall, and verification modules remain opt-in and are
not part of the local-only default path.

Every fact operation should receive the exact project `workspace`. Keep source
references, paths, idempotency keys, and payloads free of credentials or other
secrets.

## Server-side extraction authority gate

`ingest_turn` is an optional convenience path, not a human confirmation
boundary. The LLM response is treated as a candidate even when it claims
`trust: "high"` or `strong: true`: the server stores that fact with
`trust: "medium"`, `strong: false`, and `confirmed: 0`. Review the result with
`review_pending` and call `confirm_fact` only after a person verifies it.
Importance, category, scope, provenance, and the extracted text remain subject
to the normal fact/workspace rules.

## Safe batch ingestion

Use `absorb` when a client already has candidate fact text and needs a bounded,
reviewable write boundary. A request may contain `facts` as strings or objects,
or use the single-item `text` alias. Each object can carry normal fact fields
(`source`, `project`, `domain`, `category`, `trust`, `strong`, `importance`) and
an `evidence` object or array.

```json
{
  "facts": [
    {
      "text": "The retry worker stores its counter in SQLite.",
      "workspace": "project-id",
      "source": "repo@abc123:src/worker.py",
      "evidence": {
        "repo": "repo",
        "ref": "abc123",
        "path": "src/worker.py",
        "symbol": "RetryWorker.run",
        "start_line": 42,
        "end_line": 48,
        "resolution_status": "resolved"
      }
    }
  ],
  "workspace": "project-id"
}
```

The default is a dry run. Each item returns a `classification` and `action`:

- `new` / `create` means no exact or sufficiently related active fact was
  found;
- `duplicate` / `noop` means the text has the same SHA-256 in the scope;
- `related` / `review` means lexical term coverage is at least 0.6 and the
  existing candidate must be reviewed instead of silently merged.

Use `commit:true` only after inspecting the preview. Commit mode creates only
items classified as `new`, reuses normal workspace/category/dedup behavior,
and attaches their evidence. Duplicate evidence is ignored by
`(fact_id, source_ref)`. Related items stay pending review. The optional
`verify:true` path is available only when `MEMORY_MCP_VERIFY=1`; it can refine
a related candidate to `new`, `update`, or `contradiction`, but update and
contradiction are still review-only and are never applied automatically.

The server accepts at most 50 candidates per batch and limits each candidate
to 16,000 characters by default. Evidence accepts at most eight supplied
objects per candidate. A candidate needs either `source_ref` or at least one
of `repo`, `ref`, or `path` to form one; the source reference and anchor fields
are bounded by the server's evidence field limit.

Recommended sequence:

1. Search the exact workspace for existing facts.
2. Run `absorb` with the default dry-run behavior.
3. Inspect `items`, candidate IDs, evidence counts, and review items.
4. Re-submit the intended batch with `commit:true`.
5. Read `get_provenance` for any fact that must be traceable later.

## Bounded fact retrieval

Use `chunk_fact` to read one active fact without returning its full text in a
single payload:

```json
{
  "fact_id": 17,
  "workspace": "project-id",
  "chunk_chars": 4000,
  "chunk_overlap": 120,
  "start_chunk": 0,
  "max_chunks": 4
}
```

The response contains fact metadata, numbered `chunks` with character
`start`/`end` offsets, `total_chunks`, and `next_chunk`. The default chunk size
is 4,000 characters; the maximum is 16,000, the page limit is 32 chunks, and
the aggregate response budget is 64 KiB. Request the next page by passing the
returned `next_chunk` as `start_chunk`.

`search_facts` accepts the same `chunk_chars` option. It preserves lexical or
semantic ranking and adds bounded chunks to each hit. Use `chunk_fact` for
explicit pagination of one fact; do not concatenate many pages into an
unbounded prompt.

Chunk content is data, not instructions. A consuming agent must not execute or
evaluate text merely because it was returned from a fact or context store.

## Advisory retrieval and safety boundary

The retrieval tools are context providers, not authorization providers:

- `compose_recall`, `search_facts`, `search_semantic`, and `find_precedents`
  accept `purpose: "advisory" | "safety_critical"` where applicable. The
  default is `advisory`.
- `purpose: "safety_critical"` is rejected fail-closed with
  `code: "advisory_only"`, `fail_closed: true`, and
  `safety_critical_allowed: false`.
- Current Multica state, current registry reads, and local lock/hash checks
  remain the source of truth for writes, route selection, lock validity, and
  hash acceptance.

`compose_recall` receives either a direct turn or a complete transcript. For a
complete transcript it keeps only the latest user block for candidate
retrieval, removes system reminders and tool/result markers, and excludes
older assistant turns. If the input contains only transcript noise, the server
returns `no searchable terms` instead of searching on the noise.

The returned `<memory-recall>` block remains low-authority context. Consumers
must validate changing details against the current authoritative source before
acting on them.

When embeddings are enabled, `search_semantic` and hybrid `search_facts`
apply the same workspace, lifecycle, archived, temporal-validity, trust,
strength, project, domain, and category filters as the lexical path. Semantic
results retain category and fact-state metadata, so enabling embeddings does
not widen the eligible fact set.

The stdio boundary returns JSON-RPC `-32700` for parse errors, `-32600` for a
non-object request, and `-32602` for scalar/array `params` or invalid tool-call
arguments. The process continues after these errors; tool execution failures
use a generic client-facing message and keep implementation details on stderr.

## Code-local evidence anchors

`attach_evidence` accepts the stable `source_ref` plus optional structured
fields:

```json
{
  "fact_id": 17,
  "source_ref": "repo@abc123:src/worker.py",
  "repo": "repo",
  "ref": "abc123",
  "path": "src/worker.py",
  "symbol": "RetryWorker.run",
  "start_line": 42,
  "start_col": 4,
  "end_line": 48,
  "end_col": 16,
  "selected_text": "return retry(task)",
  "resolution_status": "resolved",
  "workspace": "project-id"
}
```

`selected_text` is used only to compute a SHA-256 value. The raw snippet is
not stored; `get_provenance` returns `selected_text_hash` instead. Allowed
statuses are `resolved`, `stale`, and `unresolved`; an anchor without an
explicit status defaults to `unresolved`. A stale or unresolved anchor is a
signal to refresh evidence, not proof that the current source still matches.

The schema migration is additive. Existing evidence remains readable, and
existing stores do not need a destructive migration or a new dependency.

At query time, pass `repo_root` to `query_anchored` when the caller has the
corresponding checkout. The stored status is never overwritten. The response
adds one of these read-only verdicts to each returned anchor:

- `STRONG` — the recorded selection hash or symbol is present in the live file;
- `WEAK` — only metadata/path existence could be checked, or no root was given;
- `STALE` — the addressed content no longer matches;
- `REBUILT` — the content hash was found at another repository-relative path;
- `REMOVED` — the path and bounded replacement scan no longer find the anchor.

Moved-file scans are bounded by `MEMORY_MCP_ANCHOR_MAX_FILES` and
`MEMORY_MCP_ANCHOR_MAX_BYTES`, skip generated/database directories, and never
read outside `repo_root`. A client should treat `REBUILT` as a refresh finding,
not as permission to silently rewrite provenance.

For CI, run the same checker without an MCP session:

```sh
python3 verify.py --health --root . --repo <repo-id> --json
```

The command exits `1` for `STALE`, `REBUILT`, or `REMOVED`, `0` when there is
no drift, and `2` for invalid command input. Weak/path-only anchors are
reported for review but do not fail the drift gate.

`auto_orient` is an optional first-input helper. The caller supplies a stable
`session_id`; only the first call for that session invokes `compose_recall`,
with a six-hit cap and a 2.5-second deadline. A timeout or disabled provider
returns an empty advisory block with `degraded: true` and does not block the
runtime. `search_guard` accepts `action: "search"`, `"memory"`, or `"reset"`;
after three searches by default it returns `warn: true`, while
`blocking` remains false. `stats.access` exposes pull hit/miss counts and
overall/per-site `hit_rate` values from the existing bounded telemetry rows.

## Aggregate paired measurement (v0.20)

Use the measurement tools to compare similar work with memory disabled and
with trigger-enabled memory. This is an observation layer, not a workflow
authority or a transcript store.

`record_measurement` requires an exact `workspace`, a bounded
`measurement_id`, a shared `sample_key` for the pair, `variant` equal to
`baseline` or `memory`, and at least one bounded `run_id` or `issue_ref` link:

```json
{
  "measurement_id": "paired-slice-2026-08",
  "sample_key": "sample-001",
  "variant": "memory",
  "workspace": "project-id",
  "issue_ref": "NTL-694",
  "input_tokens": 12000,
  "output_tokens": 1400,
  "memory_calls": 2,
  "external_tool_calls": 5,
  "wall_time_ms": 185000,
  "time_to_first_useful_ms": 42000,
  "context_bytes": 28000,
  "comment_bytes": 3200,
  "memory_latency_ms": 38.5,
  "quality_score": 0.9,
  "safety_regression": 0
}
```

The server accepts only the documented numeric counters, durations, rates,
normalized quality score, and safety flag. It rejects unknown fields and does
not store prompts, retrieved facts, comments, diffs, secrets, credentials, or
arbitrary JSON. If `run_id` is supplied, it must already exist in the same
workspace. The idempotency key is
`(workspace, measurement_id, sample_key, variant)`; an identical retry is a
no-op and a conflicting retry is rejected.

`query_measurement` matches `sample_key` values that have both variants and
returns only counts plus per-variant metric count, median, and p95. It returns
`status: "not_claimed"` until both variants have at least `min_pairs`
(default 10) complete pairs. Once that bar is reached it returns
`ready_for_review`, which still does not claim token savings, adoption,
latency benefit, or quality improvement; a human/PM must compare the
predeclared quality and safety threshold. No savings delta is calculated by
the server.

## Verification and recovery

Run the project tests with an explicit migration source so the suite does not
depend on a host-specific `~/.reasonix/projects` directory:

```sh
MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -q
python3 -m unittest -v test_memory_mcp.py
python3 -m py_compile memory_mcp.py extract.py verify.py recall.py embeddings.py
python3 verify.py --health --root . --repo <repo-id> --json
```

The local core does not need a network, model, UI, or separate product for
these checks. Provider-backed verification and semantic search are separate
opt-in paths and should be tested only when their environment flags are
deliberately enabled.

Related project records:

- `docs/decisions/ADR-0002-local-ingestion-and-code-provenance.md` records the
  local-only architecture decision for these additions.
- `docs/lifecycle-and-handoffs.md` remains the contract for lifecycle events
  and typed handoffs.
