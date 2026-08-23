# Ingestion, bounded retrieval, and code-local provenance

This document is the operational contract for the v0.16 and v0.17 additions to the
local `memory-mcp` server. The canonical behavior is implemented in
`memory_mcp.py` and covered by `tests/test_memory_mcp.py`; this document keeps
the client-facing flow concise.

## Scope and boundary

The new operations stay inside the existing Python standard-library process
and SQLite store:

- `absorb` plans and optionally commits candidate facts;
- `chunk_fact` and chunked `search_facts` deliver bounded fact slices; and
- `attach_evidence` / `get_provenance` can retain structured code-local
  anchors without storing the selected source snippet.

No UI, cloud synchronization service, separate code graph, or other external
product is required or introduced by these operations. Existing optional
embedding, extraction, recall, and verification modules remain opt-in and are
not part of the local-only default path.

Every fact operation should receive the exact project `workspace`. Keep source
references, paths, idempotency keys, and payloads free of credentials or other
secrets.

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

## Verification and recovery

Run the project tests with an explicit migration source so the suite does not
depend on a host-specific `~/.reasonix/projects` directory:

```sh
MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -q
python3 -m unittest -v test_memory_mcp.py
python3 -m py_compile memory_mcp.py extract.py verify.py recall.py embeddings.py
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
