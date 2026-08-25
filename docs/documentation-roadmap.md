# Documentation refresh roadmap

- Mode: comprehensive-refresh
- Baseline source: merged implementation commit `0b9efb1`
- Audience: agents and developers using the local `memory-mcp` MCP server
- First successful action: preview candidate facts or a local document, then
  read or commit only the bounded result required by the task

## Scope

Included surfaces:

- `README.md` landing page and API pointers;
- `DEPLOYMENT.md` local test/deployment notes;
- `skills/memory-mcp/SKILL.md` agent-facing usage rules;
- `tests/test_memory_mcp.py` test-run instruction in the module docstring;
- `docs/ingestion-and-provenance.md` operational contract; and
- `docs/decisions/ADR-0002-local-ingestion-and-code-provenance.md` and the
  v0.22 architecture decision record.

Excluded surfaces:

- product implementation code and test assertions, which are the canonical
  behavior sources for this documentation pass (the test module's run command
  is included above as an instruction update);
- runtime-managed `AGENTS.md`, global workflow/skill registry sources, and
  generated mirrors; and
- external products, cloud services, UI surfaces, and external publication.

## Audit findings and planned phases

1. Audit code, tests, README, deployment notes, and the project skill against
   the v0.16–v0.22 implementation. The v0.22 pass adds bounded retrieval
   profiles, a preview-first local document adapter, aggregate usage feedback,
   and canonical entity lookup keys.
2. Add the missing operational contract, synchronize the skill and landing
   page, and record the local-only architecture boundary.
3. Verify paths, links, examples, skill structure, Python syntax, and the
   stdlib regression suite. Record any environment-dependent deviation rather
   than implying a broader integration test.
4. Document the v0.17 focused retrieval contract, including the advisory-only
   safety boundary, public `purpose` schema, noise-only behavior, and the
   deployment smoke check.
5. Document the v0.22 local-only boundaries: explicit-root document reads,
  immutable context chunks, profile caps, fixed feedback signals, bounded
  retention, and additive entity normalization.
6. Document and test the issue-shaped pilot composition: strict code evidence,
   bounded context, typed handoff, read-only context mapping, run summary, and
   aggregate paired measurement without workflow authority. The contract is
   recorded in ADR-0007 and exercised with synthetic data.

## Verification record

The refresh passed internal Markdown path checks, project skill structure
checks, `git diff --check`, and the local Python checks:

- `MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -q` — 157 tests;
- `python3 -m unittest -q test_memory_mcp` — 78 tests; and
- `python3 -m py_compile memory_mcp.py extract.py verify.py recall.py embeddings.py`.
- Public stdio JSON-RPC smoke — `tools/list`, focused `compose_recall`,
  noise-only rejection, and fail-closed `purpose: "safety_critical"`.
- `python3 /usr/local/workflow-tools/skill_doctor.py --root skills/memory-mcp
  --output json` — exit 0; one expected warning notes that the project-local
  package has no global `registry-manifest.json`.
- JSON examples in `docs/ingestion-and-provenance.md` parse successfully.
- `IssueShapedPilotTest` — one temporary-repository, temporary-database
  vertical slice covering evidence, context, handoff, anchor mapping, run
  summary, and `not_claimed` paired measurement state.

No `.doc-state.json` manifest is created because documentation-health telemetry
was not requested; this roadmap is the reviewable scope record for this pass.

## Remaining debt

Provider-backed extraction, semantic search, and verification remain optional
deployment paths. They are intentionally not expanded or enabled by this
local-only documentation refresh. Retrieval output remains advisory even when
those optional paths are enabled.
