# Documentation refresh roadmap

- Mode: comprehensive-refresh
- Baseline source: implementation commit `9ddffd1`
- Audience: agents and developers using the local `memory-mcp` MCP server
- First successful action: preview candidate facts with `absorb`, then read or
  commit only the bounded result required by the task

## Scope

Included surfaces:

- `README.md` landing page and API pointers;
- `DEPLOYMENT.md` local test/deployment notes;
- `skills/memory-mcp/SKILL.md` agent-facing usage rules;
- `tests/test_memory_mcp.py` test-run instruction in the module docstring;
- `docs/ingestion-and-provenance.md` operational contract; and
- `docs/decisions/ADR-0002-local-ingestion-and-code-provenance.md` decision
  record.

Excluded surfaces:

- product implementation code and test assertions, which are the canonical
  behavior sources for this documentation pass (the test module's run command
  is included above as an instruction update);
- runtime-managed `AGENTS.md`, global workflow/skill registry sources, and
  generated mirrors; and
- external products, cloud services, UI surfaces, and external publication.

## Audit findings and planned phases

1. Audit code, tests, README, deployment notes, and the project skill against
   the v0.16 implementation. The README covered the new code, but the skill
   was stale and its contract link pointed to the lifecycle document.
2. Add the missing operational contract, synchronize the skill and landing
   page, and record the local-only architecture boundary.
3. Verify paths, links, examples, skill structure, Python syntax, and the
   stdlib regression suite. Record any environment-dependent deviation rather
   than implying a broader integration test.

## Verification record

The refresh passed internal Markdown path checks, project skill structure
checks, `git diff --check`, and the local Python checks:

- `MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -q` — 121 tests;
- `python3 -m unittest -q test_memory_mcp` — 75 tests; and
- `python3 -m py_compile memory_mcp.py extract.py verify.py recall.py embeddings.py`.
- `python3 /usr/local/workflow-tools/skill_doctor.py --root skills/memory-mcp
  --output json` — exit 0; one expected warning notes that the project-local
  package has no global `registry-manifest.json`.
- JSON examples in `docs/ingestion-and-provenance.md` parse successfully.

No `.doc-state.json` manifest is created because documentation-health telemetry
was not requested; this roadmap is the reviewable scope record for this pass.

## Remaining debt

Provider-backed extraction, semantic search, and verification remain optional
deployment paths. They are intentionally not expanded or enabled by this
local-only documentation refresh.
