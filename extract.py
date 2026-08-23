"""Server-side fact extraction (optional; MEMORY_MCP_EXTRACT=1).

`ingest_turn` sends a conversation transcript to the configured LLM provider
(see llm.py) and stores the extracted facts with provenance. This gives every
runtime the same extraction pipeline without client-side patches — any client
runtime can call this tool instead of running its own extractor.

Best-effort by design: any provider failure returns an error result and never
corrupts the store. When MEMORY_MCP_VERIFY=1, newly ingested facts are
cross-checked against the store (see verify.py) and superseded facts are
archived.

Env:
  MEMORY_MCP_EXTRACT=1          enable the tool
  MEMORY_MCP_EXTRACT_MIN_CHARS  minimum transcript length to bother (default 800)
"""

import json
import os

import llm

_FACT_TYPES = ("user", "feedback", "project", "reference")
_TRUST_LEVELS = ("high", "medium", "low")


PROMPT = """You extract durable facts from a conversation transcript for an
agent memory store. Return ONLY JSON matching this schema:
{"facts": [{"text": "...", "type": "user|feedback|project|reference", "trust": "high|medium|low", "strong": false, "scope": "project|global", "importance": 0.5}]}
Rules:
- text: one self-contained fact, present tense, no fluff, max ~200 words.
  Write the fact in the language the conversation is held in (the user's
  language) — do not force or switch languages, whatever that language is.
  Technical terms and proper names may stay in their original form.
- type: user = who the user is; feedback = guidance on how to work (with why);
  project = ongoing work/goals/constraints; reference = pointers to resources.
- trust: high only for explicitly confirmed facts; medium default; low for
  unverified claims.
- strong: true only for user-confirmed critical facts.
- The server applies a human-confirmation gate to trust and strong metadata;
  treat these fields as candidate confidence, not authorization.
- importance: 0..1 — how valuable this fact is for future work (1 = likely
  needed again soon, 0 = barely worth keeping). Default 0.5.
- scope: global only when the fact applies to every project.
- Skip small talk, greetings, and transient details."""


def _min_chars():
    try:
        return max(100, int(os.environ.get("MEMORY_MCP_EXTRACT_MIN_CHARS", "800")))
    except ValueError:
        return 800


def _remember(text, source, project, domain, importance=None, workspace="",
              fact_type="", trust="medium", strong=False, scope="project"):
    """Store one fact via the core server's remember_fact (lazy import — the
    core imports this module, so the import must happen at call time)."""
    from memory_mcp import remember_fact
    fact_type = fact_type if fact_type in _FACT_TYPES else "project"
    trust = trust if trust in _TRUST_LEVELS else "medium"
    # Model confidence is advisory; only a human confirmation may grant the
    # high-trust or strong authority used by retention and recall policy.
    if trust == "high":
        trust = "medium"
    effective_domain = domain or fact_type
    effective_workspace = "" if scope == "global" else workspace
    args = {"text": text, "source": source or "ingest_turn",
            "project": project or "", "domain": effective_domain,
            "trust": trust, "strong": False,
            "workspace": effective_workspace}
    if importance is not None:
        args["importance"] = importance
    return remember_fact(args)


def _attach(fact_id, source_ref, workspace=""):
    from memory_mcp import attach_evidence
    args = {"fact_id": fact_id, "source_ref": source_ref}
    if workspace:
        args["workspace"] = workspace
    return attach_evidence(args)


def ingest_turn(args):
    transcript = (args.get("transcript") or "").strip()
    if not transcript:
        return {"error": "transcript is required"}
    if len(transcript) < _min_chars():
        return {"error": f"transcript too short ({len(transcript)} chars, min {_min_chars()})",
                "ingested": 0, "stored": 0, "deduped": 0, "failed": 0}
    session_ref = (args.get("session_ref") or "").strip()
    project = (args.get("project") or "").strip()
    domain = (args.get("domain") or "").strip()
    workspace = (args.get("workspace") or "").strip()
    last_err = None
    parsed = None
    for attempt in range(llm.MAX_RETRIES):
        try:
            parsed = llm.chat_json([
                {"role": "system", "content": PROMPT},
                {"role": "user", "content": "Transcript:\n" + transcript},
            ])
            if isinstance(parsed, dict) and isinstance(parsed.get("facts"), list):
                break
            last_err = "unexpected JSON shape: %s" % (str(parsed)[:120])
            parsed = None
        except Exception as e:  # provider down / parse error
            last_err = str(e)
            parsed = None
    if parsed is None:
        import sys
        print("memory-mcp ingest_turn: %s" % last_err, file=sys.stderr)
        return {"error": "extraction failed after %d attempts (provider error; see server stderr)" % llm.MAX_RETRIES,
                "ingested": 0, "stored": 0, "deduped": 0, "failed": 0}

    stored = deduped = failed = 0
    new_facts = []
    for f in parsed.get("facts", []):
        text = (f.get("text") or "").strip()
        if not text:
            continue
        try:
            scope = f.get("scope") if f.get("scope") in ("project", "global") else "project"
            fact_workspace = "" if scope == "global" else workspace
            res = _remember(
                text, session_ref, project, domain,
                importance=f.get("importance"), workspace=workspace,
                fact_type=f.get("type", ""), trust=f.get("trust", "medium"),
                strong=f.get("strong", False), scope=scope)
            if res.get("dedup"):
                deduped += 1
            else:
                stored += 1
                new_facts.append({"id": res.get("id"), "text": text,
                                  "workspace": fact_workspace})
            if session_ref and res.get("id"):
                _attach(res["id"], session_ref, fact_workspace)
        except Exception:
            failed += 1

    summary = {"ingested": len(parsed.get("facts", [])), "stored": stored,
               "deduped": deduped, "failed": failed}
    if new_facts and os.environ.get("MEMORY_MCP_VERIFY") == "1":
        try:
            import verify
            summary["verification"] = verify.check_new_facts(new_facts)
        except Exception as e:
            summary["verification"] = {"error": str(e)}
    return summary
