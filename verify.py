"""Server-side fact verification (optional; MEMORY_MCP_VERIFY=1).

`verify_facts` cross-checks a candidate fact against the store with the LLM
provider (see llm.py) and reports contradictions/supersessions. `check_new_facts`
is the ingestion-time hook (called by extract.ingest_turn when enabled): a
confirmed supersession archives the OLD fact (soft delete, traceable) and
attaches `supersedes:<old_id>` evidence to the new one — graphiti-style
invalidation, deliberately conservative: only high-confidence LLM verdicts
act, everything else is reported but not applied.

Env:
  MEMORY_MCP_VERIFY=1                    enable
  MEMORY_MCP_VERIFY_MIN_CONFIDENCE       threshold to auto-apply (default 0.8)
"""

import json
import os

import llm

PROMPT = """You verify a new memory fact against existing stored facts. The
new fact may update or contradict older ones. Return ONLY JSON matching:
{"verdict": "consistent" | "supersedes" | "conflict",
 "conflicts": [{"id": <int>, "reason": "<short why>"}],
 "confidence": <0..1>}
- supersedes: the new fact makes an OLD fact obsolete (same subject, newer truth).
- conflict: the new fact contradicts an old one but does not replace it.
- confidence: how sure you are; below-threshold verdicts are not applied.
Stored facts:"""


def _min_confidence():
    try:
        return min(1.0, max(0.0, float(os.environ.get("MEMORY_MCP_VERIFY_MIN_CONFIDENCE", "0.8"))))
    except ValueError:
        return 0.8


def _candidates(text, limit=8):
    """Most relevant stored facts for the cross-check (OR-joined lexical top-N,
    like find_precedents/compose_recall; falls back to the freshest facts)."""
    from memory_mcp import fts_terms, get_db, search_facts
    terms = fts_terms(text)
    if terms:
        res = search_facts({"query": " OR ".join(terms), "limit": limit})
        if "error" not in res and res.get("facts"):
            rows = [{"id": f["id"], "text": f["text"]} for f in res["facts"]]
        else:
            rows = None
    else:
        rows = None
    if rows is None:
        con = get_db()
        try:
            rows = con.execute(
                "SELECT id, text FROM facts WHERE archived=0 ORDER BY updated_at DESC LIMIT ?",
                (limit,)).fetchall()
        finally:
            con.close()
    # strong flag rides along so the invalidation hook can protect user-confirmed facts
    con = get_db()
    try:
        strong_ids = {r["id"] for r in con.execute(
            "SELECT id FROM facts WHERE strong=1 AND archived=0")}
    finally:
        con.close()
    return [{"id": r["id"], "text": r["text"], "strong": r["id"] in strong_ids} for r in rows]


def _llm_verdict(text, candidates):
    stored = "\n".join("- id=%s: %s" % (c["id"], c["text"][:200]) for c in candidates) or "(none)"
    return llm.chat_json([
        {"role": "system", "content": PROMPT},
        {"role": "user", "content": "Stored facts:\n%s\n\nNew fact: %s" % (stored, text)},
    ])


def verify_facts(args):
    text = (args.get("text") or "").strip()
    if not text:
        return {"error": "text is required"}
    candidates = _candidates(text)
    try:
        verdict = _llm_verdict(text, candidates)
        return {"text": text, "checked_against": len(candidates), "verdict": verdict,
                "applied": False}
    except Exception:
        return {"error": "verification failed (provider error; see server stderr)",
                "checked_against": len(candidates)}


def check_new_facts(new_facts):
    """Ingestion hook: verify new facts and archive superseded old ones."""
    from memory_mcp import attach_evidence, forget_fact
    threshold = _min_confidence()
    summary = {"checked": 0, "superseded": [], "conflicts": [], "applied": 0, "skipped_low_conf": 0}
    for nf in new_facts:
        candidates = _candidates(nf["text"])
        if not candidates:
            continue
        try:
            verdict = _llm_verdict(nf["text"], candidates)
        except Exception:
            continue
        summary["checked"] += 1
        if verdict.get("verdict") not in ("supersedes", "conflict"):
            continue
        confidence = float(verdict.get("confidence", 0.0))
        if confidence < threshold:
            summary["skipped_low_conf"] += 1
            continue
        candidate_ids = {c["id"] for c in candidates}
        for c in verdict.get("conflicts", []):
            cid = c.get("id")
            # Only ids shown to the LLM may be acted on (prompt-injection
            # guard: a poisoned transcript must not steer archiving of facts
            # that were never part of the verification context).
            if cid not in candidate_ids:
                continue
            target = next((x for x in candidates if x["id"] == cid), None)
            if target is None or target.get("strong"):
                # strong (user-confirmed) facts are never auto-archived
                summary["conflicts"].append({"old_id": cid, "new_id": nf["id"],
                                             "reason": c.get("reason", "") + " (strong fact, not archived)"})
                continue
            if verdict["verdict"] == "supersedes":
                forget_fact({"id": cid})  # archive the old fact
                attach_evidence({"fact_id": nf["id"], "source_ref": "supersedes:%s" % cid})
                summary["superseded"].append({"old_id": cid, "new_id": nf["id"],
                                              "reason": c.get("reason", "")})
                summary["applied"] += 1
            else:
                summary["conflicts"].append({"old_id": cid, "new_id": nf["id"],
                                             "reason": c.get("reason", "")})
    return summary
