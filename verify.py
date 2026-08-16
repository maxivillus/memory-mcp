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


def _invalidate(old_id, new_id):
    """Bi-temporal invalidation: the old fact keeps its history but stops
    matching active searches (invalid_at set, superseded_by linked)."""
    if old_id == new_id:
        return False  # never self-invalidate
    from memory_mcp import get_db
    from datetime import datetime, timezone
    con = get_db()
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        con.execute("UPDATE facts SET invalid_at=?, superseded_by=?, updated_at=? WHERE id=?",
                    (ts, new_id, ts, old_id))
        con.commit()
        return True
    finally:
        con.close()

PROMPT = """You verify a new memory fact against existing stored facts. The
new fact may update or contradict older ones. Return ONLY JSON matching:
{"action": "add" | "update" | "supersedes" | "delete" | "noop",
 "target_id": <int or null>, "reason": "<short why>", "confidence": <0..1>}
- add: the fact is new and consistent — store it (default).
- update: the new fact refines/extends an OLD fact (same subject, more detail).
- supersedes: the new fact makes an OLD fact obsolete (same subject, newer truth).
- delete: the new fact proves an OLD fact wrong.
- conflict: the new fact contradicts an old one without replacing it — report
  as action=noop with reason=conflict.
- target_id: the old fact id, when the action targets one.
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
                "SELECT id, text FROM facts WHERE archived=0 AND invalid_at='' "
                "ORDER BY updated_at DESC LIMIT ?",
                (limit,)).fetchall()
        finally:
            con.close()
    # strong/confirmed flags ride along so the invalidation hook can protect
    # user-confirmed facts
    con = get_db()
    try:
        protected_ids = {r["id"] for r in con.execute(
            "SELECT id FROM facts WHERE (strong=1 OR confirmed=1) AND archived=0")}
    finally:
        con.close()
    return [{"id": r["id"], "text": r["text"], "strong": r["id"] in protected_ids} for r in rows]


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
        # the fact under test is excluded from the candidates it can target
        candidates = [c for c in _candidates(nf["text"]) if c["id"] != nf["id"]]
        if not candidates:
            continue
        try:
            verdict = _llm_verdict(nf["text"], candidates)
        except Exception:
            continue
        summary["checked"] += 1
        action = verdict.get("action") or "add"
        if action == "noop":
            if verdict.get("reason") == "conflict":
                summary["conflicts"].append({"new_id": nf["id"], "reason": "conflict"})
            continue
        if action == "add":
            continue
        confidence = float(verdict.get("confidence", 0.0))
        if confidence < threshold:
            summary["skipped_low_conf"] += 1
            continue
        cid = verdict.get("target_id")
        # Only ids shown to the LLM may be acted on (prompt-injection guard:
        # a poisoned transcript must not steer invalidation of facts that were
        # never part of the verification context). The fact under test itself
        # is never a valid target (models sometimes pick it).
        if cid not in {c["id"] for c in candidates} or cid == nf["id"]:
            continue
        target = next((x for x in candidates if x["id"] == cid), None)
        if target is None or target.get("strong"):
            # strong/human-confirmed facts are never auto-invalidated
            summary["conflicts"].append({"old_id": cid, "new_id": nf["id"],
                                         "reason": verdict.get("reason", "") + " (strong/confirmed fact, not invalidated)"})
            continue
        if _invalidate(cid, nf["id"]):
            summary["applied"] += 1
            bucket = "superseded" if action in ("supersedes", "update") else "deleted"
            summary.setdefault(bucket, []).append(
                {"old_id": cid, "new_id": nf["id"], "reason": verdict.get("reason", "")})
            attach_evidence({"fact_id": nf["id"], "source_ref": "%s:%s" % (action, cid)})
    return summary
