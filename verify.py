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

import argparse
import json
import os
import sys

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


CONSOLIDATE_PROMPT = """You consolidate paraphrased memory facts into a
single fact. The facts describe the same subject with overlapping content.
Return ONLY JSON matching:
{"merge": true|false, "text": "<merged fact>", "importance": 0..1, "reason": "<short why>"}
- merge=false when the facts are genuinely different and must stay separate.
- merge=true: text preserves ALL non-redundant information from the inputs
  (no invented details), in the language of the facts, present tense.
- importance: keep the highest importance among the inputs.
Facts:"""


def _min_confidence():
    try:
        return min(1.0, max(0.0, float(os.environ.get("MEMORY_MCP_VERIFY_MIN_CONFIDENCE", "0.8"))))
    except ValueError:
        return 0.8


def _candidates(text, limit=8, workspace=""):
    """Most relevant stored facts for the cross-check (OR-joined lexical top-N,
    like find_precedents/compose_recall; falls back to the freshest facts).
    Scoped to the fact's workspace when provided."""
    from memory_mcp import fts_terms, get_db, search_facts
    terms = fts_terms(text)
    if terms:
        sf = {"query": " OR ".join(terms), "limit": limit}
        if workspace:
            sf["workspace"] = workspace
        res = search_facts(sf)
        if "error" not in res and res.get("facts"):
            rows = [{"id": f["id"], "text": f["text"]} for f in res["facts"]]
        else:
            rows = None
    else:
        rows = None
    if rows is None:
        con = get_db()
        try:
            ws_clause = " AND workspace_id IN (?, '')" if workspace else " AND workspace_id = ''"
            params = [workspace] if workspace else []
            rows = con.execute(
                "SELECT id, text FROM facts WHERE archived=0 AND invalid_at=''" + ws_clause +
                " ORDER BY updated_at DESC LIMIT ?", params + [limit]).fetchall()
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
    candidates = _candidates(text, workspace=(args.get("workspace") or "").strip())
    try:
        verdict = _llm_verdict(text, candidates)
        return {"text": text, "checked_against": len(candidates), "verdict": verdict,
                "applied": False}
    except Exception:
        return {"error": "verification failed (provider error; see server stderr)",
                "checked_against": len(candidates)}

def consolidate(args):
    """LLM-merge of paraphrased facts (near-duplicates) into one fact. The
    inputs are invalidated bi-temporally (history survives via fact_history);
    strong/confirmed facts are never merged."""
    ids = []
    for v in (args.get("ids") or []):
        try:
            ids.append(int(v))
        except (TypeError, ValueError):
            continue
    ids = sorted(set(ids))  # dedupe; cap keeps the LLM prompt bounded
    if len(ids) < 2:
        return {"error": "ids: at least 2 distinct fact ids are required"}
    if len(ids) > 20:
        return {"error": "ids: at most 20 facts can be consolidated at once"}
    from memory_mcp import get_db, remember_fact, attach_evidence
    workspace = (args.get("workspace") or "").strip()
    con = get_db()
    try:
        marks = ",".join("?" * len(ids))
        ws_clause = " AND workspace_id IN (%s, '')" % "?" if workspace else " AND workspace_id = ''"
        ws_params = [workspace] if workspace else []
        rows = [dict(r) for r in con.execute(
            "SELECT id, text, strong, confirmed, importance FROM facts "
            "WHERE id IN (%s) AND archived=0 AND invalid_at=''" % marks + ws_clause,
            ids + ws_params)]
    finally:
        con.close()
    if len(rows) != len(set(ids)):
        return {"error": "some ids are not active facts", "found": len(rows)}
    protected = [r["id"] for r in rows if r.get("strong") or r.get("confirmed")]
    if protected:
        return {"error": "strong/confirmed facts cannot be consolidated",
                "protected_ids": protected}
    rows.sort(key=lambda r: r["id"])
    try:
        snippet = "\n".join("- id=%s: %s" % (r["id"], r["text"][:500]) for r in rows)
        verdict = llm.chat_json([
            {"role": "system", "content": CONSOLIDATE_PROMPT},
            {"role": "user", "content": snippet},
        ])
    except Exception:
        return {"error": "consolidation failed (provider error; see server stderr)"}
    if not verdict.get("merge"):
        return {"merged": False, "ids": ids, "reason": verdict.get("reason", "")}
    text = (verdict.get("text") or "").strip()
    if not text:
        return {"error": "consolidation returned an empty text"}
    importance = max((float(r.get("importance") or 0.5) for r in rows), default=0.5)
    rf = {"text": text, "source": "consolidate",
          "importance": verdict.get("importance", importance)}
    if workspace:
        rf["workspace"] = workspace
    res = remember_fact(rf)
    if "error" in res:
        return {"error": res["error"]}
    new_id = res["id"]
    invalidated = []
    for r in rows:
        if _invalidate(r["id"], new_id):
            ev = {"fact_id": new_id, "source_ref": "consolidated:%s" % r["id"]}
            if workspace:
                ev["workspace"] = workspace
            attach_evidence(ev)
            invalidated.append(r["id"])
    return {"merged": True, "new_id": new_id, "source_ids": invalidated,
            "reason": verdict.get("reason", ""), "text": text}



def check_new_facts(new_facts):
    """Ingestion hook: verify new facts and archive superseded old ones."""
    from memory_mcp import attach_evidence, forget_fact
    threshold = _min_confidence()
    summary = {"checked": 0, "superseded": [], "conflicts": [], "applied": 0, "skipped_low_conf": 0}
    for nf in new_facts:
        # the fact under test is excluded from the candidates it can target
        candidates = [c for c in _candidates(nf["text"], workspace=nf.get("workspace", ""))
                      if c["id"] != nf["id"]]
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
            ev = {"fact_id": nf["id"], "source_ref": "%s:%s" % (action, cid)}
            if nf.get("workspace"):
                ev["workspace"] = nf["workspace"]
            attach_evidence(ev)
    return summary


def anchor_health(args):
    """Read-only anchor drift check used by CI and local health checks."""
    from memory_mcp import (_anchor_root, _verify_anchor, _ws_check,
                            _ws_inactive_error, _workspace, get_db)

    root_value = args.get("repo_root", args.get("root", ""))
    if not isinstance(root_value, str) or not root_value.strip():
        return {"error": "repo_root is required"}
    root = _anchor_root(root_value)
    if not root:
        return {"error": "repo_root is not an accessible directory"}
    workspace = _workspace(args)
    repo = args.get("repo", "") or ""
    if not isinstance(repo, str):
        return {"error": "repo must be a string"}
    con = get_db()
    try:
        inactive = _ws_inactive_error(con, workspace)
        if inactive:
            return inactive
        facts_sql = (
            "SELECT e.id, e.fact_id, e.repo, e.ref, e.path, e.symbol, "
            "e.start_line, e.start_col, e.end_line, e.end_col, "
            "e.selected_text_hash, e.resolution_status "
            "FROM evidence e JOIN facts f ON f.id=e.fact_id "
            "WHERE f.archived=0 AND f.invalid_at='' AND f.lifecycle='active' "
            "AND e.path != ''"
        )
        facts_params = []
        facts_sql += _ws_check("f", workspace)
        if workspace:
            facts_params.append(workspace)
        if repo:
            facts_sql += " AND e.repo=?"
            facts_params.append(repo)
        rows = list(con.execute(facts_sql, facts_params))

        decisions_sql = "SELECT id, path, symbol FROM decisions WHERE path != ''"
        decisions_params = []
        decisions_sql += _ws_check("decisions", workspace)
        if workspace:
            decisions_params.append(workspace)
        decision_rows = list(con.execute(decisions_sql, decisions_params))

        counts = {name: 0 for name in ("STRONG", "WEAK", "STALE", "REBUILT", "REMOVED")}
        drift = []
        weak = []

        def record(kind, row):
            fields = dict(row)
            verdict = _verify_anchor(fields, root)
            name = verdict["verdict"]
            counts[name] = counts.get(name, 0) + 1
            item = {"kind": kind, "id": row["id"], "path": row["path"],
                    "verdict": name, "reason": verdict["reason"]}
            if verdict.get("resolved_path"):
                item["resolved_path"] = verdict["resolved_path"]
            if name in ("STALE", "REBUILT", "REMOVED"):
                drift.append(item)
            elif name == "WEAK":
                weak.append(item)

        for row in rows:
            record("fact_anchor", row)
        for row in decision_rows:
            record("decision_anchor", row)
        return {"ok": not drift, "checked": sum(counts.values()),
                "counts": counts, "drift": drift[:100], "weak": weak[:100],
                "drift_truncated": len(drift) > 100,
                "weak_truncated": len(weak) > 100,
                "workspace": workspace, "repo": repo}
    finally:
        con.close()


def _health_main(argv=None):
    parser = argparse.ArgumentParser(description="Check memory-mcp code anchors for filesystem drift")
    parser.add_argument("--health", action="store_true", help="run the read-only anchor health check (default)")
    parser.add_argument("--root", dest="repo_root", default=".",
                        help="local repository root used for verification")
    parser.add_argument("--repo", default="", help="optional exact repository id/URL filter")
    parser.add_argument("--workspace", default="", help="optional workspace scope")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="emit machine-readable JSON")
    opts = parser.parse_args(argv)
    result = anchor_health({"repo_root": opts.repo_root, "repo": opts.repo,
                            "workspace": opts.workspace})
    if opts.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    elif "error" in result:
        print("anchor health: ERROR: %s" % result["error"], file=sys.stderr)
    else:
        status = "PASS" if result["ok"] else "FAIL"
        print("anchor health: %s (checked=%d, strong=%d, weak=%d, drift=%d)" %
              (status, result["checked"], result["counts"]["STRONG"],
               result["counts"]["WEAK"], len(result["drift"])))
    if "error" in result:
        return 2
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(_health_main())
