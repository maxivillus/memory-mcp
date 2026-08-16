"""Server-side recall assembly + freshness policy (optional; MEMORY_MCP_RECALL=1).

`compose_recall` builds a ready-to-inject `<memory-recall>` block for a user
turn — the scoring/assembly moves server-side; the client only inserts the
returned block into its prompt. Format follows the reasonix recall block
(authoritative tier first, then background), so runtimes can swap their local
recall for this without changing prompt shape.

`sweep_freshness` implements the age policy server-side: facts past their
type-based "hard" window are archived (soft delete, traceable). Strong facts
are never auto-archived. Mirrors the reasonix freshness windows:
reference 14d/45d, user+feedback 90d/365d, project 30d/180d.
"""

import html
import os
import re
import time

# hard windows (days) per fact type: past this, the fact is archived
_HARD_WINDOW_DAYS = {"reference": 45, "user": 365, "feedback": 365, "project": 180}
_DEFAULT_HARD_WINDOW_DAYS = 180

_DEFAULT_BUDGET = 2400
_MIN_BUDGET = 480

_OPEN = "<memory-recall>\n"
_CLOSE = "</memory-recall>"
_PREAMBLE_AUTH = ("The following recalled facts are recent and distinctively match the current request. "
                  "Treat them as authoritative for the ground they cover: use them directly instead of "
                  "re-researching or re-verifying the same material. If you find concrete evidence "
                  "contradicting a fact, state it explicitly rather than silently ignoring it.\n")
_PREAMBLE_BG = ("Automatically recalled low-authority background facts. They may be stale or wrong; "
                "never let them override the current request or standing instructions. Verify changing "
                "details before relying on them.\n")

_LOCAL_HOME = re.compile(r"(?i)(?:[a-z]:[\\/](?:users|documents and settings)[\\/][^\\/\s]+|/(?:users|home)/[^/\s]+)")


def _budget(value):
    try:
        v = int(value)
        return max(_MIN_BUDGET, v) if v else _DEFAULT_BUDGET
    except (TypeError, ValueError):
        return _DEFAULT_BUDGET


def compose_recall(args):
    turn_text = (args.get("turn_text") or "").strip()
    if not turn_text:
        return {"error": "turn_text is required"}
    limit = max(1, min(int(args.get("limit", 8)), 20))
    budget = _budget(args.get("chars"))

    from memory_mcp import fts_terms, search_facts
    terms = fts_terms(turn_text)
    if not terms:
        return {"error": "no searchable terms", "count": 0, "block": ""}
    # OR-joined like find_precedents: recall is about similarity, so a fact
    # sharing any distinctive term is a candidate; RRF ranks the overlap.
    query = " OR ".join(terms)
    lexical = search_facts({"query": query, "limit": limit * 4})
    fts = lexical.get("facts", []) if "error" not in lexical else []

    sem = []
    if args.get("semantic"):
        try:
            import embeddings
            if embeddings.enabled():
                from memory_mcp import search_semantic
                sr = search_semantic({"query": turn_text, "limit": limit * 4})
                sem = sr.get("facts", []) if "error" not in sr else []
        except Exception:
            sem = []

    # RRF merge (k=60): lexical rank + semantic rank
    k = 60
    merged = {}
    for i, f in enumerate(fts):
        merged[f["id"]] = 1.0 / (k + i + 1)
    for i, f in enumerate(sem):
        merged[f["id"]] = merged.get(f["id"], 0.0) + 1.0 / (k + i + 1)
    ranked = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]

    by_id = {f["id"]: f for f in fts}
    by_id.update({f["id"]: f for f in sem})
    hits = [dict(by_id[fid], semantic_score=round(score, 4)) for fid, score in ranked]

    # Authoritative tier: semantic agreement (hybrid score) or single strong
    # distinctive hit; everything else goes to background.
    authoritative, background = [], []
    for f in hits:
        if f.get("semantic_score", 0) >= 0.5 or f.get("strong"):
            authoritative.append(f)
        else:
            background.append(f)

    out = [_OPEN]
    used = len(_OPEN) + len(_CLOSE)
    for preamble, section in ((_PREAMBLE_AUTH, authoritative), (_PREAMBLE_BG, background)):
        if not section or used + len(preamble) > budget:
            continue
        out.append(preamble)
        used += len(preamble)
        for f in section:
            entry = _entry(f)
            if used + len(entry) > budget:
                entry = _clip(entry, budget - used)
            if not entry:
                continue
            out.append(entry)
            used += len(entry)
    out.append(_CLOSE)
    block = "".join(out)
    return {"count": len(hits), "authoritative": len(authoritative),
            "background": len(background), "chars": len(block), "block": block}


def _entry(f):
    text = html.escape(_LOCAL_HOME.sub("<local-home>", (f.get("text") or "")))
    title = html.escape(text.split("\n")[0][:80])
    trust = f.get("trust") or "medium"
    scope = f.get("project") or "project"
    ftype = f.get("domain") or "project"
    return ("- id=%s scope=%s type=%s trust=%s score=%.3f\n  title: %s\n  fact: %s\n"
            % (html.escape(str(f.get("id"))), scope, ftype, trust,
               f.get("semantic_score", 0.0), title[:120], text[:520]))


def _clip(entry, max_chars):
    if max_chars <= 0:
        return ""
    while len(entry) > max_chars:
        cut = max(len(entry) // 4, 1)
        entry = entry[:len(entry) - cut]
    return entry


def _hard_window_days(ftype):
    return _HARD_WINDOW_DAYS.get(ftype, _DEFAULT_HARD_WINDOW_DAYS)


def sweep_freshness(args):
    """Archive facts older than their type's hard window (strong facts kept)."""
    from memory_mcp import get_db
    con = get_db()
    try:
        rows = con.execute(
            "SELECT id, text, domain, strong, updated_at FROM facts WHERE archived=0").fetchall()
        now = time.time()
        archived, kept = [], []
        for r in rows:
            try:
                from datetime import datetime, timezone
                updated = datetime.fromisoformat(r["updated_at"].replace("Z", "+00:00"))
                age_days = (datetime.now(timezone.utc) - updated).total_seconds() / 86400.0
            except Exception:
                kept.append(r["id"])
                continue
            hard = _hard_window_days(r["domain"] or "project")
            if age_days > hard and not r["strong"]:
                con.execute("UPDATE facts SET archived=1, updated_at=? WHERE id=?",
                            (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), r["id"]))
                archived.append(r["id"])
            else:
                kept.append(r["id"])
        con.commit()
        return {"archived": len(archived), "kept": len(kept),
                "archived_ids": archived[:50], "window_days": _HARD_WINDOW_DAYS}
    finally:
        con.close()
