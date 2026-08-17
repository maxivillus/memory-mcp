"""v0.7 — automatic fact decay (memory-mcp).

Lifecycle: active -> degraded -> forgotten, driven by the number of ACTIVE
days (days with at least one memory-mcp call — see activity_days) since the
fact's last search hit (or creation, if never hit).

    score = importance * DECAY_RATE ^ active_days

- degraded  (score < 0.25): hidden from plain search results; still reachable
  via entity-graph/session chains; revived after DECAY_REVIVE_HITS matching
  searches (handled in memory_mcp._revive_degraded).
- forgotten (score <= 0.1): excluded everywhere; visible only via
  list_forgotten / restore_fact.
- strong=1 and confirmed=1 facts never decay (protected, like sweep).

User downtime does NOT age facts: decay counts only days present in
activity_days, so days without any system activity are skipped.
"""

import os


def _param(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


RATE = _param("DECAY_RATE", 0.95)
ARCHIVE = _param("DECAY_ARCHIVE", 0.25)
FORGET = _param("DECAY_FORGET", 0.1)


def _active_days_since(con, day):
    """Number of active days strictly after the given ISO day ('' -> 0)."""
    if not day:
        return 0
    return con.execute("SELECT COUNT(*) FROM activity_days WHERE day > ?", [day]).fetchone()[0]


def decay_sweep(args):
    """Full lifecycle recompute over active facts. Soft transitions only:
    active->degraded->forgotten down, and back up when the score recovers
    (e.g. after restore_fact or revived hits refreshed last_accessed_at)."""
    from memory_mcp import get_db, now
    con = get_db()
    try:
        rows = con.execute(
            "SELECT id, importance, strong, confirmed, lifecycle, last_accessed_at, created_at "
            "FROM facts WHERE archived=0 AND invalid_at=''").fetchall()
        moved = {"to_degraded": 0, "to_forgotten": 0, "to_active": 0}
        counts = {"active": 0, "degraded": 0, "forgotten": 0}
        max_days = 0
        for r in rows:
            if r["strong"] or r["confirmed"]:
                target = "active"
            else:
                base_day = (r["last_accessed_at"] or r["created_at"] or "")[:10]
                n = _active_days_since(con, base_day)
                max_days = max(max_days, n)
                score = float(r["importance"] or 0.5) * (RATE ** n)
                if score <= FORGET:
                    target = "forgotten"
                elif score < ARCHIVE:
                    target = "degraded"
                else:
                    target = "active"
            if target != r["lifecycle"]:
                con.execute("UPDATE facts SET lifecycle=?, updated_at=? WHERE id=?",
                            (target, now(), r["id"]))
                moved["to_" + target] += 1
            counts[target] += 1
        con.commit()
        return {
            "scanned": len(rows),
            "active": counts["active"],
            "degraded": counts["degraded"],
            "forgotten": counts["forgotten"],
            "moved": moved,
            "max_active_days": max_days,
            "params": {"rate": RATE, "archive": ARCHIVE, "forget": FORGET},
        }
    finally:
        con.close()


def list_forgotten(args):
    """Direct review of forgotten/inactive facts (the only way to see them)."""
    from memory_mcp import get_db
    limit = max(1, min(int(args.get("limit", 50)), 200))
    con = get_db()
    try:
        rows = [dict(r) for r in con.execute(
            "SELECT id, text, source, project, domain, trust, importance, "
            "created_at, last_accessed_at, lifecycle "
            "FROM facts WHERE archived=0 AND invalid_at='' AND lifecycle='forgotten' "
            "ORDER BY importance DESC, updated_at DESC LIMIT ?", [limit])]
        return {"count": len(rows), "facts": rows}
    finally:
        con.close()


def restore_fact(args):
    """Manually bring a forgotten/degraded fact back to active."""
    from memory_mcp import get_db, now
    fid = args.get("id")
    if fid is None:
        return {"error": "id is required"}
    con = get_db()
    try:
        row = con.execute("SELECT id, lifecycle, archived FROM facts WHERE id=?", [fid]).fetchone()
        if row is None:
            return {"error": "fact not found", "id": fid}
        if row["archived"]:
            return {"error": "fact is archived (soft-deleted); re-remember it instead", "id": fid}
        ts = now()
        con.execute("UPDATE facts SET lifecycle='active', revival_count=0, "
                    "last_accessed_at=?, updated_at=? WHERE id=?", (ts, ts, fid))
        con.commit()
        return {"restored": fid, "from": row["lifecycle"], "to": "active"}
    finally:
        con.close()
