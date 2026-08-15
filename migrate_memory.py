#!/usr/bin/env python3
"""Phase 3: migrate native reasonix memory facts -> memory-mcp (shared store).

Source: reasonix project memory dir (auto-discovered: first `<project>/memory`
under `~/.reasonix/projects/`; override with MEMORY_MIGRATE_SRC).
Target: memory-mcp server (spawned once, remember_fact batch).
Mapping: text = title: description + body; trust = metadata.trust; domain = metadata.type;
project = source project slug; source = migration-20260815; strong = false.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time

HOME = os.path.expanduser("~")


def _default_src_dir():
    """First reasonix project dir that has a memory/ subdir (portable discovery).

    Symlinked project dirs are skipped: a planted symlink could redirect the
    migration source.
    """
    projects = os.path.join(HOME, ".reasonix", "projects")
    if os.path.isdir(projects):
        for name in sorted(os.listdir(projects)):
            proj = os.path.join(projects, name)
            if os.path.islink(proj):
                continue
            candidate = os.path.join(proj, "memory")
            if os.path.isdir(candidate):
                return candidate
    raise SystemExit(
        "no reasonix project memory dir found under ~/.reasonix/projects/ — "
        "set MEMORY_MIGRATE_SRC explicitly")


# All paths env-overridable; defaults are portable (no host paths):
#   MEMORY_MIGRATE_SRC     — source memory dir (default: auto-discovered)
#   MEMORY_MIGRATE_PROJECT — project slug for fact.project (default: derived
#                            from the discovered project dir name)
#   MEMORY_MCP_CMD         — server command (default: 'memory-mcp' via PATH)
#   MEMORY_MCP_DB          — target DB (default: XDG-style user data path;
#                            only propagated to the server when explicitly set,
#                            so a host wrapper pin is never overridden)
SRC_DIR = os.environ.get("MEMORY_MIGRATE_SRC") or _default_src_dir()
PROJECT_SLUG = os.environ.get("MEMORY_MIGRATE_PROJECT") or os.path.basename(os.path.dirname(SRC_DIR))
MCP_DB_EXPLICIT = "MEMORY_MCP_DB" in os.environ
MCP_DB = os.environ.get("MEMORY_MCP_DB") or os.path.join(HOME, ".local", "share", "memory-mcp", "facts.db")
MCP_CMD = os.environ.get("MEMORY_MCP_CMD") or shutil.which("memory-mcp") or "memory-mcp"

FM_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def parse_frontmatter(text):
    m = FM_RE.match(text)
    fm = {}
    if m:
        for line in m.group(1).splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                fm[k.strip()] = v.strip().strip('"')
    return fm


def load_facts():
    facts = []
    for fn in sorted(os.listdir(SRC_DIR)):
        if not fn.endswith(".md") or fn == "MEMORY.md":
            continue
        raw = open(os.path.join(SRC_DIR, fn), encoding="utf-8").read()
        fm = parse_frontmatter(raw)
        body = FM_RE.sub("", raw).strip()
        title = fm.get("title") or fn[:-3]
        desc = fm.get("description") or ""
        meta = {}
        mm = re.search(r"metadata:\n((?:  \w+: .*\n?)*)", raw)
        if mm:
            for line in mm.group(1).splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    meta[k.strip()] = v.strip()
        text = f"{title}: {desc}".strip()
        if body:
            text += "\n\n" + body
        trust = meta.get("trust", "medium")
        if trust not in ("high", "medium", "low"):
            trust = "medium"
        facts.append({
            "text": text,
            "trust": trust,
            "domain": meta.get("type", ""),
            "project": PROJECT_SLUG,
        })
    return facts


class MCPClient:
    def __init__(self, cmd, db, db_explicit):
        env = dict(os.environ)
        # Propagate the target DB only when explicitly requested: otherwise the
        # spawned server (e.g. a host wrapper pin) decides, and an implicit
        # XDG default can never silently redirect a pinned store.
        if db_explicit:
            env["MEMORY_MCP_DB"] = db
        self.proc = subprocess.Popen(
            [cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env)
        self.next_id = 0

    def call(self, method, params=None):
        self.next_id += 1
        rid = self.next_id
        payload = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            payload["params"] = params
        self.proc.stdin.write((json.dumps(payload) + "\n").encode())
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("server closed")
            msg = json.loads(line)
            if msg.get("id") == rid:
                if msg.get("error"):
                    raise RuntimeError(msg["error"])
                return msg["result"]

    def close(self):
        self.proc.stdin.close()
        self.proc.terminate()


def main():
    facts = load_facts()
    target = MCP_DB if MCP_DB_EXPLICIT else "spawned server default (e.g. host wrapper pin); set MEMORY_MCP_DB to pin"
    print(f"source: {SRC_DIR}")
    print(f"server: {MCP_CMD}")
    print(f"target db: {target}", flush=True)
    print(f"facts to migrate: {len(facts)}", flush=True)
    client = MCPClient(MCP_CMD, MCP_DB, MCP_DB_EXPLICIT)
    try:
        client.call("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                                   "clientInfo": {"name": "migration", "version": "1"}})
        inserted = dedup = errors = 0
        t0 = time.time()
        for f in facts:
            try:
                r = client.call("tools/call", {"name": "remember_fact", "arguments": f})
                res = json.loads(r["content"][0]["text"])
                if res.get("error"):
                    errors += 1
                elif res.get("dedup"):
                    dedup += 1
                else:
                    inserted += 1
            except Exception as e:  # noqa: BLE001
                errors += 1
                print("ERR", f["text"][:60], e, flush=True)
        dt = time.time() - t0
        print(f"inserted={inserted} dedup={dedup} errors={errors} time={dt:.1f}s", flush=True)
    finally:
        client.close()


if __name__ == "__main__":
    main()
