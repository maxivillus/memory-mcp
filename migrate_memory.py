#!/usr/bin/env python3
"""Phase 3: migrate native reasonix memory facts -> memory-mcp (shared store).

Source: ~/.reasonix/projects/<slug>/memory/*.md (frontmatter + body).
Target: memory-mcp server (spawned once, remember_fact batch).
Mapping: text = title: description + body; trust = metadata.trust; domain = metadata.type;
project = source project slug; source = migration-20260815; strong = false.
"""
import json
import os
import re
import subprocess
import sys
import time

HOME = os.path.expanduser("~")
# All paths env-overridable so the tool runs in any environment; defaults are
# the host layout.
SRC_DIR = os.environ.get("MEMORY_MIGRATE_SRC", f"{HOME}/.reasonix/projects/<slug>/memory")
MCP_CMD = os.environ.get("MEMORY_MCP_CMD", "/home/<user>/.local/bin/memory-mcp")
MCP_DB = os.environ.get("MEMORY_MCP_DB", "/home/<user>/shared-store/facts.db")
PROJECT_SLUG = os.environ.get("MEMORY_MIGRATE_PROJECT", "<slug>")

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
    def __init__(self, cmd, db):
        self.proc = subprocess.Popen(
            [cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env={**os.environ, "MEMORY_MCP_DB": db})
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
    print(f"facts to migrate: {len(facts)}", flush=True)
    client = MCPClient(MCP_CMD, MCP_DB)
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
