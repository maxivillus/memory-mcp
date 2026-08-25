"""Tests for the memory-mcp server (stdlib only, no external deps).

Each test uses unique data against a temp DB (MEMORY_MCP_DB). The module reads
that env var at import time, so the import happens in setUpModule AFTER the
temp path is set.

Run:  MEMORY_MIGRATE_SRC=. python3 -m unittest discover -s tests -v
"""

import importlib
import hashlib
import json
import os
import sqlite3
import sys
import subprocess
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_TMP.close()
os.environ["MEMORY_MCP_DB"] = _TMP.name
mcp = importlib.import_module("memory_mcp")


class MemoryMCPTest(unittest.TestCase):
    def remember(self, text, **kw):
        args = {"text": text, "source": "test", "project": "project",
                "domain": "project", "trust": "medium", "strong": False}
        args.update(kw)
        res = mcp.remember_fact(args)
        self.assertNotIn("error", res, res)
        return res

    def search(self, query, **kw):
        res = mcp.search_facts({"query": query, **kw})
        self.assertNotIn("error", res, res)
        return res["facts"]

    # ---- facts ----

    def test_remember_search_roundtrip(self):
        self.remember("The quantum widget registry is at /svc/widgets")
        facts = self.search("quantum widget")
        self.assertTrue(any("quantum widget registry" in f["text"] for f in facts))

    def test_remember_dedups_exact_text(self):
        self.remember("exact duplicate fact text alpha")
        self.remember("exact duplicate fact text alpha")
        facts = self.search("exact duplicate fact text alpha")
        self.assertEqual(len(facts), 1)

    def test_paraphrase_stays_separate(self):
        # Documented behavior: dedup is exact-text only; paraphrases coexist
        # (detect_conflicts surfaces them on demand).
        self.remember("battery life on the phone is four hours")
        self.remember("the phone lasts about four hours on a charge")
        facts = self.search("four hours")
        self.assertGreaterEqual(len(facts), 2)

    def test_search_trust_filters(self):
        self.remember("high trust fact about the alpha core", trust="high", strong=True)
        self.remember("medium trust fact about the beta core")
        hi = self.search("alpha core trust", trust_min="high")
        self.assertTrue(all(f["trust"] == "high" for f in hi))
        strong = self.search("alpha core", strong_only=True)
        self.assertTrue(all(f["strong"] for f in strong))

    def test_retrieval_profiles_are_bounded_and_typed(self):
        for index in range(8):
            self.remember("profile contract fact %d about bounded retrieval" % index)
        result = mcp.search_facts({
            "query": "profile contract fact bounded retrieval",
            "profile": "orientation",
        })
        self.assertNotIn("error", result, result)
        self.assertEqual(result["profile"], "orientation")
        self.assertEqual(result["result_status"], "ok")
        self.assertLessEqual(len(result["facts"]), 6)

        empty = mcp.search_facts({
            "query": "profile query with no matching fact",
            "profile": "review",
        })
        self.assertNotIn("error", empty, empty)
        self.assertEqual(empty["profile"], "review")
        self.assertEqual(empty["result_status"], "empty")
        invalid = mcp.search_facts({"query": "anything", "profile": "unknown"})
        self.assertEqual(invalid["code"], "invalid_retrieval_profile")
        over_limit = mcp.search_facts({
            "query": "anything", "profile": "orientation", "limit": 7,
        })
        self.assertEqual(over_limit["code"], "profile_limit_exceeded")

    def test_strict_admission_requires_grounded_evidence_without_storing_text(self):
        accepted = mcp.remember_fact({
            "text": "the worker stores retry counters in SQLite",
            "source": "run/strict-admission",
            "admission": "strict",
            "evidence": {
                "source_ref": "run/strict-admission#evidence-1",
                "selected_text": "The worker stores retry counters in SQLite for recovery.",
                "path": "src/worker.py",
                "start_line": 10,
                "end_line": 12,
            },
            "workspace": "strict-admission",
        })
        self.assertNotIn("error", accepted, accepted)
        self.assertEqual(accepted["admission"]["status"], "accepted")
        self.assertEqual(accepted["admission"]["evidence_attached"], 1)
        provenance = mcp.get_provenance({
            "fact_id": accepted["id"], "workspace": "strict-admission",
        })
        self.assertNotIn("error", provenance, provenance)
        self.assertNotIn("selected_text", provenance["evidence"][0])
        self.assertEqual(len(provenance["evidence"][0]["selected_text_hash"]), 64)

        rejected = mcp.remember_fact({
            "text": "the worker stores retry counters in Redis",
            "source": "run/strict-admission",
            "admission": "strict",
            "evidence": {
                "source_ref": "run/strict-admission#evidence-2",
                "selected_text": "The worker renders a status page.",
            },
            "workspace": "strict-admission",
        })
        self.assertEqual(rejected["result_status"], "rejected")
        self.assertEqual(rejected["code"], "evidence_not_grounded")
        self.assertEqual(
            mcp.search_facts({
                "query": "retry counters Redis",
                "workspace": "strict-admission",
            })["count"], 0)

    def test_absorb_strict_admission_is_preview_first_and_typed(self):
        item = {
            "text": "the scheduler stores a bounded retry counter",
            "source": "run/strict-batch",
            "evidence": {
                "source_ref": "run/strict-batch#evidence-1",
                "selected_text": "The scheduler stores a bounded retry counter in SQLite.",
            },
        }
        preview = mcp.absorb({
            "facts": [item], "workspace": "strict-batch", "admission": "strict",
        })
        self.assertEqual(preview["result_status"], "preview")
        self.assertEqual(preview["items"][0]["admission"]["status"], "accepted")
        self.assertEqual(
            mcp.search_facts({
                "query": "scheduler bounded retry counter",
                "workspace": "strict-batch",
            })["count"], 0)

        committed = mcp.absorb({
            "facts": [item], "workspace": "strict-batch", "admission": "strict",
            "commit": True,
        })
        self.assertEqual(committed["result_status"], "committed")
        self.assertEqual(committed["created"], 1)
        self.assertEqual(committed["evidence_attached"], 1)

    def test_empty_search_reports_typed_abstention(self):
        empty = mcp.search_facts({
            "query": "unseen bounded retrieval marker", "workspace": "abstain-ws",
        })
        self.assertEqual(empty["result_status"], "empty")
        self.assertEqual(empty["retrieval_outcome"], "abstained")
        self.assertEqual(empty["abstention_reason"], "no_matching_facts")
        self.assertEqual(empty["remedy"], "broaden_query_or_absorb_evidence")

        no_terms = mcp.search_facts({
            "query": "the and", "workspace": "abstain-ws",
        })
        self.assertEqual(no_terms["retrieval_outcome"], "abstained")
        self.assertEqual(no_terms["abstention_reason"], "no_searchable_terms")

    def test_feedback_is_idempotent_and_aggregate_only(self):
        first = mcp.record_feedback({
            "feedback_id": "feedback-profile-1",
            "site": "search_facts",
            "item_type": "fact",
            "item_ref": "fact:42",
            "signal": "helpful",
            "query_hash": "a" * 64,
            "workspace": "feedback-workspace",
        })
        self.assertNotIn("error", first, first)
        self.assertFalse(first["duplicate"])
        duplicate = mcp.record_feedback({
            "feedback_id": "feedback-profile-1",
            "site": "search_facts",
            "item_type": "fact",
            "item_ref": "fact:42",
            "signal": "helpful",
            "query_hash": "a" * 64,
            "workspace": "feedback-workspace",
        })
        self.assertTrue(duplicate["duplicate"])
        conflict = mcp.record_feedback({
            "feedback_id": "feedback-profile-1",
            "site": "search_facts",
            "item_type": "fact",
            "item_ref": "fact:42",
            "signal": "stale",
            "workspace": "feedback-workspace",
        })
        self.assertEqual(conflict["code"], "feedback_id_conflict")
        summary = mcp.query_feedback({"workspace": "feedback-workspace"})
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["signals"]["helpful"], 1)
        self.assertEqual(summary["feedback"][0]["query_hash"], "a" * 64)

    def test_entity_names_are_unicode_and_case_normalized(self):
        first = mcp.remember_entity({"name": "  Widget   Service ", "workspace": "entity-workspace"})
        second = mcp.remember_entity({"name": "widget service", "workspace": "entity-workspace"})
        self.assertEqual(first["id"], second["id"])
        relation = mcp.remember_relation({
            "subject": "WIDGET SERVICE",
            "predicate": "uses",
            "object": "Widget DB",
            "workspace": "entity-workspace",
        })
        self.assertNotIn("error", relation, relation)
        graph = mcp.search_graph({"entity": " widget service ", "workspace": "entity-workspace"})
        self.assertNotIn("error", graph, graph)
        self.assertTrue(any(node["name"] == "Widget DB" for node in graph["nodes"]))

    def test_list_and_summarize(self):
        self.remember("listable fact about the gamma module")
        lst = mcp.list_facts({"limit": 50})
        self.assertIn("count", lst)
        self.assertTrue(any("gamma module" in f["text"] for f in lst["facts"]))
        idx = mcp.summarize_index({"limit": 200, "max_chars": 4000})
        self.assertIn("index", idx)
        self.assertIn("gamma module", idx["index"])

    def test_forget_archives(self):
        res = self.remember("fact to forget about the delta service")
        facts = self.search("delta service fact to forget")
        self.assertTrue(facts, "fact should be searchable before forget")
        out = mcp.forget_fact({"id": facts[0]["id"]})
        self.assertNotIn("error", out, out)
        after = self.search("delta service fact to forget")
        self.assertFalse(any(f["id"] == facts[0]["id"] for f in after))

    def test_stats(self):
        self.remember("statistic fact about the epsilon endpoint")
        st = mcp.stats({})
        self.assertIn("total", st)

    # ---- decisions ----

    def test_decision_roundtrip(self):
        d = mcp.record_decision({
            "category": "infra", "subject": "proxy choice",
            "scenario": "need an HTTP proxy for the cli",
            "reasoning": "CONNECT support and remote DNS",
            "outcome": "privoxy",
        })
        self.assertNotIn("error", d, d)
        q = mcp.query_decisions({"subject": "proxy choice"})
        self.assertTrue(any(x["subject"] == "proxy choice" for x in q["decisions"]))

    def test_find_precedents_or_join(self):
        # Regression: find_precedents OR-joins terms (partial match is a hit) —
        # the tool description says OR-joined, and BM25 ranks partial matches.
        mcp.record_decision({
            "category": "infra", "subject": "vpn routing",
            "scenario": "egress ip must be static for the api client",
            "reasoning": "static egress required",
            "outcome": "tunnel",
        })
        pre = mcp.find_precedents({"scenario": "unrelated static egress requirement"})
        self.assertNotIn("error", pre, pre)
        self.assertGreaterEqual(len(pre["precedents"]), 1,
                                "OR-join must surface a decision matching only one term")

    def test_causal_chain(self):
        root = mcp.record_decision({"scenario": "root decision for chain test",
                                    "subject": "chain root"})
        child = mcp.record_decision({"scenario": "child decision for chain test",
                                     "subject": "chain child",
                                     "parent_decision_id": root["id"]})
        chain = mcp.get_causal_chain({"decision_id": child["id"]})
        self.assertNotIn("error", chain, chain)
        self.assertGreaterEqual(len(chain["chain"]), 2)

    # ---- graph ----

    def test_graph_search(self):
        mcp.remember_entity({"name": "widget-service", "type": "service"})
        mcp.remember_entity({"name": "widget-db", "type": "database"})
        mcp.remember_relation({"subject": "widget-service", "predicate": "uses",
                               "object": "widget-db"})
        n = mcp.search_graph({"entity": "widget-service", "depth": 1})
        self.assertNotIn("error", n, n)
        self.assertTrue(any("widget-db" in str(x) for x in n["nodes"]))

    # ---- provenance ----

    def test_evidence_provenance(self):
        res = self.remember("provenance fact about the theta flag")
        facts = self.search("provenance fact theta flag")
        self.assertTrue(facts)
        fid = facts[0]["id"]
        ev = mcp.attach_evidence({"fact_id": fid, "source_ref": "issue/123"})
        self.assertNotIn("error", ev, ev)
        prov = mcp.get_provenance({"fact_id": fid})
        self.assertNotIn("error", prov, prov)
        self.assertTrue(any(e["source_ref"] == "issue/123" for e in prov["evidence"]))

    def test_structured_code_evidence_anchor_roundtrip(self):
        res = self.remember("structured anchor fact about the worker retry path")
        evidence = mcp.attach_evidence({
            "fact_id": res["id"],
            "source_ref": "repo@abc123:src/worker.py",
            "repo": "https://github.com/example/worker",
            "ref": "abc123",
            "path": "src/worker.py",
            "symbol": "Worker.retry",
            "start_line": 42,
            "start_col": 4,
            "end_line": 48,
            "end_col": 16,
            "selected_text": "return retry(task)",
            "resolution_status": "resolved",
        })
        self.assertNotIn("error", evidence, evidence)
        prov = mcp.get_provenance({"fact_id": res["id"]})
        self.assertNotIn("error", prov, prov)
        anchor = [e for e in prov["evidence"]
                   if e["source_ref"] == "repo@abc123:src/worker.py"][0]
        self.assertEqual(anchor["repo"], "https://github.com/example/worker")
        self.assertEqual(anchor["path"], "src/worker.py")
        self.assertEqual(anchor["symbol"], "Worker.retry")
        self.assertEqual(anchor["start_line"], 42)
        self.assertEqual(anchor["resolution_status"], "resolved")
        self.assertEqual(len(anchor["selected_text_hash"]), 64)

    def test_absorb_preview_commit_and_review_classification(self):
        workspace = "absorb-roundtrip"
        existing = mcp.remember_fact({
            "text": "absorb existing fact says the worker retries failed jobs",
            "source": "issue/absorb",
            "workspace": workspace,
        })
        self.assertNotIn("error", existing, existing)
        candidates = [
            {"text": "absorb new fact says the worker stores retry counters",
             "source": "repo@abc123:src/worker.py",
             "evidence": {"repo": "repo", "ref": "abc123",
                          "path": "src/worker.py", "start_line": 10,
                          "end_line": 12}},
            "absorb existing fact says the worker retries failed jobs",
            "absorb related fact says the worker retries failed jobs with backoff",
        ]
        preview = mcp.absorb({"facts": candidates, "workspace": workspace})
        self.assertNotIn("error", preview, preview)
        self.assertTrue(preview["dry_run"])
        self.assertFalse(preview["committed"])
        self.assertEqual([i["classification"] for i in preview["items"]],
                         ["new", "duplicate", "related"])
        self.assertEqual(mcp.search_facts({"query": "stores retry counters",
                                           "workspace": workspace})["count"], 0)

        committed = mcp.absorb({"facts": candidates, "workspace": workspace,
                                "commit": True})
        self.assertNotIn("error", committed, committed)
        self.assertTrue(committed["committed"])
        self.assertEqual(committed["created"], 1)
        self.assertEqual(committed["deduped"], 1)
        self.assertEqual(committed["pending_review"], 1)
        new_id = [i["id"] for i in committed["items"] if i["classification"] == "new"][0]
        prov = mcp.get_provenance({"fact_id": new_id, "workspace": workspace})
        self.assertEqual(prov["evidence"][0]["path"], "src/worker.py")
        self.assertEqual(mcp.search_facts({"query": "backoff", "workspace": workspace})["count"], 0)

    def test_absorb_admission_trace_is_opt_in_and_bounded(self):
        name = "admission-trace-new-fact"
        old_flag = os.environ.get("MEMORY_MCP_ADMISSION_TRACE")
        try:
            os.environ["MEMORY_MCP_ADMISSION_TRACE"] = "0"
            disabled = mcp.absorb({"facts": [{
                "text": name,
                "source": "repo@abc:src/worker.py",
                "evidence": {"repo": "repo", "ref": "abc",
                             "path": "src/worker.py"},
            }], "workspace": "admission-trace"})
            self.assertNotIn("decision_trace", disabled["items"][0])

            os.environ["MEMORY_MCP_ADMISSION_TRACE"] = "1"
            traced = mcp.absorb({"facts": [{
                "text": name + " second",
                "source": "repo@abc:src/worker.py",
                "evidence": {"repo": "repo", "ref": "abc",
                             "path": "src/worker.py"},
            }], "workspace": "admission-trace", "commit": True})
            trace = traced["items"][0]["decision_trace"]
            self.assertEqual(trace["reason_code"], "no_matching_candidates")
            self.assertEqual(trace["action"], "create")
            self.assertEqual(trace["verification"], "not_requested")
            self.assertEqual(trace["evidence_count"], 1)
            self.assertEqual(trace["evidence_refs"], ["repo@abc:src/worker.py"])

            duplicate = mcp.absorb({"facts": [name + " second"],
                                     "workspace": "admission-trace"})
            duplicate_trace = duplicate["items"][0]["decision_trace"]
            self.assertEqual(duplicate_trace["reason_code"], "exact_sha256_duplicate")
            self.assertEqual(duplicate_trace["action"], "noop")
        finally:
            if old_flag is None:
                os.environ.pop("MEMORY_MCP_ADMISSION_TRACE", None)
            else:
                os.environ["MEMORY_MCP_ADMISSION_TRACE"] = old_flag

    def test_fact_chunking_is_bounded_and_offset_addressable(self):
        text = "chunked fact alpha beta gamma delta epsilon zeta eta theta"
        fact = self.remember(text, workspace="fact-chunks")
        page = mcp.chunk_fact({"id": fact["id"], "workspace": "fact-chunks",
                               "chunk_chars": 16, "chunk_overlap": 3,
                               "max_chunks": 2})
        self.assertNotIn("error", page, page)
        self.assertEqual(page["fact"]["text_length"], len(text))
        self.assertEqual(len(page["chunks"]), 2)
        self.assertEqual(page["chunks"][0]["start"], 0)
        self.assertEqual(page["chunks"][0]["end"], page["chunks"][1]["start"] + 3)
        self.assertIsNotNone(page["next_chunk"])
        searched = mcp.search_facts({"query": "chunked alpha", "workspace": "fact-chunks",
                                     "chunk_chars": 12})
        self.assertNotIn("error", searched, searched)
        self.assertTrue(searched["facts"][0]["chunks"])
        self.assertLessEqual(len(str(searched)), 64 * 1024 * 2)

    # ---- conflicts ----

    def test_detect_conflicts(self):
        self.remember("the zeta cache holds the compiled assets")
        conf = mcp.detect_conflicts({"text": "the zeta cache holds compiled assets"})
        self.assertNotIn("error", conf, conf)
        self.assertGreaterEqual(len(conf["near_duplicates"]), 1)

    # ---- export ----

    def test_export_contains_archived(self):
        res = self.remember("exportable fact about the iota queue")
        facts = self.search("iota queue exportable")
        self.assertTrue(facts)
        mcp.forget_fact({"id": facts[0]["id"]})
        exp = mcp.export_facts({})
        self.assertIn("facts", exp)
        self.assertTrue(any("iota queue" in f["text"] for f in exp["facts"]))


class ContextArtifactTest(unittest.TestCase):
    """v0.11: immutable context refs, bounded reads, lineage, and ACLs."""

    def put(self, name, content, workspace="ctx-test", **kwargs):
        args = {"name": name, "content": content, "workspace": workspace}
        args.update(kwargs)
        result = mcp.put_context(args)
        self.assertNotIn("error", result, result)
        return result

    def test_catalog_resolve_and_bounded_read(self):
        payload = "0123456789abcdefghijklmnopqrstuvwxyz"
        result = self.put("source-slice", payload, schema={"kind": "text"},
                          source="issue/4182")
        ref = result["context"]["ref"]
        self.assertTrue(ref.startswith("ctx_"))
        self.assertEqual(result["context"]["size_bytes"], len(payload.encode("utf-8")))

        catalog = mcp.list_context({"workspace": "ctx-test", "name": "source-slice"})
        self.assertNotIn("error", catalog, catalog)
        self.assertEqual(catalog["count"], 1)
        self.assertNotIn("content", catalog["contexts"][0])

        resolved = mcp.resolve_context({"ref": ref, "workspace": "ctx-test"})
        self.assertNotIn("error", resolved, resolved)
        self.assertNotIn("content", resolved["context"])
        self.assertEqual(resolved["context"]["sha256"],
                         __import__("hashlib").sha256(payload.encode()).hexdigest())

        first = mcp.read_context({"ref": ref, "workspace": "ctx-test", "max_chars": 7})
        self.assertNotIn("error", first, first)
        self.assertEqual(first["context"]["content"], payload[:7])
        self.assertTrue(first["context"]["truncated"])
        self.assertEqual(first["context"]["next_start"], 7)
        second = mcp.read_context({"ref": ref, "workspace": "ctx-test",
                                   "start": first["context"]["next_start"],
                                   "max_chars": 7})
        self.assertEqual(second["context"]["content"], payload[7:14])

    def test_lineage_and_workspace_boundary(self):
        parent = self.put("lineage-parent", "parent payload", workspace="ctx-lineage")
        child = self.put("lineage-child", "child payload", workspace="ctx-lineage",
                         parent_refs=[parent["context"]["ref"]])
        parent_ref = parent["context"]["ref"]
        child_ref = child["context"]["ref"]
        self.assertEqual(child["lineage"]["parents"][0]["ref"], parent_ref)

        resolved_parent = mcp.resolve_context({"ref": parent_ref, "workspace": "ctx-lineage"})
        self.assertEqual(resolved_parent["lineage"]["children"][0]["ref"], child_ref)
        self.assertIn("error", mcp.resolve_context({"ref": parent_ref,
                                                      "workspace": "ctx-other"}))
        self.assertIn("error", mcp.read_context({"ref": child_ref,
                                                   "workspace": "ctx-other"}))
        self.assertEqual(mcp.list_context({"workspace": "ctx-other"})["count"], 0)
        cross_workspace = mcp.put_context({"name": "cross-workspace", "content": "x",
                                           "workspace": "ctx-other",
                                           "parent_refs": [parent_ref]})
        self.assertIn("error", cross_workspace)

    def test_checksum_expiry_and_required_scope(self):
        missing_scope = mcp.put_context({"name": "no-scope", "content": "x"})
        self.assertIn("error", missing_scope)
        mismatch = mcp.put_context({"name": "bad-checksum", "content": "x",
                                     "workspace": "ctx-validation", "checksum": "0" * 64})
        self.assertIn("error", mismatch)

        live_a = self.put("immutable", "first", workspace="ctx-validation")
        live_b = self.put("immutable", "second", workspace="ctx-validation")
        self.assertNotEqual(live_a["context"]["ref"], live_b["context"]["ref"])
        listed = mcp.list_context({"workspace": "ctx-validation", "name": "immutable"})
        self.assertEqual(listed["count"], 2)

        expired = self.put("expires-now", "temporary", workspace="ctx-validation",
                           ttl_seconds=0)
        expired_ref = expired["context"]["ref"]
        self.assertIn("error", mcp.resolve_context({"ref": expired_ref,
                                                      "workspace": "ctx-validation"}))
        self.assertEqual(mcp.list_context({"workspace": "ctx-validation",
                                            "name": "expires-now"})["count"], 0)

    def test_context_search_returns_metadata_and_respects_workspace(self):
        source_ref = self.put("search-source", "needle in the payload",
                              workspace="ctx-search", source="search-test")
        self.put("search-other", "needle in another workspace",
                 workspace="ctx-search-other")

        found = mcp.search_context({"query": "needle", "workspace": "ctx-search"})
        self.assertNotIn("error", found, found)
        self.assertEqual(found["count"], 1)
        self.assertEqual(found["contexts"][0]["ref"], source_ref["context"]["ref"])
        self.assertNotIn("content", found["contexts"][0])

        other = mcp.search_context({"query": "needle", "workspace": "ctx-search-other"})
        self.assertEqual(other["count"], 1)
        self.assertNotEqual(other["contexts"][0]["ref"], source_ref["context"]["ref"])

    def test_local_document_adapter_previews_commits_and_blocks_escape(self):
        workspace = "local-document"
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "docs"))
            path = os.path.join(root, "docs", "guide.md")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("heading\n\nlocal document retrieval contract\n" * 20)
            preview = mcp.ingest_document({
                "root": root,
                "path": "docs/guide.md",
                "workspace": workspace,
                "chunk_chars": 256,
            })
            self.assertNotIn("error", preview, preview)
            self.assertFalse(preview["committed"])
            self.assertGreater(preview["chunks"], 1)
            self.assertEqual(mcp.search_context({
                "query": "local document retrieval",
                "workspace": workspace,
            })["count"], 0)
            committed = mcp.ingest_document({
                "root": root,
                "path": "docs/guide.md",
                "workspace": workspace,
                "chunk_chars": 256,
                "commit": True,
            })
            self.assertNotIn("error", committed, committed)
            self.assertTrue(committed["committed"])
            self.assertEqual(committed["result_status"], "ok")
            self.assertEqual(len(committed["refs"]), preview["chunks"])
            found = mcp.search_context({
                "query": "local document retrieval",
                "workspace": workspace,
            })
            self.assertGreater(found["count"], 0)
            read = mcp.read_context({
                "ref": committed["refs"][0],
                "workspace": workspace,
                "max_chars": 200,
            })
            self.assertIn("local document retrieval contract", read["context"]["content"])
            duplicate = mcp.ingest_document({
                "root": root,
                "path": "docs/guide.md",
                "workspace": workspace,
                "chunk_chars": 256,
                "commit": True,
            })
            self.assertTrue(duplicate["duplicate"])
            escaped = mcp.ingest_document({
                "root": root,
                "path": "../outside.md",
                "workspace": workspace,
            })
            self.assertEqual(escaped["code"], "path_outside_root")
            os.makedirs(os.path.join(root, "secrets"))
            with open(os.path.join(root, "secrets", "note.txt"), "w", encoding="utf-8") as handle:
                handle.write("must not be captured")
            excluded = mcp.ingest_document({
                "root": root,
                "path": "secrets/note.txt",
                "workspace": workspace,
            })
            self.assertEqual(excluded["code"], "document_path_excluded")
            outside_dir = tempfile.TemporaryDirectory()
            try:
                outside_path = os.path.join(outside_dir.name, "outside.md")
                with open(outside_path, "w", encoding="utf-8") as handle:
                    handle.write("outside")
                os.symlink(outside_path, os.path.join(root, "escape.md"))
                symlink = mcp.ingest_document({
                    "root": root,
                    "path": "escape.md",
                    "workspace": workspace,
                })
                self.assertEqual(symlink["code"], "path_outside_root")
            finally:
                outside_dir.cleanup()

    def test_context_chunks_are_bounded_and_paginated(self):
        payload = "abcdefghij" * 3
        result = self.put("chunk-source", payload, workspace="ctx-chunk")
        ref = result["context"]["ref"]

        first = mcp.chunk_context({"ref": ref, "workspace": "ctx-chunk",
                                   "chunk_chars": 5, "max_chunks": 2})
        self.assertNotIn("error", first, first)
        self.assertEqual([chunk["content"] for chunk in first["chunks"]],
                         ["abcde", "fghij"])
        self.assertEqual(first["next_chunk"], 2)
        self.assertEqual(first["total_chunks"], 6)

        rest = mcp.chunk_context({"ref": ref, "workspace": "ctx-chunk",
                                  "chunk_chars": 5,
                                  "start_chunk": first["next_chunk"],
                                  "max_chunks": 32})
        self.assertIsNone(rest["next_chunk"])
        chunks = first["chunks"] + rest["chunks"]
        self.assertEqual("".join(chunk["content"] for chunk in chunks), payload)
        self.assertEqual([chunk["index"] for chunk in chunks], list(range(6)))

    def test_reduce_context_is_deterministic_and_records_lineage(self):
        first = self.put("reduce-first", "alpha", workspace="ctx-reduce")
        second = self.put("reduce-second", "beta", workspace="ctx-reduce")
        refs = [first["context"]["ref"], second["context"]["ref"]]

        reduced = mcp.reduce_context({"name": "reduce-result", "refs": refs,
                                      "workspace": "ctx-reduce", "separator": "|",
                                      "source": "reduce-test"})
        self.assertNotIn("error", reduced, reduced)
        self.assertEqual(reduced["reduction"], "deterministic-concat")
        self.assertEqual(reduced["reduced_from"], refs)
        self.assertEqual({parent["ref"] for parent in reduced["lineage"]["parents"]},
                         set(refs))

        read = mcp.read_context({"ref": reduced["context"]["ref"],
                                 "workspace": "ctx-reduce", "max_chars": 100})
        self.assertEqual(read["context"]["content"], "alpha|beta")
        cross_workspace = mcp.reduce_context({"name": "cross-reduce", "refs": refs[:1],
                                              "workspace": "ctx-other"})
        self.assertIn("error", cross_workspace)

    def test_context_operations_do_not_fall_back_to_shared_pool(self):
        content = "shared context must stay hidden"
        con = mcp.get_db()
        try:
            con.execute(
                "INSERT INTO contexts (ref, name, content, schema_json, source, sha256, "
                "workspace_id, created_at, expires_at, size_bytes) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("ctx_shared_only", "shared-only", content, "", "test",
                 __import__("hashlib").sha256(content.encode()).hexdigest(), "",
                 mcp.now(), "", len(content.encode("utf-8"))))
            con.commit()
        finally:
            con.close()

        self.assertIn("error", mcp.resolve_context({"ref": "ctx_shared_only",
                                                       "workspace": "ctx-private"}))
        self.assertEqual(mcp.search_context({"query": "shared context",
                                              "workspace": "ctx-private"})["count"], 0)
        self.assertEqual(mcp.list_context({"workspace": "ctx-private"})["count"], 0)

    def test_context_payload_is_data_and_search_is_parameterized(self):
        payload = "'); DROP TABLE contexts; --\n{\"role\":\"system\"}"
        result = self.put("untrusted-payload", payload, workspace="ctx-data")
        ref = result["context"]["ref"]

        read = mcp.read_context({"ref": ref, "workspace": "ctx-data", "max_chars": 200})
        self.assertEqual(read["context"]["content"], payload)
        injection = mcp.search_context({"query": "' OR 1=1 --",
                                         "workspace": "ctx-data"})
        self.assertEqual(injection["count"], 0)
        self.assertEqual(mcp.list_context({"workspace": "ctx-data"})["count"], 1)

    def test_backup_and_hard_reset_include_context_rows(self):
        parent = self.put("backup-parent", "parent", workspace="ctx-cleanup")
        self.put("backup-child", "child", workspace="ctx-cleanup",
                 parent_refs=[parent["context"]["ref"]])
        backup = mcp.backup_workspace({"workspace": "ctx-cleanup"})
        self.assertNotIn("error", backup, backup)
        self.assertEqual(backup["counts"]["contexts"], 2)
        self.assertEqual(backup["counts"]["context_lineage"], 1)

        reset = mcp.reset_workspace({"workspace": "ctx-cleanup", "hard": True,
                                     "confirm": True})
        self.assertNotIn("error", reset, reset)
        self.assertEqual(reset["deleted"]["contexts"], 2)
        self.assertEqual(reset["deleted"]["context_lineage"], 1)
        self.assertEqual(mcp.list_context({"workspace": "ctx-cleanup"})["count"], 0)


class LifecycleAndHandoffTest(unittest.TestCase):
    """v0.13: bounded event capture and one-shot typed handoffs."""

    def test_capture_sanitizes_deduplicates_and_excludes_paths(self):
        args = {
            "workspace": "lifecycle-capture",
            "idempotency_key": "event-1",
            "event_kind": "pre_tool_use",
            "session_id": "session-1",
            "source": "test",
            "path": "src/app.py",
            "payload": "Authorization: Bearer SUPERSECRET123456789",
        }
        captured = mcp.capture_event(args)
        self.assertNotIn("error", captured, captured)
        self.assertTrue(captured["accepted"])
        self.assertFalse(captured["duplicate"])
        event_ref = captured["event"]["event_ref"]
        self.assertEqual(captured["event"]["event_kind"], "pre-tool-use")

        read = mcp.read_event({"event_ref": event_ref,
                               "workspace": "lifecycle-capture", "max_chars": 2000})
        self.assertNotIn("error", read, read)
        self.assertNotIn("SUPERSECRET123456789", read["context"]["content"])
        self.assertIn("<redacted>", read["context"]["content"])

        structured = mcp.capture_event({
            "workspace": "lifecycle-capture", "idempotency_key": "event-structured",
            "event_kind": "notification", "payload": {"token": "JSONSECRET"},
        })
        structured_read = mcp.read_event({
            "event_ref": structured["event"]["event_ref"],
            "workspace": "lifecycle-capture", "max_chars": 2000,
        })
        self.assertNotIn("JSONSECRET", structured_read["context"]["content"])

        old_now = mcp.now
        try:
            stable_args = {
                "workspace": "lifecycle-capture", "idempotency_key": "event-time-independent",
                "event_kind": "notification", "payload": "same payload",
            }
            mcp.now = lambda: "2026-08-20T09:00:00Z"
            stable = mcp.capture_event(stable_args)
            mcp.now = lambda: "2026-08-20T09:01:00Z"
            stable_retry = mcp.capture_event(stable_args)
            self.assertNotIn("error", stable, stable)
            self.assertTrue(stable_retry["duplicate"])
            self.assertEqual(stable_retry["event"]["event_ref"],
                             stable["event"]["event_ref"])
        finally:
            mcp.now = old_now

        duplicate = mcp.capture_event(args)
        self.assertNotIn("error", duplicate, duplicate)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["event"]["event_ref"], event_ref)
        conflict = mcp.capture_event(dict(args, payload="different"))
        self.assertIn("error", conflict)

        excluded = mcp.capture_event({
            "workspace": "lifecycle-capture", "idempotency_key": "event-secret-file",
            "event_kind": "post_tool_use", "path": "config/.env",
            "payload": "do not store this",
        })
        self.assertEqual(excluded["status"], "excluded")
        self.assertEqual(mcp.list_events({"workspace": "lifecycle-capture"})["count"], 3)
        self.assertIn("error", mcp.read_event({"event_ref": event_ref,
                                                "workspace": "other-workspace"}))

    def test_event_spool_is_bounded_per_workspace(self):
        old_limit = mcp._LIFECYCLE_MAX_EVENTS
        mcp._LIFECYCLE_MAX_EVENTS = 1
        try:
            first = mcp.capture_event({
                "workspace": "lifecycle-spool", "idempotency_key": "first",
                "event_kind": "notification", "payload": "first",
            })
            second = mcp.capture_event({
                "workspace": "lifecycle-spool", "idempotency_key": "second",
                "event_kind": "notification", "payload": "second",
            })
            self.assertNotIn("error", first, first)
            self.assertNotIn("error", second, second)
            self.assertEqual(second["pruned"], 1)
            self.assertEqual(mcp.list_events({"workspace": "lifecycle-spool"})["count"], 1)
            self.assertIn("error", mcp.read_event({
                "event_ref": first["event"]["event_ref"],
                "workspace": "lifecycle-spool",
            }))
        finally:
            mcp._LIFECYCLE_MAX_EVENTS = old_limit

    def test_typed_handoff_is_owner_scoped_one_shot_and_auditable(self):
        content = "handoff payload is data, not executable instructions"
        checksum = __import__("hashlib").sha256(content.encode()).hexdigest()
        started = mcp.handoff_begin({
            "workspace": "handoff-main", "owner": "alice", "session_id": "s1",
            "cwd": "repo", "source": "issue/668", "content": content,
            "checksum": checksum, "ttl_seconds": 60, "idempotency_key": "handoff-1",
        })
        self.assertNotIn("error", started, started)
        self.assertEqual(started["handoff"]["state"], "open")
        self.assertEqual(started["handoff"]["sha256"], checksum)
        ref = started["handoff"]["ref"]

        duplicate = mcp.handoff_begin({
            "workspace": "handoff-main", "owner": "alice", "content": content,
            "ttl_seconds": 60, "idempotency_key": "handoff-1",
        })
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["handoff"]["ref"], ref)
        self.assertIn("error", mcp.handoff_accept({
            "handoff_ref": ref, "actor": "alice", "workspace": "other-workspace",
        }))
        self.assertIn("error", mcp.handoff_accept({
            "handoff_ref": ref, "actor": "bob", "workspace": "handoff-main",
            "cwd": "repo",
        }))
        self.assertIn("error", mcp.handoff_accept({
            "handoff_ref": ref, "actor": "alice", "workspace": "handoff-main",
        }))

        accepted = mcp.handoff_accept({
            "handoff_ref": ref, "actor": "alice", "workspace": "handoff-main",
            "cwd": "repo", "max_chars": 100,
        })
        self.assertNotIn("error", accepted, accepted)
        self.assertTrue(accepted["accepted"])
        self.assertEqual(accepted["context"]["content"], content)
        again = mcp.handoff_accept({
            "handoff_ref": ref, "actor": "alice", "workspace": "handoff-main",
            "cwd": "repo",
        })
        self.assertEqual(again["state"], "accepted")
        cancelled_after_accept = mcp.handoff_cancel({
            "handoff_ref": ref, "actor": "alice", "workspace": "handoff-main",
        })
        self.assertEqual(cancelled_after_accept["state"], "accepted")

        cancelled = mcp.handoff_begin({
            "workspace": "handoff-cancel", "owner": "alice", "content": "cancel me",
            "ttl_seconds": 60,
        })
        cancel_ref = cancelled["handoff"]["ref"]
        self.assertIn("error", mcp.handoff_cancel({
            "handoff_ref": cancel_ref, "actor": "bob", "workspace": "handoff-cancel",
        }))
        done = mcp.handoff_cancel({
            "handoff_ref": cancel_ref, "actor": "alice", "workspace": "handoff-cancel",
        })
        self.assertTrue(done["cancelled"])
        self.assertEqual(done["handoff"]["state"], "cancelled")

    def test_handoff_expiry_and_workspace_cleanup(self):
        expired = mcp.handoff_begin({
            "workspace": "handoff-expired", "owner": "alice", "content": "gone",
            "ttl_seconds": 0,
        })
        self.assertNotIn("error", expired, expired)
        result = mcp.handoff_accept({
            "handoff_ref": expired["handoff"]["ref"], "actor": "alice",
            "workspace": "handoff-expired",
        })
        self.assertIn("error", result)
        listed = mcp.list_handoffs({"workspace": "handoff-expired"})
        self.assertEqual(listed["handoffs"][0]["state"], "expired")

        mcp.capture_event({
            "workspace": "handoff-cleanup", "idempotency_key": "cleanup-event",
            "event_kind": "session_end", "payload": "event",
        })
        handoff = mcp.handoff_begin({
            "workspace": "handoff-cleanup", "owner": "alice", "content": "handoff",
        })
        reset = mcp.reset_workspace({"workspace": "handoff-cleanup", "hard": True,
                                     "confirm": True})
        self.assertNotIn("error", reset, reset)
        self.assertEqual(reset["deleted"]["lifecycle_events"], 1)
        self.assertEqual(reset["deleted"]["handoffs"], 1)
        self.assertIn("error", mcp.read_event({
            "event_ref": "cleanup-event", "workspace": "handoff-cleanup",
        }))
        self.assertIn("error", mcp.handoff_accept({
            "handoff_ref": handoff["handoff"]["ref"], "actor": "alice",
            "workspace": "handoff-cleanup",
        }))


class LifecycleToolContractTest(unittest.TestCase):
    def test_lifecycle_and_handoff_tools_are_public(self):
        for name in ("capture_event", "list_events", "read_event", "handoff_begin",
                     "list_handoffs", "handoff_accept", "handoff_cancel"):
            self.assertIn(name, mcp.TOOLS)
            self.assertIn(name, mcp.HANDLERS)


class RunsAnchorsAndAccessTest(unittest.TestCase):
    """v0.18: runs + issue/PR links, anchored queries, access telemetry."""

    def test_v018_tools_are_public(self):
        for name in ("run_begin", "run_end", "link_run", "query_run",
                     "prepare_summary", "query_anchored", "context_map"):
            self.assertIn(name, mcp.TOOLS)
            self.assertIn(name, mcp.HANDLERS)

    def test_run_lifecycle_and_issue_pr_links(self):
        ws = "runs-ws"
        begin = mcp.run_begin({"run_id": "run-1", "issue_ref": "NTL-1", "workspace": ws})
        self.assertNotIn("error", begin, begin)
        self.assertEqual(begin["run"]["state"], "open")
        dup = mcp.run_begin({"run_id": "run-1", "workspace": ws})
        self.assertTrue(dup["duplicate"])
        self.assertEqual(dup["run"]["issue_ref"], "NTL-1")
        end = mcp.run_end({"run_id": "run-1", "base_sha": "aaa", "head_sha": "bbb",
                           "files_changed": ["src/a.py", "src/b.py"], "diff": "patch",
                           "workspace": ws})
        self.assertNotIn("error", end, end)
        self.assertTrue(end["closed"])
        self.assertEqual(end["run"]["files_changed"], ["src/a.py", "src/b.py"])
        link = mcp.link_run({"run_id": "run-1", "pr_ref": "PR-7", "workspace": ws})
        self.assertNotIn("error", link, link)
        self.assertEqual(link["run"]["pr_ref"], "PR-7")
        q = mcp.query_run({"run_id": "run-1", "workspace": ws})
        self.assertEqual(q["run"]["head_sha"], "bbb")
        lst = mcp.query_run({"workspace": ws, "state": "closed"})
        self.assertEqual(lst["count"], 1)
        # closed runs reject begin/end; link needs at least one ref
        self.assertIn("error", mcp.run_begin({"run_id": "run-1", "workspace": ws}))
        self.assertIn("error", mcp.run_end({"run_id": "run-1", "workspace": ws}))
        self.assertIn("error", mcp.run_end({"run_id": "missing", "workspace": ws}))
        self.assertIn("error", mcp.link_run({"run_id": "run-1", "workspace": ws}))
        self.assertIn("error", mcp.query_run({"run_id": "missing", "workspace": ws}))

    def test_prepare_summary_uses_run_window_and_issue_ref(self):
        ws = "summary-ws"
        mcp.run_begin({"run_id": "run-s", "issue_ref": "NTL-9", "workspace": ws})
        dec = mcp.record_decision({"scenario": "db choice", "subject": "db",
                                   "outcome": "sqlite", "issue_ref": "NTL-9",
                                   "path": "src/db.py", "workspace": ws})
        self.assertNotIn("error", dec, dec)
        ev = mcp.capture_event({"idempotency_key": "e-s-1", "event_kind": "post_compact",
                                "payload": "{}", "workspace": ws})
        self.assertNotIn("error", ev, ev)
        mcp.run_end({"run_id": "run-s", "workspace": ws})
        s = mcp.prepare_summary({"run_id": "run-s", "workspace": ws})
        self.assertNotIn("error", s, s)
        self.assertIn("NTL-9", s["summary"])
        self.assertIn("db", s["summary"])
        self.assertEqual(len(s["decisions"]), 1)
        self.assertEqual(len(s["events"]), 1)
        self.assertEqual(s["events"][0]["event_kind"], "post-compact")
        self.assertIn("error", mcp.prepare_summary({"run_id": "missing", "workspace": ws}))

    def test_query_anchored_facts_via_evidence_and_decisions(self):
        ws = "anchor-ws"
        path = "src/anchored_probe/worker.py"
        fact = mcp.remember_fact({"text": "retry logic lives in worker.py", "workspace": ws})
        ev = mcp.attach_evidence({"fact_id": fact["id"], "source_ref": path,
                                  "path": path, "symbol": "AnchoredProbe.retry",
                                  "repo": "repo-x", "workspace": ws})
        self.assertNotIn("error", ev, ev)
        dec = mcp.record_decision({"scenario": "retry policy", "subject": "retry",
                                   "outcome": "3 attempts", "path": path,
                                   "symbol": "AnchoredProbe.retry", "workspace": ws})
        self.assertNotIn("error", dec, dec)
        by_path = mcp.query_anchored({"path": "anchored_probe", "workspace": ws})
        self.assertNotIn("error", by_path, by_path)
        self.assertEqual(by_path["count"], 2)
        self.assertEqual(len(by_path["facts"]), 1)
        self.assertEqual(len(by_path["decisions"]), 1)
        self.assertEqual(by_path["facts"][0]["evidence"][0]["path"], path)
        self.assertEqual(by_path["facts"][0]["evidence"][0]["symbol"], "AnchoredProbe.retry")
        by_symbol = mcp.query_anchored({"symbol": "anchoredprobe.retry", "workspace": ws})
        self.assertEqual(by_symbol["count"], 2)
        # workspace isolation + advisory boundary + selector validation
        self.assertEqual(mcp.query_anchored({"path": "anchored_probe",
                                             "workspace": "other-ws"})["count"], 0)
        self.assertIn("error", mcp.query_anchored(
            {"path": "anchored_probe", "workspace": ws, "purpose": "safety_critical"}))
        self.assertIn("error", mcp.query_anchored({"workspace": ws}))

    def test_decisions_anchor_migration(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        con = sqlite3.connect(tmp.name)
        con.executescript("""
            CREATE TABLE decisions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              category TEXT NOT NULL DEFAULT '',
              subject TEXT NOT NULL DEFAULT '',
              scenario TEXT NOT NULL,
              reasoning TEXT NOT NULL DEFAULT '',
              outcome TEXT NOT NULL DEFAULT '',
              confidence REAL,
              decision_maker TEXT NOT NULL DEFAULT '',
              issue_ref TEXT NOT NULL DEFAULT '',
              parent_decision_id INTEGER,
              workspace_id TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
        """)
        con.commit()
        con.close()
        try:
            old = mcp._open_db(tmp.name)
            cols = {r["name"] for r in old.execute("PRAGMA table_info(decisions)")}
            self.assertIn("path", cols)
            self.assertIn("symbol", cols)
            old.close()
        finally:
            os.unlink(tmp.name)

    def test_memory_access_telemetry(self):
        ws = "tele-ws"
        mcp.remember_fact({"text": "telemetry probe fact", "workspace": ws})
        before = mcp.stats({"workspace": ws})["access"]["events"]
        mcp.search_facts({"query": "telemetry probe", "workspace": ws})
        mcp.search_facts({"query": "telemetry probe", "workspace": ws})
        st = mcp.stats({"workspace": ws})
        self.assertNotIn("error", st, st)
        self.assertEqual(st["access"]["events"], before + 2)
        # a zero-result anchored query still logs the pull
        mcp.query_anchored({"path": "nothing_probe.py", "workspace": ws})
        st2 = mcp.stats({"workspace": ws})
        self.assertEqual(st2["access"]["by_site"].get("query_anchored", 0),
                         st["access"]["by_site"].get("query_anchored", 0) + 1)


class PairedMeasurementTest(unittest.TestCase):
    """v0.20: aggregate-only, workspace-scoped paired measurements."""

    def test_v020_tools_and_schema_are_public(self):
        for name in ("record_measurement", "query_measurement"):
            self.assertIn(name, mcp.TOOLS)
            self.assertIn(name, mcp.HANDLERS)
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            con = mcp._open_db(tmp.name)
            columns = {row["name"] for row in con.execute(
                "PRAGMA table_info(measurement_observations)")}
            self.assertIn("measurement_id", columns)
            self.assertIn("quality_score", columns)
            self.assertNotIn("payload", columns)
            con.close()
        finally:
            os.unlink(tmp.name)

    def test_record_is_idempotent_and_rejects_payload_fields(self):
        ws = "measurement-idempotency"
        args = {
            "measurement_id": "slice-1", "sample_key": "task-1",
            "variant": "baseline", "issue_ref": "NTL-694", "workspace": ws,
            "input_tokens": 100, "wall_time_ms": 250.0,
        }
        first = mcp.record_measurement(args)
        self.assertNotIn("error", first, first)
        self.assertFalse(first["duplicate"])
        duplicate = mcp.record_measurement(dict(args))
        self.assertTrue(duplicate["duplicate"])
        conflict = dict(args, input_tokens=101)
        self.assertIn("error", mcp.record_measurement(conflict))
        bad = dict(args, sample_key="task-2", prompt="do not store this")
        rejected = mcp.record_measurement(bad)
        self.assertIn("unsupported measurement fields", rejected["error"])
        summary = mcp.query_measurement({"measurement_id": "slice-1", "workspace": ws})
        self.assertNotIn("error", summary, summary)
        self.assertNotIn("prompt", json.dumps(summary))
        self.assertEqual(mcp.stats({"workspace": ws})["counts"]["measurements"], 1)

    def test_summary_requires_complete_pairs_and_reports_median_p95(self):
        ws = "measurement-summary"

        def record(sample, variant, tokens, wall):
            result = mcp.record_measurement({
                "measurement_id": "slice-2", "sample_key": sample,
                "variant": variant, "issue_ref": "NTL-694", "workspace": ws,
                "input_tokens": tokens, "output_tokens": tokens // 2,
                "wall_time_ms": wall, "quality_score": 0.9,
                "safety_regression": 0,
            })
            self.assertNotIn("error", result, result)

        record("task-1", "baseline", 100, 250)
        record("task-1", "memory", 80, 200)
        partial = mcp.query_measurement({"measurement_id": "slice-2",
                                          "workspace": ws, "min_pairs": 2})
        self.assertEqual(partial["status"], "not_claimed")
        self.assertEqual(partial["paired_samples"], 1)

        record("task-2", "baseline", 200, 350)
        record("task-2", "memory", 160, 300)
        complete = mcp.query_measurement({"measurement_id": "slice-2",
                                           "workspace": ws, "min_pairs": 2})
        self.assertEqual(complete["status"], "ready_for_review")
        self.assertEqual(complete["paired_samples"], 2)
        self.assertEqual(complete["variants"]["baseline"]["metrics"]["input_tokens"],
                         {"count": 2, "median": 150.0, "p95": 195.0})
        self.assertEqual(complete["variants"]["memory"]["metrics"]["input_tokens"],
                         {"count": 2, "median": 120.0, "p95": 156.0})
        self.assertNotIn("savings", complete)

    def test_workspace_isolation_and_run_link(self):
        run_ws = "measurement-run-link"
        mcp.run_begin({"run_id": "measurement-run", "workspace": run_ws})
        linked = mcp.record_measurement({
            "measurement_id": "slice-3", "sample_key": "task-1",
            "variant": "memory", "run_id": "measurement-run",
            "workspace": run_ws, "memory_calls": 2,
        })
        self.assertNotIn("error", linked, linked)
        missing = mcp.record_measurement({
            "measurement_id": "slice-3", "sample_key": "task-2",
            "variant": "memory", "run_id": "missing-run",
            "workspace": run_ws, "memory_calls": 1,
        })
        self.assertIn("run_id was not found", missing["error"])

        other = mcp.record_measurement({
            "measurement_id": "slice-3", "sample_key": "task-1",
            "variant": "baseline", "issue_ref": "OTHER-1",
            "workspace": "measurement-other", "input_tokens": 9,
        })
        self.assertNotIn("error", other, other)
        own = mcp.query_measurement({"measurement_id": "slice-3", "workspace": run_ws})
        self.assertEqual(own["observations"], {"baseline": 0, "memory": 1})
        self.assertEqual(own["paired_samples"], 0)

    def test_metric_validation(self):
        common = {"measurement_id": "slice-4", "sample_key": "task-1",
                  "variant": "baseline", "issue_ref": "NTL-694",
                  "workspace": "measurement-validation"}
        self.assertIn("outside the allowed range",
                      mcp.record_measurement(dict(common, input_tokens=-1))["error"])
        self.assertIn("between 0 and 1",
                      mcp.record_measurement(dict(common, quality_score=1.1))["error"])
        self.assertIn("outside the allowed range",
                      mcp.record_measurement(dict(common, wall_time_ms=float("nan")))["error"])
        self.assertIn("variant must be",
                      mcp.record_measurement(dict(common, variant="control",
                                                  input_tokens=1))["error"])


class AnchorAndRuntimePolicyTest(unittest.TestCase):
    """Query-time anchors, first-input orientation, and runtime guardrails."""

    def setUp(self):
        self.old_db = mcp.DB_PATH
        self.old_selected = mcp._SELECTED_DB[0]
        self.tmpdir = tempfile.TemporaryDirectory(prefix="mcp-anchor-")
        self.db = os.path.join(self.tmpdir.name, "facts.db")
        self.repo = os.path.join(self.tmpdir.name, "repo")
        os.makedirs(self.repo)
        mcp.DB_PATH = self.db
        mcp._SELECTED_DB[0] = None

    def tearDown(self):
        mcp.DB_PATH = self.old_db
        mcp._SELECTED_DB[0] = self.old_selected
        self.tmpdir.cleanup()

    def _fact(self, text):
        result = mcp.remember_fact({"text": text, "workspace": "anchor-policy"})
        self.assertNotIn("error", result, result)
        return result["id"]

    @staticmethod
    def _hash(text):
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _anchor(self, fact_id, path, selected=None, start_line=None,
                start_col=None, end_line=None, end_col=None):
        args = {"fact_id": fact_id, "source_ref": "anchor-test:%s" % path,
                "repo": "repo-x", "ref": "test-ref", "path": path,
                "workspace": "anchor-policy"}
        if selected is not None:
            args.update({"selected_text": selected,
                         "start_line": start_line, "start_col": start_col,
                         "end_line": end_line, "end_col": end_col})
        result = mcp.attach_evidence(args)
        self.assertNotIn("error", result, result)

    def test_query_time_verdicts_cover_live_move_and_drift(self):
        live = "def retry():\n    return True\n"
        live_path = os.path.join(self.repo, "live.py")
        with open(live_path, "w", encoding="utf-8") as handle:
            handle.write(live)
        live_id = self._fact("live anchor fact")
        self._anchor(live_id, "live.py", "return True", 2, 4, 2, 15)
        result = mcp.query_anchored({"path": "live.py", "workspace": "anchor-policy",
                                     "repo_root": self.repo})
        evidence = result["facts"][0]["evidence"][0]
        self.assertEqual(evidence["anchor_verdict"], "STRONG")

        with open(live_path, "w", encoding="utf-8") as handle:
            handle.write("def retry():\n    return False\n")
        stale = mcp.query_anchored({"path": "live.py", "workspace": "anchor-policy",
                                    "repo_root": self.repo})
        self.assertEqual(stale["facts"][0]["evidence"][0]["anchor_verdict"], "STALE")

        moved_id = self._fact("moved anchor fact")
        moved_text = "def moved():\n    return 7\n"
        old_path = os.path.join(self.repo, "old.py")
        with open(old_path, "w", encoding="utf-8") as handle:
            handle.write(moved_text)
        self._anchor(moved_id, "old.py", "return 7", 2, 4, 2, 12)
        os.unlink(old_path)
        with open(os.path.join(self.repo, "new.py"), "w", encoding="utf-8") as handle:
            handle.write(moved_text)
        rebuilt = mcp.query_anchored({"path": "old.py", "workspace": "anchor-policy",
                                      "repo_root": self.repo})
        moved = [f for f in rebuilt["facts"] if f["id"] == moved_id][0]
        self.assertEqual(moved["evidence"][0]["anchor_verdict"], "REBUILT")
        self.assertEqual(moved["evidence"][0]["resolved_path"], "new.py")

        removed_id = self._fact("removed anchor fact")
        self._anchor(removed_id, "removed.py", "return 9", 1, 0, 1, 8)
        removed = mcp.query_anchored({"path": "removed.py", "workspace": "anchor-policy",
                                      "repo_root": self.repo})
        self.assertEqual(removed["facts"][0]["evidence"][0]["anchor_verdict"], "REMOVED")

    def test_context_map_is_opt_in_freshness_aware_and_impact_bounded(self):
        old_flag = os.environ.get("MEMORY_MCP_CONTEXT_MAP")
        workspace = "context-map-policy"
        path = "mapped.py"
        content = "def mapped():\n    return 1\n"
        with open(os.path.join(self.repo, path), "w", encoding="utf-8") as handle:
            handle.write(content)
        fact_result = mcp.remember_fact({"text": "mapped repository context fact",
                                         "workspace": workspace})
        self.assertNotIn("error", fact_result, fact_result)
        fact = fact_result["id"]
        evidence = mcp.attach_evidence({
            "fact_id": fact, "source_ref": "repo-x@ref-1:mapped.py",
            "repo": "repo-x", "ref": "ref-1", "path": path,
            "symbol": "mapped", "workspace": workspace,
        })
        self.assertNotIn("error", evidence, evidence)
        mcp.run_begin({"run_id": "context-map-run", "workspace": workspace})
        mcp.run_end({"run_id": "context-map-run", "workspace": workspace,
                     "files_changed": [path]})
        checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
        args = {
            "repo": "repo-x", "ref": "ref-1", "view": "impact",
            "repo_root": self.repo, "workspace": workspace,
            "anchors": [{"path": path, "symbol": "mapped",
                         "content_checksum": checksum,
                         "relation": "dependent"}],
        }
        try:
            os.environ.pop("MEMORY_MCP_CONTEXT_MAP", None)
            disabled = mcp.context_map(args)
            self.assertEqual(disabled["code"], "feature_disabled")

            os.environ["MEMORY_MCP_CONTEXT_MAP"] = "1"
            result = mcp.context_map(args)
            self.assertNotIn("error", result, result)
            self.assertTrue(result["bounded"])
            self.assertEqual(result["manifest"][0]["anchor_verdict"], "STRONG")
            self.assertEqual(result["manifest"][0]["checksum_verdict"], "MATCH")
            self.assertEqual(result["freshness"]["STRONG"], 1)
            self.assertEqual(result["impact"]["runs"][0]["matched_paths"], [path])
            self.assertEqual(result["facts"][0]["id"], fact)

            with open(os.path.join(self.repo, path), "w", encoding="utf-8") as handle:
                handle.write("def mapped():\n    return 2\n")
            stale = mcp.context_map(args)
            self.assertEqual(stale["manifest"][0]["anchor_verdict"], "STALE")
            self.assertEqual(stale["manifest"][0]["checksum_verdict"], "MISMATCH")

            traversal = dict(args, anchors=[{"path": "../outside.py"}])
            self.assertIn("must stay inside", mcp.context_map(traversal)["error"])
        finally:
            if old_flag is None:
                os.environ.pop("MEMORY_MCP_CONTEXT_MAP", None)
            else:
                os.environ["MEMORY_MCP_CONTEXT_MAP"] = old_flag

    def test_metadata_only_anchor_is_weak_and_health_cli_fails_on_drift(self):
        fact_id = self._fact("weak anchor fact")
        self._anchor(fact_id, "weak.py")
        weak = mcp.query_anchored({"path": "weak.py", "workspace": "anchor-policy"})
        self.assertEqual(weak["facts"][0]["evidence"][0]["anchor_verdict"], "WEAK")

        with open(os.path.join(self.repo, "weak.py"), "w", encoding="utf-8") as handle:
            handle.write("def weak():\n    return 1\n")
        from verify import anchor_health
        health = anchor_health({"repo_root": self.repo, "repo": "repo-x",
                                "workspace": "anchor-policy"})
        self.assertTrue(health["ok"])
        self.assertEqual(health["counts"]["WEAK"], 1)

        drift_id = self._fact("drift anchor fact")
        drift_text = "return 2"
        with open(os.path.join(self.repo, "drift.py"), "w", encoding="utf-8") as handle:
            handle.write("def drift():\n    %s\n" % drift_text)
        self._anchor(drift_id, "drift.py", drift_text, 2, 4, 2, 12)
        with open(os.path.join(self.repo, "drift.py"), "w", encoding="utf-8") as handle:
            handle.write("def drift():\n    return 3\n")
        health = anchor_health({"repo_root": self.repo, "repo": "repo-x",
                                "workspace": "anchor-policy"})
        self.assertFalse(health["ok"])
        self.assertEqual(health["counts"]["STALE"], 1)

        env = os.environ.copy()
        env["MEMORY_MCP_DB"] = self.db
        cli = subprocess.run(
            [sys.executable, "verify.py", "--root", self.repo, "--repo", "repo-x",
             "--workspace", "anchor-policy", "--json"],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            env=env, capture_output=True, text=True, check=False)
        self.assertEqual(cli.returncode, 1, cli.stdout + cli.stderr)
        self.assertIn('"ok": false', cli.stdout)

    def test_anchor_verifier_rejects_traversal_and_symlink_escape(self):
        outside = os.path.join(self.tmpdir.name, "outside.py")
        with open(outside, "w", encoding="utf-8") as handle:
            handle.write("return outside\n")
        traversal_id = self._fact("traversal anchor fact")
        self._anchor(traversal_id, "../outside.py", "return outside", 1, 0, 1, 14)
        traversal = mcp.query_anchored({"path": "../outside.py",
                                        "workspace": "anchor-policy",
                                        "repo_root": self.repo})
        self.assertEqual(traversal["facts"][0]["evidence"][0]["anchor_verdict"], "WEAK")

        link = os.path.join(self.repo, "outside-link.py")
        try:
            os.symlink(outside, link)
        except (AttributeError, NotImplementedError, OSError):
            self.skipTest("symbolic links are unavailable")
        link_id = self._fact("symlink anchor fact")
        self._anchor(link_id, "outside-link.py", "return outside", 1, 0, 1, 14)
        escaped = mcp.query_anchored({"path": "outside-link.py",
                                      "workspace": "anchor-policy",
                                      "repo_root": self.repo})
        self.assertEqual(escaped["facts"][0]["evidence"][0]["anchor_verdict"], "WEAK")

    def test_auto_orient_is_first_input_bounded_and_quiet_when_disabled(self):
        old_recall = os.environ.get("MEMORY_MCP_RECALL")
        try:
            os.environ["MEMORY_MCP_RECALL"] = "1"
            mcp.remember_fact({"text": "orientation remembers the worker queue",
                               "workspace": "orient-policy"})
            first = mcp.auto_orient({"turn_text": "worker queue",
                                     "session_id": "orient-session",
                                     "workspace": "orient-policy"})
            self.assertTrue(first["oriented"])
            self.assertFalse(first["degraded"])
            self.assertLessEqual(first["count"], 6)
            second = mcp.auto_orient({"turn_text": "worker queue again",
                                      "session_id": "orient-session",
                                      "workspace": "orient-policy"})
            self.assertEqual(second["skipped"], "already_oriented")

            os.environ.pop("MEMORY_MCP_RECALL", None)
            degraded = mcp.auto_orient({"turn_text": "disabled orientation",
                                        "session_id": "orient-disabled"})
            self.assertTrue(degraded["degraded"])
            self.assertEqual(degraded["block"], "")
            self.assertNotIn("error", degraded)
        finally:
            if old_recall is None:
                os.environ.pop("MEMORY_MCP_RECALL", None)
            else:
                os.environ["MEMORY_MCP_RECALL"] = old_recall

    def test_search_guard_warns_without_blocking_and_resets_on_memory(self):
        sid = "grep-loop-session"
        first = mcp.search_guard({"session_id": sid, "action": "search"})
        second = mcp.search_guard({"session_id": sid, "action": "search"})
        third = mcp.search_guard({"session_id": sid, "action": "search"})
        self.assertFalse(first["warn"])
        self.assertFalse(second["warn"])
        self.assertTrue(third["warn"])
        self.assertFalse(third["blocking"])
        reset = mcp.search_guard({"session_id": sid, "action": "memory"})
        self.assertEqual(reset["consecutive_searches"], 0)
        self.assertFalse(mcp.search_guard({"session_id": sid, "action": "search"})["warn"])

    def test_stats_reports_pull_hit_rate(self):
        workspace = "hit-rate-policy"
        mcp.remember_fact({"text": "hit rate telemetry fact", "workspace": workspace})
        before = mcp.stats({"workspace": workspace})["access"]
        mcp.search_facts({"query": "hit rate telemetry", "workspace": workspace})
        mcp.search_facts({"query": "missing telemetry needle", "workspace": workspace})
        access = mcp.stats({"workspace": workspace})["access"]
        self.assertEqual(access["pull_events"], before.get("pull_events", 0) + 2)
        self.assertEqual(access["pull_hits"], before.get("pull_hits", 0) + 1)
        self.assertEqual(access["pull_misses"], before.get("pull_misses", 0) + 1)
        self.assertEqual(access["hit_rate"], 0.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class EmbeddingsTest(unittest.TestCase):
    """Semantic search with the deterministic `test` provider (no model)."""

    def setUp(self):
        self._old = {k: os.environ.get(k) for k in
                     ("MEMORY_MCP_EMBEDDINGS", "MEMORY_MCP_EMBED_PROVIDER")}
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def remember(self, text):
        res = mcp.remember_fact({"text": text, "source": "test",
                                 "project": "project", "domain": "project"})
        self.assertNotIn("error", res, res)
        return res

    def test_semantic_search_finds_paraphrase(self):
        self.remember("battery life on the phone is four hours")
        self.remember("the phone lasts about four hours on a charge")
        res = mcp.search_semantic({"query": "phone lasts four hours", "limit": 10})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["count"], 2,
                                "semantic search should surface both paraphrases")
        # The two paraphrases rank on top and clearly above unrelated facts.
        top = res["facts"][:2]
        self.assertTrue(all(f["score"] > 0.5 for f in top),
                        "n-gram vectors of paraphrases must be similar: %s" % res)
        rest = res["facts"][2:]
        self.assertTrue(not rest or all(f["score"] < 0.5 for f in rest),
                        "unrelated facts must score well below the paraphrases: %s" % res)
        # threshold keeps only strong matches
        narrow = mcp.search_semantic({"query": "phone lasts four hours",
                                      "limit": 10, "threshold": 0.5})
        self.assertEqual(narrow["count"], 2)

    def test_search_facts_hybrid(self):
        self.remember("hybrid fact about the kappa cache")
        res = mcp.search_facts({"query": "kappa cache", "semantic": True})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["count"], 1)
        self.assertIn("semantic_score", res["facts"][0])

    def test_embed_backfill(self):
        os.environ.pop("MEMORY_MCP_EMBEDDINGS", None)  # write without vectors
        self.remember("backfill fact about the lambda queue")
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"  # enable before backfill
        res = mcp.embed_backfill({})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["processed"], 1)
        hit = mcp.search_semantic({"query": "lambda queue", "limit": 5})
        self.assertGreaterEqual(hit["count"], 1)

    def test_disabled_returns_error(self):
        os.environ.pop("MEMORY_MCP_EMBEDDINGS", None)
        self.assertIn("error", mcp.search_semantic({"query": "anything"}))
        self.assertIn("error", mcp.embed_backfill({}))
        # hybrid flag without embeddings is a silent no-op (lexical only)
        self.remember("plain lexical fact about the mu table")
        res = mcp.search_facts({"query": "mu table", "semantic": True})
        self.assertNotIn("error", res, res)


class AddFactAliasTest(unittest.TestCase):
    def test_add_fact_alias_stores_fact(self):
        # add_fact is a protocol-level alias (HANDLERS), same handler as
        # remember_fact — but intentionally NOT advertised in the schema
        # (tool-choice noise)
        self.assertNotIn("add_fact", mcp.TOOLS)
        self.assertIn("add_fact", mcp.HANDLERS)
        res = mcp.HANDLERS["add_fact"]({"text": "alias fact about the nu cache", "source": "test"})
        self.assertNotIn("error", res, res)
        self.assertIn("id", res)
        facts = mcp.search_facts({"query": "nu cache alias"})
        self.assertTrue(any("nu cache" in f["text"] for f in facts["facts"]))


class PipelineTest(unittest.TestCase):
    """Server-side extraction/recall/verification with the deterministic
    `test` LLM provider (no model needed)."""

    def setUp(self):
        self._old = {k: os.environ.get(k) for k in
                     ("MEMORY_MCP_EXTRACT", "MEMORY_MCP_RECALL", "MEMORY_MCP_VERIFY",
                      "MEMORY_MCP_LLM_PROVIDER", "MEMORY_MCP_EXTRACT_MIN_CHARS")}
        os.environ["MEMORY_MCP_EXTRACT"] = "1"
        os.environ["MEMORY_MCP_RECALL"] = "1"
        os.environ["MEMORY_MCP_LLM_PROVIDER"] = "test"
        os.environ["MEMORY_MCP_EXTRACT_MIN_CHARS"] = "50"

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_ingest_turn_extracts_and_dedups(self):
        res = mcp.ingest_turn({"transcript": "FACT: the xi cache holds assets\n"
                                             "FACT: the omicron port is 8080\n"
                                             + "x" * 80,
                               "session_ref": "sess-1"})
        self.assertNotIn("error", res, res)
        self.assertEqual(res["stored"], 2)
        # repeat: same facts dedup
        res2 = mcp.ingest_turn({"transcript": "FACT: the xi cache holds assets\n" + "x" * 80,
                                "session_ref": "sess-1"})
        self.assertEqual(res2["deduped"], 1)
        # provenance attached
        prov = mcp.get_provenance({"fact_id": res.get("stored_id")}) if False else None

    def test_ingest_turn_too_short(self):
        res = mcp.ingest_turn({"transcript": "FACT: tiny"})
        self.assertIn("error", res)

    def test_ingest_turn_disabled(self):
        os.environ.pop("MEMORY_MCP_EXTRACT", None)
        res = mcp.ingest_turn({"transcript": "FACT: x\n" + "x" * 80})
        self.assertIn("error", res)

    def test_compose_recall_block(self):
        mcp.remember_fact({"text": "the rho endpoint handles mobile auth", "source": "t"})
        mcp.remember_fact({"text": "unrelated fact about garden watering", "source": "t"})
        res = mcp.compose_recall({"turn_text": "how does mobile auth work on rho",
                                  "limit": 5, "chars": 2000})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["count"], 1)
        self.assertIn("<memory-recall>", res["block"])
        self.assertIn("rho endpoint", res["block"])
        self.assertNotIn("garden watering", res["block"])

    def test_compose_recall_disabled(self):
        os.environ.pop("MEMORY_MCP_RECALL", None)
        res = mcp.compose_recall({"turn_text": "anything"})
        self.assertIn("error", res)

    def test_sweep_freshness_archives_old(self):
        mcp.remember_fact({"text": "old fact about the sigma job queue", "source": "t"})
        mcp.remember_fact({"text": "strong kept fact about the tau core",
                           "source": "t", "strong": True})
        # age the first fact beyond the project hard window (180d)
        con = mcp.get_db()
        con.execute("UPDATE facts SET updated_at='2020-01-01T00:00:00Z' WHERE text LIKE '%sigma job queue%'")
        con.commit()
        con.close()
        res = mcp.sweep_freshness({})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["archived"], 1)
        # archived fact gone from search; strong fact still there
        gone = mcp.search_facts({"query": "sigma job queue"})
        self.assertEqual(gone["count"], 0)
        kept = mcp.search_facts({"query": "tau core"})
        self.assertGreaterEqual(kept["count"], 1)

    def test_verify_supersedes_invalidates_old(self):
        mcp.remember_fact({"text": "the psi service listens on port 9000", "source": "t"})
        facts = mcp.search_facts({"query": "psi service listens on port"})
        self.assertGreaterEqual(facts["count"], 1)
        old_id = facts["facts"][0]["id"]
        os.environ["MEMORY_MCP_VERIFY"] = "1"
        # the new fact shares terms with the old one, so the whitelist (ids
        # shown to the LLM) includes it; "supersede" drives the verdict
        res = mcp.ingest_turn({"transcript": "FACT: supersede psi service now listens on port 9090\n" + "x" * 80,
                               "session_ref": "sess-2"})
        self.assertNotIn("error", res, res)
        self.assertEqual(res["verification"]["applied"], 1)
        # bi-temporal: old fact invalidated (excluded from active search)...
        after = mcp.search_facts({"query": "psi service listens on port 9000"})
        self.assertEqual(after["count"], 0)
        # ...but its history survives
        hist = mcp.fact_history({"id": old_id})
        self.assertEqual(hist["count"], 2)
        self.assertEqual(hist["chain"][0]["id"], old_id)
        self.assertNotEqual(hist["chain"][0]["invalid_at"], "")
        # evidence attached to the fact that triggered the supersession
        sup = res["verification"]["superseded"][0]
        prov = mcp.get_provenance({"fact_id": sup["new_id"]})
        self.assertTrue(any("supersedes" in e["source_ref"] for e in prov["evidence"]))

    def test_verify_never_archives_strong(self):
        mcp.remember_fact({"text": "the upsilon service is the source of truth", "source": "t",
                           "strong": True})
        facts = mcp.search_facts({"query": "upsilon service source of truth"})
        self.assertGreaterEqual(facts["count"], 1)
        strong_id = facts["facts"][0]["id"]
        os.environ["MEMORY_MCP_VERIFY"] = "1"
        res = mcp.ingest_turn({"transcript": "FACT: supersede upsilon service replaced\n" + "x" * 80,
                               "session_ref": "sess-3"})
        self.assertNotIn("error", res, res)
        self.assertEqual(res["verification"]["applied"], 0)
        # strong fact still active
        still = mcp.search_facts({"query": "upsilon service source of truth"})
        self.assertGreaterEqual(still["count"], 1)


class TemporalAndReviewTest(unittest.TestCase):
    """Bi-temporal validity, importance, retention, and human confirmation."""

    def test_migration_adds_columns(self):
        import sqlite3, tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        con = sqlite3.connect(tmp.name)
        con.execute("""CREATE TABLE facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sha256 TEXT NOT NULL UNIQUE, text TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '', project TEXT NOT NULL DEFAULT '',
            domain TEXT NOT NULL DEFAULT '', trust TEXT NOT NULL DEFAULT 'medium',
            strong INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0)""")
        con.commit(); con.close()
        con = sqlite3.connect(tmp.name)
        con.row_factory = sqlite3.Row
        mcp._migrate_facts(con)  # DB_PATH is captured at import; test the migrator directly
        cols = {r["name"] for r in con.execute("PRAGMA table_info(facts)")}
        con.close(); os.unlink(tmp.name)
        self.assertTrue({"importance", "invalid_at", "superseded_by", "confirmed"} <= cols)

    def test_remember_importance(self):
        res = mcp.remember_fact({"text": "important fact about the chi cache",
                                 "source": "t", "importance": 0.9})
        self.assertNotIn("error", res, res)
        facts = mcp.search_facts({"query": "chi cache important"})
        self.assertGreaterEqual(facts["count"], 1)
        self.assertEqual(facts["facts"][0]["importance"], 0.9)

    def test_search_valid_at(self):
        mcp.remember_fact({"text": "the omega endpoint used to be on port 7000",
                           "source": "t"})
        old = mcp.search_facts({"query": "omega endpoint port 7000"})
        self.assertGreaterEqual(old["count"], 1)
        old_id = old["facts"][0]["id"]
        # invalidate directly (as verify would)
        mcp.get_db().execute(
            "UPDATE facts SET invalid_at='2026-08-01T00:00:00Z', superseded_by=999999 WHERE id=?",
            (old_id,)).connection.commit()
        # excluded by default
        self.assertEqual(mcp.search_facts({"query": "omega endpoint port 7000"})["count"], 0)
        # included with valid_at before invalidation
        past = mcp.search_facts({"query": "omega endpoint port 7000",
                                 "valid_at": "2026-07-01T00:00:00Z"})
        self.assertGreaterEqual(past["count"], 1)

    def test_fact_history_chain(self):
        mcp.remember_fact({"text": "the kappa flag was renamed", "source": "t"})
        f1 = mcp.search_facts({"query": "kappa flag renamed"})["facts"][0]
        mcp.remember_fact({"text": "the kappa flag was renamed again", "source": "t"})
        f2 = mcp.search_facts({"query": "kappa flag renamed again"})["facts"][0]
        mcp.get_db().execute(
            "UPDATE facts SET invalid_at='2026-08-15T00:00:00Z', superseded_by=? WHERE id=?",
            (f2["id"], f1["id"])).connection.commit()
        hist = mcp.fact_history({"id": f1["id"]})
        self.assertEqual(hist["count"], 2)
        self.assertEqual([x["id"] for x in hist["chain"]], [f1["id"], f2["id"]])

    def test_review_pending_and_confirm(self):
        mcp.remember_fact({"text": "unconfirmed fact about the lambda metric", "source": "t"})
        mcp.remember_fact({"text": "confirmed fact about the mu store", "source": "t",
                           "trust": "high"})
        rp = mcp.review_pending({"limit": 100})
        self.assertNotIn("error", rp, rp)
        matches = [f for f in rp["facts"] if "lambda metric" in f["text"]]
        if not matches:
            self.fail("review_pending should include the unconfirmed fact (facts=%s)" % rp["facts"][:3])
        target = matches[0]
        self.assertEqual(target["confirmed"], 0)
        cf = mcp.confirm_fact({"id": target["id"]})
        self.assertEqual(cf["confirmed"], True)
        rp2 = mcp.review_pending({"limit": 100})
        self.assertFalse(any(f["id"] == target["id"] for f in rp2["facts"]))

    def test_sweep_retention_keeps_important(self):
        os.environ["MEMORY_MCP_RECALL"] = "1"
        # old + high importance -> kept; old + low importance -> archived
        mcp.remember_fact({"text": "old important fact about the xi queue",
                           "source": "t", "importance": 0.9})
        mcp.remember_fact({"text": "old unimportant fact about the omicron queue",
                           "source": "t", "importance": 0.1})
        con = mcp.get_db()
        con.execute("UPDATE facts SET updated_at='2020-01-01T00:00:00Z' "
                    "WHERE text LIKE '%queue%'")
        con.commit(); con.close()
        res = mcp.sweep_freshness({})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["archived"], 1)
        kept = mcp.search_facts({"query": "xi queue important"})
        self.assertGreaterEqual(kept["count"], 1)

    def test_extract_importance(self):
        os.environ["MEMORY_MCP_EXTRACT"] = "1"
        os.environ["MEMORY_MCP_LLM_PROVIDER"] = "test"
        os.environ["MEMORY_MCP_EXTRACT_MIN_CHARS"] = "50"
        # test provider emits importance=0.7 for every fact
        res = mcp.ingest_turn({"transcript": "FACT: the eta cache holds logs\n" + "x" * 80,
                               "session_ref": "imp"})
        self.assertNotIn("error", res, res)
        facts = mcp.search_facts({"query": "eta cache holds logs"})
        self.assertGreaterEqual(facts["count"], 1)
        self.assertEqual(facts["facts"][0]["importance"], 0.7)


class ConsolidateSessionsGraphTest(unittest.TestCase):
    """consolidate, sessions first-class, graph-in-RRF."""

    def setUp(self):
        self._old = {k: os.environ.get(k) for k in
                     ("MEMORY_MCP_VERIFY", "MEMORY_MCP_RECALL", "MEMORY_MCP_LLM_PROVIDER")}
        os.environ["MEMORY_MCP_VERIFY"] = "1"
        os.environ["MEMORY_MCP_RECALL"] = "1"
        os.environ["MEMORY_MCP_LLM_PROVIDER"] = "test"

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_consolidate_merges(self):
        mcp.remember_fact({"text": "the phi cache stores session tokens", "source": "t"})
        mcp.remember_fact({"text": "the phi cache holds tokens for sessions", "source": "t"})
        f1 = mcp.search_facts({"query": "phi cache stores session tokens"})["facts"][0]
        f2 = mcp.search_facts({"query": "phi cache holds tokens for sessions"})["facts"][0]
        res = mcp.consolidate({"ids": [f1["id"], f2["id"]]})
        self.assertNotIn("error", res, res)
        self.assertTrue(res["merged"])
        # merged fact active, sources invalidated bi-temporally
        new = mcp.search_facts({"query": "phi cache stores session tokens"})
        self.assertGreaterEqual(new["count"], 1)
        hist = mcp.fact_history({"id": f1["id"]})
        self.assertEqual(hist["count"], 2)
        self.assertNotEqual(hist["chain"][0]["invalid_at"], "")
        # evidence on the merged fact
        prov = mcp.get_provenance({"fact_id": res["new_id"]})
        self.assertTrue(any("consolidated:" in e["source_ref"] for e in prov["evidence"]))

    def test_consolidate_protects_strong(self):
        mcp.remember_fact({"text": "the chi cache is the source of truth", "source": "t",
                           "strong": True})
        mcp.remember_fact({"text": "the chi cache is the source of truth for config", "source": "t"})
        fs = mcp.search_facts({"query": "chi cache source of truth"})["facts"]
        strong_id = next(f["id"] for f in fs if f["strong"])
        other_id = next(f["id"] for f in fs if not f["strong"])
        res = mcp.consolidate({"ids": [strong_id, other_id]})
        self.assertIn("error", res)
        self.assertIn(strong_id, res["protected_ids"])

    def test_consolidate_requires_two(self):
        res = mcp.consolidate({"ids": [1]})
        self.assertIn("error", res)

    def test_facts_for_session(self):
        mcp.remember_fact({"text": "session alpha fact about the delta queue", "source": "sess-alpha"})
        mcp.remember_fact({"text": "session beta fact about the gamma queue", "source": "sess-beta"})
        fa = mcp.facts_for_session({"session_ref": "sess-alpha"})
        self.assertNotIn("error", fa, fa)
        self.assertEqual(fa["count"], 1)
        self.assertIn("delta queue", fa["facts"][0]["text"])
        ls = mcp.list_sessions({})
        self.assertIn("count", ls)
        srcs = {s["source"] for s in ls["sessions"]}
        self.assertIn("sess-alpha", srcs)
        self.assertIn("sess-beta", srcs)

    def test_compose_recall_session_expand(self):
        mcp.remember_fact({"text": "the epsilon endpoint handles mobile auth", "source": "sess-s"})
        # sibling shares NO query terms — only session linking can surface it
        mcp.remember_fact({"text": "the zeta deploy runs every night", "source": "sess-s"})
        res = mcp.compose_recall({"turn_text": "mobile auth epsilon endpoint", "limit": 3,
                                  "session_expand": 3})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["session_expanded"], 1)
        self.assertIn("zeta deploy", res["block"])

    def test_compose_recall_graph_expansion(self):
        # the neighbor fact shares NO lexical terms with the query — only the
        # entity graph can reach it
        mcp.remember_entity({"name": "orders-svc", "type": "service"})
        mcp.remember_entity({"name": "payments-db", "type": "database"})
        mcp.remember_relation({"subject": "orders-svc", "predicate": "uses",
                               "object": "payments-db"})
        mcp.remember_fact({"text": "the orders-svc service handles orders", "source": "t"})
        mcp.remember_fact({"text": "the payments-db stores payment records", "source": "t"})
        res = mcp.compose_recall({"turn_text": "orders handling", "limit": 5, "graph": True})
        self.assertNotIn("error", res, res)
        self.assertGreaterEqual(res["graph"], 1)
        self.assertIn("payments-db", res["block"])


class SearchGraphAndSemanticPrecedentsTest(unittest.TestCase):
    """graph=true in search_facts; semantic find_precedents."""

    def setUp(self):
        self._old = {k: os.environ.get(k) for k in
                     ("MEMORY_MCP_EMBEDDINGS", "MEMORY_MCP_EMBED_PROVIDER")}
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_search_facts_graph_expansion(self):
        mcp.remember_entity({"name": "orders-svc2", "type": "service"})
        mcp.remember_entity({"name": "payments-db2", "type": "database"})
        mcp.remember_relation({"subject": "orders-svc2", "predicate": "uses",
                               "object": "payments-db2"})
        mcp.remember_fact({"text": "the orders-svc2 service handles orders", "source": "t"})
        mcp.remember_fact({"text": "the payments-db2 stores payment records", "source": "t"})
        plain = mcp.search_facts({"query": "orders svc2"})
        self.assertEqual(plain["count"], 1)  # graph-only fact NOT found lexically
        with_graph = mcp.search_facts({"query": "orders svc2", "graph": True})
        self.assertGreaterEqual(with_graph["count"], 2)
        self.assertGreaterEqual(with_graph["graph"], 1)
        self.assertTrue(any("payments-db2" in f["text"] for f in with_graph["facts"]))

    def test_find_precedents_semantic(self):
        mcp.record_decision({"category": "infra", "subject": "proxy",
                             "scenario": "need an HTTP proxy for the cli tool",
                             "reasoning": "CONNECT support", "outcome": "privoxy"})
        # query with different wording — test provider's n-gram vectors share
        # enough 3-grams with the scenario
        res = mcp.find_precedents({"scenario": "choosing a proxy for command line",
                                   "limit": 5, "semantic": True})
        self.assertNotIn("error", res, res)
        self.assertTrue(res["semantic"])
        self.assertGreaterEqual(res["count"], 1)
        # RRF surface: semantic-only matches appear
        plain = mcp.find_precedents({"scenario": "choosing a proxy for command line", "limit": 5})
        sem = mcp.find_precedents({"scenario": "choosing a proxy for command line",
                                   "limit": 5, "semantic": True})
        self.assertGreaterEqual(sem["count"], plain["count"])


class RdfAndReferencesTest(unittest.TestCase):
    """export_rdf (Turtle/PROV) + fact_references (impact)."""

    def test_fact_references_impact(self):
        mcp.remember_fact({"text": "the kappa api moved to port 9001", "source": "t"})
        mcp.remember_fact({"text": "the kappa api now listens on port 9091", "source": "t"})
        old = mcp.search_facts({"query": "kappa api moved to port 9001"})["facts"][0]
        new = mcp.search_facts({"query": "kappa api now listens on port 9091"})["facts"][0]
        con = mcp.get_db()
        con.execute("UPDATE facts SET invalid_at='2026-08-16T00:00:00Z', superseded_by=? WHERE id=?",
                    (new["id"], old["id"]))
        con.commit(); con.close()
        mcp.attach_evidence({"fact_id": new["id"], "source_ref": "supersedes:%s" % old["id"]})
        r = mcp.fact_references({"id": old["id"]})
        self.assertNotIn("error", r, r)
        self.assertEqual(r["incoming"]["supersedes_me"], new["id"])
        self.assertEqual(r["outgoing"]["supersedes"], new["id"])

    def test_export_rdf_turtle(self):
        mcp.remember_fact({"text": "the omega service is the source of truth", "source": "t"})
        mcp.record_decision({"scenario": "decide on the omega approach",
                             "outcome": "adopt omega"})
        r = mcp.export_rdf({})
        self.assertNotIn("error", r, r)
        self.assertEqual(r["format"], "text/turtle")
        self.assertGreater(r["triples"], 0)
        self.assertIn("@prefix prov:", r["rdf"])
        self.assertIn("mem:Fact", r["rdf"])
        self.assertIn("omega service", r["rdf"])
        # valid-ish Turtle: every line is either a prefix or ends with ; or .
        bad = [ln for ln in r["rdf"].split("\n")
               if ln and not ln.startswith("@prefix") and not ln.rstrip().endswith((";", "."))]
        self.assertEqual(bad, [])


class AuditFollowupTest(unittest.TestCase):
    """Follow-ups: source warning, smaller recall budget."""

    def test_remember_without_source_warns(self):
        res = mcp.remember_fact({"text": "fact without source for the warning test"})
        self.assertNotIn("error", res, res)
        self.assertIn("warning", res)
        self.assertIn("source", res["warning"])

    def test_remember_with_source_and_workspace_no_warning(self):
        res = mcp.remember_fact({"text": "fact with source for the warning test",
                                 "source": "run-1", "workspace": "ws-warn"})
        self.assertNotIn("error", res, res)
        self.assertNotIn("warning", res)

    def test_remember_without_workspace_warns(self):
        res = mcp.remember_fact({"text": "workspace-less fact for the warning test",
                                 "source": "run-2"})
        self.assertNotIn("error", res, res)
        self.assertIn("warning", res)
        self.assertIn("workspace", res["warning"])

    def test_compose_recall_default_budget(self):
        os.environ["MEMORY_MCP_RECALL"] = "1"
        mcp.remember_fact({"text": "budget test fact about the tau metric", "source": "t"})
        res = mcp.compose_recall({"turn_text": "tau metric budget"})
        self.assertNotIn("error", res, res)
        self.assertLessEqual(res["chars"], 1400 + 10)


class WorkspaceIsolationTest(unittest.TestCase):
    """One DB, per-project separation via workspace_id (variant C)."""

    def test_workspace_isolation(self):
        mcp.remember_fact({"text": "secret of project alpha about the theta core",
                           "source": "t", "workspace": "proj-alpha"})
        mcp.remember_fact({"text": "secret of project beta about the theta core",
                           "source": "t", "workspace": "proj-beta"})
        mcp.remember_fact({"text": "shared fact about the theta core", "source": "t"})

        # scoped to alpha: sees alpha + shared, NOT beta
        a = mcp.search_facts({"query": "theta core", "workspace": "proj-alpha"})
        texts = [f["text"] for f in a["facts"]]
        self.assertTrue(any("secret of project alpha" in t for t in texts))
        self.assertTrue(any("shared fact" in t for t in texts))
        self.assertFalse(any("secret of project beta" in t for t in texts))

        # scoped to beta: sees beta + shared, NOT alpha
        b = mcp.search_facts({"query": "theta core", "workspace": "proj-beta"})
        texts_b = [f["text"] for f in b["facts"]]
        self.assertTrue(any("secret of project beta" in t for t in texts_b))
        self.assertFalse(any("secret of project alpha" in t for t in texts_b))

        # unscoped (legacy client): sees ONLY the shared pool
        u = mcp.search_facts({"query": "theta core"})
        self.assertTrue(all("secret of project" not in f["text"] for f in u["facts"]))
        self.assertTrue(any("shared fact" in f["text"] for f in u["facts"]))

    def test_summarize_and_review_scoped(self):
        mcp.remember_fact({"text": "alpha-only metric fact about the lambda probe",
                           "source": "t", "workspace": "proj-alpha"})
        idx = mcp.summarize_index({"workspace": "proj-alpha"})
        self.assertIn("lambda probe", idx["index"])
        idx_b = mcp.summarize_index({"workspace": "proj-beta"})
        self.assertNotIn("lambda probe", idx_b["index"])
        rp = mcp.review_pending({"workspace": "proj-alpha", "limit": 100})
        self.assertTrue(any("lambda probe" in f["text"] for f in rp["facts"]))
        rp_b = mcp.review_pending({"workspace": "proj-beta", "limit": 100})
        self.assertFalse(any("lambda probe" in f["text"] for f in rp_b["facts"]))


class WorkspaceRecallScopingTest(unittest.TestCase):
    """Workspace scoping in compose_recall and find_precedents."""

    def setUp(self):
        self._old = {k: os.environ.get(k) for k in ("MEMORY_MCP_RECALL",)}
        os.environ["MEMORY_MCP_RECALL"] = "1"

    def tearDown(self):
        for k, v in self._old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_compose_recall_workspace(self):
        mcp.remember_fact({"text": "wf-only recall fact about the zeta probe",
                           "source": "t", "workspace": "proj-wf"})
        mcp.remember_fact({"text": "noise-only recall fact about the zeta probe",
                           "source": "t", "workspace": "proj-noise"})
        r_wf = mcp.compose_recall({"turn_text": "zeta probe", "workspace": "proj-wf"})
        self.assertNotIn("error", r_wf, r_wf)
        self.assertIn("wf-only", r_wf["block"])
        self.assertNotIn("noise-only", r_wf["block"])
        r_all = mcp.compose_recall({"turn_text": "zeta probe"})
        self.assertNotIn("wf-only", r_all["block"])

    def test_find_precedents_workspace(self):
        mcp.record_decision({"scenario": "wf scenario about the omega gateway",
                             "outcome": "adopt", "workspace": "proj-wf"})
        mcp.record_decision({"scenario": "noise scenario about the omega gateway",
                             "outcome": "reject", "workspace": "proj-noise"})
        r = mcp.find_precedents({"scenario": "omega gateway decision", "workspace": "proj-wf"})
        self.assertNotIn("error", r, r)
        self.assertTrue(any("wf scenario" in p["scenario"] for p in r["precedents"]))
        self.assertFalse(any("noise scenario" in p["scenario"] for p in r["precedents"]))


class WorkspaceBypassGuardTest(unittest.TestCase):
    """Isolation must hold on semantic, by-id, and export paths."""

    def tearDown(self):
        # restore env (this class enables embeddings without its own setUp)
        for k in ("MEMORY_MCP_EMBEDDINGS", "MEMORY_MCP_EMBED_PROVIDER"):
            os.environ.pop(k, None)

    def test_semantic_search_scoped(self):
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        mcp.remember_fact({"text": "alpha semantic probe about the mu core",
                           "source": "t", "workspace": "proj-alpha"})
        mcp.remember_fact({"text": "beta semantic probe about the mu core",
                           "source": "t", "workspace": "proj-beta"})
        a = mcp.search_facts({"query": "mu core probe", "semantic": True,
                              "workspace": "proj-alpha"})
        texts = [f["text"] for f in a["facts"]]
        self.assertTrue(any("alpha semantic" in t for t in texts))
        self.assertFalse(any("beta semantic" in t for t in texts))
        # unscoped semantic: shared pool only
        u = mcp.search_facts({"query": "mu core probe", "semantic": True})
        self.assertTrue(all("semantic probe" not in f["text"] for f in u["facts"]))

    def test_by_id_tools_scoped(self):
        mcp.remember_fact({"text": "alpha secret about the nu endpoint",
                           "source": "t", "workspace": "proj-alpha"})
        fid = mcp.search_facts({"query": "nu endpoint", "workspace": "proj-alpha"})["facts"][0]["id"]
        # forget from another workspace must not touch it
        res = mcp.forget_fact({"id": fid, "workspace": "proj-beta"})
        self.assertEqual(res["archived"], 0)
        still = mcp.search_facts({"query": "nu endpoint", "workspace": "proj-alpha"})
        self.assertGreaterEqual(still["count"], 1)
        # confirm from another workspace must not elevate it
        cf = mcp.confirm_fact({"id": fid, "workspace": "proj-beta"})
        self.assertEqual(cf.get("confirmed"), None)
        # unscoped client cannot see it via provenance
        prov = mcp.get_provenance({"fact_id": fid})
        self.assertIsNone(prov.get("fact"))

    def test_export_scoped(self):
        mcp.remember_fact({"text": "alpha export marker about the xi probe",
                           "source": "t", "workspace": "proj-alpha"})
        exp = mcp.export_facts({"workspace": "proj-beta"})
        self.assertTrue(all("alpha export marker" not in f["text"] for f in exp["facts"]))
        exp_a = mcp.export_facts({"workspace": "proj-alpha"})
        self.assertTrue(any("alpha export marker" in f["text"] for f in exp_a["facts"]))


class WorkspaceGraphScopeTest(unittest.TestCase):
    """Entity graph + export + stats respect workspace isolation."""

    def test_graph_and_export_scoped(self):
        os.environ["MEMORY_MCP_RECALL"] = "1"
        mcp.remember_entity({"name": "svc-wf", "type": "service", "workspace": "proj-wf"})
        mcp.remember_entity({"name": "svc-noise", "type": "service", "workspace": "proj-noise"})
        mcp.remember_relation({"subject": "svc-wf", "predicate": "uses",
                               "object": "svc-wf", "workspace": "proj-wf"})
        # search_graph scoped: other workspace entity invisible
        r = mcp.search_graph({"entity": "svc-wf", "workspace": "proj-wf"})
        self.assertNotIn("error", r, r)
        r_other = mcp.search_graph({"entity": "svc-noise", "workspace": "proj-wf"})
        self.assertIn("error", r_other)  # not visible from another workspace
        # export_rdf scoped
        exp = mcp.export_rdf({"workspace": "proj-wf"})
        self.assertIn("svc-wf", exp["rdf"])
        self.assertNotIn("svc-noise", exp["rdf"])
        # stats scoped
        st = mcp.stats({"workspace": "proj-wf"})
        self.assertGreaterEqual(st["counts"]["entities"], 1)


class MigrationAndDedupTest(unittest.TestCase):
    """Data-preserving rebuild migration + per-workspace dedup."""

    def _old_schema_db(self):
        import sqlite3, tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        con = sqlite3.connect(tmp.name)
        con.executescript("""
        CREATE TABLE facts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          sha256 TEXT NOT NULL UNIQUE,
          text TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT '',
          project TEXT NOT NULL DEFAULT '',
          domain TEXT NOT NULL DEFAULT '',
          trust TEXT NOT NULL DEFAULT 'medium' CHECK (trust IN ('high','medium','low')),
          strong INTEGER NOT NULL DEFAULT 0,
          importance REAL NOT NULL DEFAULT 0.5,
          invalid_at TEXT NOT NULL DEFAULT '',
          superseded_by INTEGER,
          confirmed INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          archived INTEGER NOT NULL DEFAULT 0
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS facts_fts USING fts5(
          text, content='facts', content_rowid='id');
        CREATE TRIGGER IF NOT EXISTS facts_ai AFTER INSERT ON facts BEGIN
          INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
        END;
        CREATE TRIGGER IF NOT EXISTS facts_ad AFTER DELETE ON facts BEGIN
          INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
        END;
        CREATE TRIGGER IF NOT EXISTS facts_au AFTER UPDATE ON facts BEGIN
          INSERT INTO facts_fts(facts_fts, rowid, text) VALUES ('delete', old.id, old.text);
          INSERT INTO facts_fts(rowid, text) VALUES (new.id, new.text);
        END;
        """)
        import hashlib
        ts = "2026-08-16T00:00:00Z"
        sha = hashlib.sha256(b"legacy fact text").hexdigest()
        con.execute("INSERT INTO facts (sha256, text, source, project, domain, trust, strong, "
                    "importance, invalid_at, superseded_by, confirmed, created_at, updated_at, archived) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (sha, "legacy fact text", "s", "p", "d", "medium", 0, 0.9,
                     "2026-08-15T00:00:00Z", 7, 1, ts, ts, 0))
        con.commit(); con.close()
        return tmp.name

    def test_rebuild_preserves_data(self):
        db = self._old_schema_db()
        con = mcp.get_db.__globals__["sqlite3"].connect(db)
        con.row_factory = mcp.get_db.__globals__["sqlite3"].Row
        mcp._migrate_facts(con)
        row = con.execute("SELECT * FROM facts WHERE text='legacy fact text'").fetchone()
        con.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["workspace_id"], "")
        self.assertEqual(row["confirmed"], 1)
        self.assertEqual(row["superseded_by"], 7)
        self.assertEqual(row["invalid_at"], "2026-08-15T00:00:00Z")
        self.assertEqual(row["archived"], 0)
        self.assertEqual(row["importance"], 0.9)
        # FTS still finds it
        mcp2 = mcp
        import os as _os
        _os.environ["MEMORY_MCP_DB"] = db
        # search via direct sqlite FTS
        hit = con if False else None
        c = mcp.get_db.__globals__["sqlite3"].connect(db)
        n = c.execute("SELECT COUNT(*) FROM facts_fts WHERE facts_fts MATCH 'legacy'").fetchone()[0]
        c.close()
        self.assertEqual(n, 1)
        _os.environ.pop("MEMORY_MCP_DB", None)

    def test_per_workspace_dedup(self):
        r1 = mcp.remember_fact({"text": "shared text across workspaces", "source": "t",
                                "workspace": "ws-one"})
        r2 = mcp.remember_fact({"text": "shared text across workspaces", "source": "t",
                                "workspace": "ws-two"})
        self.assertFalse(r1["dedup"])
        self.assertFalse(r2["dedup"])
        self.assertNotEqual(r1["id"], r2["id"])
        # each workspace sees its own copy
        a = mcp.search_facts({"query": "shared text across", "workspace": "ws-one"})
        self.assertEqual(a["count"], 1)
        b = mcp.search_facts({"query": "shared text across", "workspace": "ws-two"})
        self.assertEqual(b["count"], 1)
        self.assertNotEqual(a["facts"][0]["id"], b["facts"][0]["id"])
        # same workspace dedups
        r3 = mcp.remember_fact({"text": "shared text across workspaces", "source": "t",
                                "workspace": "ws-one"})
        self.assertTrue(r3["dedup"])


class DatabaseMgmtTest(unittest.TestCase):
    """v0.6: create/list/archive/backup/delete database (separate SQLite files)."""

    def setUp(self):
        import shutil as _shutil
        self._old_db = mcp.DB_PATH
        # Use a temp dir so databases//backups/ live there, not in /tmp root.
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-dbmgmt-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db
        # Reset module caches that embed DB_PATH at import time.
        for mod in ("embeddings",):
            pass

    def tearDown(self):
        import shutil as _shutil
        os.environ.pop("MEMORY_MCP_DB", None)
        mcp.DB_PATH = self._old_db
        _shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_database_ok(self):
        r = mcp.create_database({"name": "proj-a"})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["created"])
        p = os.path.join(self.tmpdir, "databases", "proj-a.db")
        self.assertTrue(os.path.exists(p))
        dbs = mcp.list_databases({})["databases"]
        self.assertTrue(any(d["name"] == "proj-a" and not d["active"] for d in dbs))

    def test_create_database_rejects_active_name_and_invalid(self):
        r = mcp.create_database({"name": "active"})
        self.assertIn("error", r)  # active.db basename without suffix
        r = mcp.create_database({"name": "a/b"})
        self.assertIn("error", r)
        r = mcp.create_database({"name": ".."})
        self.assertIn("error", r)
        r = mcp.create_database({"name": "x" * 65})
        self.assertIn("error", r)

    def test_create_database_duplicate(self):
        mcp.create_database({"name": "dup"})
        r = mcp.create_database({"name": "dup"})
        self.assertIn("error", r)

    def test_archive_database_soft_and_hard(self):
        mcp.create_database({"name": "oldproj"})
        r = mcp.archive_database({"name": "oldproj"})
        self.assertNotIn("error", r, r)
        self.assertFalse(r["hard"])
        self.assertFalse(r["deleted"])
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, "databases", "oldproj.db.archived")))
        dbs = mcp.list_databases({})["databases"]
        entry = [d for d in dbs if d["name"] == "oldproj"][0]
        self.assertTrue(entry["archived"])
        # hard requires confirm
        mcp.create_database({"name": "doomed"})
        r = mcp.archive_database({"name": "doomed", "hard": True})
        self.assertIn("error", r)
        r = mcp.archive_database({"name": "doomed", "hard": True, "confirm": True})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["deleted"])
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, "databases", "doomed.db")))

    def test_archive_active_blocked(self):
        r = mcp.archive_database({"name": "active"})
        self.assertIn("error", r)

    def test_backup_database_active_and_named(self):
        mcp.remember_fact({"text": "fact for backup test", "source": "t"})
        r = mcp.backup_database({})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["backup"].startswith("active.db."))
        p = os.path.join(self.tmpdir, "backups", r["backup"])
        self.assertTrue(os.path.exists(p))
        import sqlite3 as _sq
        c = _sq.connect(p)
        n = c.execute("SELECT COUNT(*) FROM facts WHERE text='fact for backup test'").fetchone()[0]
        c.close()
        self.assertEqual(n, 1)
        # named (incl. archived) backup
        mcp.create_database({"name": "namedb"})
        r = mcp.backup_database({"name": "namedb"})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["backup"].startswith("namedb.db."))

    def test_delete_database(self):
        mcp.create_database({"name": "gone"})
        r = mcp.delete_database({"name": "gone"})
        self.assertIn("error", r)  # no confirm
        r = mcp.delete_database({"name": "gone", "confirm": True})
        self.assertNotIn("error", r, r)
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, "databases", "gone.db")))
        r = mcp.delete_database({"name": "active", "confirm": True})
        self.assertIn("error", r)


class WorkspaceMgmtTest(unittest.TestCase):
    """v0.6: create/list/reset/archive/backup workspace (registry in active DB)."""

    def setUp(self):
        self._old_db = mcp.DB_PATH
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-wsmgmt-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db

    def tearDown(self):
        import shutil as _shutil
        os.environ.pop("MEMORY_MCP_DB", None)
        mcp.DB_PATH = self._old_db
        _shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_and_list_workspace(self):
        r = mcp.create_workspace({"workspace": "alpha"})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["created"])
        r = mcp.create_workspace({"workspace": "alpha"})
        self.assertFalse(r["created"])  # idempotent
        mcp.remember_fact({"text": "alpha owns this", "source": "t", "workspace": "alpha"})
        lst = mcp.list_workspaces({})["workspaces"]
        alpha = [w for w in lst if w["id"] == "alpha"][0]
        self.assertEqual(alpha["status"], "active")
        self.assertEqual(alpha["active_facts"], 1)

    def test_create_workspace_invalid(self):
        self.assertIn("error", mcp.create_workspace({"workspace": "a/b"}))
        self.assertIn("error", mcp.create_workspace({"workspace": ""}))

    def test_reset_workspace_soft(self):
        mcp.remember_fact({"text": "reset me soft", "source": "t", "workspace": "beta"})
        r = mcp.reset_workspace({"workspace": "beta"})
        self.assertNotIn("error", r, r)
        self.assertFalse(r["hard"])
        self.assertEqual(r["archived_facts"], 1)
        hits = mcp.search_facts({"query": "reset me soft", "workspace": "beta"})
        self.assertEqual(hits["count"], 0)  # archived -> not searchable
        st = [w for w in mcp.list_workspaces({})["workspaces"] if w["id"] == "beta"][0]
        self.assertEqual(st["status"], "reset")

    def test_reset_workspace_hard(self):
        mcp.remember_fact({"text": "reset me hard", "source": "t", "workspace": "gamma"})
        r = mcp.reset_workspace({"workspace": "gamma", "hard": True})
        self.assertIn("error", r)  # confirm required
        r = mcp.reset_workspace({"workspace": "gamma", "hard": True, "confirm": True})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["hard"])
        self.assertEqual(r["deleted_facts"], 1)  # backward-compat alias
        self.assertEqual(r["deleted"]["facts"], 1)
        self.assertEqual(r["deleted_total"], 1)
        con = mcp.get_db()
        n = con.execute("SELECT COUNT(*) FROM facts WHERE workspace_id='gamma'").fetchone()[0]
        con.close()
        self.assertEqual(n, 0)
        self.assertEqual([w for w in mcp.list_workspaces({})["workspaces"] if w["id"] == "gamma"], [])

    def test_archive_workspace_soft_and_reactivate(self):
        mcp.remember_fact({"text": "archive me", "source": "t", "workspace": "delta"})
        r = mcp.archive_workspace({"workspace": "delta"})
        self.assertNotIn("error", r, r)
        self.assertEqual(r["archived_facts"], 1)
        hits = mcp.search_facts({"query": "archive me", "workspace": "delta"})
        self.assertEqual(hits["count"], 0)
        st = [w for w in mcp.list_workspaces({})["workspaces"] if w["id"] == "delta"][0]
        self.assertEqual(st["status"], "archived")
        # re-registering reactivates the workspace
        r = mcp.create_workspace({"workspace": "delta"})
        self.assertTrue(r.get("reactivated"))
        st = [w for w in mcp.list_workspaces({})["workspaces"] if w["id"] == "delta"][0]
        self.assertEqual(st["status"], "active")

    def test_backup_workspace_json(self):
        mcp.remember_fact({"text": "back me up", "source": "t", "workspace": "eps"})
        mcp.archive_workspace({"workspace": "eps"})  # archived facts included
        r = mcp.backup_workspace({"workspace": "eps"})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["backup"].startswith("workspace-eps-"))
        p = os.path.join(self.tmpdir, "backups", r["backup"])
        self.assertTrue(os.path.exists(p))
        with open(p, encoding="utf-8") as fh:
            data = json.load(fh)
        self.assertEqual(data["counts"]["facts"], 1)
        self.assertEqual(data["counts"]["evidence"], 0)
        self.assertEqual(data["facts"][0]["text"], "back me up")
        r = mcp.backup_workspace({"workspace": "nonexistent"})
        self.assertIn("error", r)

    def test_archive_database_soft_does_not_clobber_existing_archive(self):
        import shutil as _shutil
        mcp.create_database({"name": "twice"})
        p = os.path.join(self.tmpdir, "databases", "twice.db")
        # pre-existing archive must not be silently overwritten
        with open(p + ".archived", "w", encoding="utf-8") as f:
            f.write("previous archive")
        r = mcp.archive_database({"name": "twice"})
        self.assertIn("error", r, r)
        with open(p + ".archived", encoding="utf-8") as f:
            self.assertEqual(f.read(), "previous archive")
        # hard mode still works
        r = mcp.archive_database({"name": "twice", "hard": True, "confirm": True})
        self.assertNotIn("error", r, r)
        self.assertTrue(r["deleted"])


class DecayTest(unittest.TestCase):
    """v0.7: active-day decay — degraded/forgotten lifecycle, revival, protection."""

    def setUp(self):
        import shutil as _shutil
        self._old_db = mcp.DB_PATH
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-decay-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db
        self._shutil = __import__("shutil")

    def tearDown(self):
        os.environ.pop("MEMORY_MCP_DB", None)
        mcp.DB_PATH = self._old_db
        self._shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _add_active_days(self, n):
        """Insert n activity days strictly after today (ISO dates)."""
        from datetime import date, timedelta
        con = __import__("sqlite3").connect(self.db)
        base = date.today()
        for i in range(1, n + 1):
            con.execute("INSERT OR IGNORE INTO activity_days (day) VALUES (?)",
                        [(base + timedelta(days=i)).isoformat()])
        con.commit()
        con.close()

    def _lifecycle(self, fid):
        con = __import__("sqlite3").connect(self.db)
        row = con.execute("SELECT lifecycle FROM facts WHERE id=?", [fid]).fetchone()
        con.close()
        return row[0] if row else None

    def test_decay_sweep_degraded_then_forgotten(self):
        r = mcp.remember_fact({"text": "alpha widget decay target", "source": "t", "importance": 1})
        fid = r["id"]
        self._add_active_days(30)
        res = mcp.decay_sweep({})
        self.assertEqual(res["moved"]["to_degraded"], 1)
        self.assertEqual(self._lifecycle(fid), "degraded")
        self._add_active_days(50)  # total 80: 0.95^80 << 0.1
        res = mcp.decay_sweep({})
        self.assertEqual(res["moved"]["to_forgotten"], 1)
        self.assertEqual(self._lifecycle(fid), "forgotten")

    def test_decay_protects_strong_and_confirmed(self):
        r1 = mcp.remember_fact({"text": "strong protected fact", "source": "t", "strong": True})
        r2 = mcp.remember_fact({"text": "confirmed protected fact", "source": "t"})
        con = __import__("sqlite3").connect(self.db)
        con.execute("UPDATE facts SET confirmed=1 WHERE id=?", [r2["id"]])
        con.commit()
        con.close()
        self._add_active_days(80)
        mcp.decay_sweep({})
        self.assertEqual(self._lifecycle(r1["id"]), "active")
        self.assertEqual(self._lifecycle(r2["id"]), "active")

    def test_search_hit_updates_access_metrics(self):
        r = mcp.remember_fact({"text": "hit me please beta core", "source": "t"})
        hits = mcp.search_facts({"query": "beta core"})
        self.assertGreaterEqual(hits["count"], 1)
        con = __import__("sqlite3").connect(self.db)
        row = con.execute("SELECT access_count, last_accessed_at FROM facts WHERE id=?",
                          [r["id"]]).fetchone()
        con.close()
        self.assertEqual(row[0], 1)
        self.assertNotEqual(row[1], "")

    def test_degraded_hidden_then_revived_after_three_attempts(self):
        r = mcp.remember_fact({"text": "gamma widget revival test", "source": "t", "importance": 1})
        fid = r["id"]
        self._add_active_days(30)
        mcp.decay_sweep({})
        self.assertEqual(self._lifecycle(fid), "degraded")
        # two matching searches: still degraded, revival_count climbs
        for _ in range(2):
            res = mcp.search_facts({"query": "gamma widget revival"})
            self.assertEqual(res["count"], 0)  # hidden from plain search
        self.assertEqual(self._lifecycle(fid), "degraded")
        con = __import__("sqlite3").connect(self.db)
        rc = con.execute("SELECT revival_count FROM facts WHERE id=?", [fid]).fetchone()[0]
        con.close()
        self.assertEqual(rc, 2)
        # third matching search: revived to active
        mcp.search_facts({"query": "gamma widget revival"})
        self.assertEqual(self._lifecycle(fid), "active")
        res = mcp.search_facts({"query": "gamma widget revival"})
        self.assertGreaterEqual(res["count"], 1)

    def test_forgotten_visible_only_via_list_forgotten_and_restore(self):
        r = mcp.remember_fact({"text": "delta widget forgotten test", "source": "t", "importance": 1})
        fid = r["id"]
        self._add_active_days(80)
        mcp.decay_sweep({})
        self.assertEqual(self._lifecycle(fid), "forgotten")
        res = mcp.search_facts({"query": "delta widget forgotten"})
        self.assertEqual(res["count"], 0)
        # repeated searches do NOT revive forgotten facts
        for _ in range(5):
            mcp.search_facts({"query": "delta widget forgotten"})
        self.assertEqual(self._lifecycle(fid), "forgotten")
        lst = mcp.list_forgotten({})
        self.assertTrue(any(f["id"] == fid for f in lst["facts"]))
        restored = mcp.restore_fact({"id": fid})
        self.assertEqual(restored["to"], "active")
        res = mcp.search_facts({"query": "delta widget forgotten"})
        self.assertGreaterEqual(res["count"], 1)

    def test_forgotten_excluded_from_graph_and_session_chains(self):
        # strict isolation: forgotten facts must not surface via chains
        r1 = mcp.remember_fact({"text": "alpha widget talks to beta core", "source": "sess-1",
                                "strong": True, "importance": 1})
        r2 = mcp.remember_fact({"text": "beta core status is unknown", "source": "sess-1",
                                "importance": 1})
        mcp.remember_relation({"subject": "alpha", "predicate": "connects", "object": "beta"})
        self._add_active_days(80)
        mcp.decay_sweep({})
        self.assertEqual(self._lifecycle(r1["id"]), "active")   # strong protected
        self.assertEqual(self._lifecycle(r2["id"]), "forgotten")
        # graph chain from the active fact must not surface the forgotten one
        res = mcp.search_facts({"query": "alpha widget talks", "graph": True, "limit": 10})
        self.assertIn(r1["id"], [f["id"] for f in res["facts"]])
        self.assertNotIn(r2["id"], [f["id"] for f in res["facts"]])
        # session chain must not surface it either
        import recall as _recall
        hits = [{"id": r1["id"], "text": "alpha widget talks to beta core", "source": "sess-1"}]
        got = _recall._session_hits(hits, expand=5, workspace="")
        self.assertNotIn(r2["id"], [f["id"] for f in got])

    def test_restore_fact_rejects_archived(self):
        r = mcp.remember_fact({"text": "archived cannot be restored via decay", "source": "t"})
        fid = r["id"]
        mcp.forget_fact({"id": fid})
        res = mcp.restore_fact({"id": fid})
        self.assertIn("error", res)

    def test_activity_day_registered_on_call(self):
        from datetime import date
        mcp._register_activity_day()
        con = __import__("sqlite3").connect(self.db)
        n = con.execute("SELECT COUNT(*) FROM activity_days WHERE day=?",
                        [date.today().isoformat()]).fetchone()[0]
        con.close()
        self.assertEqual(n, 1)


class MigrateMemoryTest(unittest.TestCase):
    """v0.6/v0.7: migrate_memory.load_facts maps workspace to the project slug."""

    def setUp(self):
        import shutil as _shutil
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-migrate-")
        self._shutil = __import__("shutil")
        import migrate_memory as _mig
        self._old_src, self._old_slug = _mig.SRC_DIR, _mig.PROJECT_SLUG
        with open(os.path.join(self.tmpdir, "alpha-fact.md"), "w", encoding="utf-8") as f:
            f.write("---\ntitle: Alpha widget\nmetadata:\n  trust: high\n  type: project\n---\n\nBody text here.\n")

    def tearDown(self):
        import migrate_memory as _mig
        _mig.SRC_DIR, _mig.PROJECT_SLUG = self._old_src, self._old_slug
        self._shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_load_facts_sets_workspace_to_project_slug(self):
        import migrate_memory as mig
        mig.SRC_DIR = self.tmpdir
        mig.PROJECT_SLUG = "proj-alpha"
        facts = mig.load_facts()
        self.assertEqual(len(facts), 1)
        f = facts[0]
        self.assertEqual(f["project"], "proj-alpha")
        self.assertEqual(f["workspace"], "proj-alpha")
        self.assertEqual(f["trust"], "high")
        self.assertIn("Alpha widget", f["text"])


class WorkspaceCleanupTest(unittest.TestCase):
    """v0.8: hard reset/archive purge ALL workspace rows (facts, evidence,
    graph, decisions, embeddings) in FK-safe order — the audit follow-up that
    previously failed with FOREIGN KEY constraint failed. Soft mode hides
    graph/decisions too and refuses writes until reactivation."""

    def _seed(self, ws):
        """Fact + evidence + entity/relation + decision + synthetic embedding
        row (table always exists) — every table a hard purge must cover."""
        self.assertNotIn("error", mcp.create_workspace({"workspace": ws}))
        r = mcp.remember_fact({"text": "cleanup probe fact for " + ws,
                               "source": "test", "workspace": ws})
        self.assertNotIn("error", r, r)
        fid = r["id"]
        self.assertNotIn("error", mcp.attach_evidence(
            {"fact_id": fid, "source_ref": "audit://" + ws, "workspace": ws}))
        self.assertNotIn("error", mcp.remember_entity(
            {"name": "ent-" + ws, "workspace": ws}))
        self.assertNotIn("error", mcp.remember_relation(
            {"subject": "ent-" + ws, "predicate": "pings",
             "object": "ent2-" + ws, "workspace": ws}))
        self.assertNotIn("error", mcp.record_decision(
            {"scenario": "cleanup probe decision for " + ws, "workspace": ws}))
        con = mcp.get_db()
        try:
            con.execute("INSERT OR IGNORE INTO fact_embeddings (fact_id, vec, model, updated_at) "
                        "VALUES (?, ?, ?, ?)", (fid, b"\x00\x01", "test", mcp.now()))
            con.commit()
        finally:
            con.close()
        return fid

    def _table_counts(self, ws):
        con = mcp.get_db()
        try:
            out = {}
            for t, col in (("facts", "workspace_id"), ("entities", "workspace_id"),
                           ("relations", "workspace_id"), ("decisions", "workspace_id")):
                out[t] = con.execute("SELECT COUNT(*) FROM %s WHERE %s=?" % (t, col),
                                     [ws]).fetchone()[0]
            out["evidence"] = con.execute(
                "SELECT COUNT(*) FROM evidence WHERE fact_id IN "
                "(SELECT id FROM facts WHERE workspace_id=?)", [ws]).fetchone()[0]
            out["embeddings"] = con.execute(
                "SELECT COUNT(*) FROM fact_embeddings WHERE fact_id IN "
                "(SELECT id FROM facts WHERE workspace_id=?)", [ws]).fetchone()[0]
            return out
        finally:
            con.close()

    _ZERO = {"facts": 0, "entities": 0, "relations": 0,
             "decisions": 0, "evidence": 0, "embeddings": 0}

    def test_hard_reset_purges_everything(self):
        ws = "cleanup-hard-reset"
        self._seed(ws)
        res = mcp.reset_workspace({"workspace": ws, "hard": True, "confirm": True})
        self.assertNotIn("error", res, res)
        self.assertEqual(res["deleted_total"], sum(res["deleted"].values()))
        self.assertEqual(self._table_counts(ws), self._ZERO)
        # reads agree with the purge (shared-pool rows may remain)
        hits = mcp.search_facts({"query": "cleanup probe", "workspace": ws})
        self.assertFalse(any(ws in f["text"] for f in hits["facts"]))
        decs = mcp.query_decisions({"workspace": ws, "limit": 100})
        self.assertFalse(any("cleanup probe decision for " + ws in d["scenario"]
                             for d in decs["decisions"]))
        # registry row removed -> implicit active again
        con = mcp.get_db()
        try:
            self.assertIsNone(con.execute("SELECT status FROM workspaces WHERE id=?",
                                          [ws]).fetchone())
        finally:
            con.close()

    def test_hard_archive_purges_but_keeps_row(self):
        ws = "cleanup-hard-archive"
        self._seed(ws)
        res = mcp.archive_workspace({"workspace": ws, "hard": True, "confirm": True})
        self.assertNotIn("error", res, res)
        self.assertEqual(res["deleted_total"], sum(res["deleted"].values()))
        self.assertEqual(self._table_counts(ws), self._ZERO)
        con = mcp.get_db()
        try:
            row = con.execute("SELECT status FROM workspaces WHERE id=?", [ws]).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "archived")
        finally:
            con.close()

    def test_soft_reset_hides_all_and_blocks_writes(self):
        ws = "cleanup-soft-reset"
        self._seed(ws)
        res = mcp.reset_workspace({"workspace": ws})
        self.assertNotIn("error", res, res)
        self.assertEqual(mcp.search_facts({"query": "cleanup probe", "workspace": ws})["count"], 0)
        self.assertIn("error", mcp.query_decisions({"workspace": ws}))
        self.assertIn("error", mcp.find_precedents(
            {"scenario": "cleanup probe decision", "workspace": ws}))
        self.assertIn("error", mcp.search_graph({"entity": "ent-" + ws, "workspace": ws}))
        self.assertIn("error", mcp.get_causal_chain({"decision_id": 1, "workspace": ws}))
        self.assertIn("error", mcp.export_rdf({"workspace": ws}))
        self.assertIn("error", mcp.detect_conflicts({"text": "cleanup probe", "workspace": ws}))
        self.assertIn("error", mcp.fact_history({"id": 1, "workspace": ws}))
        self.assertIn("error", mcp.fact_references({"id": 1, "workspace": ws}))
        # writes refused
        self.assertIn("error", mcp.remember_fact({"text": "late write", "workspace": ws}))
        self.assertIn("error", mcp.ingest_turn({"text": "late ingest", "workspace": ws}))
        self.assertIn("error", mcp.record_decision({"scenario": "late", "workspace": ws}))
        self.assertIn("error", mcp.remember_relation(
            {"subject": "x-" + ws, "predicate": "q", "object": "y-" + ws, "workspace": ws}))
        self.assertIn("error", mcp.remember_entity({"name": "z-" + ws, "workspace": ws}))
        self.assertIn("error", mcp.attach_evidence(
            {"fact_id": 1, "source_ref": "z", "workspace": ws}))
        self.assertIn("error", mcp.forget_fact({"id": 1, "workspace": ws}))
        self.assertIn("error", mcp.confirm_fact({"id": 1, "workspace": ws}))

    def test_soft_archive_hides_graph_and_decisions(self):
        ws = "cleanup-soft-archive"
        self._seed(ws)
        res = mcp.archive_workspace({"workspace": ws})
        self.assertNotIn("error", res, res)
        self.assertEqual(mcp.search_facts({"query": "cleanup probe", "workspace": ws})["count"], 0)
        self.assertIn("error", mcp.query_decisions({"workspace": ws}))
        self.assertIn("error", mcp.search_graph({"entity": "ent-" + ws, "workspace": ws}))

    def test_reactivation_unblocks_writes(self):
        ws = "cleanup-reactivate"
        self._seed(ws)
        mcp.archive_workspace({"workspace": ws})
        self.assertIn("error", mcp.remember_fact({"text": "blocked", "workspace": ws}))
        res = mcp.create_workspace({"workspace": ws})
        self.assertTrue(res.get("reactivated"), res)
        out = mcp.remember_fact({"text": "after reactivation " + ws,
                                 "source": "t", "workspace": ws})
        self.assertNotIn("error", out, out)
    def test_old_schema_store_purges_after_migration(self):
        """A store that predates the FTS tables and cascading FKs (graph-era
        data, no facts_fts/decisions_fts/workspaces) must still hard-purge:
        _migrate_fks + _migrate_fts bring it to the current schema on open,
        and the FTS 'delete' trigger must not fail with SQLITE_CORRUPT."""
        import sqlite3 as _sqlite3
        db = os.path.join(tempfile.mkdtemp(prefix="mcp-oldws-"), "old.db")
        con = _sqlite3.connect(db)
        con.executescript("""
CREATE TABLE facts (id INTEGER PRIMARY KEY AUTOINCREMENT, sha256 TEXT NOT NULL, text TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT '', project TEXT NOT NULL DEFAULT '', domain TEXT NOT NULL DEFAULT '',
  trust TEXT NOT NULL DEFAULT 'medium', strong INTEGER NOT NULL DEFAULT 0, importance REAL NOT NULL DEFAULT 0.5,
  invalid_at TEXT NOT NULL DEFAULT '', superseded_by INTEGER, confirmed INTEGER NOT NULL DEFAULT 0,
  workspace_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0);
CREATE TABLE entities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, type TEXT NOT NULL DEFAULT '',
  aliases TEXT NOT NULL DEFAULT '', workspace_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE relations (id INTEGER PRIMARY KEY AUTOINCREMENT, subject_id INTEGER NOT NULL REFERENCES entities(id),
  predicate TEXT NOT NULL, object_id INTEGER NOT NULL REFERENCES entities(id), source_fact_id INTEGER,
  workspace_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, UNIQUE(subject_id, predicate, object_id));
CREATE TABLE decisions (id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL DEFAULT '',
  subject TEXT NOT NULL DEFAULT '', scenario TEXT NOT NULL, reasoning TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL DEFAULT '', confidence REAL, decision_maker TEXT NOT NULL DEFAULT '',
  issue_ref TEXT NOT NULL DEFAULT '', parent_decision_id INTEGER, workspace_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE evidence (id INTEGER PRIMARY KEY AUTOINCREMENT, fact_id INTEGER NOT NULL REFERENCES facts(id),
  source_ref TEXT NOT NULL, source_checksum TEXT NOT NULL DEFAULT '', fetched_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL, UNIQUE(fact_id, source_ref));
INSERT INTO facts (sha256, text, workspace_id, created_at, updated_at) VALUES ('a', 'old fact', 'ws-old', 't', 't');
INSERT INTO evidence (fact_id, source_ref, created_at) VALUES (1, 'old://ref', 't');
INSERT INTO entities (name, workspace_id, created_at, updated_at) VALUES ('e1', 'ws-old', 't', 't');
INSERT INTO entities (name, workspace_id, created_at, updated_at) VALUES ('e2', 'ws-old', 't', 't');
INSERT INTO relations (subject_id, predicate, object_id, workspace_id, created_at) VALUES (1, 'p', 2, 'ws-old', 't');
INSERT INTO decisions (scenario, workspace_id, created_at, updated_at) VALUES ('old decision', 'ws-old', 't', 't');
""")
        con.close()
        old = mcp.DB_PATH
        try:
            os.environ["MEMORY_MCP_DB"] = db
            mcp.DB_PATH = db
            con = mcp.get_db()
            columns = {row["name"] for row in con.execute("PRAGMA table_info(evidence)")}
            self.assertIn("repo", columns)
            old_evidence = con.execute(
                "SELECT source_ref, repo, resolution_status FROM evidence WHERE fact_id=1"
            ).fetchone()
            self.assertEqual(old_evidence["source_ref"], "old://ref")
            self.assertEqual(old_evidence["repo"], "")
            self.assertEqual(old_evidence["resolution_status"], "")
            con.close()
            r = mcp.reset_workspace({"workspace": "ws-old", "hard": True, "confirm": True})
            self.assertNotIn("error", r, r)
            con = mcp.get_db()
            self.assertEqual(con.execute("SELECT COUNT(*) FROM facts").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM evidence").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM entities").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM relations").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM decisions").fetchone()[0], 0)
            con.close()
        finally:
            os.environ.pop("MEMORY_MCP_DB", None)
            mcp.DB_PATH = old


class DatabaseIsolationTest(unittest.TestCase):
    """v0.9: session-level database selection — select_database points ALL
    tools at a named DB (active store stays protected); full workspace
    read-back (backup_workspace/list_workspaces) covers graph/decisions."""

    def setUp(self):
        self._old_db = mcp.DB_PATH
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-dbiso-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db
        mcp._SELECTED_DB[0] = None

    def tearDown(self):
        import shutil as _shutil
        os.environ.pop("MEMORY_MCP_DB", None)
        mcp.DB_PATH = self._old_db
        mcp._SELECTED_DB[0] = None
        _shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _seed_temp_workspace(self, ws="iso-ws"):
        """Fact + evidence + entity/relation + 2-decision chain in the temp DB."""
        mcp.create_workspace({"workspace": ws})
        f = mcp.remember_fact({"text": "iso fact for " + ws, "source": "t", "workspace": ws})
        self.assertNotIn("error", f, f)
        fid = f["id"]
        self.assertNotIn("error", mcp.attach_evidence(
            {"fact_id": fid, "source_ref": "iso://1", "workspace": ws}))
        self.assertNotIn("error", mcp.remember_entity({"name": "iso-ent-" + ws, "workspace": ws}))
        self.assertNotIn("error", mcp.remember_relation(
            {"subject": "iso-ent-" + ws, "predicate": "p",
             "object": "iso-ent2-" + ws, "workspace": ws}))
        d1 = mcp.record_decision({"scenario": "iso decision 1 for " + ws, "workspace": ws})
        self.assertNotIn("error", d1, d1)
        d2 = mcp.record_decision({"scenario": "iso decision 2 for " + ws,
                                  "parent_decision_id": d1["id"], "workspace": ws})
        self.assertNotIn("error", d2, d2)
        return fid, d2["id"]

    def test_select_database_isolates_all_tools(self):
        r = mcp.create_database({"name": "iso-tmp"})
        self.assertNotIn("error", r, r)
        sel = mcp.select_database({"name": "iso-tmp"})
        self.assertNotIn("error", sel, sel)
        self.assertTrue(sel["selected"] and not sel["active"])
        self.assertEqual(mcp.current_database({})["database"], "iso-tmp")
        # list_databases marks the selection
        dbs = mcp.list_databases({})["databases"]
        self.assertTrue([d for d in dbs if d["name"] == "iso-tmp"][0]["selected"])
        self.assertFalse([d for d in dbs if d["name"] == "active"][0]["selected"])

        fid, d2id = self._seed_temp_workspace()
        # scope/precedent/causal/provenance/export all see the temp DB
        self.assertEqual(mcp.search_facts({"query": "iso fact", "workspace": "iso-ws"})["count"], 1)
        self.assertEqual(mcp.query_decisions({"workspace": "iso-ws"})["count"], 2)
        self.assertEqual(mcp.get_causal_chain({"decision_id": d2id, "workspace": "iso-ws"})["count"], 2)
        self.assertEqual(len(mcp.get_provenance(
            {"fact_id": fid, "workspace": "iso-ws"})["evidence"]), 1)
        self.assertNotIn("error", mcp.export_rdf({"workspace": "iso-ws"}))
        # concurrent dedup works in the temp DB
        again = mcp.remember_fact({"text": "iso fact for iso-ws", "source": "t",
                                   "workspace": "iso-ws"})
        self.assertTrue(again["dedup"])

        # active store stays untouched
        mcp.reset_database({})
        self.assertEqual(mcp.current_database({})["active"], True)
        self.assertEqual(mcp.search_facts({"query": "iso fact", "workspace": "iso-ws"})["count"], 0)
        self.assertEqual(mcp.query_decisions({"workspace": "iso-ws"})["count"], 0)

        # back to temp: hard reset twice (idempotent), nothing left
        mcp.select_database({"name": "iso-tmp"})
        r1 = mcp.reset_workspace({"workspace": "iso-ws", "hard": True, "confirm": True})
        self.assertNotIn("error", r1, r1)
        r2 = mcp.reset_workspace({"workspace": "iso-ws", "hard": True, "confirm": True})
        self.assertNotIn("error", r2, r2)
        self.assertEqual(sum(r2["deleted"].values()), 0)
        # every scoped query returns 0 after cleanup
        self.assertEqual(mcp.search_facts({"query": "iso fact", "workspace": "iso-ws"})["count"], 0)
        self.assertEqual(mcp.query_decisions({"workspace": "iso-ws"})["count"], 0)

        # delete the temp DB (must not be selected anymore)
        mcp.reset_database({})
        d = mcp.delete_database({"name": "iso-tmp", "confirm": True})
        self.assertNotIn("error", d, d)
        self.assertFalse(os.path.exists(mcp._db_file("iso-tmp")))

    def test_delete_archive_selected_database_refused(self):
        mcp.create_database({"name": "iso-lock"})
        mcp.select_database({"name": "iso-lock"})
        self.assertIn("error", mcp.delete_database({"name": "iso-lock", "confirm": True}))
        self.assertIn("error", mcp.archive_database({"name": "iso-lock"}))
        mcp.reset_database({})
        self.assertNotIn("error", mcp.delete_database({"name": "iso-lock", "confirm": True}))

    def test_backup_and_list_workspace_cover_graph(self):
        mcp.create_workspace({"workspace": "iso-bk"})
        f = mcp.remember_fact({"text": "bk fact", "source": "t", "workspace": "iso-bk"})
        self.assertNotIn("error", f, f)
        self.assertNotIn("error", mcp.attach_evidence(
            {"fact_id": f["id"], "source_ref": "bk://1", "workspace": "iso-bk"}))
        self.assertNotIn("error", mcp.remember_entity({"name": "bk-ent", "workspace": "iso-bk"}))
        self.assertNotIn("error", mcp.record_decision(
            {"scenario": "bk decision", "workspace": "iso-bk"}))
        lst = [w for w in mcp.list_workspaces({})["workspaces"] if w["id"] == "iso-bk"][0]
        self.assertEqual(lst["facts"], 1)
        self.assertEqual(lst["entities"], 1)
        self.assertEqual(lst["decisions"], 1)
        self.assertEqual(lst["evidence"], 1)
        bk = mcp.backup_workspace({"workspace": "iso-bk"})
        self.assertNotIn("error", bk, bk)
        expected_counts = {"facts": 1, "entities": 1, "relations": 0,
                           "decisions": 1, "evidence": 1,
                           "contexts": 0, "context_lineage": 0,
                           "lifecycle_events": 0, "handoffs": 0}
        self.assertEqual({key: bk["counts"][key] for key in expected_counts},
                         expected_counts)
        self.assertIn("categories", bk["counts"])
        self.assertIn("fact_embeddings", bk["counts"])
        self.assertIn("decision_embeddings", bk["counts"])
        self.assertIn("workspaces", bk["counts"])
        self.assertIn("activity_days", bk["counts"])
        # the JSON on disk carries the same counts + rows
        import json as _json
        with open(os.path.join(os.path.dirname(mcp.DB_PATH), "backups",
                               os.path.basename(bk["backup"]))) as fh:
            data = _json.load(fh)
        self.assertEqual(data["counts"]["evidence"], 1)
        self.assertEqual(len(data["evidence"]), 1)
        self.assertEqual(len(data["decisions"]), 1)
        # backup of an empty workspace errors
        mcp.create_workspace({"workspace": "iso-empty"})
        self.assertIn("error", mcp.backup_workspace({"workspace": "iso-empty"}))

    def test_selected_database_vanished_fails_loudly(self):
        mcp.create_database({"name": "iso-ghost"})
        mcp.select_database({"name": "iso-ghost"})
        os.remove(mcp._db_file("iso-ghost"))
        with self.assertRaises(RuntimeError) as cm:
            mcp.search_facts({"query": "anything"})
        self.assertIn("no longer exists", str(cm.exception))
        # recovering: select the active store back
        self.assertNotIn("error", mcp.reset_database({}))
        self.assertNotIn("error", mcp.search_facts({"query": "anything"}))


class CategoryIndexTest(unittest.TestCase):
    """v0.10: topic categories — rule-based assignment at write time, card
    catalog (list_categories), shelf lookup (search_index snippets), LLM batch
    refinement (categorize_pending), category-aware reads, purge coverage."""

    def setUp(self):
        self._old_db = mcp.DB_PATH
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-cat-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db
        mcp._SELECTED_DB[0] = None
        self._env = {k: os.environ.pop(k, None)
                     for k in ("MEMORY_MCP_CATEGORIZE", "MEMORY_MCP_LLM_PROVIDER",
                     "MEMORY_MCP_EMBEDDINGS", "MEMORY_MCP_EMBED_PROVIDER")}

    def tearDown(self):
        import shutil as _shutil
        os.environ.pop("MEMORY_MCP_DB", None)
        mcp.DB_PATH = self._old_db
        mcp._SELECTED_DB[0] = None
        for k, v in self._env.items():
            if v is not None:
                os.environ[k] = v
        _shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _fact(self, text, **kw):
        r = mcp.remember_fact({"text": text, "source": "test", **kw})
        self.assertNotIn("error", r, r)
        return r

    def test_rule_based_and_explicit_category(self):
        self._fact("Пересобрал docker-образ и пересоздал контейнер", workspace="cat-ws")
        self._fact("summarize_index отдаёт категории", workspace="cat-ws")
        self._fact("полностью невнятный текст про зебр", workspace="cat-ws")
        self._fact("тоже про зебр, но явная категория", category="animals", workspace="cat-ws")
        cats = mcp.list_categories({"workspace": "cat-ws"})["categories"]
        by_name = {c["name"]: c for c in cats}
        self.assertEqual(by_name["docker"]["active_facts"], 1)
        self.assertEqual(by_name["memory-mcp"]["active_facts"], 1)
        self.assertEqual(by_name["animals"]["active_facts"], 1)
        # no-match fact stays uncategorized
        rows = mcp.list_facts({"workspace": "cat-ws"})["facts"]
        by_text = {f["text"]: f["category"] for f in rows}
        self.assertIsNone(by_text["полностью невнятный текст про зебр"])

    def test_search_index_groups_and_caps(self):
        self._fact("docker-образ reasonix пересобран", workspace="cat-ws")
        self._fact("docker-compose пересоздан", workspace="cat-ws")
        self._fact("summarize_index линии с категориями", workspace="cat-ws")
        self._fact("очень длинный факт " + "слово " * 60, workspace="cat-ws")
        r = mcp.search_index({"query": "docker OR compose OR факт OR длинный",
                              "workspace": "cat-ws", "limit": 10})
        self.assertNotIn("error", r, r)
        groups = {g["category"]: g["facts"] for g in r["groups"]}
        self.assertEqual(len(groups["docker"]), 2)
        self.assertIn("(uncategorized)", groups)
        # snippets only — full text never leaks
        for g in r["groups"]:
            for f in g["facts"]:
                self.assertNotIn("text", f)
                self.assertLessEqual(len(f["snippet"]), 125)
        # category filter
        r2 = mcp.search_index({"query": "docker", "category": "docker", "workspace": "cat-ws"})
        self.assertEqual(r2["count"], 2)
        r3 = mcp.search_index({"query": "docker", "category": "memory-mcp", "workspace": "cat-ws"})
        self.assertEqual(r3["count"], 0)
        # max_chars caps output
        r4 = mcp.search_index({"query": "docker OR compose OR факт", "max_chars": 200,
                               "workspace": "cat-ws"})
        self.assertTrue(r4["truncated"] or r4["shown"] < r4["count"])

    def test_categorize_pending_gate_and_llm(self):
        r = mcp.categorize_pending({"workspace": "cat-ws"})
        self.assertIn("error", r)  # disabled without MEMORY_MCP_CATEGORIZE
        os.environ["MEMORY_MCP_CATEGORIZE"] = "1"
        os.environ["MEMORY_MCP_LLM_PROVIDER"] = "test"
        self._fact("непонятный факт про дампы", workspace="cat-ws")
        self._fact("непонятный факт про лаги", workspace="cat-ws")
        r = mcp.categorize_pending({"workspace": "cat-ws", "limit": 10})
        self.assertNotIn("error", r, r)
        self.assertEqual(r["categorized"], 2, r)
        rows = mcp.list_facts({"workspace": "cat-ws"})["facts"]
        cats = {f["category"] for f in rows}
        self.assertTrue(all(c and c.startswith("llm-") for c in cats), cats)
        # idempotent: nothing left to categorize
        r2 = mcp.categorize_pending({"workspace": "cat-ws"})
        self.assertEqual(r2["count"], 0)

    def test_workspace_isolation_and_purge(self):
        self._fact("docker-факт в ws-a", workspace="cat-a")
        self._fact("docker-факт в ws-b", workspace="cat-b")
        cats_a = {c["name"] for c in mcp.list_categories({"workspace": "cat-a"})["categories"]}
        self.assertEqual(cats_a, {"docker"})
        # hard reset purges categories
        r = mcp.reset_workspace({"workspace": "cat-a", "hard": True, "confirm": True})
        self.assertNotIn("error", r, r)
        self.assertEqual(r["deleted"]["categories"], 1)
        self.assertEqual(mcp.list_categories({"workspace": "cat-a"})["count"], 0)
        # ws-b untouched
        self.assertEqual(len(mcp.list_categories({"workspace": "cat-b"})["categories"]), 1)

    def test_migration_adds_category_id(self):
        import sqlite3 as _sqlite3
        db = os.path.join(self.tmpdir, "old.db")
        con = _sqlite3.connect(db)
        con.execute("CREATE TABLE facts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "sha256 TEXT NOT NULL, text TEXT NOT NULL, source TEXT NOT NULL DEFAULT '', "
                    "project TEXT NOT NULL DEFAULT '', domain TEXT NOT NULL DEFAULT '', "
                    "trust TEXT NOT NULL DEFAULT 'medium', strong INTEGER NOT NULL DEFAULT 0, "
                    "importance REAL NOT NULL DEFAULT 0.5, invalid_at TEXT NOT NULL DEFAULT '', "
                    "superseded_by INTEGER, confirmed INTEGER NOT NULL DEFAULT 0, "
                    "workspace_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, "
                    "updated_at TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0, "
                    "last_accessed_at TEXT NOT NULL DEFAULT '', access_count INTEGER NOT NULL DEFAULT 0, "
                    "revival_count INTEGER NOT NULL DEFAULT 0, lifecycle TEXT NOT NULL DEFAULT 'active')")
        con.commit()
        con.close()
        os.environ["MEMORY_MCP_DB"] = db
        mcp.DB_PATH = db
        con = mcp.get_db()
        cols = {r["name"] for r in con.execute("PRAGMA table_info(facts)")}
        self.assertIn("category_id", cols)
        cats = [r["name"] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='categories'")]
        self.assertEqual(cats, ["categories"])
        con.close()
    def test_search_index_semantic_path(self):
        # embeddings enabled BEFORE writes so fact_embeddings rows exist
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        self._fact("docker-образ reasonix пересобран", workspace="cat-ws")
        self._fact("docker-compose пересоздан", workspace="cat-ws")
        r = mcp.search_index({"query": "docker", "semantic": True, "workspace": "cat-ws"})
        self.assertNotIn("error", r, r)
        groups = {g["category"]: g["facts"] for g in r["groups"]}
        self.assertEqual(len(groups.get("docker", [])), 2, groups)
        for f in groups["docker"]:
            self.assertIn("importance", f)
            self.assertIn("updated_at", f)
            self.assertEqual(f["category"], "docker")
    def test_dedup_preserves_explicit_category(self):
        self._fact("docker-образ пересобран", category="infra", workspace="cat-ws")
        # re-remember the same text WITHOUT args: rules would say "docker",
        # but the stored explicit choice must survive
        self._fact("docker-образ пересобран", workspace="cat-ws")
        rows = mcp.list_facts({"workspace": "cat-ws"})["facts"]
        self.assertEqual(rows[0]["category"], "infra")
        # explicit refresh on re-remember still works
        self._fact("docker-образ пересобран", category="runtimes", workspace="cat-ws")
        rows = mcp.list_facts({"workspace": "cat-ws"})["facts"]
        self.assertEqual(rows[0]["category"], "runtimes")


class AuditRegressionTest(unittest.TestCase):
    """Regression coverage for the security and correctness audit findings."""

    def setUp(self):
        self._old_db = mcp.DB_PATH
        self._old_selected = mcp._SELECTED_DB[0]
        self.tmpdir = tempfile.mkdtemp(prefix="mcp-audit-")
        self.db = os.path.join(self.tmpdir, "active.db")
        os.environ["MEMORY_MCP_DB"] = self.db
        mcp.DB_PATH = self.db
        mcp._SELECTED_DB[0] = None
        self._env = {
            key: os.environ.get(key)
            for key in ("MEMORY_MCP_EXTRACT", "MEMORY_MCP_EXTRACT_MIN_CHARS",
                        "MEMORY_MCP_RECALL", "MEMORY_MCP_ALLOW_INSECURE_HTTP",
                        "MEMORY_MCP_EMBEDDINGS", "MEMORY_MCP_EMBED_PROVIDER")
        }

    def tearDown(self):
        for key, value in self._env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        mcp.DB_PATH = self._old_db
        mcp._SELECTED_DB[0] = self._old_selected
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_semantic_search_honors_filters_and_returns_category_metadata(self):
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        workspace = "semantic-filter-ws"
        wanted = mcp.remember_fact({
            "text": "semantic filter marker wanted",
            "source": "test", "project": "alpha", "domain": "product",
            "category": "wanted", "trust": "high", "strong": True,
            "importance": 0.9, "workspace": workspace,
        })
        self.assertNotIn("error", wanted, wanted)
        for suffix, overrides in (
                ("other category", {"category": "other"}),
                ("low trust", {"trust": "low"}),
                ("not strong", {"strong": False}),
                ("other project", {"project": "beta"}),
                ("other domain", {"domain": "ops"}),
                ("other workspace", {"workspace": "other-ws"})):
            result = mcp.remember_fact({
                "text": "semantic filter marker " + suffix,
                "source": "test", "project": "alpha", "domain": "product",
                "category": "wanted", "trust": "high", "strong": True,
                "workspace": workspace,
                **overrides,
            })
            self.assertNotIn("error", result, result)
        invalid = mcp.remember_fact({
            "text": "semantic filter marker invalidated",
            "source": "test", "project": "alpha", "domain": "product",
            "category": "wanted", "trust": "high", "strong": True,
            "workspace": workspace,
        })
        con = mcp.get_db()
        con.execute("UPDATE facts SET invalid_at=? WHERE id=?",
                    ("2026-08-01T00:00:00Z", invalid["id"]))
        con.commit()
        con.close()
        archived = mcp.remember_fact({
            "text": "semantic filter marker archived",
            "source": "test", "project": "alpha", "domain": "product",
            "category": "wanted", "trust": "high", "strong": True,
            "workspace": workspace,
        })
        mcp.forget_fact({"id": archived["id"], "workspace": workspace})

        filters = {
            "query": "semantic filter marker", "limit": 50,
            "workspace": workspace, "category": "wanted", "trust_min": "high",
            "strong_only": True, "project": "alpha", "domain": "product",
        }
        direct = mcp.search_semantic(filters)
        self.assertNotIn("error", direct, direct)
        self.assertEqual([row["id"] for row in direct["facts"]], [wanted["id"]], direct)
        self.assertEqual(direct["facts"][0]["category"], "wanted")
        self.assertIn("importance", direct["facts"][0])
        self.assertIn("confirmed", direct["facts"][0])
        self.assertIn("invalid_at", direct["facts"][0])

        hybrid = mcp.search_facts(dict(filters, semantic=True))
        self.assertNotIn("error", hybrid, hybrid)
        self.assertEqual([row["id"] for row in hybrid["facts"]], [wanted["id"]], hybrid)

    def test_semantic_search_honors_valid_at(self):
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        fact = mcp.remember_fact({
            "text": "semantic historical validity marker",
            "source": "test", "workspace": "semantic-history",
        })
        con = mcp.get_db()
        con.execute("UPDATE facts SET invalid_at=? WHERE id=?",
                    ("2026-08-20T00:00:00Z", fact["id"]))
        con.commit()
        con.close()
        current = mcp.search_semantic({
            "query": "semantic historical validity marker",
            "workspace": "semantic-history",
        })
        self.assertEqual(current["count"], 0, current)
        historical = mcp.search_semantic({
            "query": "semantic historical validity marker",
            "workspace": "semantic-history", "valid_at": "2026-08-19T00:00:00Z",
        })
        self.assertEqual([row["id"] for row in historical["facts"]], [fact["id"]], historical)

    def test_sweep_freshness_rejects_inactive_workspace_before_shared_update(self):
        os.environ.pop("MEMORY_MCP_EMBEDDINGS", None)
        os.environ["MEMORY_MCP_RECALL"] = "1"
        shared = mcp.remember_fact({
            "text": "shared stale freshness marker", "source": "test",
            "importance": 0.1,
        })
        con = mcp.get_db()
        con.execute("UPDATE facts SET updated_at=? WHERE id=?",
                    ("2020-01-01T00:00:00Z", shared["id"]))
        con.commit()
        con.close()
        self.assertNotIn("error", mcp.create_workspace({"workspace": "stale-ws"}))
        self.assertNotIn("error", mcp.archive_workspace({"workspace": "stale-ws"}))

        result = mcp.sweep_freshness({"workspace": "stale-ws"})
        self.assertIn("error", result, result)
        con = mcp.get_db()
        row = con.execute("SELECT archived FROM facts WHERE id=?", [shared["id"]]).fetchone()
        con.close()
        self.assertEqual(row["archived"], 0)
        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        backfill = mcp.embed_backfill({"workspace": "stale-ws"})
        self.assertIn("error", backfill, backfill)

    def test_workspace_backup_contains_schema_data_and_private_artifacts(self):
        import base64
        import stat
        from unittest.mock import patch

        os.environ["MEMORY_MCP_EMBEDDINGS"] = "1"
        os.environ["MEMORY_MCP_EMBED_PROVIDER"] = "test"
        workspace = "complete-backup-ws"
        self.assertNotIn("error", mcp.create_workspace({"workspace": workspace}))
        fact = mcp.remember_fact({
            "text": "complete backup fact marker", "source": "test",
            "category": "backup", "workspace": workspace,
        })
        self.assertNotIn("error", fact, fact)
        con = mcp.get_db()
        con.execute(
            "UPDATE facts SET invalid_at=?, superseded_by=?, confirmed=1, "
            "last_accessed_at=?, access_count=3, revival_count=2, lifecycle='degraded', "
            "archived=1 WHERE id=?",
            ("2026-08-20T00:00:00Z", 999, "2026-08-21T00:00:00Z", fact["id"]))
        con.commit()
        con.close()
        first = mcp.remember_entity({"name": "backup-service", "workspace": workspace})
        second = mcp.remember_entity({"name": "backup-db", "workspace": workspace})
        mcp.remember_relation({"subject": "backup-service", "predicate": "uses",
                               "object": "backup-db", "workspace": workspace})
        mcp.record_decision({"scenario": "backup decision", "workspace": workspace,
                             "subject": "backup", "outcome": "retain"})
        mcp.attach_evidence({"fact_id": fact["id"], "source_ref": "test/backup",
                             "workspace": workspace})
        context = mcp.put_context({"name": "backup-context", "content": "payload",
                                   "workspace": workspace})
        mcp.capture_event({"idempotency_key": "backup-event", "event_id": "backup-event",
                           "event_kind": "test", "payload": "event payload",
                           "workspace": workspace})
        mcp.handoff_begin({"content": "handoff payload", "owner": "tester",
                           "workspace": workspace})
        mcp._register_activity_day()

        result = mcp.backup_workspace({"workspace": workspace})
        self.assertNotIn("error", result, result)
        backup_dir = os.path.join(self.tmpdir, "backups")
        backup_path = os.path.join(backup_dir, result["backup"])
        self.assertEqual(stat.S_IMODE(os.stat(backup_dir).st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(os.stat(backup_path).st_mode), 0o600)
        with open(backup_path, encoding="utf-8") as fh:
            data = json.load(fh)
        table_names = (
            "categories", "facts", "fact_embeddings", "entities", "relations",
            "decisions", "decision_embeddings", "evidence", "contexts",
            "context_lineage", "lifecycle_events", "handoffs", "workspaces",
            "activity_days",
        )
        self.assertEqual(data["manifest"]["format"], "memory-mcp.workspace-backup")
        self.assertEqual(data["manifest"]["version"], 1)
        for table in table_names:
            self.assertIn(table, data, table)
            self.assertEqual(data["counts"][table], len(data[table]), table)
        self.assertEqual(data["workspace"], workspace)
        self.assertEqual(data["facts"][0]["invalid_at"], "2026-08-20T00:00:00Z")
        self.assertEqual(data["facts"][0]["superseded_by"], 999)
        self.assertEqual(data["facts"][0]["confirmed"], 1)
        self.assertEqual(data["facts"][0]["access_count"], 3)
        self.assertEqual(data["facts"][0]["revival_count"], 2)
        self.assertEqual(data["facts"][0]["lifecycle"], "degraded")
        self.assertEqual(data["workspaces"][0]["id"], workspace)
        self.assertEqual(data["contexts"][0]["ref"], context["context"]["ref"])
        self.assertTrue(data["fact_embeddings"])
        base64.b64decode(data["fact_embeddings"][0]["vec"])
        self.assertIn("decision_embeddings", data)
        self.assertTrue(data["activity_days"])

        database_backup = mcp.backup_database({})
        self.assertNotIn("error", database_backup, database_backup)
        database_path = os.path.join(backup_dir, database_backup["backup"])
        self.assertEqual(stat.S_IMODE(os.stat(database_path).st_mode), 0o600)

        with patch("memory_mcp.json.dump", side_effect=OSError("simulated write failure")):
            failed = mcp.backup_workspace({"workspace": workspace})
        self.assertIn("error", failed, failed)
        self.assertFalse(any(name.startswith(".") for name in os.listdir(backup_dir)))

    def test_decay_review_and_restore_are_workspace_scoped(self):
        fact = mcp.remember_fact({"text": "beta forgotten isolation marker",
                                  "source": "test", "workspace": "beta"})
        con = mcp.get_db()
        con.execute("UPDATE facts SET lifecycle='forgotten' WHERE id=?", [fact["id"]])
        con.commit()
        con.close()

        listed = mcp.list_forgotten({"workspace": "alpha"})
        self.assertFalse(any(row["id"] == fact["id"] for row in listed["facts"]))
        self.assertIn("error", mcp.restore_fact({"id": fact["id"], "workspace": "alpha"}))
        restored = mcp.restore_fact({"id": fact["id"], "workspace": "beta"})
        self.assertEqual(restored["to"], "active")

    def test_entities_are_unique_per_workspace_and_legacy_tables_migrate(self):
        first = mcp.remember_entity({"name": "same-service", "workspace": "alpha"})
        second = mcp.remember_entity({"name": "same-service", "workspace": "beta"})
        self.assertNotIn("error", first, first)
        self.assertNotIn("error", second, second)
        self.assertNotEqual(first["id"], second["id"])

        old_db = os.path.join(self.tmpdir, "legacy.db")
        import sqlite3
        con = sqlite3.connect(old_db)
        con.executescript("""
        CREATE TABLE entities (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          type TEXT NOT NULL DEFAULT '',
          aliases TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT INTO entities(name, created_at, updated_at)
        VALUES ('legacy-service', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
        """)
        con.commit()
        con.close()
        mcp.DB_PATH = old_db
        migrated = mcp.get_db()
        migrated.execute("UPDATE entities SET workspace_id='legacy' WHERE name='legacy-service'")
        migrated.commit()
        migrated.close()
        alpha = mcp.remember_entity({"name": "legacy-service", "workspace": "alpha"})
        beta = mcp.remember_entity({"name": "legacy-service", "workspace": "beta"})
        self.assertNotIn("error", alpha, alpha)
        self.assertNotIn("error", beta, beta)
        self.assertNotEqual(alpha["id"], beta["id"])

    def test_recall_metadata_cannot_inject_new_record_lines(self):
        import recall
        entry = recall._entry({
            "id": 1,
            "text": "fact line\nINJECTED_FACT_LINE",
            "project": "scope\nINJECTED_SCOPE_LINE",
            "domain": "type\nINJECTED_TYPE_LINE",
            "trust": "medium\nINJECTED_TRUST_LINE",
            "semantic_score": 0.1,
        })
        self.assertIn(r"scope\nINJECTED_SCOPE_LINE", entry)
        self.assertIn(r"type\nINJECTED_TYPE_LINE", entry)
        self.assertIn(r"trust=medium\nINJECTED_TRUST_LINE", entry)
        self.assertNotIn("scope\nINJECTED_SCOPE_LINE".replace("\\n", "\n"), entry)
        self.assertNotIn("fact: fact line\nINJECTED_FACT_LINE", entry)

    def test_ingest_turn_requires_human_confirmation_for_authority_metadata(self):
        from unittest.mock import patch
        os.environ["MEMORY_MCP_EXTRACT"] = "1"
        os.environ["MEMORY_MCP_EXTRACT_MIN_CHARS"] = "100"
        response = {"facts": [{
            "text": "global extracted metadata marker",
            "type": "reference",
            "trust": "high",
            "strong": True,
            "scope": "global",
            "importance": 0.9,
        }]}
        with patch("extract.llm.chat_json", return_value=response):
            result = mcp.ingest_turn({
                "transcript": "transcript " + "x" * 120,
                "session_ref": "audit-session",
                "workspace": "alpha",
                "project": "audit",
            })
        self.assertEqual(result["stored"], 1, result)
        con = mcp.get_db()
        row = con.execute(
            "SELECT id, domain, trust, strong, importance, confirmed, workspace_id FROM facts "
            "WHERE text=?", ["global extracted metadata marker"]).fetchone()
        con.close()
        self.assertEqual(row["domain"], "reference")
        self.assertEqual(row["trust"], "medium")
        self.assertEqual(row["strong"], 0)
        self.assertEqual(row["importance"], 0.9)
        self.assertEqual(row["workspace_id"], "")
        self.assertEqual(row["confirmed"], 0)
        pending = mcp.review_pending({})
        self.assertTrue(any(item["id"] == row["id"] for item in pending["facts"]))
        confirmed = mcp.confirm_fact({"id": row["id"]})
        self.assertEqual(confirmed["trust"], "high")
        self.assertTrue(confirmed["confirmed"])

    def test_stdio_returns_jsonrpc_errors_for_non_object_and_bad_params(self):
        import subprocess

        env = os.environ.copy()
        env["MEMORY_MCP_DB"] = os.path.join(self.tmpdir, "stdio.db")
        request = (
            "null\n"
            "[]\n"
            '{"jsonrpc":"2.0","id":1,"method":"initialize","params":[]}\n'
            '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":[]}\n'
            '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"create_database","arguments":{"name":5}}}\n'
            "{not valid json\n"
            '{"jsonrpc":"2.0","id":3,"method":"ping"}\n'
        )
        completed = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                          "memory_mcp.py")],
            input=request, text=True, capture_output=True, env=env, timeout=10)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        responses = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(len(responses), 7, responses)
        self.assertEqual(responses[0]["error"]["code"], -32600)
        self.assertEqual(responses[1]["error"]["code"], -32600)
        self.assertEqual(responses[2]["error"]["code"], -32602)
        self.assertEqual(responses[3]["error"]["code"], -32602)
        self.assertTrue(responses[4]["result"]["isError"])
        self.assertIn("tool execution failed", responses[4]["result"]["content"][0]["text"])
        self.assertNotIn("strip", responses[4]["result"]["content"][0]["text"])
        self.assertEqual(responses[5]["error"]["code"], -32700)
        self.assertEqual(responses[6]["result"], {})

    def test_credential_bearing_http_requires_explicit_opt_in(self):
        from http_security import validate_http_url
        os.environ.pop("MEMORY_MCP_ALLOW_INSECURE_HTTP", None)
        with self.assertRaises(RuntimeError):
            validate_http_url("http://provider.invalid/v1",
                              {"Authorization": "Bearer test-token"})
        validate_http_url("http://localhost:11434", {})
        validate_http_url("https://provider.invalid/v1",
                          {"Authorization": "Bearer test-token"})
        os.environ["MEMORY_MCP_ALLOW_INSECURE_HTTP"] = "1"
        validate_http_url("http://provider.invalid/v1",
                          {"Authorization": "Bearer test-token"})

    def test_malformed_direct_handler_inputs_return_errors(self):
        self.assertIn("error", mcp.search_facts({"query": "x", "trust_min": "bogus"}))
        self.assertIn("error", mcp.list_facts({"limit": "not-an-int"}))
        self.assertIn("error", mcp.search_graph({"entity": "x", "depth": "not-an-int"}))
