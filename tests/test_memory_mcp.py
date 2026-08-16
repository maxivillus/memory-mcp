"""Tests for the memory-mcp server (stdlib only, no external deps).

Each test uses unique data against a temp DB (MEMORY_MCP_DB). The module reads
that env var at import time, so the import happens in setUpModule AFTER the
temp path is set.

Run:  python3 -m unittest discover -s tests -v
"""

import importlib
import os
import sys
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
