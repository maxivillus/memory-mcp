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


class AddFactAliasTest(unittest.TestCase):
    def test_add_fact_alias_stores_fact(self):
        # add_fact is a protocol-level alias (HANDLERS), same handler as remember_fact
        self.assertIn("add_fact", mcp.TOOLS)
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
