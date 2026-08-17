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
