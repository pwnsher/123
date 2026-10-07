"""Gem Radar automated tests (stdlib unittest). No test touches the network:
the HTTP transport is replaced, and every mock value is labeled TEST/MOCK.

Run:  python3 -m unittest discover -s tests -v      (from the gem-radar folder)
"""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import fixtures as fx  # noqa: E402
from gem_radar.analysis import fast_scan  # noqa: E402
from gem_radar.chains import detect as det  # noqa: E402
from gem_radar.chains.evm import keccak256, to_checksum  # noqa: E402
from gem_radar.commands import gem, radar, recheck, watch  # noqa: E402
from gem_radar.core.config import load_config  # noqa: E402
from gem_radar.core.enums import ProviderStatus, Status  # noqa: E402
from gem_radar.core.errors import InvalidAddressError, redact  # noqa: E402
from gem_radar.core.types import ProviderReport, Reading  # noqa: E402
from gem_radar.data import http  # noqa: E402
from gem_radar.data.cache import ProviderCache  # noqa: E402
from gem_radar.data.providers import (  # noqa: E402
    DexScreener,
    EvmRPC,
    GoPlusEVM,
    HoneypotIs,
    LocalFiles,
    RugCheck,
    SolanaRPC,
    StaticProvider,
)
from gem_radar.data.providers.base import FetchContext  # noqa: E402
from gem_radar.storage import db, history, watchlist  # noqa: E402
from gem_radar.ui import formatters  # noqa: E402

DAY = 86400.0
NOW = time.time()

CLEAN = {  # TEST/MOCK: a clean, established token — every input present
    "liquidity_usd": 3_000_000.0, "lp_locked_pct": 99.0, "is_honeypot": False,
    "buy_tax_pct": 0.0, "sell_tax_pct": 0.0, "mint_authority_active": False,
    "transfer_pausable": False, "blacklist_enabled": False, "is_open_source": True,
    "is_proxy": False, "hidden_owner": False, "can_take_back_ownership": False,
    "owner_can_change_balance": False, "ownership_renounced": True, "cannot_sell_all": False,
    "top10_pct": 18.0, "top1_pct": 3.0, "dev_pct": 0.5, "holder_count": 150_000,
    "volume_24h_usd": 5_000_000.0, "buys_24h": 6000, "sells_24h": 5500,
    "pool_created_at": NOW - 500 * DAY, "has_website": True, "social_link_count": 3,
}
CORROBORATION = {"liquidity_usd": 2_900_000.0, "is_honeypot": False, "sell_tax_pct": 0.0}


def network_forbidden(*a, **k):
    raise AssertionError("a test attempted a real network request")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self._env = {k: os.environ.get(k) for k in
                     ("GEM_RADAR_HOME", "GEM_RADAR_DATA_DIR", "GEM_RADAR_CONFIG", "GEM_RADAR_OFFLINE")}
        os.environ["GEM_RADAR_HOME"] = str(self.home)
        os.environ["GEM_RADAR_DATA_DIR"] = str(self.home / "radar-data")
        os.environ.pop("GEM_RADAR_CONFIG", None)
        os.environ.pop("GEM_RADAR_OFFLINE", None)
        http.set_transport(network_forbidden)
        http.set_network_allowed(True)
        http.call_log.clear()
        self.cfg = load_config()
        self.cfg["http"]["backoff_seconds"] = 0.0
        self.conn = db.connect(self.home / "radar.db")

    def tearDown(self):
        self.conn.close()
        http.set_transport(None)
        http.set_network_allowed(True)
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.tmp.cleanup()

    def scan(self, providers, address=fx.EVM_TOKEN, chain="ethereum", **kw):
        return gem.scan(address, chain=chain, providers=providers, cfg=self.cfg, conn=self.conn, **kw)

    def clean_providers(self, **over):
        main = dict(CLEAN)
        main.update(over)
        return [StaticProvider("dex+sec", main), StaticProvider("corroborate", CORROBORATION)]


# ---------------------------------------------------------------- required scenarios
class RequiredScenarios(Base):
    def test_1_clean_established_token(self):
        r = self.scan(self.clean_providers())
        self.assertEqual(r["kind"], "verdict")
        self.assertFalse([f for f in r["red_flags"] if f["severity"] == "CRITICAL"])
        self.assertEqual(r["final_score"], 100)
        self.assertEqual(r["verdict"], "GEM")
        self.assertGreaterEqual(r["confidence"]["score"], 80)
        # complete provenance: every reading has source, timestamp and status
        readings = [rd for m in r["metrics"].values() for rd in m["readings"]]
        self.assertTrue(readings)
        for rd in readings:
            self.assertTrue(rd["source"].startswith("mock:"))
            self.assertGreater(rd["fetched_at"], 0)
            self.assertIn(rd["status"], {"LIVE", "CACHED", "STALE"})
        text = formatters.render(r)
        self.assertTrue(text.startswith("GEM — 100/100 — Confidence "))
        self.assertIn("TEST/MOCK DATA", text)
        self.assertTrue(text.rstrip().endswith("Not financial advice. Heuristic risk analysis only."))

    def test_2_honeypot_critical_cap(self):
        r = self.scan(self.clean_providers(is_honeypot=True, sell_tax_pct=99.0))
        codes = {f["code"] for f in r["red_flags"] if f["severity"] == "CRITICAL"}
        self.assertIn("HONEYPOT", codes)
        self.assertIn("EXTREME_SELL_TAX", codes)
        self.assertGreater(r["calculated_score"], 20)
        self.assertEqual(r["final_score"], 20)
        self.assertTrue(r["cap_applied"])
        self.assertEqual(r["verdict"], "AVOID")
        text = formatters.render(r)
        self.assertTrue(text.startswith("AVOID — 20/100 — Confidence"))
        self.assertLess(text.index("CRITICAL FLAGS"), text.index("STRENGTHS"))
        self.assertLess(text.index("Score capped at 20"), text.index("STRENGTHS"))

    def test_3_missing_data_stays_unknown(self):
        partial = StaticProvider("dex", {"liquidity_usd": 400_000.0, "volume_24h_usd": 90_000.0,
                                         "buys_24h": 300, "sells_24h": 250})
        broken = StaticProvider("security", {}, fail=ProviderStatus.TIMEOUT)
        r = self.scan([partial, broken])
        self.assertEqual(r["kind"], "verdict")  # scan completes
        for name in ("top10_pct", "dev_pct", "is_honeypot", "mint_authority_active"):
            self.assertEqual(r["metrics"][name]["status"], "UNKNOWN")
            self.assertIsNone(r["metrics"][name]["value"])
            self.assertEqual(r["metrics"][name]["readings"], [])  # nothing fabricated
            self.assertIn(name, r["missing"])
        self.assertFalse(r["red_flags"])  # unknown never fires a flag
        full = self.scan(self.clean_providers())
        self.assertLess(r["confidence"]["score"], full["confidence"]["score"])
        self.assertLessEqual(r["confidence"]["score"], 50)
        prov = {p["name"]: p["status"] for p in r["providers"]}
        self.assertEqual(prov["mock:security"], "TIMEOUT")
        self.assertIn("MISSING / UNKNOWN DATA", formatters.render(r))

    def test_4_conflicting_liquidity(self):
        dex = StaticProvider("dex", dict(CLEAN, liquidity_usd=1_000_000.0))
        other = StaticProvider("security", {"liquidity_usd": 200_000.0})
        deep = StaticProvider("deep-check", {"is_honeypot": False}, tier="deep")
        r = self.scan([dex, other, deep])
        liq = r["metrics"]["liquidity_usd"]
        self.assertEqual(liq["status"], "CONFLICT")
        vals = sorted(x["value"] for x in liq["disagreement"]["readings"])
        self.assertEqual(vals, [200_000.0, 1_000_000.0])  # both readings preserved
        self.assertEqual(liq["value"], 200_000.0)  # conservative, never averaged
        self.assertNotEqual(liq["value"], 600_000.0)
        agree = self.scan([StaticProvider("dex", dict(CLEAN, liquidity_usd=1_000_000.0)),
                           StaticProvider("security", {"liquidity_usd": 950_000.0}),
                           StaticProvider("deep-check", {"is_honeypot": False}, tier="deep")],
                          deep="force")
        self.assertLess(r["confidence"]["score"], agree["confidence"]["score"])
        self.assertEqual(r["analysis"]["mode"], "DEEP")
        self.assertTrue(any("liquidity" in x for x in r["analysis"]["reasons"]))
        self.assertEqual(deep.calls, 1)
        text = formatters.render(r)
        self.assertIn("Sources disagree on liquidity", text)


# ---------------------------------------------------------------- addresses and chains
class Chains(Base):
    def test_malformed_contract(self):
        for bad in ("0x123", "", "hello world", "0x" + "g" * 40, "abc;rm -rf /", "0O0O0O"):
            r = gem.scan(bad, cfg=self.cfg, providers=[], conn=self.conn)
            self.assertEqual(r["kind"], "rejected", bad)
        # wrong EIP-55 checksum (one letter case flipped)
        bad_ck = "0x6982508145454Ce325dDbE47a25d4ec3d2311933".replace("Ce3", "ce3")
        with self.assertRaises(InvalidAddressError):
            det.detect(bad_ck)
        self.assertEqual(http.call_log, [])

    def test_keccak_and_checksum(self):
        self.assertEqual(keccak256(b"").hex(),
                         "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
        self.assertEqual(to_checksum("0x6982508145454ce325ddbe47a25d4ec3d2311933"),
                         "0x6982508145454Ce325dDbE47a25d4ec3d2311933")

    def test_unsupported_chain(self):
        r = gem.scan("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", cfg=self.cfg, providers=[],
                     conn=self.conn)
        self.assertEqual(r["kind"], "chain")
        self.assertEqual(r["chain_resolution"]["status"], "UNSUPPORTED")
        self.assertEqual(r["chain_resolution"]["chain"], "tron")
        self.assertIn("STATUS: UNSUPPORTED", formatters.render(r))
        r2 = gem.scan(fx.EVM_TOKEN, chain="pulsechain", cfg=self.cfg, providers=[], conn=self.conn)
        self.assertEqual(r2["chain_resolution"]["status"], "UNSUPPORTED")
        # DEX lists the token only on a chain Gem Radar does not support
        http.set_transport(lambda *a: (200, json.dumps(fx.dexscreener(("pulsechain",))).encode(), {}))
        r3 = gem.scan(fx.EVM_TOKEN, cfg=self.cfg, providers=[DexScreener()], conn=self.conn)
        self.assertEqual(r3["chain_resolution"]["status"], "UNSUPPORTED")
        self.assertNotIn("metrics", r3)  # no fabricated analysis

    def test_ambiguous_evm_chain_never_guessed(self):
        http.set_transport(lambda *a: (200, json.dumps(fx.dexscreener(("ethereum", "base"))).encode(),
                                       {}))
        r = gem.scan(fx.EVM_TOKEN, cfg=self.cfg, providers=[DexScreener()], conn=self.conn)
        self.assertEqual(r["kind"], "chain")
        self.assertEqual(r["chain_resolution"]["status"], "AMBIGUOUS")
        self.assertEqual(sorted(r["chain_resolution"]["candidates"]), ["base", "ethereum"])
        self.assertIn("--chain", formatters.render(r))

    def test_single_chain_resolved_from_dex(self):
        http.set_transport(lambda *a: (200, json.dumps(fx.dexscreener(("base",))).encode(), {}))
        r = gem.scan(fx.EVM_TOKEN, cfg=self.cfg, providers=[DexScreener()], conn=self.conn)
        self.assertEqual(r["chain"], "base")
        self.assertEqual(r["chain_resolution"]["method"], "DexScreener pairs")

    def test_evm_chain_unknown_without_evidence(self):
        r = gem.scan(fx.EVM_TOKEN, cfg=self.cfg, providers=[], conn=self.conn)
        self.assertEqual(r["chain_resolution"]["status"], "UNKNOWN")

    def test_solana_detected(self):
        d = det.detect(fx.SOL_MINT)
        self.assertEqual(d.family, "solana")
        self.assertTrue(d.notes)  # SVM-format ambiguity is reported, not hidden


# ---------------------------------------------------------------- scoring and flags
class Scoring(Base):
    def test_boundaries_39_40_79_80(self):
        self.assertEqual(fast_scan.verdict_for(39, self.cfg).value, "AVOID")
        self.assertEqual(fast_scan.verdict_for(39.9, self.cfg).value, "AVOID")
        self.assertEqual(fast_scan.verdict_for(40, self.cfg).value, "WATCH")
        self.assertEqual(fast_scan.verdict_for(79, self.cfg).value, "WATCH")
        self.assertEqual(fast_scan.verdict_for(79.9, self.cfg).value, "WATCH")
        self.assertEqual(fast_scan.verdict_for(80, self.cfg).value, "GEM")

    def test_pipeline_lands_on_80_and_79(self):
        eighty = dict(social_link_count=0, has_website=False, holder_count=600,
                      volume_24h_usd=300_000.0, pool_created_at=NOW - 40 * DAY, top10_pct=25.0,
                      dev_pct=3.0, lp_locked_pct=85.0)
        r80 = self.scan(self.clean_providers(**eighty))
        self.assertEqual((r80["final_score"], r80["verdict"]), (80, "GEM"))
        r79 = self.scan(self.clean_providers(**eighty, top1_pct=7.0))
        self.assertEqual((r79["final_score"], r79["verdict"]), (79, "WATCH"))
        self.assertTrue(formatters.render(r79).startswith("WATCH — 79/100"))

    def test_readme_scoring_example(self):
        ex = dict(liquidity_usd=620_000.0, lp_locked_pct=97.0, sell_tax_pct=3.0, top10_pct=28.0,
                  top1_pct=6.0, dev_pct=0.4, holder_count=15_000, volume_24h_usd=750_000.0,
                  buys_24h=900, sells_24h=700, pool_created_at=NOW - 200 * DAY, social_link_count=2)
        r = self.scan([StaticProvider("readme", dict(CLEAN, **ex))])
        pts = {k: v["points"] for k, v in r["components"].items()}
        self.assertEqual(pts, {"liquidity": 23, "contract": 23, "holders": 17, "volume": 13,
                               "age_social": 13})
        self.assertEqual((r["final_score"], r["verdict"]), (89, "GEM"))
        minted = self.scan([StaticProvider("readme", dict(CLEAN, **ex, mint_authority_active=True))])
        self.assertTrue(formatters.render(minted).startswith("AVOID — 20/100"))

    def test_critical_cap_is_min(self):
        hi = self.scan(self.clean_providers(owner_can_change_balance=True))
        self.assertEqual(hi["final_score"], 20)
        low = self.scan([StaticProvider("x", {"owner_can_change_balance": True,
                                              "liquidity_usd": 6000.0})])
        self.assertLess(low["calculated_score"], 20)
        self.assertEqual(low["final_score"], low["calculated_score"])
        self.assertEqual(low["verdict"], "AVOID")

    def test_live_mint_authority_solana(self):
        sol = dict(CLEAN, mint_authority_active=True, freeze_authority_active=False,
                   transfer_tax_pct=0.0, permanent_delegate=False, transfer_hook=False,
                   non_transferable=False, metadata_mutable=False)
        r = self.scan([StaticProvider("sol", sol)], address=fx.SOL_MINT, chain="solana")
        self.assertEqual(r["red_flags"][0]["code"], "ACTIVE_MINT_AUTHORITY")
        self.assertEqual(r["final_score"], 20)
        text = formatters.render(r)
        self.assertIn("Active mint authority allows additional supply creation.", text)
        self.assertLess(text.index("Active mint authority"), text.index("STRENGTHS"))

    def test_dev_concentration_threshold(self):
        at = self.scan(self.clean_providers(dev_pct=15.0))
        self.assertNotIn("DEV_CONCENTRATION", {f["code"] for f in at["red_flags"]})
        above = self.scan(self.clean_providers(dev_pct=15.5))
        self.assertIn("DEV_CONCENTRATION", {f["code"] for f in above["red_flags"]})
        self.assertEqual(above["verdict"], "AVOID")
        self.cfg["thresholds"]["dev_critical_pct"] = 10.0
        cfgd = self.scan(self.clean_providers(dev_pct=12.0))
        self.assertIn("DEV_CONCENTRATION", {f["code"] for f in cfgd["red_flags"]})

    def test_removable_liquidity_with_concentration(self):
        r = self.scan(self.clean_providers(lp_locked_pct=10.0, top10_pct=70.0))
        self.assertIn("REMOVABLE_LIQUIDITY_CONCENTRATED",
                      {f["code"] for f in r["red_flags"] if f["severity"] == "CRITICAL"})

    def test_high_score_poor_data_low_confidence(self):
        few = {k: CLEAN[k] for k in ("liquidity_usd", "lp_locked_pct", "is_honeypot",
                                     "buy_tax_pct", "sell_tax_pct", "mint_authority_active")}
        r = self.scan([StaticProvider("only", few)])
        self.assertLessEqual(r["confidence"]["score"], 50)
        self.assertEqual(r["verdict"], "WATCH")

    def test_no_data_is_unrated_not_avoid(self):
        r = self.scan([StaticProvider("down", {}, fail=ProviderStatus.TIMEOUT)])
        self.assertEqual(r["verdict"], "UNRATED")
        self.assertEqual(r["red_flags"], [])
        self.assertTrue(formatters.render(r).startswith("UNRATED — 0/100 — Confidence 0/100"))

    def test_deep_scan_on_mid_score(self):
        r = self.scan(self.clean_providers(lp_locked_pct=None, top10_pct=50.0, dev_pct=8.0,
                                           pool_created_at=NOW - 3 * DAY, social_link_count=0,
                                           has_website=False, liquidity_usd=60_000.0))
        self.assertTrue(40 <= r["final_score"] <= 80, r["final_score"])
        self.assertEqual(r["analysis"]["mode"], "DEEP")
        self.assertTrue(any("band" in x for x in r["analysis"]["reasons"]))
        self.assertIn("UNKNOWN as unknown", r["analysis"]["llm"]["system"])
        off = self.scan(self.clean_providers(top10_pct=50.0, dev_pct=8.0, liquidity_usd=60_000.0,
                                             pool_created_at=NOW - 3 * DAY), deep="off")
        self.assertEqual(off["analysis"]["mode"], "FAST")
        self.assertTrue(off["analysis"]["escalation_suppressed"])


# ---------------------------------------------------------------- data layer
class DataLayer(Base):
    def test_provider_timeout_degrades(self):
        def timeout(*a):
            raise socket.timeout("timed out")
        http.set_transport(timeout)
        self.cfg["http"]["retries"] = 1
        r = self.scan([GoPlusEVM(), StaticProvider("dex", {"liquidity_usd": 500_000.0})])
        prov = {p["name"]: p for p in r["providers"]}
        self.assertEqual(prov["goplus"]["status"], "TIMEOUT")
        self.assertEqual(r["metrics"]["is_honeypot"]["status"], "UNKNOWN")
        self.assertEqual(len([c for c in http.call_log if c[0] == "goplus"]), 2)  # 1 + 1 retry

    def test_rate_limit_retry_then_success(self):
        answers = [(429, b"", {"Retry-After": "0"}), (200, json.dumps(fx.goplus_evm()).encode(), {})]
        http.set_transport(lambda *a: answers.pop(0))
        rep = GoPlusEVM().run(FetchContext("ethereum", fx.EVM_TOKEN, self.cfg))
        self.assertEqual(rep.status, ProviderStatus.AVAILABLE)
        http.set_transport(lambda *a: (429, b"", {}))
        self.cfg["http"]["retries"] = 0
        rep2 = GoPlusEVM().run(FetchContext("ethereum", fx.EVM_TOKEN, self.cfg))
        self.assertEqual(rep2.status, ProviderStatus.RATE_LIMITED)

    def test_blocked_host_reported(self):
        import urllib.error

        def blocked(*a):
            raise urllib.error.URLError("Tunnel connection failed: 403 Forbidden")
        http.set_transport(blocked)
        rep = DexScreener().run(FetchContext("ethereum", fx.EVM_TOKEN, self.cfg))
        self.assertEqual(rep.status, ProviderStatus.BLOCKED)

    def test_stale_cache_not_scored(self):
        cache = ProviderCache(self.conn)
        old = NOW - 3 * DAY
        cache.put("ethereum", fx.EVM_TOKEN, ProviderReport(
            "dexscreener", ProviderStatus.AVAILABLE, old, "", [
                Reading("liquidity_usd", 900_000.0, "dexscreener", old, Status.LIVE)]))
        # fresh-cache lookup refuses it (older than cache TTL)
        self.assertIsNone(cache.get("dexscreener", "ethereum", fx.EVM_TOKEN, now=NOW, cfg=self.cfg,
                                    max_age=self.cfg["cache_ttl_seconds"]))
        # live fetch fails -> fallback to cached answer, classified STALE, not scored
        http.set_transport(lambda *a: (503, b"", {}))
        self.cfg["http"]["retries"] = 0
        r = self.scan([DexScreener()])
        liq = r["metrics"]["liquidity_usd"]
        self.assertEqual(liq["status"], "STALE")
        self.assertIsNone(liq["value"])
        self.assertIn("liquidity_usd", r["missing"])
        self.assertIn("STALE", r["missing_detail"]["liquidity_usd"])

    def test_recent_cache_reused_and_recheck_bypasses(self):
        calls = []

        def ok(method, url, *rest):
            calls.append(url)
            return 200, json.dumps(fx.dexscreener()).encode(), {}
        http.set_transport(ok)
        self.scan([DexScreener()])
        self.scan([DexScreener()])
        self.assertEqual(len(calls), 1)  # second /gem used the very recent cache
        r = self.scan([DexScreener()], refresh=True)
        self.assertEqual(len(calls), 2)
        self.assertEqual(r["metrics"]["liquidity_usd"]["status"], "LIVE")

    def test_local_fallback_json_csv(self):
        d = self.home / "radar-data"
        d.mkdir()
        fresh = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(NOW - 60))
        (d / "a.json").write_text(json.dumps({"records": [
            {"source": "manual-export TEST", "captured_at": fresh, "chain": "ethereum",
             "contract": fx.EVM_TOKEN.upper().replace("0X", "0x"), "metric": "liquidity_usd",
             "value": 250000},
            {"source": "x", "captured_at": fresh, "chain": "ethereum", "contract": fx.EVM_TOKEN,
             "metric": "made_up_metric", "value": 1},
            {"source": "x", "captured_at": "yesterday", "chain": "ethereum",
             "contract": fx.EVM_TOKEN, "metric": "top10_pct", "value": 10},
        ]}))
        (d / "b.csv").write_text(
            "source,captured_at,chain,contract,metric,value,unit\n"
            f"csv TEST,{NOW - 10 * DAY:.0f},ethereum,{fx.EVM_TOKEN},holder_count,5000,\n"
            f"csv TEST,{NOW - 60:.0f},ethereum,{fx.EVM_TOKEN},top10_pct,0.25,fraction\n"
            f"csv TEST,{NOW - 60:.0f},ethereum,{fx.EVM_TOKEN},top1_pct,250,\n")
        rep = LocalFiles().run(FetchContext("ethereum", fx.EVM_TOKEN, self.cfg))
        by = {r.metric: r for r in rep.readings}
        self.assertEqual(by["liquidity_usd"].status, Status.CACHED)  # never LIVE
        self.assertEqual(by["top10_pct"].value, 25.0)
        self.assertEqual(by["holder_count"].status, Status.STALE)  # 10 days > 1h window
        self.assertNotIn("top1_pct", by)  # 250% rejected, not clamped
        self.assertEqual(len(rep.extra["rejected"]), 3)
        self.assertTrue(by["liquidity_usd"].is_mock)

    def test_local_superseded_by_network(self):
        d = self.home / "radar-data"
        d.mkdir()
        (d / "x.csv").write_text("source,captured_at,chain,contract,metric,value\n"
                                 f"export,{NOW - 30:.0f},ethereum,{fx.EVM_TOKEN},liquidity_usd,10\n")
        r = self.scan([StaticProvider("dex", {"liquidity_usd": 900_000.0}), LocalFiles()])
        self.assertEqual(r["metrics"]["liquidity_usd"]["status"], "LIVE")
        self.assertEqual(r["metrics"]["liquidity_usd"]["value"], 900_000.0)


class Adapters(Base):
    """Provider adapters parse documented response shapes (TEST/MOCK fixtures)."""

    def serve(self, routes):
        def t(method, url, body, headers, timeout):
            for frag, payload in routes.items():
                if frag in url:
                    return 200, json.dumps(payload).encode(), {}
            return 404, b"", {}
        http.set_transport(t)

    def test_dexscreener(self):
        self.serve({"dexscreener": fx.dexscreener()})
        ctx = FetchContext("ethereum", fx.EVM_TOKEN, self.cfg)
        rep = DexScreener().run(ctx)
        v = {r.metric: r.value for r in rep.readings}
        self.assertEqual(v["liquidity_usd"], 1_000_000.0)
        self.assertEqual(v["buys_24h"], 900)
        self.assertEqual(v["social_link_count"], 2)
        self.assertAlmostEqual(v["pool_created_at"], fx.NOW_MS / 1000 - 200 * DAY, delta=1)
        self.assertIn(fx.EVM_PAIR, ctx.shared["pool_addresses"])

    def test_goplus_units_and_pool_exclusion(self):
        self.serve({"gopluslabs": fx.goplus_evm()})
        ctx = FetchContext("ethereum", fx.EVM_TOKEN, self.cfg)
        v = {r.metric: r.value for r in GoPlusEVM().run(ctx).readings}
        self.assertEqual(v["sell_tax_pct"], 3.0)  # fraction 0.03 -> 3 %
        self.assertEqual(v["dev_pct"], 0.4)
        self.assertAlmostEqual(v["top1_pct"], 4.0)  # pair (30 %) and burn (10 %) excluded
        self.assertAlmostEqual(v["top10_pct"], 9.0)
        self.assertEqual(v["lp_locked_pct"], 97.0)
        self.assertFalse(v["mint_authority_active"])
        self.assertTrue(v["ownership_renounced"])

    def test_goplus_mintable_with_active_owner(self):
        self.serve({"gopluslabs": fx.goplus_evm(
            is_mintable="1", owner_address="0x8888888888888888888888888888888888888888")})
        v = {r.metric: r.value for r in GoPlusEVM().run(
            FetchContext("ethereum", fx.EVM_TOKEN, self.cfg)).readings}
        self.assertTrue(v["mint_authority_active"])

    def test_honeypot_is(self):
        self.serve({"honeypot.is": fx.honeypot_is(is_honeypot=True, sell_tax=100)})
        v = {r.metric: r.value for r in HoneypotIs().run(
            FetchContext("ethereum", fx.EVM_TOKEN, self.cfg)).readings}
        self.assertTrue(v["is_honeypot"])
        self.assertEqual(v["sell_tax_pct"], 100.0)

    def test_rugcheck_and_rpc(self):
        self.serve({"rugcheck": fx.rugcheck(mint_authority="Auth111"),
                    "mainnet-beta": fx.solana_rpc_mint(mint_authority="Auth111")})
        ctx = FetchContext("solana", fx.SOL_MINT, self.cfg)
        v = {r.metric: r.value for r in RugCheck().run(ctx).readings}
        self.assertTrue(v["mint_authority_active"])
        self.assertEqual(v["top1_pct"], 6.0)  # pool vault excluded
        self.assertEqual(v["dev_pct"], 4.0)   # creator-owned account
        self.assertEqual(v["lp_locked_pct"], 99.5)
        rv = {r.metric: r.value for r in SolanaRPC().run(ctx).readings}
        self.assertTrue(rv["mint_authority_active"])
        self.assertFalse(rv["freeze_authority_active"])
        self.assertFalse(rv["permanent_delegate"])

    def test_token2022_permanent_delegate(self):
        self.serve({"mainnet-beta": fx.solana_rpc_mint(token_2022=True, extensions=[
            {"extension": "permanentDelegate", "state": {"delegate": "Del111"}}])})
        r = self.scan([SolanaRPC()], address=fx.SOL_MINT, chain="solana")
        self.assertIn("PERMANENT_DELEGATE", {f["code"] for f in r["red_flags"]})

    def test_full_solana_pipeline_with_adapters(self):
        sol_pairs = fx.dexscreener(("solana",), token=fx.SOL_MINT, liquidity=520_000.0)
        self.serve({"dexscreener": sol_pairs, "rugcheck": fx.rugcheck(),
                    "mainnet-beta": fx.solana_rpc_mint()})
        r = gem.scan(fx.SOL_MINT, cfg=self.cfg, conn=self.conn,
                     providers=[DexScreener(), RugCheck(), SolanaRPC()])
        self.assertEqual(r["chain"], "solana")
        self.assertEqual(r["metrics"]["liquidity_usd"]["status"], "LIVE")  # 520k vs 500k agree
        self.assertEqual(len(r["metrics"]["mint_authority_active"]["readings"]), 2)
        self.assertFalse([f for f in r["red_flags"] if f["severity"] == "CRITICAL"])

    def test_evm_rpc_bytecode(self):
        http.set_transport(lambda *a: (200, json.dumps({"jsonrpc": "2.0", "id": 1,
                                                        "result": "0x"}).encode(), {}))
        r = self.scan([EvmRPC(), StaticProvider("dex", {"liquidity_usd": 1e6})])
        self.assertIn("NO_BYTECODE", {f["code"] for f in r["red_flags"]})


# ---------------------------------------------------------------- persistence and UI
class Persistence(Base):
    def test_history_persists_and_diffs(self):
        a = self.scan(self.clean_providers())
        b = self.scan(self.clean_providers(is_honeypot=True))
        self.conn.close()
        self.conn = db.connect(self.home / "radar.db")  # a new process would see this
        scans = history.for_contract(self.conn, fx.EVM_TOKEN, "ethereum")
        self.assertEqual([s["id"] for s in scans], [b["scan_id"], a["scan_id"]])
        s = scans[0]
        for key in ("components", "red_flags", "evidence", "providers", "missing", "conflicts"):
            self.assertIsNotNone(s[key], key)
        self.assertEqual(s["analysis_mode"], b["analysis"]["mode"])
        d = history.diff(scans[1], scans[0])
        self.assertEqual(d["score"]["delta"], 20 - 100)
        self.assertIn("HONEYPOT", d["flags_added"])
        self.assertEqual(history.movement_line(d), "100 → 20 (-80)")
        text, _ = watch.show_history(self.conn, fx.EVM_TOKEN)
        self.assertIn("flags added: HONEYPOT", text)

    def test_watchlist_persists(self):
        self.scan(self.clean_providers())
        self.assertIn("Added", watch.add(self.conn, fx.EVM_TOKEN))  # chain from history
        self.conn.close()
        self.conn = db.connect(self.home / "radar.db")
        self.scan(self.clean_providers(top10_pct=40.0))
        e = watchlist.entries(self.conn)
        self.assertEqual(len(e), 1)
        self.assertEqual(e[0]["chain"], "ethereum")
        self.assertEqual(e[0]["movement"], "100 → 95 (-5)")
        self.assertTrue(any("Holders" in c for c in e[0]["changes"]))
        self.assertIn("Not added", watch.add(self.conn, "0x" + "9" * 40))  # EVM, chain unknown
        self.assertIn("Removed 1", watch.remove(self.conn, fx.EVM_TOKEN))
        self.assertEqual(watchlist.entries(self.conn), [])

    def test_recheck_shows_movement(self):
        self.scan(self.clean_providers())
        r = recheck.recheck(fx.EVM_TOKEN, conn=self.conn, cfg=self.cfg,
                            providers=self.clean_providers(sell_tax_pct=12.0))
        self.assertIn("100 →", r["movement"]["line"])

    def test_panel_data_makes_no_requests(self):
        self.scan(self.clean_providers())
        http.call_log.clear()
        http.set_transport(network_forbidden)
        data = radar.panel(self.conn)
        st = radar.status(self.conn)
        self.assertEqual(http.call_log, [])
        self.assertEqual(data["latest"]["verdict"], "GEM")
        self.assertEqual(st["scans_stored"], 1)
        # and the switch really blocks providers if anything tried
        rep = DexScreener().run(FetchContext("ethereum", fx.EVM_TOKEN, self.cfg))
        self.assertEqual(rep.status, ProviderStatus.SKIPPED)

    def test_status_lists_credential_names_not_values(self):
        os.environ["SOLANA_RPC_URL"] = "https://rpc.example.invalid/?api-key=SUPERSECRET123"
        try:
            st = json.dumps(radar.status(self.conn))
        finally:
            os.environ.pop("SOLANA_RPC_URL")
        self.assertIn("SOLANA_RPC_URL", st)
        self.assertNotIn("SUPERSECRET123", st)


class Security(Base):
    def test_redaction(self):
        self.assertNotIn("abc123", redact("GET https://x/?api-key=abc123&x=1"))
        self.assertNotIn("K" * 24, redact("https://mainnet.helius-rpc.com/v0/" + "K" * 24))

    def test_secret_url_not_persisted(self):
        os.environ["SOLANA_RPC_URL"] = "https://rpc.example.invalid/?api-key=SUPERSECRET123"
        try:
            http.set_transport(lambda *a: (500, b"", {}))
            self.cfg["http"]["retries"] = 0
            r = self.scan([SolanaRPC()], address=fx.SOL_MINT, chain="solana")
        finally:
            os.environ.pop("SOLANA_RPC_URL")
        self.assertNotIn("SUPERSECRET123", json.dumps(r, default=str))
        dump = "\n".join(self.conn.iterdump())
        self.assertNotIn("SUPERSECRET123", dump)

    def test_example_config_has_no_values(self):
        env = (ROOT / ".env.example").read_text()
        for line in env.splitlines():
            if line and not line.startswith("#"):
                self.assertTrue(line.rstrip().endswith("="), line)

    def test_non_https_refused(self):
        from gem_radar.core.errors import ProviderError
        with self.assertRaises(ProviderError):
            http.request_json("x", "http://insecure.invalid/")


if __name__ == "__main__":
    unittest.main()
