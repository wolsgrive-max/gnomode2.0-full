"""Tests for follow-up store and mcap alert gate."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.followup import should_alert_deal
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


@pytest.fixture(autouse=True)
def _disable_gmgn_for_legacy_scan_tests(monkeypatch):
    """Blockscout scan tests explicitly exercise the GMGN-empty fallback."""
    from app.gmgn_portfolio import UniqueBuysResult

    async def empty_gmgn_buys(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=[], ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", empty_gmgn_buys)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: False)


def test_should_alert_deal_low_mcap_only():
    assert should_alert_deal(2, 10_000, max_mcap_alert=15_000, alert_on_deals=[2, 3])
    assert should_alert_deal(3, 15_000, max_mcap_alert=15_000, alert_on_deals=[2, 3])
    assert not should_alert_deal(2, 50_000, max_mcap_alert=15_000, alert_on_deals=[2, 3])
    assert not should_alert_deal(1, 5_000, max_mcap_alert=15_000, alert_on_deals=[2, 3])
    assert not should_alert_deal(2, None, max_mcap_alert=15_000, alert_on_deals=[2, 3])


def test_should_alert_min_mcap_and_usd():
    assert not should_alert_deal(
        2,
        500,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
        min_mcap_alert=1_000,
    )
    assert should_alert_deal(
        2,
        2_000,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
        min_mcap_alert=1_000,
    )
    assert not should_alert_deal(
        2,
        5_000,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
        bought_usd=5,
        min_bought_usd=50,
    )
    assert should_alert_deal(
        2,
        5_000,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
        bought_usd=100,
        min_bought_usd=50,
        max_bought_usd=500,
    )


def test_ingest_and_second_deal(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    b1 = BuyerRow(
        wallet="0xAAA0000000000000000000000000000000000001",
        token="0xBBB0000000000000000000000000000000000001",
        token_symbol="T1",
        bought_tokens=1.0,
        bought_usd=100.0,
        mcap_at_first_buy=8_000.0,
        buys_count=1,
        first_tx="0xtx1",
    )
    inserted = store.ingest_buyers([b1], max_deals=3, max_mcap_alert=15_000)
    assert len(inserted) == 1
    assert inserted[0].deal_index == 1

    watching = store.list_watching()
    assert len(watching) == 1

    deal2 = store.record_deal(
        wallet=b1.wallet,
        token="0xCCC0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=12_000.0,
        max_deals=3,
    )
    assert deal2 is not None
    assert deal2.deal_index == 2
    assert should_alert_deal(
        deal2.deal_index,
        deal2.mcap_at_buy,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
    )

    # High mcap still recorded, but gate says no alert
    deal3 = store.record_deal(
        wallet=b1.wallet,
        token="0xDDD0000000000000000000000000000000000003",
        token_symbol="T3",
        mcap_at_buy=80_000.0,
        max_deals=3,
    )
    assert deal3 is not None
    assert deal3.deal_index == 3
    assert not should_alert_deal(
        deal3.deal_index,
        deal3.mcap_at_buy,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
    )

    rows = store.list_wallets()
    assert rows[0].status == "done"
    assert rows[0].deal_count == 3


def test_ingest_seeds_last_seen_block(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    b1 = BuyerRow(
        wallet="0xAAA0000000000000000000000000000000000001",
        token="0xBBB0000000000000000000000000000000000001",
        token_symbol="T1",
        bought_tokens=1.0,
        bought_usd=100.0,
        mcap_at_first_buy=8_000.0,
        buys_count=1,
        first_tx="0xtx1",
        first_block=1_234_567,
    )
    store.ingest_buyers([b1], max_deals=3, max_mcap_alert=15_000)
    block, deal_count, status = store.get_wallet_scan_meta(b1.wallet)
    assert block == 1_234_567
    assert deal_count == 1
    assert status == "watching"
    store.advance_last_seen_block(b1.wallet, 1_234_600)
    block2, _, _ = store.get_wallet_scan_meta(b1.wallet)
    assert block2 == 1_234_600
    store.advance_last_seen_block(b1.wallet, 1_234_500)  # older — no regress
    block3, _, _ = store.get_wallet_scan_meta(b1.wallet)
    assert block3 == 1_234_600


def test_list_wallets_batches_deals(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    for i in range(3):
        store.ingest_buyers(
            [
                BuyerRow(
                    wallet=f"0xaaa000000000000000000000000000000000000{i}",
                    token=f"0xbbb000000000000000000000000000000000000{i}",
                    bought_tokens=1.0,
                    bought_usd=10.0,
                    mcap_at_first_buy=5_000.0,
                    buys_count=1,
                    first_block=100 + i,
                )
            ],
            max_deals=3,
        )
    rows = store.list_wallets()
    assert len(rows) == 3
    assert all(len(r.deals) == 1 for r in rows)


def test_prune_settings_for_wallet():
    from app.followup import prune_settings_for_wallet
    from app.models import FollowupConfig, WalletAlertFilters

    cfg = FollowupConfig(
        prune_enabled=True,
        prune_min_ath_mcap=50_000,
        prune_after_hours=48,
    )
    assert prune_settings_for_wallet(cfg, None) == (True, 50_000.0, 48.0)
    assert prune_settings_for_wallet(cfg, WalletAlertFilters()) == (True, 50_000.0, 48.0)

    custom = WalletAlertFilters(
        custom=True,
        prune_enabled=False,
        prune_min_ath_mcap=100_000,
        prune_after_hours=72,
    )
    assert prune_settings_for_wallet(cfg, custom) == (False, 100_000.0, 72.0)

    partial = WalletAlertFilters(custom=True, prune_after_hours=24)
    enabled, ath, hours = prune_settings_for_wallet(cfg, partial)
    assert enabled is True
    assert ath == 50_000.0
    assert hours == 24.0


@pytest.mark.asyncio
async def test_prune_stale_wallets_deletes_low_ath(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.ingest_buyers(
        [
            BuyerRow(
                wallet="0xAAA0000000000000000000000000000000000001",
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    # Backdate discovery beyond 48h
    import sqlite3
    import time as time_mod

    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        conn.execute(
            "UPDATE wallets SET discovered_at=? WHERE address=?",
            (past, "0xaaa0000000000000000000000000000000000001"),
        )
        conn.commit()

    async def fake_peak(_token: str, *, min_needed: float = 0.0):
        return PeakMcapEstimate(peak=12_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    cfg = FollowupConfig(
        prune_enabled=True,
        prune_min_ath_mcap=50_000,
        prune_after_hours=48,
    )
    removed = await runner._prune_stale_wallets(cfg)
    assert removed == 1
    assert store.list_watching() == []


@pytest.mark.asyncio
async def test_prune_keeps_unreliable_spot_only(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig
    import sqlite3
    import time as time_mod

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.ingest_buyers(
        [
            BuyerRow(
                wallet="0xAAA0000000000000000000000000000000000001",
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        conn.execute(
            "UPDATE wallets SET discovered_at=? WHERE address=?",
            (past, "0xaaa0000000000000000000000000000000000001"),
        )
        conn.commit()

    async def fake_peak(_token: str, *, min_needed: float = 0.0):
        return PeakMcapEstimate(peak=8_000.0, reliable=False)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    removed = await runner._prune_stale_wallets(
        FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    )
    assert removed == 0
    assert len(store.list_watching()) == 1


@pytest.mark.asyncio
async def test_prune_deal2_ath_fail_deletes(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig
    import sqlite3
    import time as time_mod

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w = "0xAAA0000000000000000000000000000000000001"
    t2 = "0xCCC0000000000000000000000000000000000002"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    store.record_deal(wallet=w, token=t2, token_symbol="T2", mcap_at_buy=9_000.0, max_deals=3)
    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        conn.execute(
            "UPDATE deals SET created_at=? WHERE wallet=? AND deal_index=2",
            (past, w.lower()),
        )
        conn.commit()

    async def fake_peak(token: str, *, min_needed: float = 0.0):
        # Only T2 fails; discovery would also fail but deal_count>1 skips #1 path.
        return PeakMcapEstimate(peak=2_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    removed = await runner._prune_stale_wallets(
        FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    )
    assert removed == 1
    assert store.list_watching() == []


@pytest.mark.asyncio
async def test_prune_deal2_window_not_expired_keeps(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    store.record_deal(
        wallet=w,
        token="0xCCC0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=9_000.0,
        max_deals=3,
    )

    async def fake_peak(_token: str, *, min_needed: float = 0.0):
        return PeakMcapEstimate(peak=1_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    removed = await runner._prune_stale_wallets(
        FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    )
    assert removed == 0
    assert w.lower() in store.list_watching()


@pytest.mark.asyncio
async def test_prune_after_deal3_done_ath_fail(tmp_path, monkeypatch):
    """After 3rd deal (status=done), failed #3 ATH still deletes the wallet."""
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig
    import sqlite3
    import time as time_mod

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    store.record_deal(
        wallet=w,
        token="0xCCC0000000000000000000000000000000000002",
        mcap_at_buy=9_000.0,
        max_deals=3,
    )
    store.record_deal(
        wallet=w,
        token="0xDDD0000000000000000000000000000000000003",
        mcap_at_buy=10_000.0,
        max_deals=3,
    )
    rows = store.list_wallets()
    assert rows[0].status == "done"
    assert rows[0].deal_count == 3

    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        conn.execute(
            "UPDATE deals SET created_at=? WHERE wallet=? AND deal_index IN (2, 3)",
            (past, w.lower()),
        )
        conn.commit()

    async def fake_peak(token: str, *, min_needed: float = 0.0):
        # #2 passed, #3 failed
        if token.endswith("0002"):
            return PeakMcapEstimate(peak=80_000.0, reliable=True)
        return PeakMcapEstimate(peak=5_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    removed = await runner._prune_stale_wallets(
        FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    )
    assert removed == 1
    assert store.list_wallets() == []


@pytest.mark.asyncio
async def test_prune_marks_ath_passed_and_skips_next(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig
    import sqlite3
    import time as time_mod

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w = "0xAAA0000000000000000000000000000000000001"
    t2 = "0xCCC0000000000000000000000000000000000002"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    store.record_deal(wallet=w, token=t2, mcap_at_buy=9_000.0, max_deals=3)
    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        conn.execute(
            "UPDATE deals SET created_at=? WHERE wallet=? AND deal_index=2",
            (past, w.lower()),
        )
        conn.commit()

    calls = {"n": 0}

    async def fake_peak(_token: str, *, min_needed: float = 0.0):
        calls["n"] += 1
        return PeakMcapEstimate(peak=80_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    # Avoid index short-circuit so we count network-style calls via fake_peak path
    monkeypatch.setattr(
        "app.token_index.token_index.mcap_peaks",
        lambda _addrs=None: {},
    )
    runner = FollowupRunner(store=store)
    cfg = FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    assert await runner._prune_stale_wallets(cfg) == 0
    assert calls["n"] == 1
    row = store.list_for_ath_prune()[0]
    assert any(d["deal_index"] == 2 and d["ath_passed"] for d in row["deals"])
    # Second cycle must skip due to ath_passed (no peak fetch).
    assert await runner._prune_stale_wallets(cfg) == 0
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_prune_deal1_not_queued_when_followup_exists(tmp_path, monkeypatch):
    from app.followup import FollowupRunner, PeakMcapEstimate
    from app.followup_store import FollowupStore
    from app.models import BuyerRow, FollowupConfig
    import sqlite3
    import time as time_mod

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token="0xBBB0000000000000000000000000000000000001",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=100,
            )
        ],
        max_deals=3,
    )
    store.record_deal(
        wallet=w,
        token="0xCCC0000000000000000000000000000000000002",
        mcap_at_buy=9_000.0,
        max_deals=3,
    )
    past = time_mod.time() - 50 * 3600
    with sqlite3.connect(str(tmp_path / "followup.db")) as conn:
        # Old discovery, but #2 window NOT expired — must not prune on #1.
        conn.execute("UPDATE wallets SET discovered_at=? WHERE address=?", (past, w.lower()))
        conn.commit()

    async def fake_peak(_token: str, *, min_needed: float = 0.0):
        return PeakMcapEstimate(peak=1_000.0, reliable=True)

    monkeypatch.setattr("app.followup.estimate_token_peak_mcap", fake_peak)
    runner = FollowupRunner(store=store)
    removed = await runner._prune_stale_wallets(
        FollowupConfig(prune_enabled=True, prune_min_ath_mcap=50_000, prune_after_hours=48)
    )
    assert removed == 0
    assert w.lower() in store.list_watching()


@pytest.mark.asyncio
async def test_estimate_peak_short_circuits_gecko(monkeypatch):
    from app.followup import estimate_token_peak_mcap

    calls = {"gecko": 0}

    async def fake_quote(_token: str):
        return 60_000.0, 0.001

    async def fake_gecko(_token: str):
        calls["gecko"] += 1
        raise AssertionError("gecko should be skipped")

    monkeypatch.setattr("app.followup.estimate_token_quote", fake_quote)
    monkeypatch.setattr("app.ath_gecko.fetch_token_ath_mcap", fake_gecko)
    monkeypatch.setattr(
        "app.token_index.token_index.mcap_peaks",
        lambda _addrs=None: {},
    )
    est = await estimate_token_peak_mcap(
        "0xbbb0000000000000000000000000000000000001",
        min_needed=50_000,
    )
    assert est is not None
    assert est.peak >= 50_000
    assert calls["gecko"] == 0


def test_skip_high_mcap_on_ingest(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    b = BuyerRow(
        wallet="0xAAA0000000000000000000000000000000000001",
        token="0xBBB0000000000000000000000000000000000001",
        bought_tokens=1.0,
        bought_usd=100.0,
        mcap_at_first_buy=99_000.0,
        buys_count=1,
    )
    inserted = store.ingest_buyers([b], max_mcap_alert=15_000)
    assert inserted == []


def test_config_roundtrip(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(enabled=True, max_mcap_alert=12_000, raybot_enabled=True)
    store.save_config(cfg)
    loaded = store.load_config()
    assert loaded.enabled is True
    assert loaded.max_mcap_alert == 12_000
    assert loaded.raybot_enabled is True


def test_should_alert_requires_bought_usd_when_min_set():
    assert not should_alert_deal(
        2,
        5_000,
        max_mcap_alert=15_000,
        alert_on_deals=[2, 3],
        bought_usd=None,
        min_bought_usd=50,
    )


def test_estimate_bought_usd_from_transfer():
    from app.followup import estimate_bought_usd, _transfer_token_amount

    item = {
        "total": {"value": "1000000000000000000", "decimals": "18"},
        "token": {"decimals": "18"},
    }
    assert _transfer_token_amount(item) == pytest.approx(1.0)
    assert estimate_bought_usd(item, 2.5) == pytest.approx(2.5)
    assert estimate_bought_usd(item, None) is None


@pytest.mark.asyncio
async def test_is_buy_like_transfer_gates():
    from app.followup import _is_buy_like_transfer

    wallet = "0xaaa0000000000000000000000000000000000001"
    dex_in = {
        "to": {"hash": wallet},
        "from": {"hash": "0xdex", "is_contract": True},
        "method": "multicall",
        "transaction_hash": "0xown",
    }
    eoa_in = {
        "to": {"hash": wallet},
        "from": {"hash": "0xeoa", "is_contract": False},
        "method": "transfer",
    }
    out_tx = {
        "to": {"hash": "0xother"},
        "from": {"hash": wallet, "is_contract": False},
    }
    disperse = {
        "to": {"hash": wallet},
        "from": {"hash": "0xair", "is_contract": True},
        "method": "disperseToken",
        "transaction_hash": "0xairtx",
    }
    third_party = {
        "to": {"hash": wallet},
        "from": {"hash": "0xpool", "is_contract": True},
        "method": "multicall",
        "transaction_hash": "0xother",
    }

    async def fake_sender(tx: str) -> str | None:
        if tx.lower() == "0xown":
            return wallet
        return "0xsomebodyelse00000000000000000000000001"

    with patch("app.buy_gate.transaction_sender", new=fake_sender):
        assert await _is_buy_like_transfer(
            dex_in, wallet, buys_only=True, track_transfers=False
        )
        assert not await _is_buy_like_transfer(
            eoa_in, wallet, buys_only=True, track_transfers=False
        )
        assert not await _is_buy_like_transfer(
            eoa_in, wallet, buys_only=False, track_transfers=False
        )
        assert await _is_buy_like_transfer(
            eoa_in, wallet, buys_only=False, track_transfers=True
        )
        assert not await _is_buy_like_transfer(
            out_tx, wallet, buys_only=True, track_transfers=True
        )
        assert not await _is_buy_like_transfer(
            disperse, wallet, buys_only=True, track_transfers=False
        )
        assert not await _is_buy_like_transfer(
            third_party, wallet, buys_only=True, track_transfers=False
        )


def test_config_track_transfers_default(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = store.load_config()
    assert cfg.buys_only is True
    assert cfg.track_transfers is False
    cfg = cfg.model_copy(update={"track_transfers": True, "buys_only": False})
    store.save_config(cfg)
    loaded = store.load_config()
    assert loaded.track_transfers is True
    assert loaded.buys_only is False


def test_record_deal_stores_bought_usd(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    b1 = BuyerRow(
        wallet="0xAAA0000000000000000000000000000000000001",
        token="0xBBB0000000000000000000000000000000000001",
        token_symbol="T1",
        bought_tokens=1.0,
        bought_usd=100.0,
        mcap_at_first_buy=8_000.0,
        buys_count=1,
        wallet_balance_eth=1.5,
        tokens_traded_7d=4,
    )
    store.ingest_buyers([b1], max_deals=3, max_mcap_alert=15_000)
    deal2 = store.record_deal(
        wallet=b1.wallet,
        token="0xCCC0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=9_000.0,
        bought_usd=250.0,
        max_deals=3,
    )
    assert deal2 is not None
    assert deal2.bought_usd == 250.0
    rows = store.list_wallets(include_deals=True)
    assert rows[0].wallet_balance_eth == 1.5
    assert rows[0].tokens_traded_7d == 4
    assert any(d.bought_usd == 250.0 for d in rows[0].deals)


@pytest.mark.asyncio
async def test_scan_transfers_inbound_and_watermark_catchup(monkeypatch):
    """Inbound filter + no tip advance until catch-up past after_block."""
    from app import blockscout

    wallet = "0xaaa0000000000000000000000000000000000001"
    buy_token = "0xbbb0000000000000000000000000000000000002"
    # Newest-first inbound pages (as Blockscout returns with filter=to).
    pages = [
        {
            "items": [
                {
                    "block_number": 1100,
                    "to": {"hash": wallet},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {"address_hash": "0xtip", "symbol": "TIP"},
                    "transaction_hash": "0xtip",
                }
            ],
            "next_page_params": {"block_number": 1050, "index": 0},
        },
        {
            "items": [
                {
                    "block_number": 1050,
                    "to": {"hash": wallet},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {"address_hash": buy_token, "symbol": "NEW"},
                    "transaction_hash": "0xbuy",
                },
                {
                    "block_number": 1000,
                    "to": {"hash": wallet},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {"address_hash": "0xfirst", "symbol": "T1"},
                    "transaction_hash": "0xfirst",
                },
            ],
            "next_page_params": None,
        },
    ]
    calls: list[dict] = []

    async def fake_get(path: str, params=None, **_kw):
        calls.append(dict(params or {}))
        idx = len(calls) - 1
        assert idx < len(pages)
        return 200, pages[idx]

    monkeypatch.setattr(blockscout, "_get_json", fake_get)

    items, tip, caught = await blockscout.scan_address_token_transfers(
        wallet, max_pages=2, after_block=1000, direction="to"
    )
    assert all(c.get("filter") == "to" for c in calls)
    assert tip == 1100
    assert caught is True
    assert [i["transaction_hash"] for i in items] == ["0xtip", "0xbuy"]

    # Only 1 page budget: buy @1050 still unread → not caught up.
    calls.clear()
    items2, tip2, caught2 = await blockscout.scan_address_token_transfers(
        wallet, max_pages=1, after_block=1000, direction="to"
    )
    assert caught2 is False
    assert tip2 == 1100
    assert [i["transaction_hash"] for i in items2] == ["0xtip"]


@pytest.mark.asyncio
async def test_scan_wallet_records_buy_before_watermark_advance(tmp_path, monkeypatch):
    from app.followup import FollowupRunner
    from app.models import BuyerRow, FollowupConfig

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xBBB0000000000000000000000000000000000001",
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xtx1",
            )
        ],
        max_deals=3,
        max_mcap_alert=50_000,
    )
    store.advance_last_seen_block(wallet, 1000)

    buy_token = "0xccc0000000000000000000000000000000000002"

    async def fake_scan(addr, *, max_pages=8, after_block=0, direction="to"):
        assert direction == "to"
        assert after_block == 1000
        return (
            [
                {
                    "block_number": 1050,
                    "to": {"hash": wallet.lower()},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {"address_hash": buy_token, "symbol": "NEW"},
                    "transaction_hash": "0xbuy2",
                }
            ],
            1050,
            True,
        )

    async def fake_quote(_token: str):
        return 9_000.0, 0.01

    async def fake_hp(_token: str):
        return None

    async def fake_sender(tx: str) -> str | None:
        return wallet.lower()

    monkeypatch.setattr("app.followup.scan_address_token_transfers", fake_scan)
    monkeypatch.setattr("app.followup.estimate_token_quote", fake_quote)
    monkeypatch.setattr("app.security.honeypot_reason_for_token", fake_hp)
    monkeypatch.setattr("app.buy_gate.transaction_sender", fake_sender)

    runner = FollowupRunner(store=store)
    deals, source = await runner._scan_wallet(
        wallet.lower(), FollowupConfig(buys_only=True, scan_max_pages=3)
    )
    assert source == "blockscout"
    assert len(deals) == 1
    deal, hp = deals[0]
    assert deal.deal_index == 2
    assert deal.token == buy_token
    assert hp is None
    last_seen, _, _ = store.get_wallet_scan_meta(wallet.lower())
    assert last_seen == 1050


@pytest.mark.asyncio
async def test_scan_wallet_does_not_advance_watermark_when_not_caught_up(
    tmp_path, monkeypatch
):
    from app.followup import FollowupRunner
    from app.models import BuyerRow, FollowupConfig

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xBBB0000000000000000000000000000000000001",
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xtx1",
            )
        ],
        max_deals=3,
        max_mcap_alert=50_000,
    )
    store.advance_last_seen_block(wallet, 1000)

    async def fake_scan(addr, *, max_pages=8, after_block=0, direction="to"):
        # Tip sell noise only — buy still unread below page budget.
        return [], 1200, False

    monkeypatch.setattr("app.followup.scan_address_token_transfers", fake_scan)

    runner = FollowupRunner(store=store)
    deals, source = await runner._scan_wallet(
        wallet.lower(), FollowupConfig(buys_only=True, scan_max_pages=1)
    )
    assert source == "blockscout"
    assert deals == []
    last_seen, _, _ = store.get_wallet_scan_meta(wallet.lower())
    assert last_seen == 1000  # must NOT jump to tip


@pytest.mark.asyncio
async def test_scan_wallet_uses_only_post_seed_gmgn_unique_buys(tmp_path, monkeypatch):
    """Lifetime GMGN buys before the seed must never consume follow-up ranks."""
    from app.followup import FollowupRunner
    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    seed = "0xBBB0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=3,
    )
    ancient = GmgnBuy("0xaaa", "OLD", "", 10)
    seed_buy = GmgnBuy(seed.lower(), "SEED", "0xseed", 100)
    second = GmgnBuy("0xccc", "SECOND", "", 110, cost_usd=20.0)
    third = GmgnBuy("0xddd", "THIRD", "", 120, cost_usd=30.0)

    async def gmgn_buys(_wallet: str, **_kwargs):
        return UniqueBuysResult(
            buys=[ancient, seed_buy, second, third],
            ok=True,
            rate_limited=False,
        )

    async def fake_quote(_token: str):
        return 9_000.0, 0.01

    async def fake_hp(_token: str):
        return None

    async def no_blockscout(*_args, **_kwargs):
        raise AssertionError("GMGN post-seed sync should not use Blockscout")

    monkeypatch.setattr("app.followup.fetch_unique_buys", gmgn_buys)
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_api_configured", lambda: True
    )
    monkeypatch.setattr("app.followup.estimate_token_quote", fake_quote)
    monkeypatch.setattr("app.followup.scan_address_token_transfers", no_blockscout)
    monkeypatch.setattr("app.security.honeypot_reason_for_token", fake_hp)

    deals, source = await FollowupRunner(store=store)._scan_wallet(
        wallet, FollowupConfig(max_deals=3)
    )

    assert source == "gmgn"
    assert [(deal.token, deal.deal_index) for deal, _hp in deals] == [
        ("0xccc", 2),
        ("0xddd", 3),
    ]
    row = store.list_wallets()[0]
    assert [(deal.token, deal.deal_index) for deal in row.deals] == [
        (seed.lower(), 1),
        ("0xccc", 2),
        ("0xddd", 3),
    ]
    assert row.deal_count == 3
    assert row.status == "done"


@pytest.mark.asyncio
async def test_scan_wallet_gmgn_seed_only_skips_blockscout(tmp_path, monkeypatch):
    """Paid GMGN found the seed but no new buys → no Blockscout flood."""
    from app.followup import FollowupRunner
    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000002"
    seed = "0xBBB0000000000000000000000000000000000002"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=3,
    )

    async def gmgn_buys(_wallet: str, **_kwargs):
        return UniqueBuysResult(
            buys=[GmgnBuy(seed.lower(), "SEED", "0xseed", 100)],
            ok=True,
            rate_limited=False,
        )

    async def no_blockscout(*_args, **_kwargs):
        raise AssertionError("seed-only GMGN must not fall back to Blockscout")

    monkeypatch.setattr("app.followup.fetch_unique_buys", gmgn_buys)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.followup.scan_address_token_transfers", no_blockscout)

    deals, source = await FollowupRunner(store=store)._scan_wallet(
        wallet, FollowupConfig(max_deals=3)
    )
    assert source == "gmgn"
    assert deals == []
    row = store.list_wallets()[0]
    assert row.deal_count == 1
    assert row.status == "watching"


@pytest.mark.asyncio
async def test_scan_wallet_gmgn_429_falls_back_to_blockscout(tmp_path, monkeypatch):
    """Circuit/429 empty must not look like 'no new buys' — use Blockscout."""
    from app.followup import FollowupRunner
    from app.gmgn_portfolio import UniqueBuysResult

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000003"
    seed = "0xBBB0000000000000000000000000000000000003"
    buy_token = "0xccc0000000000000000000000000000000000003"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xseed",
            )
        ],
        max_deals=3,
    )
    store.advance_last_seen_block(wallet, 1000)

    async def rate_limited(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=[], ok=False, rate_limited=True)

    async def fake_scan(addr, *, max_pages=8, after_block=0, direction="to"):
        return (
            [
                {
                    "block_number": 1100,
                    "to": {"hash": wallet.lower()},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {"address_hash": buy_token, "symbol": "NEW"},
                    "transaction_hash": "0xbuy2",
                }
            ],
            1100,
            True,
        )

    async def fake_quote(_token: str):
        return 9_000.0, 0.01

    async def fake_hp(_token: str):
        return None

    async def fake_sender(tx: str) -> str | None:
        return wallet.lower()

    monkeypatch.setattr("app.followup.fetch_unique_buys", rate_limited)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.followup.scan_address_token_transfers", fake_scan)
    monkeypatch.setattr("app.followup.estimate_token_quote", fake_quote)
    monkeypatch.setattr("app.security.honeypot_reason_for_token", fake_hp)
    monkeypatch.setattr("app.buy_gate.transaction_sender", fake_sender)

    deals, source = await FollowupRunner(store=store)._scan_wallet(
        wallet, FollowupConfig(buys_only=True, max_deals=3, scan_max_pages=2)
    )
    assert source == "blockscout_fallback"
    assert len(deals) == 1
    assert deals[0][0].token == buy_token
    assert deals[0][0].deal_index == 2


def test_apply_gmgn_buy_order_purges_stale_blockscout_duplicate_index(tmp_path):
    """Blockscout dust at #2 must not collide with GMGN post-seed chronology."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0x989856685116f622d3ab906e7fe9d88c6e71b29d"
    seed = "0x6e69f04e1db0bec227d08352cc2de1f48f22d1e3"
    stale = "0x39040bb70e53ad07d26437e237c3b8f871fdf7a5"
    pipe = "0x2a12328001fc8bdda45405b2ecb18a8cf4dda584"
    feel = "0x5e57ad68ddd713d855ba7736bd85b6f1ff915515"
    shih = "0x5f62e48fd77ff9de6f089f5bf4f342756c7d0019"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="DREAM",
                bought_tokens=1.0,
                bought_usd=0.5,
                mcap_at_first_buy=17_810.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    # Simulate pre-GMGN Blockscout dust that stole deal #2.
    assert store.record_deal(
        wallet=wallet,
        token=stale,
        token_symbol="CASHCAT",
        mcap_at_buy=560_486.0,
        bought_usd=0.01,
        tx_hash="0xstale",
        block_number=0,
        max_deals=5,
    )
    inserted = store.apply_gmgn_buy_order(
        wallet,
        [
            {"token": pipe, "symbol": "PIPESHIF", "tx_hash": "0xpipe", "bought_usd": 1.97},
            {"token": feel, "symbol": "FEEL", "tx_hash": "0xfeell", "bought_usd": 1.98},
            {"token": shih, "symbol": "SHIH", "tx_hash": "0xshih", "bought_usd": 1.98},
        ],
        max_deals=5,
    )
    assert [(d.token_symbol, d.deal_index) for d in inserted] == [
        ("PIPESHIF", 2),
        ("FEEL", 3),
        ("SHIH", 4),
    ]
    rows = store.list_deals_for_wallet(wallet)
    assert [(r["token_symbol"], r["deal_index"]) for r in rows] == [
        ("DREAM", 1),
        ("PIPESHIF", 2),
        ("FEEL", 3),
        ("SHIH", 4),
    ]
    assert stale not in {r["token"] for r in rows}
    indices = [int(r["deal_index"]) for r in rows]
    assert len(indices) == len(set(indices))
    w = next(x for x in store.list_wallets() if x.address == wallet.lower())
    assert w.deal_count == 4
    assert w.status == "watching"


def test_record_deal_renumbers_by_block_not_insert_order(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xBBB0000000000000000000000000000000000001",
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xtx1",
            )
        ],
        max_deals=5,
        max_mcap_alert=50_000,
    )
    # Insert later token first (higher block), then earlier token (lower block).
    d_late = store.record_deal(
        wallet=wallet,
        token="0xCCC0000000000000000000000000000000000003",
        token_symbol="LATE",
        mcap_at_buy=5_000.0,
        tx_hash="0xlate",
        block_number=3000,
        max_deals=5,
    )
    d_early = store.record_deal(
        wallet=wallet,
        token="0xDDD0000000000000000000000000000000000002",
        token_symbol="EARLY",
        mcap_at_buy=4_000.0,
        tx_hash="0xearly",
        block_number=2000,
        max_deals=5,
    )
    assert d_late is not None and d_early is not None
    assert d_early.deal_index == 2
    rows = store.list_wallets()
    w = next(x for x in rows if x.address == wallet.lower())
    by_sym = {d.token_symbol: d.deal_index for d in w.deals}
    assert by_sym == {"T1": 1, "EARLY": 2, "LATE": 3}


def test_delete_airdrop_deal_renumbers(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xBBB0000000000000000000000000000000000001",
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xtx1",
            )
        ],
        max_deals=5,
        max_mcap_alert=50_000,
    )
    store.record_deal(
        wallet=wallet,
        token="0xair0000000000000000000000000000000000002",
        token_symbol="AIR",
        mcap_at_buy=99_000.0,
        block_number=1500,
        max_deals=5,
    )
    store.record_deal(
        wallet=wallet,
        token="0xreal000000000000000000000000000000000003",
        token_symbol="REAL",
        mcap_at_buy=5_000.0,
        block_number=2000,
        max_deals=5,
    )
    assert store.delete_deal(wallet, "0xair0000000000000000000000000000000000002", max_deals=5)
    w = next(x for x in store.list_wallets() if x.address == wallet.lower())
    assert [(d.token_symbol, d.deal_index) for d in sorted(w.deals, key=lambda d: d.deal_index)] == [
        ("T1", 1),
        ("REAL", 2),
    ]
    assert w.deal_count == 2
    assert w.status == "watching"


@pytest.mark.asyncio
async def test_scan_watermark_stops_at_last_recorded_when_max_deals(
    tmp_path, monkeypatch
):
    """Leftover candidates must not be skipped by jumping watermark to tip."""
    from app.followup import FollowupRunner
    from app.models import BuyerRow, FollowupConfig

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xAAA0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xBBB0000000000000000000000000000000000001",
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_block=1000,
                first_tx="0xtx1",
            )
        ],
        max_deals=2,  # only one slot left
        max_mcap_alert=50_000,
    )
    store.advance_last_seen_block(wallet, 1000)

    async def fake_scan(addr, *, max_pages=8, after_block=0, direction="to"):
        return (
            [
                {
                    "block_number": 1100,
                    "to": {"hash": wallet.lower()},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {
                        "address_hash": "0xccc0000000000000000000000000000000000002",
                        "symbol": "A",
                    },
                    "transaction_hash": "0xa",
                },
                {
                    "block_number": 1200,
                    "to": {"hash": wallet.lower()},
                    "from": {"hash": "0xdex", "is_contract": True},
                    "token": {
                        "address_hash": "0xddd0000000000000000000000000000000000003",
                        "symbol": "B",
                    },
                    "transaction_hash": "0xb",
                },
            ],
            1500,  # tip beyond both buys
            True,
        )

    async def fake_quote(_token: str):
        return 5_000.0, 0.01

    async def fake_hp(_token: str):
        return None

    async def fake_sender(tx: str) -> str | None:
        return wallet.lower()

    monkeypatch.setattr("app.followup.scan_address_token_transfers", fake_scan)
    monkeypatch.setattr("app.followup.estimate_token_quote", fake_quote)
    monkeypatch.setattr("app.security.honeypot_reason_for_token", fake_hp)
    monkeypatch.setattr("app.buy_gate.transaction_sender", fake_sender)

    runner = FollowupRunner(store=store)
    deals, source = await runner._scan_wallet(
        wallet.lower(),
        FollowupConfig(buys_only=True, max_deals=2, scan_max_pages=3),
    )
    assert source == "blockscout"
    assert len(deals) == 1
    assert deals[0][0].deal_index == 2
    last_seen, count, status = store.get_wallet_scan_meta(wallet.lower())
    assert count == 2 and status == "done"
    # Must NOT jump to tip 1500 — leftover buy @1200 would be lost forever.
    assert last_seen == 1100


def test_order_deals_for_alerts_ascending_deal_index():
    """Telegram must fire #3 before #4 even if discovery list was reversed."""
    from app.followup import order_deals_for_alerts
    from app.models import FollowupDealRow

    d4 = FollowupDealRow(
        wallet="0xaaa",
        token="0xlll",
        token_symbol="LILUNI",
        deal_index=4,
        mcap_at_buy=15_000.0,
        bought_usd=100.0,
        tx_hash="0xl",
        block_number=100,
        notified=False,
        created_at=1.0,
    )
    d3 = FollowupDealRow(
        wallet="0xaaa",
        token="0xppp",
        token_symbol="PONSI",
        deal_index=3,
        mcap_at_buy=17_000.0,
        bought_usd=68.0,
        tx_hash="0xp",
        block_number=200,
        notified=False,
        created_at=2.0,
    )
    ordered = order_deals_for_alerts([(d4, None), (d3, "hp")])
    assert [d.deal_index for d, _ in ordered] == [3, 4]
    assert ordered[0][1] == "hp"


def test_gmgn_block_keeps_earlier_buy_before_later_blockscout(tmp_path):
    """LILUNI@block N must stay before PONSI@N+1 — not get pushed by block=0."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xfef6f13e1d0647df6460202a7a2e5a787fe65b5d"
    seed = "0x7a4340740305e361d1583b67a9dbd4227d7fc3d3"
    alcor = "0xe0455d8815f627f782e698c2ffa662c0ef07994d"
    liluni = "0xf152df5fec2f074294f6b791d63ea68ae0f76805"
    ponsi = "0x72c75f85d86706d63884eb9d4b0e0945b6dd58f6"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="IGNOTUS",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=12_000.0,
                buys_count=1,
                first_block=26_334_310,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    # GMGN sync with real blocks (the bug was writing block_number=0).
    inserted = store.apply_gmgn_buy_order(
        wallet,
        [
            {
                "token": alcor,
                "symbol": "ALCOR",
                "tx_hash": "0xalcor",
                "block_number": 28_607_589,
                "mcap_at_buy": 10_000.0,
                "bought_usd": 50.0,
            },
            {
                "token": liluni,
                "symbol": "LILUNI",
                "tx_hash": "0xliluni",
                "block_number": 28_678_912,
                "mcap_at_buy": 15_529.0,
                "bought_usd": 100.0,
            },
        ],
        max_deals=5,
    )
    assert [(d.token_symbol, d.deal_index) for d in inserted] == [
        ("ALCOR", 2),
        ("LILUNI", 3),
    ]
    # Later Blockscout discovers PONSI (bought after LILUNI on-chain).
    ponsi_deal = store.record_deal(
        wallet=wallet,
        token=ponsi,
        token_symbol="PONSI",
        mcap_at_buy=17_980.0,
        bought_usd=68.0,
        tx_hash="0xponsi",
        block_number=28_693_205,
        max_deals=5,
    )
    assert ponsi_deal is not None
    assert ponsi_deal.deal_index == 4
    rows = store.list_deals_for_wallet(wallet)
    assert [(r["token_symbol"], r["deal_index"]) for r in rows] == [
        ("IGNOTUS", 1),
        ("ALCOR", 2),
        ("LILUNI", 3),
        ("PONSI", 4),
    ]


def test_zero_block_gmgn_row_gets_pushed_by_renumber(tmp_path):
    """Regression document: block=0 GMGN rows sort after real blocks → #3 after #4."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xfef6f13e1d0647df6460202a7a2e5a787fe65b5d"
    seed = "0x7a4340740305e361d1583b67a9dbd4227d7fc3d3"
    alcor = "0xe0455d8815f627f782e698c2ffa662c0ef07994d"
    liluni = "0xf152df5fec2f074294f6b791d63ea68ae0f76805"
    ponsi = "0x72c75f85d86706d63884eb9d4b0e0945b6dd58f6"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="IGNOTUS",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=12_000.0,
                buys_count=1,
                first_block=26_334_310,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    store.apply_gmgn_buy_order(
        wallet,
        [
            {
                "token": alcor,
                "symbol": "ALCOR",
                "tx_hash": "0xalcor",
                "block_number": 28_607_589,
                "mcap_at_buy": 10_000.0,
            },
            {
                "token": liluni,
                "symbol": "LILUNI",
                "tx_hash": "0xliluni",
                "block_number": 0,  # the defect
                "mcap_at_buy": 15_529.0,
            },
        ],
        max_deals=5,
    )
    ponsi_deal = store.record_deal(
        wallet=wallet,
        token=ponsi,
        token_symbol="PONSI",
        mcap_at_buy=17_980.0,
        tx_hash="0xponsi",
        block_number=28_693_205,
        max_deals=5,
    )
    assert ponsi_deal is not None
    # Without a real LILUNI block, PONSI steals a lower index — the Walter bug.
    by_sym = {
        r["token_symbol"]: r["deal_index"] for r in store.list_deals_for_wallet(wallet)
    }
    assert by_sym["PONSI"] < by_sym["LILUNI"]
