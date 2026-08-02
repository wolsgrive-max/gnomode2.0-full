"""Tests for follow-up store and mcap alert gate."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from app.followup import should_alert_deal
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


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
    deals = await runner._scan_wallet(
        wallet.lower(), FollowupConfig(buys_only=True, scan_max_pages=3)
    )
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
    deals = await runner._scan_wallet(
        wallet.lower(), FollowupConfig(buys_only=True, scan_max_pages=1)
    )
    assert deals == []
    last_seen, _, _ = store.get_wallet_scan_meta(wallet.lower())
    assert last_seen == 1000  # must NOT jump to tip
