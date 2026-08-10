"""Unit tests for follow-up hot/warm/zero priority scheduler."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from app.followup import FollowupRunner
from app.followup_schedule import (
    ScheduleConfig,
    WalletScheduleRow,
    classify_tier,
    needs_balance_refresh,
    next_due_at,
    select_due_batch,
)
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


def _row(
    addr: str,
    *,
    status: str = "watching",
    deal_count: int = 1,
    discovered_at: float | None = None,
    last_activity_at: float | None = None,
    last_scanned_at: float | None = None,
    last_balance_check_at: float | None = None,
    wallet_balance_eth: float | None = 1.0,
) -> WalletScheduleRow:
    now = time.time()
    disc = discovered_at if discovered_at is not None else now - 7200
    act = last_activity_at if last_activity_at is not None else disc
    return WalletScheduleRow(
        address=addr.lower(),
        status=status,
        deal_count=deal_count,
        discovered_at=disc,
        last_activity_at=act,
        last_scanned_at=last_scanned_at,
        last_balance_check_at=last_balance_check_at,
        wallet_balance_eth=wallet_balance_eth,
    )


def test_classify_hot_vs_warm_vs_done():
    now = time.time()
    cfg = ScheduleConfig(hot_activity_sec=1800)
    hot = _row("0xhot", discovered_at=now - 60, last_activity_at=now - 60)
    warm = _row("0xwarm", discovered_at=now - 10_000, last_activity_at=now - 10_000)
    done = _row("0xdone", status="done", deal_count=5)
    assert classify_tier(hot, now=now, max_deals=5, cfg=cfg) == "hot"
    assert classify_tier(warm, now=now, max_deals=5, cfg=cfg) == "warm"
    assert classify_tier(done, now=now, max_deals=5, cfg=cfg) == "done"


def test_classify_zero_when_confirmed_zero_balance():
    now = time.time()
    cfg = ScheduleConfig()
    zero = _row(
        "0xzero",
        wallet_balance_eth=0.0,
        last_balance_check_at=now - 60,
        discovered_at=now - 60,
        last_activity_at=now - 60,
    )
    assert classify_tier(zero, now=now, max_deals=5, cfg=cfg) == "zero"
    # None balance is NOT zero (fail-open path later).
    unk = _row("0xunk", wallet_balance_eth=None, last_balance_check_at=None)
    assert classify_tier(unk, now=now, max_deals=5, cfg=cfg) in ("hot", "warm")


def test_select_due_prefers_hot_and_reserves_warm():
    now = time.time()
    cfg = ScheduleConfig(
        hot_revisit_sec=20,
        warm_revisit_sec=180,
        max_due_per_cycle=10,
        warm_fair_share=0.3,
        hot_activity_sec=1800,
    )
    rows: list[WalletScheduleRow] = []
    # 20 overdue hot
    for i in range(20):
        rows.append(
            _row(
                f"0x{'%040d' % i}",
                discovered_at=now - 100,
                last_activity_at=now - 100,
                last_scanned_at=now - 60,
                wallet_balance_eth=1.0,
                last_balance_check_at=now - 10,
            )
        )
    # 20 overdue warm
    for i in range(20, 40):
        rows.append(
            _row(
                f"0x{'%040d' % i}",
                discovered_at=now - 10_000,
                last_activity_at=now - 10_000,
                last_scanned_at=now - 300,
                wallet_balance_eth=1.0,
                last_balance_check_at=now - 10,
            )
        )
    due = select_due_batch(rows, now=now, max_deals=5, cfg=cfg)
    assert len(due) == 10
    hot_n = sum(1 for d in due if d.tier == "hot")
    warm_n = sum(1 for d in due if d.tier == "warm")
    assert hot_n >= 6
    assert warm_n >= 3  # ~30% fair share reserved


def test_never_scanned_is_immediately_due():
    now = time.time()
    cfg = ScheduleConfig(hot_revisit_sec=20)
    row = _row("0xnew", last_scanned_at=None, discovered_at=now - 10)
    tier, due_ts = next_due_at(row, now=now, max_deals=5, cfg=cfg)
    assert tier == "hot"
    assert due_ts == 0.0
    due = select_due_batch([row], now=now, max_deals=5, cfg=cfg)
    assert len(due) == 1


def test_warm_not_due_until_revisit_elapsed():
    now = time.time()
    cfg = ScheduleConfig(warm_revisit_sec=180, hot_activity_sec=60)
    row = _row(
        "0xwarm",
        discovered_at=now - 10_000,
        last_activity_at=now - 10_000,
        last_scanned_at=now - 30,
    )
    due = select_due_batch([row], now=now, max_deals=5, cfg=cfg)
    assert due == []
    due2 = select_due_batch([row], now=now + 200, max_deals=5, cfg=cfg)
    assert len(due2) == 1
    assert due2[0].tier == "warm"


def test_zero_balance_becomes_due_for_recheck_only():
    now = time.time()
    cfg = ScheduleConfig(zero_balance_recheck_sec=900, hot_activity_sec=1800)
    row = _row(
        "0xzero",
        discovered_at=now - 100,
        last_activity_at=now - 100,
        wallet_balance_eth=0.0,
        last_balance_check_at=now - 60,
        last_scanned_at=now - 60,
    )
    # Fresh zero → not due yet
    assert select_due_batch([row], now=now, max_deals=5, cfg=cfg) == []
    # After recheck window → due as zero tier
    due = select_due_batch([row], now=now + 950, max_deals=5, cfg=cfg)
    assert len(due) == 1
    assert due[0].tier == "zero"
    assert needs_balance_refresh(row, now=now + 950, cfg=cfg) is True


def test_needs_balance_refresh_fail_open_on_none():
    now = time.time()
    cfg = ScheduleConfig(balance_fresh_sec=600)
    unk = _row("0xunk", wallet_balance_eth=None, last_balance_check_at=None)
    assert needs_balance_refresh(unk, now=now, cfg=cfg) is True
    fresh = _row(
        "0xok",
        wallet_balance_eth=0.5,
        last_balance_check_at=now - 10,
    )
    assert needs_balance_refresh(fresh, now=now, cfg=cfg) is False


def test_store_schedule_rows_and_mark_scanned(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "fu.db"),
        config_path=str(tmp_path / "fu.json"),
    )
    b1 = BuyerRow(
        wallet="0xAAA0000000000000000000000000000000000001",
        token="0xBBB0000000000000000000000000000000000001",
        token_symbol="T1",
        bought_tokens=1.0,
        bought_usd=100.0,
        mcap_at_first_buy=8_000.0,
        wallet_balance_eth=1.5,
        buys_count=1,
        first_tx="0xtx1",
    )
    store.ingest_buyers([b1], max_deals=5, max_mcap_alert=20_000)
    rows = store.list_watching_schedule_rows()
    assert len(rows) == 1
    assert rows[0]["last_scanned_at"] is None
    assert rows[0]["wallet_balance_eth"] == 1.5

    ts = time.time()
    store.mark_scanned([b1.wallet], scanned_at=ts)
    store.update_wallet_balances({b1.wallet.lower(): 0.0}, checked_at=ts)
    rows2 = store.list_watching_schedule_rows()
    assert abs(rows2[0]["last_scanned_at"] - ts) < 0.01
    assert rows2[0]["wallet_balance_eth"] == 0.0
    assert abs(rows2[0]["last_balance_check_at"] - ts) < 0.01
    # Still watching — zero does not auto-delete
    assert store.list_watching() == [b1.wallet.lower()]


@pytest.mark.asyncio
async def test_cycle_skips_confirmed_zero_fail_open_none(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "fu.db"),
        config_path=str(tmp_path / "fu.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            interval_sec=10,
            max_deals=5,
            scan_concurrency=2,
            max_due_per_cycle=10,
            prune_enabled=False,
            logwatch_enabled=False,
        )
    )
    zero_w = "0xaaa0000000000000000000000000000000000001"
    open_w = "0xbbb0000000000000000000000000000000000002"
    for w, tok in (
        (zero_w, "0x1110000000000000000000000000000000000001"),
        (open_w, "0x2220000000000000000000000000000000000002"),
    ):
        store.ingest_buyers(
            [
                BuyerRow(
                    wallet=w,
                    token=tok,
                    token_symbol="T",
                    bought_tokens=1.0,
                    bought_usd=50.0,
                    mcap_at_first_buy=5_000.0,
                    wallet_balance_eth=None,
                    buys_count=1,
                    first_tx="0xtx",
                )
            ],
            max_deals=5,
            max_mcap_alert=20_000,
        )

    runner = FollowupRunner(store=store)
    scanned: list[str] = []

    async def fake_scan(wallet, cfg, *, rpc=None, skip_gmgn=False):
        scanned.append(wallet.lower())
        return [], "blockscout"

    async def fake_balances(_rpc, wallets):
        out: dict[str, float | None] = {}
        for w in wallets:
            wl = w.lower()
            if wl == zero_w:
                out[wl] = 0.0
            else:
                out[wl] = None  # fail-open → must still scan
        return out

    with (
        patch("app.followup.batch_wallet_balances", fake_balances),
        patch.object(runner, "_scan_wallet", side_effect=fake_scan),
        patch.object(runner, "_prune_stale_wallets", AsyncMock(return_value=0)),
        patch("app.followup.gmgn_api_configured", lambda: False, create=True),
    ):
        cfg = store.load_config()
        await runner.run_cycle(cfg)
        # Balance/legacy scan lives in maintenance (off hist watchdog).
        await runner._maintenance_pass(cfg)

    assert zero_w in [a.lower() for a in store.list_watching()]
    assert runner.status().last_skipped_zero_balance >= 1
    assert open_w in scanned
    assert zero_w not in scanned
