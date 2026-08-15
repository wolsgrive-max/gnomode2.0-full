"""Speedups that must not drop wallets/tokens: enrich outside parse, chunk grow-back."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
from app.wallet_metrics import (
    enrich_and_filter_buyers,
    reset_unique_enrich_semaphore_for_tests,
)


def test_transfer_scan_bounds_uses_early_buys_not_sticky_stop() -> None:
    from app.replay import _transfer_scan_bounds

    early = [
        {"blockNumber": 100},
        {"blockNumber": 150},
        {"blockNumber": 120},
    ]
    # Sticky stop would be ~100+36000; transfers only need early range.
    lo, hi = _transfer_scan_bounds(
        early, start_block=1, end_block=100_000, cushion=5
    )
    assert lo == 100
    assert hi == 155
    lo2, hi2 = _transfer_scan_bounds(
        early, start_block=130, end_block=140, cushion=5
    )
    assert lo2 == 130
    assert hi2 == 140


def test_unique_wallet_fanout_default() -> None:
    from app.watch_qualify import PARSE_UNIQUE_WALLET_FANOUT

    assert PARSE_UNIQUE_WALLET_FANOUT == 3


def test_unique_wall_scales_with_shortlist() -> None:
    from app.watch_qualify import (
        PARSE_UNIQUE_WALL_MAX_SEC,
        PARSE_UNIQUE_WALL_SEC,
        unique_wall_sec,
    )

    assert unique_wall_sec(0) == PARSE_UNIQUE_WALL_SEC
    assert unique_wall_sec(10) >= PARSE_UNIQUE_WALL_SEC
    assert unique_wall_sec(80) == PARSE_UNIQUE_WALL_MAX_SEC
    assert unique_wall_sec(63) >= 55.0


@pytest.mark.asyncio
async def test_unique_wall_fail_closed_when_max_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With max unique, wall must not TG-spam unexamined / unknown wallets."""
    from unittest.mock import AsyncMock, patch

    from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
    from app.wallet_metrics import (
        enrich_and_filter_buyers,
        reset_unique_enrich_semaphore_for_tests,
    )

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 1)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 60)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 8)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 0.05)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_PER_WALLET_SEC", 0.001)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_MAX_SEC", 0.05)
    reset_unique_enrich_semaphore_for_tests()

    calls = 0

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.08)  # exceed wall so only first batch may finish
        # Reject everyone examined (unique=2 > max=1)
        return {w.lower(): 2 for w in wallets}

    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_symbol="TST",
            bought_tokens=1000.0,
            bought_usd=50.0,
            mcap_at_first_buy=5_000.0,
            buys_count=1,
            first_tx=f"0x{i:064x}",
            wallet_balance_eth=0.5,
        )
        for i in range(24)
    ]
    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        mcap_threshold=30_000,
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=1,
        tokens_unique_period=TokensUniquePeriod.d7,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )
    meta: dict = {}
    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        with patch(
            "app.wallet_metrics._gmgn_unique_buys_in_window",
            new=AsyncMock(return_value=None),
        ):
            kept = await enrich_and_filter_buyers(
                AsyncMock(),
                token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                buyers=buyers,
                req=req,
                start_block=0,
                end_block=0,
                out_meta=meta,
            )
    # Examined rejects + unexamined tail both stay out when max is set.
    assert kept == []
    assert meta.get("unique_wall") is True
    assert meta.get("unique_partial") is True
    reset_unique_enrich_semaphore_for_tests()


@pytest.mark.asyncio
async def test_unique_wall_fail_open_min_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Min-only unique filter still fail-opens the unexamined wall tail."""
    from unittest.mock import AsyncMock, patch

    from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
    from app.wallet_metrics import (
        enrich_and_filter_buyers,
        reset_unique_enrich_semaphore_for_tests,
    )

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 1)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 60)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 8)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 0.05)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_PER_WALLET_SEC", 0.001)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_MAX_SEC", 0.05)
    reset_unique_enrich_semaphore_for_tests()

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        await asyncio.sleep(0.08)
        return {w.lower(): 1 for w in wallets}

    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_symbol="TST",
            bought_tokens=1000.0,
            bought_usd=50.0,
            mcap_at_first_buy=5_000.0,
            buys_count=1,
            first_tx=f"0x{i:064x}",
            wallet_balance_eth=0.5,
        )
        for i in range(24)
    ]
    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        mcap_threshold=30_000,
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=None,
        tokens_unique_period=TokensUniquePeriod.d7,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )
    meta: dict = {}
    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        kept = await enrich_and_filter_buyers(
            AsyncMock(),
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            buyers=buyers,
            req=req,
            start_block=0,
            end_block=0,
            out_meta=meta,
        )
    assert len(kept) >= 1
    assert meta.get("unique_wall") is True
    reset_unique_enrich_semaphore_for_tests()


@pytest.mark.asyncio
async def test_batch_tokens_fanout_caps_parallel(monkeypatch: pytest.MonkeyPatch) -> None:
    """In-flight unique wallet lookups ≤ PARSE_UNIQUE_WALLET_FANOUT."""
    from app.wallet_metrics import batch_tokens_traded_7d

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALLET_FANOUT", 2)
    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def fake_one(wallet, cutoff, enough=None, too_many=None):
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.05)
            return (1, True)
        finally:
            async with lock:
                active -= 1

    wallets = [f"0x{i:040x}" for i in range(8)]
    with patch("app.wallet_metrics._tokens_traded_7d_one", new=fake_one):
        out = await batch_tokens_traded_7d(wallets, lookback_hours=720.0)
    assert max_active <= 2
    assert sum(1 for v in out.values() if v == 1) == 8


def test_chunk_grow_back_policy() -> None:
    """Simulate shrink-then-grow policy used in V3/V4 replay loops."""
    target = 50_000
    chunk = target
    ok_streak = 0

    def on_fail() -> None:
        nonlocal chunk, ok_streak
        chunk = max(chunk // 2, 2_000)
        ok_streak = 0

    def on_ok() -> None:
        nonlocal chunk, ok_streak
        ok_streak += 1
        if chunk < target and ok_streak >= 2:
            chunk = min(target, max(chunk * 2, chunk + 2_000))
            ok_streak = 0

    on_fail()
    assert chunk == 25_000
    on_fail()
    assert chunk == 12_500
    on_ok()
    on_ok()
    assert chunk >= 25_000
    on_ok()
    on_ok()
    assert chunk == target


@pytest.mark.asyncio
async def test_parse_token_can_skip_wallet_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    """apply_wallet_filters=False leaves early buyers untouched (watch enriches later)."""
    from app.replay import parse_token

    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_symbol="TST",
            bought_tokens=1000.0,
            bought_usd=50.0,
            mcap_at_first_buy=5_000.0,
            buys_count=1,
            first_tx=f"0x{i:064x}",
        )
        for i in range(3)
    ]

    enrich_calls = {"n": 0}

    async def fake_enrich(*_a, **_k):
        enrich_calls["n"] += 1
        return buyers[:1]

    class FakePool:
        address = "0xpool"
        dex = "uniswap_v3"
        quote = "0xquote"
        quote_symbol = "WETH"
        token0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        token1 = "0xquote"
        pool_id = None

    async def fake_gather(*_a, **_k):
        return (
            None,
            {
                "symbol": "TST",
                "name": "Test",
                "decimals": 18,
                "total_supply_raw": 10**24,
            },
            FakePool(),
            2000.0,
            100,
        )

    monkeypatch.setattr("app.replay.honeypot_reason_for_token", AsyncMock(return_value=None))
    monkeypatch.setattr("app.replay.pick_best_pool", AsyncMock(return_value=FakePool()))
    monkeypatch.setattr("app.replay._eth_usd_price", AsyncMock(return_value=2000.0))
    monkeypatch.setattr("app.replay._quote_usd_price", AsyncMock(return_value=2000.0))
    monkeypatch.setattr("app.replay.estimate_start_block", AsyncMock(return_value=1))
    monkeypatch.setattr("app.replay._replay_v3", AsyncMock(return_value=list(buyers)))
    monkeypatch.setattr("app.replay._discover_launch_buyers", AsyncMock(return_value=[]))
    monkeypatch.setattr("app.replay.enrich_and_filter_buyers", fake_enrich)

    rpc = MagicMock()
    rpc.w3.to_checksum_address = lambda x: x
    rpc.token_meta = AsyncMock(
        return_value={
            "symbol": "TST",
            "name": "Test",
            "decimals": 18,
            "total_supply_raw": 10**24,
        }
    )
    rpc.block_number = AsyncMock(return_value=100)

    # Bypass the real gather bootstrap by patching asyncio.gather only for the
    # honeypot/meta/pool call — easier: patch parse internals already done.
    with patch("app.replay.asyncio.gather", new=AsyncMock(side_effect=[
        (
            None,
            {
                "symbol": "TST",
                "name": "Test",
                "decimals": 18,
                "total_supply_raw": 10**24,
            },
            FakePool(),
            2000.0,
            100,
        )
    ])):
        req = ParseRequest(
            tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            min_tokens_traded_7d=1,
            max_tokens_traded_7d=1,
            tokens_unique_period=TokensUniquePeriod.d30,
        )
        result = await parse_token(
            rpc,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            20_000,
            wallet_filters=req,
            apply_wallet_filters=False,
            exclude_honeypots=False,
        )

    assert enrich_calls["n"] == 0
    assert len(result.buyers) == 3
    assert result.stats.get("wallet_filters_pending") is True
    assert result.stats.get("buyers_before_wallet_filters") == 3


@pytest.mark.asyncio
async def test_enrich_outside_parse_sem_keeps_slots_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While unique enrich runs, parse_sem can be held by another discovery."""
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 1)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 3)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 3)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 30.0)
    reset_unique_enrich_semaphore_for_tests()

    parse_sem = asyncio.Semaphore(1)
    order: list[str] = []

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        order.append("unique_start")
        await asyncio.sleep(0.1)
        order.append("unique_end")
        return {w.lower(): 1 for w in wallets}

    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token_symbol="TST",
            bought_tokens=1000.0,
            bought_usd=50.0,
            mcap_at_first_buy=5_000.0,
            buys_count=1,
            first_tx=f"0x{i:064x}",
            wallet_balance_eth=0.5,
        )
        for i in range(5)
    ]
    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=1,
        tokens_unique_period=TokensUniquePeriod.d30,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )

    async def discovery_then_enrich() -> None:
        async with parse_sem:
            order.append("discover")
            await asyncio.sleep(0.02)
        await enrich_and_filter_buyers(
            AsyncMock(),
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            buyers=buyers,
            req=req,
            start_block=0,
            end_block=0,
        )

    async def other_discovery() -> None:
        await asyncio.sleep(0.03)  # start after first discover begins
        async with parse_sem:
            order.append("discover2")
            await asyncio.sleep(0.02)

    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        await asyncio.gather(discovery_then_enrich(), other_discovery())

    # Second discovery must start while first is still in unique (outside sem).
    assert "discover" in order and "discover2" in order
    assert order.index("discover2") < order.index("unique_end")
    reset_unique_enrich_semaphore_for_tests()
