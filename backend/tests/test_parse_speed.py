"""Parse throughput helpers: worker pool, TG outside sem, adaptive unique."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
from app.wallet_metrics import enrich_and_filter_buyers
from app.watch import drain_work_queue
from app.watch_qualify import unique_lookup_batch_size


def test_unique_lookup_batch_size_adaptive() -> None:
    assert unique_lookup_batch_size(pass_cap=20, n_survivors=0, max_batch=40) == 40
    # Near the cap: do not pull a full 40 when only a few slots remain.
    assert unique_lookup_batch_size(pass_cap=20, n_survivors=15, max_batch=40) == 10
    assert unique_lookup_batch_size(pass_cap=20, n_survivors=18, max_batch=40) == 8
    assert unique_lookup_batch_size(pass_cap=20, n_survivors=20, max_batch=40) == 0
    assert unique_lookup_batch_size(pass_cap=3, n_survivors=0, max_batch=2) == 2


@pytest.mark.asyncio
async def test_drain_work_queue_keeps_slots_busy() -> None:
    """A slow item must not block other workers from starting (no wave barrier)."""
    items = ["slow", "a", "b", "c"]
    started: dict[str, float] = {}
    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def handle(tok: str) -> None:
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
            started[tok] = time.monotonic()
        try:
            if tok == "slow":
                await asyncio.sleep(0.25)
            else:
                await asyncio.sleep(0.02)
        finally:
            async with lock:
                active -= 1

    t0 = time.monotonic()
    await drain_work_queue(items, concurrency=3, handle=handle, idle_poll_sec=0.01)
    elapsed = time.monotonic() - t0

    assert not items
    assert max_active == 3
    # Wave-barrier of 3 would finish slow+2 then last → ~0.25+0.25; pool ≈ 0.25.
    assert elapsed < 0.45
    assert started["a"] < started["slow"] + 0.05


@pytest.mark.asyncio
async def test_parse_sem_releases_before_deliver() -> None:
    """Next parse may start while a previous token's deliver (TG) is still running."""
    parse_sem = asyncio.Semaphore(1)
    post_lock = asyncio.Lock()
    parse_started: list[float] = []
    deliver_started: list[float] = []
    deliver_ended: list[float] = []

    async def parse_then_deliver(token: str) -> None:
        async with parse_sem:
            parse_started.append(time.monotonic())
            await asyncio.sleep(0.04)
        async with post_lock:
            deliver_started.append(time.monotonic())
            await asyncio.sleep(0.2 if token == "t1" else 0.01)
            deliver_ended.append(time.monotonic())

    await asyncio.gather(parse_then_deliver("t1"), parse_then_deliver("t2"))
    assert len(parse_started) == 2
    assert len(deliver_started) == 2
    # Second parse begins before first deliver finishes.
    assert parse_started[1] < deliver_ended[0]


@pytest.mark.asyncio
async def test_adaptive_unique_first_batch_smaller_near_need(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With pass_cap=5 all-passers, first BS batch is adaptive (not full 40)."""
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 5)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 40)

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
        for i in range(30)
    ]
    calls: list[int] = []

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        calls.append(len(wallets))
        return {w.lower(): 1 for w in wallets}

    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        mcap_threshold=30_000,
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=1,
        tokens_unique_period=TokensUniquePeriod.d30,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )

    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        kept = await enrich_and_filter_buyers(
            AsyncMock(),
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            buyers=buyers,
            req=req,
            start_block=0,
            end_block=0,
        )

    assert len(kept) == 5
    assert calls
    assert calls[0] == 10  # min(40, max(8, 5*2))
    assert sum(calls) < len(buyers)
