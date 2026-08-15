"""Express/bulk dual-lane drain + classify helpers."""

from __future__ import annotations

import asyncio
import time

import pytest

from app.watch import drain_express_bulk_queues
from app.watch_qualify import (
    PARSE_EXPRESS_WORKERS,
    PARSE_MAX_PASSING_BUYERS,
    PARSE_UNIQUE_WALL_SEC,
    is_express_candidate,
    split_express_bulk,
)


def test_pass_cap_and_wall_defaults() -> None:
    from app.watch_qualify import PARSE_UNIQUE_CONCURRENCY

    assert PARSE_MAX_PASSING_BUYERS >= 100
    assert PARSE_UNIQUE_WALL_SEC >= 55.0
    assert PARSE_EXPRESS_WORKERS == 4
    assert PARSE_UNIQUE_CONCURRENCY == 3


def test_express_fresh_vs_bulk_mid_age() -> None:
    now = time.time()
    hold = {
        "0xfresh": {"queued_at": now, "first_seen": now, "ath_mcap": 50_000.0},
        "0xbulk": {
            "queued_at": now - 10 * 3600,
            "first_seen": now - 10 * 3600,
            "ath_mcap": 200_000.0,
        },
        "0xurgent": {
            "queued_at": now - 22 * 3600,
            "first_seen": now - 22 * 3600,
            "ath_mcap": 80_000.0,
        },
    }
    ages = {
        "0xfresh": 2.0,
        "0xbulk": 10.0,
        "0xurgent": 22.5,  # remaining 1.5h ≤ 3h → express
    }
    assert is_express_candidate(
        "0xfresh",
        hold=hold,
        pair_age_hours=ages,
        max_pair_age_hours=24.0,
        now=now,
    )
    assert not is_express_candidate(
        "0xbulk",
        hold=hold,
        pair_age_hours=ages,
        max_pair_age_hours=24.0,
        now=now,
    )
    assert is_express_candidate(
        "0xurgent",
        hold=hold,
        pair_age_hours=ages,
        max_pair_age_hours=24.0,
        now=now,
    )
    ex, bu = split_express_bulk(
        ["0xbulk", "0xfresh", "0xurgent"],
        hold=hold,
        pair_age_hours=ages,
        max_pair_age_hours=24.0,
        now=now,
    )
    assert ex == ["0xfresh", "0xurgent"]
    assert bu == ["0xbulk"]


def test_unknown_age_recent_queued_not_express() -> None:
    """Mass-stamped first_seen/queued must not flood the express lane."""
    now = time.time()
    hold = {
        "0xunknown": {
            "queued_at": now,
            "first_seen": now,
            "ath_mcap": 177_000.0,
        },
    }
    assert not is_express_candidate(
        "0xunknown",
        hold=hold,
        pair_age_hours={},
        max_pair_age_hours=24.0,
        now=now,
    )
    ex, bu = split_express_bulk(
        ["0xunknown"],
        hold=hold,
        pair_age_hours={},
        max_pair_age_hours=24.0,
        now=now,
    )
    assert ex == []
    assert bu == ["0xunknown"]


def test_express_requires_screen_age_not_approx_dict() -> None:
    """Ages invented from first_seen must not be treated as screen ages."""
    now = time.time()
    hold = {"0xfake": {"queued_at": now, "first_seen": now, "ath_mcap": 90_000.0}}
    # If watch wrongly stuffed approx into pair_age_hours, this would be express.
    # Callers must pass screen-only ages; empty → bulk.
    assert not is_express_candidate(
        "0xfake",
        hold=hold,
        pair_age_hours={},
        max_pair_age_hours=24.0,
        now=now,
    )
    assert is_express_candidate(
        "0xfake",
        hold=hold,
        pair_age_hours={"0xfake": 2.0},
        max_pair_age_hours=24.0,
        now=now,
    )


def test_parse_queue_unknown_age_not_fresh_band() -> None:
    from app.watch_qualify import parse_queue_sort_key

    now = 1_000.0
    hold = {"0xunk": {"queued_at": now, "first_seen": now, "ath_mcap": 90_000.0}}
    k = parse_queue_sort_key(
        "0xunk",
        hold=hold,
        pair_age_hours={},
        ath_mcap={"0xunk": 90_000.0},
        max_pair_age_hours=24.0,
        now=now,
    )
    assert k[0] == 1  # not urgent
    assert k[1] == 1  # not fresh

@pytest.mark.asyncio
async def test_drain_express_workers_prefer_express_lane() -> None:
    """Reserved express workers claim fresh before bulk when both nonempty."""
    express = ["e1", "e2", "e3"]
    bulk = ["b1", "b2", "b3", "b4"]
    order: list[str] = []
    lock = asyncio.Lock()

    async def handle(tok: str) -> None:
        async with lock:
            order.append(tok)
        await asyncio.sleep(0.02)

    await drain_express_bulk_queues(
        express=express,
        bulk=bulk,
        concurrency=4,
        express_workers=3,
        handle=handle,
        idle_poll_sec=0.005,
    )
    assert not express and not bulk
    # First four starts: three prefer_express workers claim e* first.
    first4 = order[:4]
    assert sum(1 for t in first4 if t.startswith("e")) >= 3


@pytest.mark.asyncio
async def test_unique_enrich_concurrency_caps_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At most PARSE_UNIQUE_CONCURRENCY tokens run unique BS enrich at once."""
    from unittest.mock import AsyncMock, patch

    from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
    from app.wallet_metrics import (
        enrich_and_filter_buyers,
        reset_unique_enrich_semaphore_for_tests,
    )

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 2)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 5)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 5)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 30.0)
    reset_unique_enrich_semaphore_for_tests()

    active = 0
    max_active = 0
    lock = asyncio.Lock()

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        nonlocal active, max_active
        async with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.08)
            return {w.lower(): 1 for w in wallets}
        finally:
            async with lock:
                active -= 1

    def buyers_for(token: str) -> list[BuyerRow]:
        return [
            BuyerRow(
                wallet=f"0x{i:040x}",
                token=token,
                token_symbol="TST",
                bought_tokens=1000.0,
                bought_usd=50.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx=f"0x{i:064x}",
                wallet_balance_eth=0.5,
            )
            for i in range(8)
        ]

    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        mcap_threshold=30_000,
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=1,
        tokens_unique_period=TokensUniquePeriod.d30,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )

    async def one(tok: str) -> None:
        await enrich_and_filter_buyers(
            AsyncMock(),
            token=tok,
            buyers=buyers_for(tok),
            req=req,
            start_block=0,
            end_block=0,
        )

    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        await asyncio.gather(
            one("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            one("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            one("0xcccccccccccccccccccccccccccccccccccccccc"),
            one("0xdddddddddddddddddddddddddddddddddddddddd"),
        )

    assert max_active <= 2
    reset_unique_enrich_semaphore_for_tests()


@pytest.mark.asyncio
async def test_unique_wall_does_not_burn_while_waiting_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue wait for unique slot must not consume the per-token wall budget."""
    from unittest.mock import AsyncMock, patch

    from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
    from app.wallet_metrics import (
        enrich_and_filter_buyers,
        reset_unique_enrich_semaphore_for_tests,
    )

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 1)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 20)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 5)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 0.25)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_PER_WALLET_SEC", 0.01)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_MAX_SEC", 0.25)
    reset_unique_enrich_semaphore_for_tests()

    gate = asyncio.Event()
    calls_second = 0

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        nonlocal calls_second
        # First holder blocks unique until gate opens.
        if not gate.is_set():
            await asyncio.sleep(0.2)
            gate.set()
            return {w.lower(): 1 for w in wallets}
        calls_second += 1
        await asyncio.sleep(0.02)
        return {w.lower(): 1 for w in wallets}

    def mk_buyers(n: int = 15) -> list[BuyerRow]:
        return [
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
            for i in range(n)
        ]

    req = ParseRequest(
        tokens=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        mcap_threshold=30_000,
        min_tokens_traded_7d=1,
        max_tokens_traded_7d=1,
        tokens_unique_period=TokensUniquePeriod.d30,
        min_wallet_balance_eth=None,
        max_wallet_balance_eth=None,
    )

    async def run(tok: str) -> list:
        return await enrich_and_filter_buyers(
            AsyncMock(),
            token=tok,
            buyers=mk_buyers(),
            req=req,
            start_block=0,
            end_block=0,
        )

    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        kept1, kept2 = await asyncio.gather(
            run("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            run("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )

    # Second token waited ~0.2s for the slot; wall is 0.25s after acquire —
    # it must still examine wallets (not instantly wall_stop with 0 kept).
    assert len(kept2) >= 1
    assert calls_second >= 1
    reset_unique_enrich_semaphore_for_tests()


@pytest.mark.asyncio
async def test_unique_wall_stops_new_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wall stops starting new BS batches; already-fetched batch is still decided."""
    from unittest.mock import AsyncMock, patch

    from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
    from app.wallet_metrics import (
        enrich_and_filter_buyers,
        reset_unique_enrich_semaphore_for_tests,
    )

    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_CONCURRENCY", 1)
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 50)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 5)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_SEC", 0.05)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_PER_WALLET_SEC", 0.001)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_WALL_MAX_SEC", 0.05)
    reset_unique_enrich_semaphore_for_tests()

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
        for i in range(40)
    ]
    calls = 0

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.04)
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
    # Few new batches under tiny wall; max set → fail-closed on unexamined.
    assert calls <= 3
    assert isinstance(kept, list)
    reset_unique_enrich_semaphore_for_tests()
