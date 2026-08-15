"""Unique enrich early-stop (pass-cap) for watch parse speed."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.models import BuyerRow, ParseRequest, TokensUniquePeriod
from app.wallet_metrics import enrich_and_filter_buyers
from app.watch_qualify import PARSE_MAX_PASSING_BUYERS, PARSE_UNIQUE_BATCH


def _buyer(i: int) -> BuyerRow:
    # Start at 1 — the zero address is not a valid EOA buyer fixture.
    n = i + 1
    return BuyerRow(
        wallet=f"0x{n:040x}",
        token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        token_symbol="TST",
        bought_tokens=1000.0,
        bought_usd=50.0,
        mcap_at_first_buy=5_000.0,
        buys_count=1,
        first_tx=f"0x{n:064x}",
        wallet_balance_eth=0.5,
    )


@pytest.mark.asyncio
async def test_unique_enrich_stops_at_pass_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """After PARSE_MAX_PASSING_BUYERS pass unique, remaining wallets are skipped."""
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 3)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 2)

    buyers = [_buyer(i) for i in range(10)]
    # All pass unique=1
    calls: list[list[str]] = []

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        calls.append(list(wallets))
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
    rpc = AsyncMock()

    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        kept = await enrich_and_filter_buyers(
            rpc,
            token="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            buyers=buyers,
            req=req,
            start_block=0,
            end_block=0,
        )

    assert len(kept) == 3
    # First batch of 2 + second batch starts; stop once 3 pass — at most 2 batches
    # of size 2 (4 wallets fetched) when cap=3.
    assert len(calls) <= 2
    fetched = sum(len(c) for c in calls)
    assert fetched < len(buyers)
    assert PARSE_MAX_PASSING_BUYERS >= 1
    assert PARSE_UNIQUE_BATCH >= 1


@pytest.mark.asyncio
async def test_unique_enrich_no_cap_when_few_passers(monkeypatch: pytest.MonkeyPatch) -> None:
    """If fewer than the cap pass, all wallets are examined."""
    monkeypatch.setattr("app.watch_qualify.PARSE_MAX_PASSING_BUYERS", 20)
    monkeypatch.setattr("app.watch_qualify.PARSE_UNIQUE_BATCH", 5)

    buyers = [_buyer(i) for i in range(8)]
    pass_set = {buyers[0].wallet.lower(), buyers[1].wallet.lower()}

    async def fake_batch(wallets, on_progress=None, enough=None, too_many=None, lookback_hours=168.0):
        return {w.lower(): (1 if w.lower() in pass_set else 5) for w in wallets}

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

    assert len(kept) == 2
    assert {k.wallet.lower() for k in kept} == pass_set
