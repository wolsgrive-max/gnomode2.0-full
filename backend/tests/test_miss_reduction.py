"""Miss-reduction: balance fail-open, soft age-out helpers, launch BS gate."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.models import BuyerRow, ParseRequest
from app.wallet_metrics import enrich_and_filter_buyers
from app.watch_qualify import (
    LAUNCH_BS_SKIP_MIN_UNISWAP_BUYERS,
    PARSE_MAX_PASSING_BUYERS,
    estimate_drain_eta_hours,
)


def test_pass_cap_raised_for_pro_era() -> None:
    assert PARSE_MAX_PASSING_BUYERS >= 100
    assert LAUNCH_BS_SKIP_MIN_UNISWAP_BUYERS >= 8


def test_age_out_requires_known_pair_age_semantics() -> None:
    """Document: soft age-out must not use first_seen-only ages.

    Unknown screen age → keep pending (watch.py uses age_by_addr only).
    """
    age_by_addr: dict[str, float | None] = {"0xknown": 30.0}
    max_age = 24.0
    dropped = []
    for addr in ("0xknown", "0xunknown"):
        age = age_by_addr.get(addr)
        if age is None:
            continue
        if float(age) > max_age:
            dropped.append(addr)
    assert dropped == ["0xknown"]


def test_estimate_drain_eta_hours() -> None:
    # 400 tokens / 6 slots * 90s ≈ 1.666…h
    assert abs(estimate_drain_eta_hours(400) - (400 / 6) * 90 / 3600) < 1e-6
    assert estimate_drain_eta_hours(0) == 0.0


@pytest.mark.asyncio
async def test_balance_filter_fail_open_on_unknown() -> None:
    token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token=token,
            token_symbol="TST",
            bought_tokens=100.0,
            bought_usd=10.0,
            mcap_at_first_buy=5_000.0,
            buys_count=1,
            first_tx=f"0x{i:064x}",
        )
        for i in range(3)
    ]

    async def fake_balances(rpc, wallets):
        del rpc
        # First unknown, others known in-range.
        out = {}
        for i, w in enumerate(wallets):
            out[w.lower()] = None if i == 0 else 0.5
        return out

    req = ParseRequest(
        tokens=[token],
        min_wallet_balance_eth=0.1,
        max_wallet_balance_eth=2.0,
        min_tokens_traded_7d=None,
        max_tokens_traded_7d=None,
    )
    with patch("app.wallet_metrics.batch_wallet_balances", new=fake_balances):
        kept = await enrich_and_filter_buyers(
            SimpleNamespace(),
            token=token,
            buyers=buyers,
            req=req,
            start_block=0,
            end_block=0,
        )
    assert len(kept) == 3
    assert kept[0].wallet_balance_eth is None
