"""Unit tests for wallet filter helpers (hold-time math / range checks)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.constants import BLOCKS_PER_SECOND
from app.models import BuyerRow
from app.wallet_metrics import (
    _blocks_for_minutes,
    _hold_minutes,
    _passes,
    compute_hold_times,
)


def test_blocks_for_minutes():
    assert _blocks_for_minutes(1) == 60 * BLOCKS_PER_SECOND
    assert _blocks_for_minutes(0.1) >= 1


def test_hold_minutes():
    # 10 blocks/sec → 600 blocks = 1 minute
    assert _hold_minutes(1000, 1000 + 600) == 1.0
    assert _hold_minutes(100, 50) == 0.0


def test_passes_range():
    assert _passes(5.0, 1.0, 10.0) is True
    assert _passes(0.5, 1.0, 10.0) is False
    assert _passes(11.0, 1.0, 10.0) is False
    assert _passes(None, 1.0, None) is False
    assert _passes(None, None, None) is True


@pytest.mark.asyncio
async def test_hold_max_stops_before_tip():
    """With only max_hold set, scan must not walk all the way to tip."""
    buyer = BuyerRow(
        wallet="0x" + "11" * 20,
        token="0x" + "22" * 20,
        first_block=1_000,
        first_tx="0xabc",
        bought_tokens=1.0,
        bought_usd=1.0,
        mcap_at_first_buy=1_000.0,
        buys_count=1,
    )
    tip = 5_000_000
    max_minutes = 1.0  # 600 blocks
    fetch_calls: list[tuple[int, int]] = []

    async def fake_fetch(rpc, *, token, from_block, to_block):
        fetch_calls.append((from_block, to_block))
        return []

    rpc = MagicMock()
    with patch("app.wallet_metrics._fetch_logs_range", new=AsyncMock(side_effect=fake_fetch)):
        with patch("app.wallet_metrics._hold_cache", {}):
            out = await compute_hold_times(
                rpc,
                token="0x" + "22" * 20,
                buyers=[buyer],
                start_block=1_000,
                end_block=tip,
                max_minutes=max_minutes,
            )

    w = buyer.wallet.lower()
    assert out[w] is not None
    assert out[w] == pytest.approx(max_minutes)
    assert fetch_calls, "expected at least one getLogs wave"
    assert max(b for _, b in fetch_calls) < tip // 2, (
        f"scan walked too far toward tip: {fetch_calls}"
    )
