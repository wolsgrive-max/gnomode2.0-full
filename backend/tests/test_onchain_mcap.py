"""On-chain spot mcap fallback: pool reserves × supply × quote_usd."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app.replay as replay
from app.constants import (
    UNI_V4_POOL_MANAGER,
    USDG,
    V4_INITIALIZE_TOPIC,
    V4_SWAP_TOPIC,
    WETH,
)
from app.models import PoolInfo


def _clear_cache():
    replay._SPOT_MCAP_CACHE.clear()


def _v2_pool(token: str) -> PoolInfo:
    return PoolInfo(
        address="0xpool0000000000000000000000000000000000v2",
        dex="uniswap_v2",
        quote=USDG,
        quote_symbol="USDG",
        token0=token,
        token1=USDG,
    )


def _fake_rpc_v2(reserve_token: int, reserve_quote: int) -> MagicMock:
    rpc = MagicMock()

    async def _call(factory):
        return [reserve_token, reserve_quote]

    rpc._call = AsyncMock(side_effect=_call)
    rpc.v2_pair = MagicMock(return_value=MagicMock())
    return rpc


@pytest.mark.asyncio
async def test_onchain_v2_spot_mcap_from_reserves():
    _clear_cache()
    token = "0xaaa0000000000000000000000000000000000abc"
    pool = _v2_pool(token)
    # 1,000,000 tokens vs 5,000 USDG in the pool → price 0.005 USDG/token.
    # supply 1,000,000 → mcap ≈ $5,000. USDG has 6 decimals.
    reserve_token = 1_000_000 * 10**18
    reserve_quote = 5_000 * 10**6
    rpc = _fake_rpc_v2(reserve_token, reserve_quote)
    rpc.token_meta = AsyncMock(
        return_value={"decimals": 18, "total_supply_raw": 1_000_000 * 10**18}
    )

    with (
        patch("app.replay.pick_best_pool", AsyncMock(return_value=pool)),
        patch.object(replay, "_resolve_quote_usd", AsyncMock(return_value=1.0)),
    ):
        mcap = await replay.estimate_onchain_spot_mcap(token, rpc=rpc)

    assert mcap is not None
    assert 4_900 < mcap < 5_100


@pytest.mark.asyncio
async def test_onchain_v4_spot_mcap_from_state_view():
    _clear_cache()
    token = "0xbbb0000000000000000000000000000000000abc"
    pool = PoolInfo(
        address="0xmanager",
        dex="uniswap_v4",
        quote=WETH,
        quote_symbol="WETH",
        token0=token,
        token1=WETH,
        pool_id="0x" + "11" * 32,
    )
    rpc = MagicMock()
    rpc.token_meta = AsyncMock(
        return_value={"decimals": 18, "total_supply_raw": 1_000_000 * 10**18}
    )
    rpc.get_v4_slot0 = AsyncMock(return_value=(2**96, 0, 0, 3000))
    with (
        patch("app.replay.pick_best_pool", AsyncMock(return_value=pool)),
        patch.object(replay, "_resolve_quote_usd", AsyncMock(return_value=1.0)),
    ):
        mcap = await replay.estimate_onchain_spot_mcap(token, rpc=rpc)
    assert mcap == pytest.approx(1_000_000.0)
    rpc.get_v4_slot0.assert_awaited_once_with(pool.pool_id)


def test_v4_pool_is_resolved_from_buy_receipt():
    token = "0xbbb0000000000000000000000000000000000abc"
    pool_id = "0x" + "11" * 32
    address_topic = "0x" + "0" * 24 + token[2:]
    logs = [
        {
            "address": UNI_V4_POOL_MANAGER,
            "topics": [
                V4_INITIALIZE_TOPIC,
                pool_id,
                "0x" + "0" * 64,
                address_topic,
            ],
            "data": "0x",
            "blockNumber": "0x123",
        },
        {
            "address": UNI_V4_POOL_MANAGER,
            "topics": [V4_SWAP_TOPIC, pool_id, "0x" + "0" * 64],
            "data": "0x",
            "blockNumber": "0x123",
        },
    ]

    pool = replay._v4_pool_from_receipt(token, logs)

    assert pool is not None
    assert pool.dex == "uniswap_v4"
    assert pool.pool_id == pool_id
    assert pool.quote.lower() == replay.ZERO.lower()
    assert pool.token1.lower() == token.lower()


@pytest.mark.asyncio
async def test_onchain_none_when_no_pool():
    _clear_cache()
    token = "0xccc0000000000000000000000000000000000abc"
    rpc = MagicMock()
    rpc.token_meta = AsyncMock(
        return_value={"decimals": 18, "total_supply_raw": 1_000_000 * 10**18}
    )
    with patch("app.replay.pick_best_pool", AsyncMock(return_value=None)):
        mcap = await replay.estimate_onchain_spot_mcap(token, rpc=rpc)
    assert mcap is None


@pytest.mark.asyncio
async def test_onchain_none_when_supply_zero():
    _clear_cache()
    token = "0xddd0000000000000000000000000000000000abc"
    pool = _v2_pool(token)
    rpc = _fake_rpc_v2(1, 1)
    rpc.token_meta = AsyncMock(
        return_value={"decimals": 18, "total_supply_raw": 0}
    )
    with (
        patch("app.replay.pick_best_pool", AsyncMock(return_value=pool)),
        patch.object(replay, "_resolve_quote_usd", AsyncMock(return_value=1.0)),
    ):
        mcap = await replay.estimate_onchain_spot_mcap(token, rpc=rpc)
    assert mcap is None
