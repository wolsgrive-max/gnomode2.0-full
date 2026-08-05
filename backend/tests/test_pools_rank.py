"""Pool ranking: deepest book wins over USDG quote bias."""

from __future__ import annotations

import pytest

from app.constants import USDG, WETH
from app.models import PoolInfo
import app.pools as pools_mod


@pytest.mark.asyncio
async def test_pick_best_pool_prefers_deep_weth_v3_over_dust_usdg_v4(monkeypatch):
    token = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
    deep_v3 = PoolInfo(
        address="0xdB1b57704d5122058FF925C1E765c17B21D065EC",
        dex="uniswap_v3",
        quote=WETH,
        quote_symbol="WETH",
        token0=token,
        token1=WETH,
        liquidity_usd=45_000.0,
        fee=10_000,
    )
    dust_v4 = PoolInfo(
        address="0x8366A39cC670b4001A1121B8f6a443A643e40951",
        dex="uniswap_v4",
        quote=USDG,
        quote_symbol="USDG",
        token0=token,
        token1=USDG,
        liquidity_usd=400.0,
        pool_id="0xe1349f1767e7f01c1c55fdc206ee7c8689a344384adab22c6d0bc8380faeec9e",
    )

    async def fake_discover(rpc, token, *, deep=False):  # noqa: ANN001, ARG001
        return [dust_v4, deep_v3]

    monkeypatch.setattr(pools_mod, "discover_pools", fake_discover)

    best = await pools_mod.pick_best_pool(None, token)  # type: ignore[arg-type]
    assert best is not None
    assert best.dex == "uniswap_v3"
    assert best.liquidity_usd == 45_000.0
