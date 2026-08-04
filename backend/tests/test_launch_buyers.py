"""Launch-pad first acquisitions count as early buyers (GMGN «Покупка»)."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.buy_gate import (
    method_is_creator_launch,
    method_is_launch_buy,
    method_is_non_buy,
)
from app.constants import TRANSFER_TOPIC
from app.models import BuyerRow, PoolInfo
from app.replay import _discover_launch_buyers, _merge_buyer_rows


LAUNCHER = "0x7f4bb222243d7be9a3fa6cfe224ccdbb4a1e8aed"
TOKEN = "0x64ced9204e91ecd246f523abe8dfd7d28cbc888f"
PAD = "0xcd29a7530841022a908f35f70b5040394ce57b19"
TX = "0x578a05405504041b83bbfb78751a6f89af44916290b2de7ea730b6372162d7d9"
# GUH false-positive pattern: creator ``launch`` (not launchToken).
GUH_CREATOR = "0xec52f09db1723867852bf7ddb7efd793df5e39a7"
GUH_TOKEN = "0xe10f8125dc02336f28824e62332dbaf6e90f83cc"
GUH_TX = "0xbead62e9b33db4ec67d46f80ef047c780ccb56858f31ab2a235632ecf550ee8d"
GUH_SEL = "0x75154d70"
# ~745M of 1B supply @ ~4 ETH paid → FDV well under $30k gate.
AMOUNT_raw = 744_968_953_642_960_459_909_355_950
WEI_PAID = 4_000_500_000_000_000_000
# GUH: ~600M of 1B @ ~2.05 ETH
GUH_AMOUNT_raw = 600_000_010_477_493_012_473_894_813
GUH_WEI = 2_054_527_000_000_000_000


def test_launch_token_is_buy_not_airdrop() -> None:
    assert method_is_launch_buy("launchToken") is True
    assert method_is_launch_buy("launchAndBuy") is True
    assert method_is_launch_buy("0x686399cb") is True
    assert method_is_non_buy("launchToken") is False
    assert method_is_non_buy("mint") is True


def test_creator_launch_is_not_buy() -> None:
    """Bare pad ``launch`` / creator selectors = token create, not market buy."""
    meme_v2_sel = "0xbf388406"  # MemeLaunchV2; MemeCreatorInitialBuyV2
    assert method_is_creator_launch("launch") is True
    assert method_is_creator_launch(GUH_SEL) is True
    assert method_is_creator_launch("0x75154d70deadbeef") is True
    assert method_is_creator_launch(meme_v2_sel) is True
    assert method_is_launch_buy("launch") is False
    assert method_is_launch_buy(GUH_SEL) is False
    assert method_is_launch_buy(meme_v2_sel) is False
    assert method_is_non_buy("launch") is True
    assert method_is_non_buy(GUH_SEL) is True
    assert method_is_non_buy(meme_v2_sel) is True
    # launcher / launchpad noise
    assert method_is_launch_buy("launcher") is False
    assert method_is_launch_buy("launchpad") is False


def test_merge_prefers_earlier_launch_block() -> None:
    swap = BuyerRow(
        wallet=LAUNCHER,
        token=TOKEN,
        bought_tokens=1.0,
        bought_usd=10.0,
        mcap_at_first_buy=20_000.0,
        buys_count=1,
        first_tx="0xswap",
        first_block=100,
    )
    launch = BuyerRow(
        wallet=LAUNCHER,
        token=TOKEN,
        bought_tokens=745_000_000.0,
        bought_usd=9_900.0,
        mcap_at_first_buy=13_000.0,
        buys_count=1,
        first_tx=TX,
        first_block=50,
    )
    merged = _merge_buyer_rows([swap], [launch])
    assert len(merged) == 1
    assert merged[0].first_block == 50
    assert merged[0].first_tx == TX
    assert merged[0].mcap_at_first_buy == 13_000.0
    assert merged[0].buys_count == 2


def _rpc_mock() -> AsyncMock:
    rpc = AsyncMock()
    # Empty RPC logs → Blockscout supplement path unless overridden.
    rpc.get_logs_chunked = AsyncMock(return_value=[])
    rpc.block_number = AsyncMock(return_value=26_900_000)
    return rpc


@pytest.mark.asyncio
async def test_discover_launch_token_recipient_as_early_buyer() -> None:
    transfer = {
        "method": "launchToken",
        "from": {"hash": PAD, "is_contract": True},
        "to": {"hash": LAUNCHER, "is_contract": False},
        "transaction_hash": TX,
        "block_number": 26_839_326,
        "total": {"value": str(AMOUNT_raw), "decimals": "18"},
    }

    async def fake_iter(_token: str):
        yield transfer

    pool = PoolInfo(
        address="0xpool000000000000000000000000000000000001",
        dex="uniswap_v3",
        quote="0x0bd7d308",
        quote_symbol="WETH",
        token0=TOKEN,
        token1="0x0bd7d308",
    )

    async def prog(*_a, **_k) -> None:
        return None

    with (
        patch("app.replay.iter_token_transfers", fake_iter),
        patch("app.replay.transaction_sender", new=AsyncMock(return_value=LAUNCHER)),
        patch("app.replay._tx_native_value_wei", new=AsyncMock(return_value=WEI_PAID)),
        patch("app.replay.estimate_mcap_at_tx", new=AsyncMock(return_value=None)),
    ):
        rows = await _discover_launch_buyers(
            _rpc_mock(),
            token=TOKEN,
            pool=pool,
            decimals=18,
            supply_tokens=1_000_000_000.0,
            eth_usd=2500.0,
            mcap_threshold=30_000.0,
            start_block=26_839_300,
            end_block=26_900_000,
            on_progress=prog,
        )

    assert len(rows) == 1
    assert rows[0].wallet.lower() == LAUNCHER
    assert rows[0].first_tx == TX
    assert rows[0].mcap_at_first_buy < 30_000.0
    assert rows[0].bought_tokens > 0
    # ~4 ETH * $2500 / (~0.745 supply) ≈ $13.4k FDV
    assert 5_000.0 < rows[0].mcap_at_first_buy < 25_000.0


@pytest.mark.asyncio
async def test_discover_skips_creator_launch_guh_pattern() -> None:
    """GUH: creator ``launch`` + firstBuy — not an early buyer (Walter FP)."""
    transfer = {
        "method": "launch",
        "from": {"hash": "0x8366a39cc670b4001a1121b8f6a443a643e40951", "is_contract": True},
        "to": {"hash": GUH_CREATOR, "is_contract": False},
        "transaction_hash": GUH_TX,
        "block_number": 27_017_269,
        "total": {"value": str(GUH_AMOUNT_raw), "decimals": "18"},
    }

    async def fake_iter(_token: str):
        yield transfer

    pool = PoolInfo(
        address="0x8366a39cc670b4001a1121b8f6a443a643e40951",
        dex="uniswap_v4",
        quote="0x0bd7d308",
        quote_symbol="WETH",
        token0=GUH_TOKEN,
        token1="0x0bd7d308",
    )

    async def prog(*_a, **_k) -> None:
        return None

    with (
        patch("app.replay.iter_token_transfers", fake_iter),
        patch(
            "app.replay.transaction_sender",
            new=AsyncMock(return_value=GUH_CREATOR),
        ),
        patch("app.replay._tx_native_value_wei", new=AsyncMock(return_value=GUH_WEI)),
        patch("app.replay.estimate_mcap_at_tx", new=AsyncMock(return_value=None)),
    ):
        rows = await _discover_launch_buyers(
            _rpc_mock(),
            token=GUH_TOKEN,
            pool=pool,
            decimals=18,
            supply_tokens=1_000_000_000.0,
            eth_usd=1870.0,
            mcap_threshold=20_000.0,
            start_block=27_017_200,
            end_block=27_020_000,
            on_progress=prog,
        )
    assert rows == []


@pytest.mark.asyncio
async def test_tx_is_launch_buy_rejects_creator_selector() -> None:
    from app.replay import _tx_is_launch_buy

    rpc = AsyncMock()
    rpc._call = AsyncMock(
        return_value={
            "from": GUH_CREATOR,
            "input": bytes.fromhex(GUH_SEL[2:] + "00" * 64),
        }
    )
    assert (
        await _tx_is_launch_buy(GUH_TX, expected_wallet=GUH_CREATOR, rpc=rpc)
        is False
    )


@pytest.mark.asyncio
async def test_creator_launch_tx_hashes_filters_guh() -> None:
    from app.replay import _creator_launch_tx_hashes

    rpc = AsyncMock()
    rpc._jsonrpc_batch = AsyncMock(
        return_value=[{"from": GUH_CREATOR, "input": GUH_SEL + "00" * 64}]
    )
    skip = await _creator_launch_tx_hashes(rpc, [GUH_TX])
    assert GUH_TX.lower() in skip


@pytest.mark.asyncio
async def test_discover_launch_via_rpc_transfer_log() -> None:
    """RPC Transfer log + launch selector on tx → early buyer."""
    pool_addr = "0xpool000000000000000000000000000000000001"
    pad_topic = "0x" + "0" * 24 + PAD[2:].lower()
    to_topic = "0x" + "0" * 24 + LAUNCHER[2:].lower()
    log = {
        "address": TOKEN,
        "topics": [TRANSFER_TOPIC, pad_topic, to_topic],
        "data": AMOUNT_raw.to_bytes(32, "big"),  # HexBytes-like
        "blockNumber": 26_839_326,
        "logIndex": 1,
        "transactionHash": bytes.fromhex(TX[2:]),
    }
    rpc = _rpc_mock()
    rpc.get_logs_chunked = AsyncMock(return_value=[log])

    pool = PoolInfo(
        address=pool_addr,
        dex="uniswap_v3",
        quote="0x0bd7d308",
        quote_symbol="WETH",
        token0=TOKEN,
        token1="0x0bd7d308",
    )

    async def prog(*_a, **_k) -> None:
        return None

    async def fake_iter(_token: str):
        if False:  # pragma: no cover
            yield {}

    with (
        patch("app.replay.iter_token_transfers", fake_iter),
        patch("app.replay._tx_is_launch_buy", new=AsyncMock(return_value=True)),
        patch("app.replay._tx_native_value_wei", new=AsyncMock(return_value=WEI_PAID)),
        patch("app.replay.estimate_mcap_at_tx", new=AsyncMock(return_value=None)),
    ):
        rows = await _discover_launch_buyers(
            rpc,
            token=TOKEN,
            pool=pool,
            decimals=18,
            supply_tokens=1_000_000_000.0,
            eth_usd=2500.0,
            mcap_threshold=30_000.0,
            start_block=26_839_300,
            end_block=26_900_000,
            on_progress=prog,
        )
    assert len(rows) == 1
    assert rows[0].wallet.lower() == LAUNCHER


@pytest.mark.asyncio
async def test_launch_buyer_skipped_when_mcap_above_gate() -> None:
    """Paid-ETH FDV already above early-buyer gate → not an early buyer."""
    transfer = {
        "method": "0x686399cb",
        "from": {"hash": PAD, "is_contract": True},
        "to": {"hash": LAUNCHER, "is_contract": False},
        "transaction_hash": TX,
        "block_number": 1,
        "total": {"value": str(AMOUNT_raw), "decimals": "18"},
    }

    async def fake_iter(_token: str):
        yield transfer

    pool = PoolInfo(
        address="0xpool000000000000000000000000000000000001",
        dex="uniswap_v3",
        quote="0x0bd7d308",
        quote_symbol="WETH",
        token0=TOKEN,
        token1="0x0bd7d308",
    )

    async def prog(*_a, **_k) -> None:
        return None

    # ~20 ETH * $2500 / 0.745 supply ≈ $67k FDV > $30k gate
    fat_wei = 20 * 10**18

    with (
        patch("app.replay.iter_token_transfers", fake_iter),
        patch("app.replay.transaction_sender", new=AsyncMock(return_value=LAUNCHER)),
        patch("app.replay._tx_native_value_wei", new=AsyncMock(return_value=fat_wei)),
        patch("app.replay.estimate_mcap_at_tx", new=AsyncMock(return_value=None)),
    ):
        rows = await _discover_launch_buyers(
            _rpc_mock(),
            token=TOKEN,
            pool=pool,
            decimals=18,
            supply_tokens=1_000_000_000.0,
            eth_usd=2500.0,
            mcap_threshold=30_000.0,
            start_block=1,
            end_block=100,
            on_progress=prog,
        )
    assert rows == []
