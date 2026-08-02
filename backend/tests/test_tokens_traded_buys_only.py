"""Unique-token metric: wallet-initiated DEX buys only."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from app.buy_gate import (
    is_dex_buy_transfer,
    is_wallet_initiated_buy,
    method_is_non_buy,
)
from app.wallet_metrics import _tokens_traded_7d_one


WALLET = "0xaaa0000000000000000000000000000000000001"
CUTOFF = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _tok(addr: str, symbol: str = "TOK") -> dict:
    return {"address": addr, "symbol": symbol}


def test_sync_prefilter_dex_in() -> None:
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xdex", "is_contract": True},
        "method": "multicall",
        "token": _tok("0xtok1"),
    }
    assert is_dex_buy_transfer(item, WALLET) is True


def test_sync_prefilter_ignores_eoa_gift() -> None:
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xeoa", "is_contract": False},
        "method": "transfer",
        "token": _tok("0xtok1"),
    }
    assert is_dex_buy_transfer(item, WALLET) is False


def test_sync_prefilter_ignores_disperse() -> None:
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xb29e", "is_contract": True},
        "method": "disperseToken",
        "token": _tok("0xtok_air"),
    }
    assert method_is_non_buy("disperseToken") is True
    assert is_dex_buy_transfer(item, WALLET) is False


@pytest.mark.asyncio
async def test_tokens_traded_requires_wallet_as_tx_sender() -> None:
    """Third-party multicall credit must not count; only wallet-initiated buy."""
    items = [
        {
            "timestamp": "2026-07-01T12:00:00.000000Z",
            "token": _tok("0xtok_buy"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xpool", "is_contract": True},
            "method": "execute",
            "transaction_hash": "0xtx_own",
        },
        {
            "timestamp": "2026-07-01T11:30:00.000000Z",
            "token": _tok("0xtok_other"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xpool2", "is_contract": True},
            "method": "multicall",
            "transaction_hash": "0xtx_other",
        },
        {
            "timestamp": "2026-07-01T11:00:00.000000Z",
            "token": _tok("0xtok_air"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xdisp", "is_contract": True},
            "method": "disperseToken",
            "transaction_hash": "0xtx_air",
        },
    ]
    resp = SimpleNamespace(
        status_code=200, json=lambda: {"items": items, "next_page_params": None}
    )

    async def fake_sender(tx: str) -> str | None:
        tx = tx.lower()
        if tx == "0xtx_own":
            return WALLET
        if tx == "0xtx_other":
            return "0xsomebodyelse00000000000000000000000001"
        return "0xairdropper000000000000000000000000001"

    with (
        patch("app.wallet_metrics._bs_get", new=AsyncMock(return_value=resp)),
        patch("app.buy_gate.transaction_sender", new=fake_sender),
    ):
        result = await _tokens_traded_7d_one(WALLET, CUTOFF)
        assert result == (1, True)


@pytest.mark.asyncio
async def test_tokens_traded_two_own_buys() -> None:
    items = [
        {
            "timestamp": "2026-07-01T12:00:00.000000Z",
            "token": _tok("0xtok_a"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xpool", "is_contract": True},
            "method": "multicall",
            "transaction_hash": "0xta",
        },
        {
            "timestamp": "2026-07-01T11:00:00.000000Z",
            "token": _tok("0xtok_b"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xpool2", "is_contract": True},
            "method": "execute",
            "transaction_hash": "0xtb",
        },
    ]
    resp = SimpleNamespace(
        status_code=200, json=lambda: {"items": items, "next_page_params": None}
    )

    async def fake_sender(tx: str) -> str | None:
        return WALLET

    with (
        patch("app.wallet_metrics._bs_get", new=AsyncMock(return_value=resp)),
        patch("app.buy_gate.transaction_sender", new=fake_sender),
    ):
        result = await _tokens_traded_7d_one(WALLET, CUTOFF, too_many=1)
        assert result == (2, False)


@pytest.mark.asyncio
async def test_smart_wallet_counts_when_quote_spent() -> None:
    """Tx submitted by another address, but wallet spent WETH in same tx."""
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xpool", "is_contract": True, "name": "Uniswap V3 Pool"},
        "method": "multicall",
        "transaction_hash": "0xtx_sw",
        "token": _tok("0xtok1"),
    }

    async def fake_sender(tx: str) -> str | None:
        return "0xrouter00000000000000000000000000000001"

    async def fake_quote(wallet: str, tx: str) -> bool | None:
        return True

    with (
        patch("app.buy_gate.transaction_sender", new=fake_sender),
        patch("app.buy_gate.wallet_sent_quote_in_tx", new=fake_quote),
    ):
        assert await is_wallet_initiated_buy(item, WALLET) is True


@pytest.mark.asyncio
async def test_sender_unknown_accepts_pool_swap() -> None:
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xpool", "is_contract": True, "name": "Uniswap V2: PAIR"},
        "method": "swap",
        "transaction_hash": "0xtx_miss",
        "token": _tok("0xtok1"),
    }

    async def fake_sender(tx: str) -> str | None:
        return None

    with patch("app.buy_gate.transaction_sender", new=fake_sender):
        assert await is_wallet_initiated_buy(item, WALLET) is True


def test_airdrop_method_blacklist_broader() -> None:
    assert method_is_non_buy("batchAirdrop") is True
    assert method_is_non_buy("claimRewards") is True
    assert method_is_non_buy("distributeToken") is True
    assert method_is_non_buy("exactInputSingle") is False
