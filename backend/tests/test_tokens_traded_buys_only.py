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
    assert method_is_non_buy("claimDividend") is True
    assert method_is_non_buy("claimDividends") is True
    assert method_is_non_buy("exactInputSingle") is False
    assert method_is_non_buy("launchToken") is False
    assert method_is_non_buy("0xa9059cbb000000000000000000000000dead") is True
    assert method_is_non_buy("0x23b872dd000000000000000000000000dead") is True


def test_claim_dividend_not_dex_buy() -> None:
    """Reflection claim credits must not inflate unique-token count."""
    item = {
        "to": {"hash": WALLET},
        "from": {"hash": "0xdiv", "is_contract": True},
        "method": "claimDividend",
        "token": _tok("0xgme"),
    }
    assert is_dex_buy_transfer(item, WALLET) is False


@pytest.mark.asyncio
async def test_launch_token_counts_toward_unique() -> None:
    """Pons launchToken (GMGN «Покупка») is a wallet-initiated buy for unique."""
    from app.buy_gate import method_is_launch_buy

    assert method_is_launch_buy("launchToken") is True
    items = [
        {
            "timestamp": "2026-08-03T14:37:07.000000Z",
            "token": _tok("0x64ced9204e91ecd246f523abe8dfd7d28cbc888f", "PCC"),
            "to": {"hash": WALLET},
            "from": {"hash": "0xcd29a753", "is_contract": True},
            "method": "launchToken",
            "transaction_hash": "0xtx_launch",
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
        assert result == (1, True)


@pytest.mark.asyncio
async def test_unique_fail_soft_bs_zero_gmgn_buy() -> None:
    """Blockscout exact 0 + GMGN in-window buy → pass min=1 (not fail)."""
    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult
    from app.models import BuyerRow, ParseRequest
    from app.wallet_metrics import enrich_and_filter_buyers

    token = "0xtoken0000000000000000000000000000000001"
    buyer = BuyerRow(
        wallet=WALLET,
        token=token,
        bought_tokens=1.0,
        bought_usd=10.0,
        mcap_at_first_buy=50_000.0,
        buys_count=1,
    )

    async def fake_batch(wallets, **kwargs):
        del kwargs
        return {w.lower(): 0 for w in wallets}

    async def fake_gmgn(wallet, **kwargs):
        del kwargs
        return UniqueBuysResult(
            buys=[
                GmgnBuy(
                    token=token,
                    symbol="FROG",
                    tx_hash="0xgmgn",
                    timestamp=int(datetime.now(timezone.utc).timestamp()),
                )
            ],
            ok=True,
            rate_limited=False,
        )

    with (
        patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch),
        patch("app.gmgn_portfolio.fetch_unique_buys", new=fake_gmgn),
    ):
        kept = await enrich_and_filter_buyers(
            SimpleNamespace(),  # type: ignore[arg-type]
            token=token,
            buyers=[buyer],
            req=ParseRequest(
                tokens=[token],
                min_tokens_traded_7d=1.0,
                max_tokens_traded_7d=1.0,
            ),
            start_block=1,
            end_block=2,
        )
    assert len(kept) == 1
    assert kept[0].tokens_traded_7d == 1


@pytest.mark.asyncio
async def test_unique_fail_soft_does_not_rescue_over_max() -> None:
    """BS=0 must not soft-pass when GMGN unique exceeds max."""
    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult
    from app.models import BuyerRow, ParseRequest
    from app.wallet_metrics import enrich_and_filter_buyers

    token = "0xtoken0000000000000000000000000000000001"
    other = "0xtoken0000000000000000000000000000000002"
    buyer = BuyerRow(
        wallet=WALLET,
        token=token,
        bought_tokens=1.0,
        bought_usd=10.0,
        mcap_at_first_buy=50_000.0,
        buys_count=1,
    )
    now_ts = int(datetime.now(timezone.utc).timestamp())

    async def fake_batch(wallets, **kwargs):
        del kwargs
        return {w.lower(): 0 for w in wallets}

    async def fake_gmgn(wallet, **kwargs):
        del kwargs
        return UniqueBuysResult(
            buys=[
                GmgnBuy(token=token, symbol="A", tx_hash="0x1", timestamp=now_ts),
                GmgnBuy(token=other, symbol="B", tx_hash="0x2", timestamp=now_ts),
            ],
            ok=True,
            rate_limited=False,
        )

    with (
        patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch),
        patch("app.gmgn_portfolio.fetch_unique_buys", new=fake_gmgn),
    ):
        kept = await enrich_and_filter_buyers(
            SimpleNamespace(),  # type: ignore[arg-type]
            token=token,
            buyers=[buyer],
            req=ParseRequest(
                tokens=[token],
                min_tokens_traded_7d=1.0,
                max_tokens_traded_7d=1.0,
            ),
            start_block=1,
            end_block=2,
        )
    assert kept == []


@pytest.mark.asyncio
async def test_cheap_reject_multi_trader_few_sender_lookups() -> None:
    """Multi-trader (>too_many) should stop after ~too_many+1 full buy-gates."""
    items = []
    for i in range(12):
        items.append(
            {
                "timestamp": f"2026-07-01T{12 - i // 60:02d}:{i % 60:02d}:00.000000Z",
                "token": _tok(f"0xtok{i:040x}"),
                "to": {"hash": WALLET},
                "from": {"hash": f"0xpool{i}", "is_contract": True},
                "method": "multicall",
                "transaction_hash": f"0xtx{i:02d}",
            }
        )
    resp = SimpleNamespace(
        status_code=200, json=lambda: {"items": items, "next_page_params": None}
    )
    sender_calls = {"n": 0}

    async def fake_sender(tx: str) -> str | None:
        sender_calls["n"] += 1
        return WALLET

    with (
        patch("app.wallet_metrics._bs_get", new=AsyncMock(return_value=resp)),
        patch("app.buy_gate.transaction_sender", new=fake_sender),
    ):
        result = await _tokens_traded_7d_one(WALLET, CUTOFF, too_many=3)
    assert result == (4, False)
    assert sender_calls["n"] <= 4


@pytest.mark.asyncio
async def test_cheap_reject_unique_one_skips_airdrop() -> None:
    """Airdrop must not inflate unique=1; only wallet-initiated buy counts."""
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
    sender_calls: list[str] = []

    async def fake_sender(tx: str) -> str | None:
        sender_calls.append(tx.lower())
        return WALLET

    with (
        patch("app.wallet_metrics._bs_get", new=AsyncMock(return_value=resp)),
        patch("app.buy_gate.transaction_sender", new=fake_sender),
    ):
        result = await _tokens_traded_7d_one(WALLET, CUTOFF, too_many=3)
    assert result == (1, True)
    assert sender_calls == ["0xtx_own"]
