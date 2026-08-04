"""Unit tests for GMGN OpenAPI portfolio helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.gmgn_portfolio import _parse_buy, compare_followup_to_gmgn, unique_buy_tokens


def test_parse_buy():
    row = {
        "event_type": "buy",
        "tx_hash": "0xabc",
        "timestamp": 100,
        "cost_usd": "12.5",
        "token": {"address": "0xTok", "symbol": "TOK"},
    }
    b = _parse_buy(row)
    assert b is not None
    assert b.token == "0xtok"
    assert b.symbol == "TOK"
    assert b.cost_usd == 12.5


def test_parse_ignores_sell():
    assert _parse_buy({"event_type": "sell", "token": {"address": "0x1"}}) is None


@pytest.mark.asyncio
async def test_unique_buy_tokens_keeps_earliest():
    from app.gmgn_portfolio import ActivityFetchResult

    rows = [
        {
            "event_type": "buy",
            "timestamp": 200,
            "tx_hash": "0x2",
            "token": {"address": "0xaaa", "symbol": "A"},
        },
        {
            "event_type": "buy",
            "timestamp": 100,
            "tx_hash": "0x1",
            "token": {"address": "0xaaa", "symbol": "A"},
        },
        {
            "event_type": "buy",
            "timestamp": 150,
            "tx_hash": "0x3",
            "token": {"address": "0xbbb", "symbol": "B"},
        },
    ]
    with patch(
        "app.gmgn_portfolio.fetch_wallet_activity_result",
        new=AsyncMock(
            return_value=ActivityFetchResult(rows=rows, ok=True, rate_limited=False)
        ),
    ):
        buys = await unique_buy_tokens("0xwallet")
    assert [b.token for b in buys] == ["0xaaa", "0xbbb"]
    assert buys[0].timestamp == 100


@pytest.mark.asyncio
async def test_compare_followup_to_gmgn():
    from app.gmgn_portfolio import ActivityFetchResult

    gmgn_rows = [
        {
            "event_type": "buy",
            "timestamp": 0,
            "tx_hash": "0xold",
            "token": {"address": "0xold", "symbol": "OLD"},
        },
        {
            "event_type": "buy",
            "timestamp": 1,
            "tx_hash": "0x1",
            "token": {"address": "0xaa", "symbol": "A"},
        },
        {
            "event_type": "buy",
            "timestamp": 2,
            "tx_hash": "0x2",
            "token": {"address": "0xbb", "symbol": "B"},
        },
    ]
    deals = [
        {"token": "0xaa", "deal_index": 1, "token_symbol": "A"},
        {"token": "0xbb", "deal_index": 2, "token_symbol": "B"},
    ]
    with patch(
        "app.gmgn_portfolio.fetch_wallet_activity_result",
        new=AsyncMock(
            return_value=ActivityFetchResult(
                rows=gmgn_rows, ok=True, rate_limited=False
            )
        ),
    ):
        out = await compare_followup_to_gmgn("0xwallet", deals)
    assert out["gmgn_unique_buys"] == 3
    assert out["gmgn_post_seed_buys"] == 1
    assert out["seed_found_in_gmgn"] is True
    assert all(r["match"] for r in out["deals"])
    assert out["missing_from_db"] == []
    assert "key_pool_size" in out
    assert "using_docs_api_key" in out


def test_collect_paid_keys_pool(monkeypatch):
    from app import gmgn_portfolio as gp

    class _S:
        gmgn_api_key = "gmgn_aaa111111111111111111111111111"
        gmgn_api_key_2 = "gmgn_bbb222222222222222222222222222"
        gmgn_api_key_3 = ""
        gmgn_api_keys = (
            "gmgn_aaa111111111111111111111111111,"
            "gmgn_ccc333333333333333333333333333"
        )

    monkeypatch.setattr(gp, "settings", _S())
    # reset slot cache
    gp._slots = None
    gp._slots_sig = None
    keys = gp._collect_paid_keys()
    assert keys == [
        "gmgn_aaa111111111111111111111111111",
        "gmgn_ccc333333333333333333333333333",
        "gmgn_bbb222222222222222222222222222",
    ]
    assert gp.gmgn_api_configured() is True
    assert gp.gmgn_key_pool_size() == 3
    slots = gp._ensure_slots()
    assert len(slots) == 3


@pytest.mark.asyncio
async def test_fetch_unique_buys_reports_circuit_open(monkeypatch):
    from app import gmgn_portfolio as gp

    class _S:
        gmgn_api_key = "gmgn_aaa111111111111111111111111111"
        gmgn_api_key_2 = ""
        gmgn_api_key_3 = ""
        gmgn_api_keys = ""

    monkeypatch.setattr(gp, "settings", _S())
    gp._slots = None
    gp._slots_sig = None
    slots = gp._ensure_slots()
    slots[0].circuit_until = __import__("time").time() + 60
    result = await gp.fetch_unique_buys("0xwallet")
    assert result.buys == []
    assert result.ok is False
    assert result.rate_limited is True
    assert gp._MAX_KEY_RETRIES == 0
    assert gp._POOL_MIN_INTERVAL >= 0.35
    assert gp._CIRCUIT_COOLDOWN_SEC >= 60
