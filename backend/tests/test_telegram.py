"""Tests for Telegram message formatting and send helper."""

from __future__ import annotations

import asyncio

import pytest

from app.models import BuyerRow
from app.telegram import (
    chunk_buyers,
    format_buyer_block,
    resolve_topic_id,
    send_buyers,
    send_message,
)


@pytest.fixture(autouse=True)
def _isolate_telegram_env(monkeypatch):
    """Do not load the real project .env during unit tests."""
    monkeypatch.setattr("app.telegram._reload_env_files", lambda: None)
    monkeypatch.setattr("app.telegram._env_mtime", None)
    for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_TOPIC_ID"):
        monkeypatch.delenv(key, raising=False)


def test_resolve_topic_id(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "telegram_topic_id", "42")
    assert resolve_topic_id() == 42
    # Empty override falls back to settings/env (same as chat id).
    assert resolve_topic_id("") == 42
    assert resolve_topic_id("0") is None
    assert resolve_topic_id("99") == 99

    monkeypatch.setattr(settings, "telegram_topic_id", "")
    assert resolve_topic_id("") is None
    assert resolve_topic_id(None) is None


def _buyer(i: int = 0) -> BuyerRow:
    return BuyerRow(
        wallet=f"0x{'a' * 40}",
        token=f"0x{'b' * 38}{i:02d}",
        token_symbol="TST",
        bought_tokens=1000.0,
        bought_usd=12.5,
        mcap_at_first_buy=8000.0,
        buys_count=1,
        wallet_balance_eth=0.5,
        hold_time_minutes=90.0,
        tokens_traded_7d=3,
    )


def test_format_buyer_block_contains_links():
    text = format_buyer_block(_buyer())
    assert "TST" in text
    assert "gmgn.ai/robinhood/token/" in text
    assert "gmgn.ai/robinhood/address/" in text
    assert "12.5" in text or "12.50" in text


def test_format_followup_deal_honeypot_banner():
    from app.telegram import format_followup_deal

    plain = format_followup_deal(
        wallet="0xabc",
        token="0xdef",
        token_symbol="SAFE",
        deal_index=2,
        mcap_at_buy=5000,
    )
    assert "HONEYPOT" not in plain
    assert "сделка #2" in plain

    hp = format_followup_deal(
        wallet="0xabc",
        token="0xdef",
        token_symbol="TRAP",
        deal_index=3,
        mcap_at_buy=8000,
        bought_usd=100,
        honeypot_reason="gmgn:honeypot",
    )
    assert hp.index("HONEYPOT") < hp.index("сделка #3")
    assert "🔴" in hp
    assert "gmgn:honeypot" in hp
    assert hp.count("𝗛𝗢𝗡𝗘𝗬𝗣𝗢𝗧") >= 3


def test_chunk_buyers_splits_large_batches():
    buyers = [
        BuyerRow(
            wallet=f"0x{i:040x}",
            token=f"0x{(i + 1):040x}",
            token_symbol="X" * 20,
            bought_tokens=1,
            bought_usd=1,
            mcap_at_first_buy=1,
            buys_count=1,
        )
        for i in range(80)
    ]
    chunks = chunk_buyers(buyers)
    assert len(chunks) >= 2
    assert sum(len(c[0]) for c in chunks) == len(buyers)
    for _chunk, text in chunks:
        assert len(text) <= 3500


def test_send_message_posts_telegram(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "telegram_bot_token", "bot-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")

    calls: list[dict] = []

    class FakeResp:
        status_code = 200

        def json(self):
            return {"ok": True}

        text = ""

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            calls.append({"url": url, "json": json})
            return FakeResp()

    monkeypatch.setattr("app.telegram.httpx.AsyncClient", FakeClient)
    asyncio.run(send_message("12345", "<b>hi</b>", topic_id=7))
    assert calls
    assert "bot-token" in calls[0]["url"]
    assert calls[0]["json"]["chat_id"] == "12345"
    assert calls[0]["json"]["parse_mode"] == "HTML"
    assert calls[0]["json"]["message_thread_id"] == 7


def test_send_buyers_returns_sent(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "telegram_bot_token", "bot-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")

    class FakeResp:
        status_code = 200

        def json(self):
            return {"ok": True}

        text = ""

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json=None):
            return FakeResp()

    monkeypatch.setattr("app.telegram.httpx.AsyncClient", FakeClient)
    buyers = [_buyer(0), _buyer(1)]
    msgs, sent = asyncio.run(send_buyers("99", buyers))
    assert msgs >= 1
    assert len(sent) == 2
