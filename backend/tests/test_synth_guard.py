"""Unit tests for synthetic address / pytest Telegram guards."""

from __future__ import annotations

import pytest

from app.synth_guard import is_synthetic_evm_address, refuse_telegram_if_unsafe


def test_fixture_addresses_are_synthetic():
    assert is_synthetic_evm_address("0xaaa0000000000000000000000000000000000001")
    assert is_synthetic_evm_address("0xbbb00000000000000000000000000000000000bb")
    assert is_synthetic_evm_address("0x" + ("0" * 40))


def test_realish_addresses_pass():
    # Permit2-style short leading zeros must not trip the guard.
    assert not is_synthetic_evm_address(
        "0x000000000022D473030F116dDEE9F6B43aC78BA3"
    )
    assert not is_synthetic_evm_address(
        "0x40d1cd34a1b2c3d4e5f60718293a4b5c6d7e8f90"
    )


@pytest.mark.asyncio
async def test_send_followup_refuses_under_pytest(monkeypatch):
    from app import telegram as tg

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-1")
    with pytest.raises(RuntimeError, match="pytest|synthetic"):
        await tg.send_followup_deal(
            "-1",
            wallet="0xaaa0000000000000000000000000000000000001",
            token="0xbbb00000000000000000000000000000000000bb",
            token_symbol="SEED",
            deal_index=2,
            mcap_at_buy=5000.0,
            bought_usd=50.0,
            check_honeypot=False,
        )


def test_refuse_helper_detects_pytest():
    with pytest.raises(RuntimeError, match="pytest"):
        refuse_telegram_if_unsafe()
