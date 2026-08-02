"""Tests for ATH gate classification and hold promote."""

from __future__ import annotations

from app.models import ScreenedToken
from app.watch_qualify import (
    HOLD_TTL_SEC,
    ath_gate_enabled,
    classify_for_parse,
    should_mark_parsed,
)


def _tok(addr: str, mcap: float, ath: float | None = None, symbol: str = "T") -> ScreenedToken:
    peak = ath if ath is not None else mcap
    return ScreenedToken(
        address=addr,
        symbol=symbol,
        market_cap=mcap,
        ath_mcap=peak,
    )


def test_ath_gate_enabled():
    assert ath_gate_enabled(50_000) is True
    assert ath_gate_enabled(None) is False
    assert ath_gate_enabled(0) is False


def test_ath_peak_from_screen_and_hold():
    screened = [_tok("0xAAA", mcap=30_000, ath=40_000)]
    hold = {
        "0xaaa": {"first_seen": 1.0, "ath_mcap": 10_000.0, "symbol": "T"},
    }
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold=hold,
        parsed=set(),
        index_addresses={"0xaaa"},
        now=100.0,
    )
    assert d.candidates == []
    assert "0xaaa" in d.held
    assert d.ath_updates["0xaaa"][0] == 40_000.0


def test_qualify_when_current_crosses_threshold():
    screened = [_tok("0xBBB", mcap=60_000, ath=60_000)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed=set(),
        index_addresses={"0xbbb"},
        now=100.0,
    )
    assert d.candidates == ["0xbbb"]
    assert d.held == []


def test_promote_from_hold_without_screen_row():
    hold = {
        "0xccc": {"first_seen": 1.0, "ath_mcap": 55_000.0, "symbol": "C"},
    }
    d = classify_for_parse(
        [],
        min_ath_mcap=50_000,
        hold=hold,
        parsed=set(),
        index_addresses={"0xccc"},
        now=100.0,
    )
    assert d.candidates == ["0xccc"]
    assert d.held == []


def test_skip_already_parsed():
    screened = [_tok("0xDDD", mcap=80_000)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={"0xddd"},
        index_addresses={"0xddd"},
        now=100.0,
    )
    assert d.candidates == []
    assert d.held == []


def test_gate_off_parses_all_screened():
    screened = [_tok("0xEEE", mcap=1_000), _tok("0xFFF", mcap=2_000)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=None,
        hold={},
        parsed={"0xeee"},  # ignored when gate off
        now=100.0,
    )
    assert d.candidates == ["0xeee", "0xfff"]
    assert d.held == []


def test_hold_ttl_expires():
    hold = {
        "0xold": {
            "first_seen": 1.0,
            "ath_mcap": 12_000.0,
            "symbol": "OLD",
        },
    }
    d = classify_for_parse(
        [],
        min_ath_mcap=50_000,
        hold=hold,
        parsed=set(),
        index_addresses=set(),  # left index
        now=1.0 + HOLD_TTL_SEC + 10,
    )
    assert "0xold" in d.expired
    assert d.candidates == []
    assert d.held == []


def test_should_mark_parsed():
    assert should_mark_parsed(None) is True
    assert should_mark_parsed("Honeypot skipped (gmgn)") is True
    assert should_mark_parsed("No Uniswap V2/V3 pool found for this token") is False
    assert should_mark_parsed("no pool available") is False
    # Early buyers existed but wallet filters wiped all — retry later
    assert (
        should_mark_parsed(
            None, buyers_before_filters=3, buyers_after_filters=0
        )
        is False
    )
    assert (
        should_mark_parsed(
            None, buyers_before_filters=2, buyers_after_filters=1
        )
        is True
    )
