"""Tests for ATH gate classification and hold promote."""

from __future__ import annotations

from app.models import ScreenedToken
from app.watch_qualify import (
    HOLD_TTL_SEC,
    REPARSE_YOUNG_COOLDOWN_SEC,
    ath_gate_enabled,
    classify_for_parse,
    should_mark_parsed,
)


def _tok(
    addr: str,
    mcap: float,
    ath: float | None = None,
    symbol: str = "T",
    *,
    pair_age_hours: float | None = None,
) -> ScreenedToken:
    peak = ath if ath is not None else mcap
    return ScreenedToken(
        address=addr,
        symbol=symbol,
        market_cap=mcap,
        ath_mcap=peak,
        pair_age_hours=pair_age_hours,
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
        parsed={},
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
        parsed={},
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
        parsed={},
        index_addresses={"0xccc"},
        now=100.0,
    )
    assert d.candidates == ["0xccc"]
    assert d.held == []


def test_skip_already_parsed():
    screened = [_tok("0xDDD", mcap=80_000, pair_age_hours=30.0)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={"0xddd": 50.0},
        index_addresses={"0xddd"},
        now=100.0,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == []
    assert d.held == []
    assert d.skipped_parsed == 1
    assert d.requeued_young == []


def test_requeue_young_parsed_after_cooldown():
    """parsed ≠ old: still ≤24h → re-parse after cooldown (fixes bad unique pass)."""
    screened = [_tok("0xDREAM", mcap=80_000, pair_age_hours=10.0)]
    now = 1_000_000.0
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={"0xdream": now - REPARSE_YOUNG_COOLDOWN_SEC - 1},
        index_addresses={"0xdream"},
        now=now,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == ["0xdream"]
    assert d.requeued_young == ["0xdream"]
    assert d.skipped_parsed == 0


def test_young_parsed_within_cooldown_stays_skipped():
    screened = [_tok("0xHIM", mcap=80_000, pair_age_hours=20.0)]
    now = 1_000_000.0
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={"0xhim": now - 60.0},  # just parsed
        index_addresses={"0xhim"},
        now=now,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == []
    assert d.skipped_parsed == 1
    assert d.requeued_young == []


def test_legacy_parsed_set_zero_ts_requeues_young_immediately():
    screened = [_tok("0xLEG", mcap=80_000, pair_age_hours=5.0)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={"0xleg"},  # set → parsed_at 0
        index_addresses={"0xleg"},
        now=1_000_000.0,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == ["0xleg"]
    assert d.requeued_young == ["0xleg"]


def test_gate_off_parses_all_screened():
    screened = [_tok("0xEEE", mcap=1_000), _tok("0xFFF", mcap=2_000)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=None,
        hold={},
        parsed={"0xeee": 1.0},  # ignored when gate off
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
        parsed={},
        index_addresses=set(),  # left index
        now=1.0 + HOLD_TTL_SEC + 10,
    )
    assert "0xold" in d.expired
    assert d.candidates == []
    assert d.held == []


def test_hold_peak_survives_dump_for_young_token():
    """After a pump dump, live mcap is low but hold/ath peak still qualifies."""
    screened = [_tok("0xPCC", mcap=2_600.0, ath=2_600.0, pair_age_hours=2.0)]
    hold = {
        "0xpcc": {
            "first_seen": 1.0,
            "ath_mcap": 523_000.0,  # brief pump captured earlier
            "symbol": "PCC",
        },
    }
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold=hold,
        parsed={},
        index_addresses={"0xpcc"},
        now=100.0,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == ["0xpcc"]
    assert d.held == []
    assert d.ath_updates["0xpcc"][0] == 523_000.0


def test_screen_row_ath_peak_qualifies_after_spot_dump():
    """Screener spot dumped, but row.ath_mcap still holds the pump peak."""
    screened = [
        _tok("0xPUMP", mcap=3_000.0, ath=120_000.0, pair_age_hours=1.5)
    ]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={},
        index_addresses={"0xpump"},
        now=100.0,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == ["0xpump"]
    assert d.ath_updates["0xpump"][0] == 120_000.0


def test_should_mark_parsed():
    assert should_mark_parsed(None) is True
    assert should_mark_parsed("Honeypot skipped (gmgn)") is True
    assert should_mark_parsed("No Uniswap V2/V3 pool found for this token") is False
    assert should_mark_parsed("no pool available") is False
    # Filter wipe still marks parsed — young requeue retries on cooldown
    # (immediate unparsed retries starved never-seen tokens).
    assert (
        should_mark_parsed(
            None, buyers_before_filters=3, buyers_after_filters=0
        )
        is True
    )
    assert (
        should_mark_parsed(
            None, buyers_before_filters=2, buyers_after_filters=1
        )
        is True
    )
