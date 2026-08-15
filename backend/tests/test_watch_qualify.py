"""Tests for ATH gate classification and hold promote."""

from __future__ import annotations

from app.models import ScreenedToken
from app.watch_qualify import (
    HOLD_TTL_SEC,
    REPARSE_YOUNG_COOLDOWN_SEC,
    ath_gate_enabled,
    classify_for_parse,
    select_ath_probe_batch,
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


def test_dump_after_pump_held_not_dropped():
    """MEATSPIN-class: DS spot ATH < gate must enter hold (not vanish)."""
    screened = [
        _tok("0xMeat", mcap=15_690, ath=15_690, symbol="MEATSPIN", pair_age_hours=20.0)
    ]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={},
        parsed={},
        index_addresses={"0xmeat"},
        now=100.0,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == []
    assert "0xmeat" in d.held
    assert d.ath_updates["0xmeat"][0] == 15_690.0


def test_dump_after_pump_promotes_when_gecko_peak_in_hold():
    screened = [
        _tok("0xMeat", mcap=15_690, ath=15_690, symbol="MEATSPIN", pair_age_hours=20.0)
    ]
    hold = {
        "0xmeat": {
            "first_seen": 1.0,
            "ath_mcap": 148_000.0,
            "symbol": "MEATSPIN",
        }
    }
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold=hold,
        parsed={},
        index_addresses={"0xmeat"},
        now=100.0,
        max_pair_age_hours=24.0,
    )
    assert "0xmeat" in d.candidates
    assert d.ath_updates["0xmeat"][0] == 148_000.0


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
    assert should_mark_parsed("Honeypot skipped (gmgn)") is False
    assert should_mark_parsed("USDG USD price unavailable") is False
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


def test_requeue_young_when_pair_age_unknown_uses_first_seen():
    """Remote/pending-rescue rows often lack pair_age — use hold first_seen."""
    now = 1_000_000.0
    screened = [_tok("0xRESCUE", mcap=80_000, pair_age_hours=None)]
    d = classify_for_parse(
        screened,
        min_ath_mcap=50_000,
        hold={"0xrescue": {"first_seen": now - 5 * 3600, "ath_mcap": 80_000.0}},
        parsed={"0xrescue": now - REPARSE_YOUNG_COOLDOWN_SEC - 1},
        index_addresses=None,
        now=now,
        max_pair_age_hours=24.0,
    )
    assert d.candidates == ["0xrescue"]
    assert d.requeued_young == ["0xrescue"]


def test_deadline_urgency_expands_with_drain_eta():
    from app.watch_qualify import parse_queue_sort_key

    hold = {
        "0xfresh": {"queued_at": 100.0, "first_seen": 100.0},
        "0xmid": {"queued_at": 50.0, "first_seen": 50.0},
    }
    # 20h old → 4h remaining — not urgent under fixed 3h window…
    ages = {"0xfresh": 2.0, "0xmid": 20.0}
    ath = {"0xfresh": 50_000.0, "0xmid": 200_000.0}
    k_mid_fixed = parse_queue_sort_key(
        "0xmid",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=1_000.0,
        drain_eta_hours=None,
    )
    assert k_mid_fixed[0] == 1  # not urgent
    # …but with 4h drain ETA (within ETA cap) it becomes urgent.
    k_mid_deadline = parse_queue_sort_key(
        "0xmid",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=1_000.0,
        drain_eta_hours=4.0,
    )
    k_fresh = parse_queue_sort_key(
        "0xfresh",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=1_000.0,
        drain_eta_hours=4.0,
    )
    assert k_mid_deadline[0] == 0
    assert k_mid_deadline < k_fresh


def test_deadline_eta_cap_keeps_mid_age_off_urgent():
    """ETA may expand urgency only up to PARSE_DEADLINE_ETA_CAP_HOURS."""
    from app.watch_qualify import parse_queue_sort_key

    hold = {"0xmid": {"queued_at": 50.0, "first_seen": 50.0}}
    # 15h old → 9h remaining; even a 12h ETA is capped at 6h → still not urgent.
    k = parse_queue_sort_key(
        "0xmid",
        hold=hold,
        pair_age_hours={"0xmid": 15.0},
        ath_mcap={"0xmid": 200_000.0},
        max_pair_age_hours=24.0,
        now=1_000.0,
        drain_eta_hours=12.0,
    )
    assert k[0] == 1

def test_ath_probe_prefers_screened_over_dust():
    """LEGATE-class: screened near-gate must beat thousands of $1k dust."""
    dust = [
        (f"0xdust{i:04d}", 1_000.0 + i, "DUST", None, None) for i in range(200)
    ]
    legate = ("0xlegate", 31_124.0, "LEGATE", 2.0, 0)
    batch = select_ath_probe_batch(
        [*dust, legate],
        probe_cap=8,
        now=1_000.0,
        probed_at={},
    )
    addrs = [t[0] for t in batch]
    assert "0xlegate" in addrs
    assert addrs[0] == "0xlegate"


def test_ath_probe_near_gate_hold_beats_lowest_first():
    dust = [(f"0xdust{i:04d}", float(i + 1), "D", None, None) for i in range(100)]
    near = ("0xnear", 31_000.0, "NEAR", None, None)
    batch = select_ath_probe_batch(
        [*dust, near],
        probe_cap=12,
        now=1_000.0,
        probed_at={},
    )
    assert "0xnear" in {t[0] for t in batch}


def test_ath_probe_cooldown_rotates_hold_queue():
    near_a = ("0xa", 39_000.0, "A", None, None)
    near_b = ("0xb", 38_000.0, "B", None, None)
    first = select_ath_probe_batch(
        [near_a, near_b],
        probe_cap=1,
        now=1_000.0,
        probed_at={},
    )
    assert [t[0] for t in first] == ["0xa"]
    second = select_ath_probe_batch(
        [near_a, near_b],
        probe_cap=1,
        now=1_000.0,
        probed_at={"0xa": 1_000.0},
        reprobe_cooldown_sec=3_600.0,
    )
    assert [t[0] for t in second] == ["0xb"]


def test_hold_enrich_batch_near_gate_capped():
    from app.watch_qualify import select_hold_enrich_batch

    hold = {
        f"0xdust{i:04x}": {"ath_mcap": 1_000.0 + i, "first_seen": 1.0}
        for i in range(50)
    }
    hold["0xnear"] = {"ath_mcap": 35_000.0, "first_seen": 10.0}
    hold["0xpass"] = {"ath_mcap": 50_000.0, "first_seen": 1.0}  # already ≥ gate
    batch = select_hold_enrich_batch(hold, min_ath_mcap=40_000, cap=5)
    assert "0xpass" not in batch
    assert batch[0] == "0xnear"
    assert len(batch) == 5


def test_parse_queue_urgency_beats_fifo():
    from app.watch_qualify import parse_queue_sort_key

    hold = {
        "0xold": {"queued_at": 100.0},
        "0xurgent": {"queued_at": 200.0},
    }
    ages = {"0xold": 2.0, "0xurgent": 23.0}
    ath = {"0xold": 200_000.0, "0xurgent": 40_000.0}
    k_old = parse_queue_sort_key(
        "0xold",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=300.0,
    )
    k_u = parse_queue_sort_key(
        "0xurgent",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=300.0,
    )
    assert k_u < k_old  # urgency beats higher ATH


def test_parse_queue_ath_beats_fifo_when_not_urgent():
    """Fresh high-ATH pumps must not sit behind older mid-ATH qualify."""
    from app.watch_qualify import parse_queue_sort_key

    hold = {
        "0xmid": {"queued_at": 100.0},
        "0xhot": {"queued_at": 500.0},
    }
    ages = {"0xmid": 8.0, "0xhot": 2.0}
    ath = {"0xmid": 45_000.0, "0xhot": 245_000.0}
    k_mid = parse_queue_sort_key(
        "0xmid",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
    )
    k_hot = parse_queue_sort_key(
        "0xhot",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
    )
    assert k_hot < k_mid


def test_parse_queue_fresh_mid_ath_beats_stale_mega_ath():
    """CATSTRAT-class: young mid-ATH before older mega-ATH still in the 24h window."""
    from app.watch_qualify import parse_queue_sort_key

    hold = {
        "0xmega": {"queued_at": 100.0},
        "0xcats": {"queued_at": 500.0},
    }
    ages = {"0xmega": 12.0, "0xcats": 4.0}
    ath = {"0xmega": 189_000_000.0, "0xcats": 75_000.0}
    k_mega = parse_queue_sort_key(
        "0xmega",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
    )
    k_cats = parse_queue_sort_key(
        "0xcats",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
    )
    assert k_cats < k_mega


def test_parse_queue_priority_ath_floor_defers_dust():
    """ATH ≥ priority floor sorts before sub-floor dust in the same urgent/fresh band."""
    from app.watch_qualify import parse_queue_sort_key

    hold = {
        "0xhi": {"queued_at": 200.0},
        "0xlo": {"queued_at": 100.0},
    }
    ages = {"0xhi": 10.0, "0xlo": 10.0}
    ath = {"0xhi": 60_000.0, "0xlo": 45_000.0}
    k_hi = parse_queue_sort_key(
        "0xhi",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
        priority_min_ath=50_000.0,
    )
    k_lo = parse_queue_sort_key(
        "0xlo",
        hold=hold,
        pair_age_hours=ages,
        ath_mcap=ath,
        max_pair_age_hours=24.0,
        now=600.0,
        priority_min_ath=50_000.0,
    )
    assert k_hi[2] == 0  # not deferred
    assert k_lo[2] == 1  # deferred tail
    assert k_hi < k_lo


def test_dust_hold_expires_faster_than_near_gate():
    """Far-below-gate dust TTL 6h; near-gate keeps 48h hold TTL."""
    from app.watch_qualify import HOLD_DUST_TTL_SEC, HOLD_TTL_SEC, classify_for_parse

    assert HOLD_DUST_TTL_SEC < HOLD_TTL_SEC
    now = 100_000.0
    hold = {
        "0xdust": {
            "first_seen": now - HOLD_DUST_TTL_SEC - 10,
            "ath_mcap": 5_000.0,  # < 25% of 40k
            "symbol": "DUST",
        },
        "0xnear": {
            "first_seen": now - HOLD_DUST_TTL_SEC - 10,
            "ath_mcap": 32_000.0,
            "symbol": "NEAR",
        },
    }
    d = classify_for_parse(
        [],
        min_ath_mcap=40_000,
        hold=hold,
        parsed={},
        index_addresses=None,
        now=now,
        max_pair_age_hours=24.0,
    )
    assert "0xdust" in d.expired
    assert "0xnear" in d.held
    assert "0xdust" not in d.held
