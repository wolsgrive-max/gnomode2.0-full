"""Catch-up lookback and screen filter merge."""

from __future__ import annotations

from app.models import WatchScreenFilters
from app.watch import apply_catchup_to_screen
from app.watch_store import WatchStore, catchup_lookback_hours


def test_catchup_lookback_never_ran():
    assert catchup_lookback_hours(None, now=1_000_000) == 24.0


def test_catchup_lookback_over_24h():
    now = 1_000_000.0
    last = now - 30 * 3600
    assert catchup_lookback_hours(last, now=now) == 24.0


def test_catchup_lookback_exact_gap():
    now = 1_000_000.0
    last = now - 3.5 * 3600
    assert catchup_lookback_hours(last, now=now) == 3.5


def test_apply_catchup_sets_max_pair_age():
    screen = WatchScreenFilters(min_liq=100.0)
    out = apply_catchup_to_screen(screen, 5.0)
    assert out.max_pair_age_hours == 5.0
    assert out.min_liq == 100.0


def test_apply_catchup_respects_tighter_user_max():
    screen = WatchScreenFilters(max_pair_age_hours=2.0)
    out = apply_catchup_to_screen(screen, 10.0)
    assert out.max_pair_age_hours == 2.0


def test_apply_catchup_caps_looser_user_max():
    screen = WatchScreenFilters(max_pair_age_hours=48.0)
    out = apply_catchup_to_screen(screen, 24.0)
    assert out.max_pair_age_hours == 24.0


def test_last_success_ts_persists(tmp_path):
    store = WatchStore(
        config_path=tmp_path / "watch.json",
        seen_path=tmp_path / "seen.json",
        state_path=tmp_path / "state.json",
    )
    assert store.load_last_success_ts() is None
    store.save_last_success_ts(123456.0)
    store2 = WatchStore(
        config_path=tmp_path / "watch.json",
        seen_path=tmp_path / "seen.json",
        state_path=tmp_path / "state.json",
    )
    assert store2.load_last_success_ts() == 123456.0


def test_should_drain_without_sleep():
    from app.watch import should_drain_without_sleep

    assert should_drain_without_sleep(
        pending_count=12, enabled=True, user_stopped=False
    )
    assert not should_drain_without_sleep(
        pending_count=0, enabled=True, user_stopped=False
    )
    assert not should_drain_without_sleep(
        pending_count=5, enabled=True, user_stopped=True
    )
    assert not should_drain_without_sleep(
        pending_count=5, enabled=False, user_stopped=False
    )
    assert not should_drain_without_sleep(
        pending_count=5, enabled=True, user_stopped=False, force_run=True
    )


def test_should_defer_ath_probe():
    from app.watch_qualify import should_defer_ath_probe

    assert not should_defer_ath_probe(1)
    assert not should_defer_ath_probe(11)
    assert should_defer_ath_probe(12)
    assert should_defer_ath_probe(1282)
    assert not should_defer_ath_probe(0)
    assert not should_defer_ath_probe(2, min_pending=5)
    assert should_defer_ath_probe(5, min_pending=5)
