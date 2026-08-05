"""Early-mcap window reopens after a brief pump (dump → buy under gate)."""

from __future__ import annotations

from app.replay import _mcap_above_streak, _MCAP_SUSTAINED_ABOVE_BLOCKS


def test_mcap_streak_reopens_after_dump():
    th = 30_000.0
    stop, since = _mcap_above_streak(
        mcap_now=31_000.0, threshold=th, block=100, above_since=None
    )
    assert stop is False
    assert since == 100

    # Still above, but not sustained (~30s = 315 blocks)
    stop, since = _mcap_above_streak(
        mcap_now=30_500.0, threshold=th, block=100 + 315, above_since=since
    )
    assert stop is False
    assert since == 100

    # Dump under gate → reopen
    stop, since = _mcap_above_streak(
        mcap_now=18_000.0, threshold=th, block=100 + 400, above_since=since
    )
    assert stop is False
    assert since is None


def test_mcap_streak_sticky_after_sustained_hour():
    th = 30_000.0
    start = 1_000
    stop, since = _mcap_above_streak(
        mcap_now=40_000.0, threshold=th, block=start, above_since=None
    )
    assert stop is False and since == start
    stop, since = _mcap_above_streak(
        mcap_now=50_000.0,
        threshold=th,
        block=start + _MCAP_SUSTAINED_ABOVE_BLOCKS,
        above_since=since,
    )
    assert stop is True
    assert since == start
