"""Hist catch-up span / multi-chunk / ops lag alert (hist-only vs live unhealthy)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.followup import FollowupRunner, hist_span_for_lag, live_span_for_watchlist
from app.followup_store import FollowupStore
from app.models import FollowupConfig


def test_hist_span_large_lag_uses_burst_not_800():
    cfg = FollowupConfig(
        logwatch_max_span=3_000,
        logwatch_catchup_span=1_500,
        logwatch_burst_catchup_span=10_000,
        logwatch_hist_rpc_chunk=100,
    )
    assert hist_span_for_lag(118_000, cfg) == 10_000
    assert hist_span_for_lag(118_000, cfg) != 800
    # Plateau ~45k must burst (threshold 15k), not crawl at catchup_span.
    assert hist_span_for_lag(45_000, cfg) == 10_000
    assert hist_span_for_lag(8_000, cfg) == 1_500
    assert hist_span_for_lag(500, cfg) == 3_000
    # Large watchlist: shrink vs 10k, but stay ahead of tip (~300+ not ~75).
    capped = hist_span_for_lag(45_000, cfg, n_wallets=548)
    assert 250 <= capped <= 1_000


def test_hist_span_caps_for_large_watchlist():
    """400+ wallets → capped windows (not 10k), still fast enough vs tip."""
    cfg = FollowupConfig(
        logwatch_max_span=3_000,
        logwatch_catchup_span=4_000,
        logwatch_burst_catchup_span=10_000,
        logwatch_hist_rpc_chunk=100,
    )
    span = hist_span_for_lag(45_000, cfg, n_wallets=548)
    assert 250 <= span <= 1_000
    # Heavy watchlist → tiny tip window so getLogs finishes before TG lag.
    assert live_span_for_watchlist(300, 548) <= 16
    assert live_span_for_watchlist(300, 50) == 300


@pytest.mark.asyncio
async def test_skip_enrich_progressive_partial_advance(tmp_path):
    """Timeout mid-window must keep blocks already fetched (not zero progress)."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(FollowupConfig(enabled=True, logwatch_enabled=True))
    runner = FollowupRunner(store=store)
    cfg = store.load_config()
    calls = {"n": 0}

    async def flaky_fetch(rpc, wallets, *, from_block, to_block, chunk_size=50_000):
        calls["n"] += 1
        if calls["n"] <= 2:
            return []
        raise asyncio.TimeoutError()

    with patch("app.followup.fetch_inbound_transfers", side_effect=flaky_fetch):
        res = await runner._logwatch_scan_window(
            cfg,
            rpc=AsyncMock(),
            watching=[f"0x{i:040x}" for i in range(450)],  # 3 topic batches
            from_block=1000,
            to_block=1200,
            fetch_timeout=30.0,
            label="hist",
            skip_enrich=True,
        )
    assert res is not None
    assert res["advanced"] is True
    assert res["advance_to"] is not None
    assert int(res["advance_to"]) >= 1000


@pytest.mark.asyncio
async def test_hist_multi_chunk_advances_cursor(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 200_000
    hist_cursor = 10_000
    store.set_logwatch_cursor(hist_cursor)
    store.set_logwatch_live_cursor(tip)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=3_000,
        logwatch_burst_catchup_span=10_000,
        logwatch_catchup_chunks_per_pass=3,
        logwatch_catchup_time_budget_sec=90.0,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    windows: list[tuple[int, int]] = []

    async def fake_scan(
        cfg,
        *,
        rpc,
        watching,
        from_block,
        to_block,
        fetch_timeout,
        label,
        cursor_floor=None,
        skip_enrich=False,
        enrich_budget_sec=None,
        queue_mcap_retry=False,
        soft_partial=False,
    ):
        windows.append((from_block, to_block))
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": to_block,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    # Huge lag fast-forwards past stale alert window; remaining near tip.
    assert store.get_logwatch_cursor() >= tip - 2_500
    assert len(windows) >= 1
    # Post-FF windows are the leftover near-tip band (not 3×10k crawl).
    assert all((b - a + 1) <= 10_000 for a, b in windows)


@pytest.mark.asyncio
async def test_hist_fast_forward_past_stale_alert_window(tmp_path):
    """Huge hist lag past alert_max_block_lag must jump, not crawl."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 200_000
    hist_cursor = 10_000
    store.set_logwatch_cursor(hist_cursor)
    store.set_logwatch_live_cursor(tip)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=3_000,
        logwatch_burst_catchup_span=10_000,
        logwatch_catchup_chunks_per_pass=1,
        logwatch_live_span=300,
        alert_max_block_lag=2_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    scanned: list[tuple[int, int]] = []

    async def fake_scan(
        cfg,
        *,
        rpc,
        watching,
        from_block,
        to_block,
        fetch_timeout,
        label,
        cursor_floor=None,
        skip_enrich=False,
        enrich_budget_sec=None,
        queue_mcap_retry=False,
        soft_partial=False,
    ):
        scanned.append((from_block, to_block))
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": to_block,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    # Fast-forward to tip - alert_lag - live_span = 200000 - 2000 - 300
    assert store.get_logwatch_cursor() >= tip - 2_000 - 300
    assert any(
        "fast-forward" in (getattr(e, "message", None) or "")
        for e in (runner._log or [])
    )


@pytest.mark.asyncio
async def test_cursor_lag_alert_info_when_live_near_tip(tmp_path):
    """Hist lag alone → ℹ️ when live watermark is healthy and near tip."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 30_405_000
    store.set_logwatch_cursor(tip - 120_000)
    store.set_logwatch_live_cursor(tip - 500)  # live near tip
    cfg = FollowupConfig(
        enabled=True,
        cursor_lag_alert_blocks=6_000,
        ops_alert_cooldown_sec=60,
        telegram_chat_id="1",
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    alerts: list[str] = []

    async def capture_ops(cfg, *, kind, text):
        alerts.append(text)

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_ops_alert", side_effect=capture_ops):
        await runner._maybe_alert_cursor_lag(cfg, rpc=rpc)
    assert alerts
    assert alerts[0].startswith("ℹ️")
    assert "hist-курсор" in alerts[0]
    assert "Live tip в порядке" in alerts[0]
    assert "live_behind=120000" not in alerts[0]
    assert "отставание 120000" not in alerts[0]


@pytest.mark.asyncio
async def test_cursor_lag_alert_never_says_live_ok_when_far_behind(tmp_path):
    """Recent live tick ≠ healthy when watermark is 128k behind tip."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 30_405_000
    store.set_logwatch_cursor(tip - 120_000)
    store.set_logwatch_live_cursor(tip - 128_000)
    cfg = FollowupConfig(
        enabled=True,
        cursor_lag_alert_blocks=6_000,
        ops_alert_cooldown_sec=60,
        telegram_chat_id="1",
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    alerts: list[str] = []

    async def capture_ops(cfg, *, kind, text):
        alerts.append(text)

    rpc = MagicMock()
    with patch.object(runner, "_ops_alert", side_effect=capture_ops):
        await runner._maybe_alert_cursor_lag(cfg, rpc=rpc)
    assert alerts
    assert alerts[0].startswith("⚠️")
    assert "Live tip в порядке" not in alerts[0]
    assert "live_behind=128000" in alerts[0]
    assert "нездоров" in alerts[0]


@pytest.mark.asyncio
async def test_cursor_lag_alert_warns_when_live_unhealthy(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_cursor(10_000)
    store.set_logwatch_live_cursor(10_000)
    cfg = FollowupConfig(
        enabled=True,
        cursor_lag_alert_blocks=6_000,
        ops_alert_cooldown_sec=60,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = 0.0
    alerts: list[str] = []

    async def capture_ops(cfg, *, kind, text):
        alerts.append(text)

    rpc = MagicMock()
    with patch.object(runner, "_ops_alert", side_effect=capture_ops):
        await runner._maybe_alert_cursor_lag(cfg, rpc=rpc)
    assert alerts
    assert alerts[0].startswith("⚠️")
    assert "Live tip тоже нездоров" in alerts[0]


@pytest.mark.asyncio
async def test_cursor_lag_alert_info_at_live_behind_2002(tmp_path):
    """live_behind just above old near_tip=2000 must not force ⚠️ when healthy."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 30_523_334
    store.set_logwatch_cursor(tip - 33_136)
    store.set_logwatch_live_cursor(tip - 2_002)
    cfg = FollowupConfig(
        enabled=True,
        cursor_lag_alert_blocks=6_000,
        ops_alert_cooldown_sec=60,
        telegram_chat_id="1",
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    alerts: list[str] = []

    async def capture_ops(cfg, *, kind, text):
        alerts.append(text)

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_ops_alert", side_effect=capture_ops):
        await runner._maybe_alert_cursor_lag(cfg, rpc=rpc)
    assert alerts
    assert alerts[0].startswith("ℹ️")
    assert "Live tip в порядке" in alerts[0]


@pytest.mark.asyncio
async def test_hist_soft_fail_force_advances_under_high_lag(tmp_path):
    """Soft-fail mid-catchup must force-advance and keep multi-chunk going."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    # Keep cursor above stale_floor so hist FF does not erase the soft-fail path,
    # but lag still > catching_up threshold and cursor past alert horizon so
    # force-advance is allowed (never skips unscanned TG-alertable blocks).
    # stale_floor = tip - alert_lag - live_span = 100000 - 10000 - 300 = 89700
    # alert floor = tip - alert_lag = 90000
    hist_cursor = tip - 10_150  # 89850; lag≈10150; no FF; past_alert=True
    store.set_logwatch_cursor(hist_cursor)
    store.set_logwatch_live_cursor(tip)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=3_000,
        logwatch_burst_catchup_span=10_000,
        logwatch_catchup_chunks_per_pass=4,
        logwatch_catchup_time_budget_sec=90.0,
        logwatch_live_span=300,
        alert_max_block_lag=10_000,
        cursor_lag_alert_blocks=6_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    calls = {"n": 0}

    async def soft_then_ok(
        cfg,
        *,
        rpc,
        watching,
        from_block,
        to_block,
        fetch_timeout,
        cursor_floor,
        label,
        force_after=None,
        min_span=None,
    ):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "new_deals": 0,
                "alerts": 0,
                "skipped": 0,
                "advanced": False,
                "advance_to": None,
            }
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": to_block,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_hist_scan_shrink_retry", side_effect=soft_then_ok):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    # Soft-fail force-advance + at least one more chunk.
    assert calls["n"] >= 2
    assert store.get_logwatch_cursor() > hist_cursor
    assert any(
        "soft-fail force-advance" in (getattr(e, "message", None) or "")
        for e in (runner._log or [])
    )


@pytest.mark.asyncio
async def test_hist_pass_dual_jumps_stale_live_cursor(tmp_path):
    """Hist pass must snap a far-behind live watermark without waiting live loop."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 200_000
    store.set_logwatch_cursor(tip - 50_000)
    store.set_logwatch_live_cursor(tip - 12_000)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=3_000,
        logwatch_burst_catchup_span=10_000,
        logwatch_catchup_chunks_per_pass=1,
        logwatch_live_span=300,
        live_gap_enrich_max_blocks=2_000,
        alert_max_block_lag=2_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)

    async def fake_scan(
        cfg,
        *,
        rpc,
        watching,
        from_block,
        to_block,
        fetch_timeout,
        label,
        cursor_floor=None,
        skip_enrich=False,
        enrich_budget_sec=None,
        queue_mcap_retry=False,
        soft_partial=False,
    ):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": to_block,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    live = store.get_logwatch_live_cursor()
    assert live is not None
    assert tip - int(live) <= 2_000
    assert any(
        "dual-jump" in (getattr(e, "message", None) or "")
        for e in (runner._log or [])
    )
    # Watermark park must not fake a live tip success.
    assert runner._last_live_success_ts == 0.0
