"""Hist catch-up span / multi-chunk / ops lag alert (hist-only vs live unhealthy)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.followup import FollowupRunner, hist_span_for_lag
from app.followup_store import FollowupStore
from app.models import FollowupConfig


def test_hist_span_large_lag_uses_burst_not_800():
    cfg = FollowupConfig(
        logwatch_max_span=3_000,
        logwatch_catchup_span=1_500,
        logwatch_burst_catchup_span=10_000,
    )
    assert hist_span_for_lag(118_000, cfg) == 10_000
    assert hist_span_for_lag(118_000, cfg) != 800
    assert hist_span_for_lag(8_000, cfg) == 1_500
    assert hist_span_for_lag(500, cfg) == 3_000


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
    assert len(windows) == 3
    assert all((b - a + 1) == 10_000 for a, b in windows)
    assert store.get_logwatch_cursor() == hist_cursor + 30_000


@pytest.mark.asyncio
async def test_cursor_lag_alert_hist_only_when_live_healthy(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 30_405_000
    store.set_logwatch_cursor(tip - 120_000)
    # Stale/wrong live meta that used to produce scary live_behind=124k.
    store.set_logwatch_live_cursor(tip - 120_000)
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
    assert "⚠️" not in alerts[0]
    assert "live_behind=120000" not in alerts[0]
    assert "отставание 120000" not in alerts[0]
    assert "недавний live success" in alerts[0]


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
