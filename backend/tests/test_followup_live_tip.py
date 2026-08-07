"""Live tip loop: fresh buys alert without waiting on hist catch-up."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.followup import FollowupRunner
from app.followup_store import FollowupStore
from app.models import FollowupConfig


@pytest.mark.asyncio
async def test_live_tip_pass_scans_and_advances_only_on_success(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_cursor(10_000)
    store.set_logwatch_live_cursor(99_700)  # inside tip window (span=300)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        live_enrich_budget_sec=3.0,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    scanned: list[tuple[int, int, str, bool]] = []

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
        scanned.append((from_block, to_block, label, skip_enrich))
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
            ok = await runner._live_tip_pass(cfg, rpc=rpc)
    assert ok is True
    assert scanned
    assert scanned[0][2] == "live"
    assert scanned[0][3] is False
    assert scanned[0][0] == 99_701
    assert scanned[0][1] == tip
    assert store.get_logwatch_live_cursor() == tip


@pytest.mark.asyncio
async def test_live_tip_does_not_advance_cursor_on_soft_timeout(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(99_700)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)

    async def fake_scan(*_a, **_k):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_tip_pass(cfg, rpc=rpc)
    assert store.get_logwatch_live_cursor() == 99_700
    assert runner._live_timeout_streak == 1


@pytest.mark.asyncio
async def test_hist_pass_skips_enrich_and_does_not_embed_live(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    hist_cursor = 10_000
    store.set_logwatch_cursor(hist_cursor)
    store.set_logwatch_live_cursor(tip)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=1_500,
        logwatch_burst_catchup_span=10_000,
        logwatch_catchup_chunks_per_pass=4,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    scanned: list[tuple[str, bool, int, int]] = []

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
        scanned.append((label, skip_enrich, from_block, to_block))
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
    assert scanned
    assert all(lab.startswith("hist") for lab, *_ in scanned)
    assert all(skip for _, skip, *_ in scanned)
    # Large lag must use burst span (>>800), not the old death-spiral cap.
    first_span = scanned[0][3] - scanned[0][2] + 1
    assert first_span >= 5_000
    assert first_span != 800
    assert len(scanned) >= 2  # multi-chunk catch-up
    assert store.get_logwatch_cursor() > hist_cursor


@pytest.mark.asyncio
async def test_live_gap_backfill_when_cursor_far_behind(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(50_000)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    labels: list[str] = []

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
        labels.append(label)
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
            await runner._live_tip_pass(cfg, rpc=rpc)
    assert labels[0] == "live"
    assert "live_gap" in labels
    # Tip scan does not snap over the gap — watermark only moves via gap backfill.
    assert store.get_logwatch_live_cursor() == 50_000 + 300
