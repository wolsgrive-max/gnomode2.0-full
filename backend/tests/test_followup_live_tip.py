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
        live_gap_enrich_max_blocks=2_000,
        logwatch_burst_catchup_span=5_000,
        logwatch_catchup_chunks_per_pass=4,
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
        assert skip_enrich is True  # far behind → burst skip_enrich only
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
    assert any(lab.startswith("live_burst") for lab in labels)
    # Burst must jump far past the old ~300/tick crawl.
    assert store.get_logwatch_live_cursor() >= 50_000 + 10_000
    # Still not at tip — leave enrich_cap for near-tip alerts.
    assert store.get_logwatch_live_cursor() <= tip - 2_000
    ok, behind = runner._live_tip_healthy(tip=tip)
    assert ok is False
    assert behind is not None and behind > 2_000


@pytest.mark.asyncio
async def test_hist_force_advances_after_repeated_soft_timeout(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 200_000
    hist_cursor = 100_000
    store.set_logwatch_cursor(hist_cursor)
    store.set_logwatch_live_cursor(tip)  # live near tip — old code only nudged then
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_confirmations=0,
        logwatch_max_span=3_000,
        logwatch_catchup_span=1_500,
        logwatch_catchup_chunks_per_pass=1,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    # Live far behind → old nudge path refused; force-advance must still move.
    store.set_logwatch_live_cursor(tip - 40_000)
    runner._last_known_tip = tip

    async def always_timeout(*_a, **_k):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    rpc._prefer_non_alchemy = MagicMock(return_value=True)
    rpc._bind_url = MagicMock()
    rpc.rpc_url = "https://rpc.mainnet.chain.robinhood.com"
    rpc.active_rpc_label = MagicMock(return_value="https://rpc…")
    with patch.object(runner, "_logwatch_scan_window", side_effect=always_timeout):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            with patch("app.chain.reset_followup_rpc_pressure", return_value=0):
                ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    assert store.get_logwatch_cursor() > hist_cursor
    assert runner._last_hist_advanced is True


@pytest.mark.asyncio
async def test_hist_fail_does_not_degrade_when_live_near_tip(tmp_path):
    """Hist hard-fail while live covers tip must not flip DEGRADED / GMGN."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_cursor(50_000)
    store.set_logwatch_live_cursor(tip - 100)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_fail_threshold=3,
        interval_sec=0,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    runner._logwatch_fail_streak = 2

    async def hard_fail_pass(_cfg, *, rpc):
        return False

    rpc_mod = MagicMock()
    rpc_mod.active_rpc_label = MagicMock(return_value="https://rpc…")
    with patch.object(runner, "_logwatch_pass", side_effect=hard_fail_pass):
        with patch.object(runner, "_ops_alert", new_callable=AsyncMock) as ops:
            with patch("app.chain.RpcClient", return_value=rpc_mod):
                with patch.object(
                    runner, "_maybe_alert_cursor_lag", new_callable=AsyncMock
                ):
                    await runner._cycle_body(cfg, force_all_due=False)
    assert runner._logwatch_degraded is False
    assert runner._logwatch_fail_streak < 3
    ops.assert_not_called()



@pytest.mark.asyncio
async def test_live_gap_near_tip_still_enriches(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    # Gap of ~800 blocks: inside tip_from(=99701) below live window, under enrich cap.
    store.set_logwatch_live_cursor(99_200)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        live_gap_enrich_max_blocks=2_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    gap_skip: list[bool] = []

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
        if label == "live_gap":
            gap_skip.append(skip_enrich)
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
    assert gap_skip and gap_skip[0] is False


def test_deal_is_fresh_for_alert_age_and_block():
    from app.followup import deal_is_fresh_for_alert
    import time as _t

    now = _t.time()
    tip = 100_000
    assert deal_is_fresh_for_alert(
        bought_at=now - 60,
        block_number=99_900,
        tip=tip,
        now=now,
        max_buy_age_sec=900,
        max_block_lag=2_000,
    )
    assert not deal_is_fresh_for_alert(
        bought_at=now - 2_000,
        block_number=99_900,
        tip=tip,
        now=now,
        max_buy_age_sec=900,
        max_block_lag=2_000,
    )
    assert not deal_is_fresh_for_alert(
        bought_at=now - 60,
        block_number=90_000,
        tip=tip,
        now=now,
        max_buy_age_sec=900,
        max_block_lag=2_000,
    )


def test_live_tip_healthy_rejects_far_behind_despite_success_ts(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(50_000)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    ok, behind = runner._live_tip_healthy(tip=tip)
    assert ok is False
    assert behind == 50_000
