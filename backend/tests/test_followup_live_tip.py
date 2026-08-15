"""Live tip loop: fresh buys alert without waiting on hist catch-up."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.followup import FollowupRunner
from app.followup_store import FollowupStore
from app.gmgn_portfolio import GmgnBuy
from app.models import BuyerRow, FollowupConfig


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
        soft_partial=False,
        tip_deadline_mono=None,
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
async def test_tip_estimates_forward_when_block_number_times_out(tmp_path):
    """Stale tip must not freeze the scan on the same 4 blocks forever."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    verified = 100_000
    store.set_logwatch_live_cursor(verified - 10)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = verified
    runner._last_tip_ts = time.time() - 30.0  # 30s since verified
    scanned: list[tuple[int, int]] = []

    async def fake_scan(*_a, **k):
        scanned.append((int(k["from_block"]), int(k["to_block"])))
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": k["to_block"],
            "incomplete": False,
            "fetched": 0,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(side_effect=TimeoutError())
    rpc._prefer_non_alchemy = MagicMock(return_value=True)
    rpc._bind_url = MagicMock()
    rpc.active_rpc_label = MagicMock(return_value="public")
    with (
        patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan),
        patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ),
        patch("app.chain.reset_followup_rpc_pressure", return_value=0),
    ):
        await runner._live_tip_pass(cfg, rpc=rpc)
    assert scanned
    _frm, to_b = scanned[0]
    # Estimated tip ≈ verified + 30*2 = 100060 — may scan ahead…
    assert to_b > verified
    # …but durable cursor must not advance past last verified tip.
    assert int(store.get_logwatch_live_cursor() or 0) <= verified
    # Verified tip/ts must not be overwritten by the estimate.
    assert runner._last_known_tip == verified
    assert time.time() - runner._last_tip_ts >= 25.0


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
        soft_partial=False,
        tip_deadline_mono=None,
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
    # Huge lag fast-forwards past stale TG window; leftover near tip.
    assert store.get_logwatch_cursor() >= tip - 2_500
    first_span = scanned[0][3] - scanned[0][2] + 1
    assert first_span != 800
    assert any(
        "fast-forward" in (getattr(e, "message", None) or "")
        for e in (runner._log or [])
    )
    assert store.get_logwatch_cursor() > hist_cursor


@pytest.mark.asyncio
async def test_live_gap_backfill_when_cursor_far_behind(tmp_path):
    """Far-behind watermark jump lives on catchup — not the tip tick."""
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

    async def fake_scan(*_a, **k):
        labels.append(k.get("label") or "")
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": k["to_block"],
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    rpc._prefer_non_alchemy = MagicMock()
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            # Tip tick must NOT jump — only scan tip window.
            await runner._live_tip_pass(cfg, rpc=rpc)
            tip_cursor = int(store.get_logwatch_live_cursor() or 0)
            assert tip_cursor == 50_000
            assert "live" in labels
            assert "burst_jump" not in labels and "live_jump" not in labels
            # Catchup owns the watermark jump.
            await runner._live_catchup_pass(cfg, rpc=rpc)
    live_now = int(store.get_logwatch_live_cursor() or 0)
    assert live_now >= tip - 2_100
    assert live_now <= tip


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
async def test_live_near_tip_only_enriches_tip_window(tmp_path):
    """Live tip tick is tip-window only — gap park deferred to catchup."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
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
    labels: list[str] = []

    async def fake_scan(*_a, **k):
        labels.append(k.get("label") or "")
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": k["to_block"],
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
    assert labels == ["live"]
    assert not any(lab.startswith("live_burst") or lab == "live_gap" for lab in labels)
    # Tip does not park over the gap — cursor stays until catchup.
    assert int(store.get_logwatch_live_cursor() or 0) == 99_200


@pytest.mark.asyncio
async def test_tip_success_stamps_healthy_despite_catchup_lag(tmp_path):
    """Tip-window ok must stamp success even when watermark is tip-only-lagged."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(95_500)  # behind=4500 → tip-only
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        live_gap_enrich_max_blocks=2_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_live_success_ts = 0.0

    async def tip_ok(*_a, **k):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": k["to_block"],
            "incomplete": False,
            "fetched": 0,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=tip_ok):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_tip_pass(cfg, rpc=rpc)
    assert runner._last_live_success_ts > 0
    ok, behind = runner._live_tip_healthy(tip=tip)
    assert ok is True
    assert behind == 4_500


@pytest.mark.asyncio
async def test_hist_fail_streak_capped(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_cursor(50_000)
    store.set_logwatch_live_cursor(50_000)  # extreme behind, tip not recent
    cfg = FollowupConfig(
        enabled=True,
        logwatch_enabled=True,
        logwatch_fail_threshold=8,
        interval_sec=0,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = 0.0
    runner._logwatch_fail_streak = 47

    async def hard_fail_pass(_cfg, *, rpc):
        return False

    rpc_mod = MagicMock()
    rpc_mod.active_rpc_label = MagicMock(return_value="https://rpc…")
    with patch.object(runner, "_logwatch_pass", side_effect=hard_fail_pass):
        with patch.object(runner, "_ops_alert", new_callable=AsyncMock):
            with patch("app.chain.RpcClient", return_value=rpc_mod):
                with patch.object(
                    runner, "_maybe_alert_cursor_lag", new_callable=AsyncMock
                ):
                    await runner._cycle_body(cfg, force_all_due=False)
    assert runner._logwatch_fail_streak == 48


@pytest.mark.asyncio
async def test_tip_partial_empty_increments_streak_no_park(tmp_path):
    """Incomplete empty getLogs must not reset streak or skip the gap."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    # Far enough that tip window is not contiguous with live cursor.
    store.set_logwatch_live_cursor(tip - 500)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        live_gap_enrich_max_blocks=2_000,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._live_timeout_streak = 0

    async def incomplete_empty(*_a, **_k):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
            "incomplete": True,
            "fetched": 0,
        }

    wallets = [f"0x{i:040x}" for i in range(250)]
    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=incomplete_empty):
        with patch.object(store, "list_watching", return_value=wallets):
            await runner._live_tip_pass(cfg, rpc=rpc)
    assert runner._live_timeout_streak == 1
    # Must NOT park over the unscanned gap while hist may still catch it.
    assert int(store.get_logwatch_live_cursor() or 0) == tip - 500


@pytest.mark.asyncio
async def test_soft_fail_does_not_stamp_live_success(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(99_990)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        buys_only=False,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._last_live_success_ts = 0.0

    async def soft_fail(*_a, **_k):
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
            "incomplete": False,
            "fetched": 0,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    with patch.object(runner, "_logwatch_scan_window", side_effect=soft_fail):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_tip_pass(cfg, rpc=rpc)
    assert runner._live_timeout_streak == 1
    assert runner._last_live_success_ts == 0.0


@pytest.mark.asyncio
async def test_catchup_drains_pending_even_when_tip_soft_failing(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(enabled=True, buys_only=False)
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._live_timeout_streak = 5
    drained = {"n": 0}

    async def fake_drain(*_a, **_k):
        drained["n"] += 1
        return {"new_deals": 0, "alerts": 0}

    from app.followup_logwatch import InboundTransfer

    runner._pending_skip_transfers = [
        (
            InboundTransfer(
                wallet="0xaaa0000000000000000000000000000000000001",
                token="0xbbb0000000000000000000000000000000000002",
                sender="0xccc0000000000000000000000000000000000003",
                tx_hash="0xabc",
                block_number=99_900,
                bought_at=__import__("time").time() - 10,
            ),
            __import__("time").time(),
        )
    ]
    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=100_000)
    with patch.object(runner, "_drain_pending_skip_transfers", side_effect=fake_drain):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_catchup_pass(cfg, rpc=rpc)
    assert drained["n"] == 1


def test_queue_skip_enrich_preserves_queued_at(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    runner = FollowupRunner(store=store)
    from app.followup_logwatch import InboundTransfer
    import time as _t

    tr = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xbbb0000000000000000000000000000000000002",
        sender="0xccc0000000000000000000000000000000000003",
        tx_hash="0xabc",
        block_number=99_900,
        bought_at=_t.time() - 10,
    )
    first = _t.time() - 100
    runner._pending_skip_transfers = [(tr, first)]
    runner._queue_skip_enrich_transfers([tr])
    assert len(runner._pending_skip_transfers) == 1
    assert runner._pending_skip_transfers[0][1] == first


def test_live_span_tiny_for_heavy_watchlist():
    from app.followup import live_span_for_watchlist

    assert live_span_for_watchlist(300, 634) <= 16
    assert live_span_for_watchlist(300, 150) <= 40
    assert live_span_for_watchlist(300, 50) == 300


def test_alert_filter_skip_reason_mcap_cap():
    from app.followup import alert_filter_skip_reason, should_alert_deal

    gate = dict(
        max_mcap_alert=30_000.0,
        alert_on_deals=[2, 3, 4, 5],
        min_mcap_alert=None,
        min_bought_usd=None,
        max_bought_usd=None,
    )
    assert should_alert_deal(2, 39_437.0, bought_usd=None, **gate) is False
    why = alert_filter_skip_reason(2, 39_437.0, bought_usd=None, **gate)
    assert why and "mcap=" in why and "max=" in why


@pytest.mark.asyncio
async def test_hist_soft_mode_when_tip_soft_fail_streak(tmp_path):
    """Tip soft-fail must not freeze hist — catch-up continues with a budget cap."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_cursor(50_000)
    store.set_logwatch_live_cursor(tip - 100)
    cfg = FollowupConfig(enabled=True, logwatch_enabled=True, buys_only=False)
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    runner._live_timeout_streak = 2
    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    scanned: list[str] = []

    async def fake_scan(*_a, **k):
        scanned.append(k.get("label") or "")
        # Advance cursor so the pass exits after one chunk.
        store.set_logwatch_cursor(tip)
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": tip,
        }

    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            ok = await runner._logwatch_pass(cfg, rpc=rpc)
    assert ok is True
    assert scanned  # soft-mode still clears lag
    assert any(
        "hist soft-mode" in (getattr(e, "message", None) or "")
        for e in (runner._log or [])
    )


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
    # Lag recovery only loosens *block* lag — old bought_at still rejects.
    assert not deal_is_fresh_for_alert(
        bought_at=now - 1_200,
        block_number=99_500,
        tip=tip,
        now=now,
        max_buy_age_sec=900,
        max_block_lag=2_000,
        discovered_at=now - 30,
    )
    # Recent buy with large block lag can still pass via discovered_at.
    assert deal_is_fresh_for_alert(
        bought_at=now - 60,
        block_number=96_000,
        tip=tip,
        now=now,
        max_buy_age_sec=900,
        max_block_lag=2_000,
        discovered_at=now - 30,
    )
    # GMGN ghost: no buy time and no block — never "fresh".
    assert not deal_is_fresh_for_alert(
        bought_at=None,
        block_number=0,
        tip=tip,
        now=now,
        discovered_at=now,
    )


@pytest.mark.asyncio
async def test_live_tip_enriches_even_when_behind_enrich_cap(tmp_path):
    """Regression: tip growth after burst must not suppress tip enrich forever."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    # Just outside enrich_cap — old code tip-skipped and never alerted.
    store.set_logwatch_live_cursor(tip - 2_500)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=300,
        live_gap_enrich_max_blocks=2_000,
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
        soft_partial=False,
        tip_deadline_mono=None,
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
    assert "live" in labels  # tip enrich must run (tip-first)


@pytest.mark.asyncio
async def test_skip_enrich_queues_transfers_for_live_drain(tmp_path):
    from app.followup_logwatch import InboundTransfer

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    tr = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xbbb0000000000000000000000000000000000002",
        sender="0xccc0000000000000000000000000000000000003",
        tx_hash="0xabc",
        block_number=99_800,
        bought_at=__import__("time").time() - 30,
    )
    runner._queue_skip_enrich_transfers([tr])
    assert len(runner._pending_skip_transfers) == 1


def test_live_tip_healthy_rejects_extreme_behind_despite_success_ts(tmp_path):
    """Extreme watermark lag (>> catch-up) stays unhealthy even with a tip stamp."""
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


def test_live_tip_healthy_ok_with_catchup_lag_when_tip_recent(tmp_path):
    """Tip-only catch-up lag (~4–8k) must not false-flag DEGRADED when tip scans."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(95_500)  # behind=4500
    runner = FollowupRunner(store=store)
    runner._last_known_tip = tip
    runner._last_live_success_ts = __import__("time").time()
    ok, behind = runner._live_tip_healthy(tip=tip)
    assert ok is True
    assert behind == 4_500


@pytest.mark.asyncio
async def test_live_dead_zone_2k_to_8k_bursts_not_bare_skip(tmp_path):
    """behind in (enrich_cap, old live_span×10] must burst — no tip-skip forever."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    # live_span=800 → old burst_behind=8000; enrich_cap=2000; behind=3000 was dead zone
    store.set_logwatch_live_cursor(tip - 3_000)
    cfg = FollowupConfig(
        enabled=True,
        logwatch_confirmations=0,
        logwatch_live_span=800,
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
        soft_partial=False,
        tip_deadline_mono=None,
    ):
        labels.append((label, skip_enrich))
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": to_block,
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    rpc._prefer_non_alchemy = MagicMock()
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_tip_pass(cfg, rpc=rpc)
            # Tip-only: still enriches tip; no burst on tip tick.
            assert any(lab == "live" and not skip for lab, skip in labels)
            assert not any(lab.startswith("live_burst") for lab, _skip in labels)
            assert int(store.get_logwatch_live_cursor() or 0) == tip - 3_000
            await runner._live_catchup_pass(cfg, rpc=rpc)
    live_now = int(store.get_logwatch_live_cursor() or 0)
    assert live_now >= tip - 2_100
    # Catchup must not stall at tip-3000 (old dead zone bare tip-skip).
    assert store.get_logwatch_live_cursor() > tip - 3_000


@pytest.mark.asyncio
async def test_catchup_advances_watermark_even_when_tip_soft_fail(tmp_path):
    """Tip soft-fail must NOT pause skip-enrich watermark catch-up (death spiral)."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    tip = 100_000
    store.set_logwatch_live_cursor(tip - 5_000)
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
    runner._live_timeout_streak = 3
    labels: list[str] = []

    async def fake_scan(*_a, **k):
        labels.append(str(k.get("label") or ""))
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": True,
            "advance_to": k["to_block"],
        }

    rpc = MagicMock()
    rpc.block_number = AsyncMock(return_value=tip)
    rpc._prefer_non_alchemy = MagicMock()
    with patch.object(runner, "_logwatch_scan_window", side_effect=fake_scan):
        with patch.object(
            store,
            "list_watching",
            return_value=["0xaaa0000000000000000000000000000000000001"],
        ):
            await runner._live_catchup_pass(cfg, rpc=rpc)
    assert int(store.get_logwatch_live_cursor() or 0) > tip - 5_000
    assert any("burst" in lab or lab == "live_burst" or "burst" in lab for lab in labels) or (
        int(store.get_logwatch_live_cursor() or 0) >= tip - 2_100
    )


def test_purge_stale_pending_tip_transfers(tmp_path):
    import time as _t

    from app.followup_logwatch import InboundTransfer

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(
        enabled=True,
        alert_max_buy_age_sec=900,
        alert_max_block_lag=4_000,
    )
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    tip = 100_000
    runner._last_known_tip = tip
    now = _t.time()
    stale = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xbbb0000000000000000000000000000000000001",
        sender="0xccc0000000000000000000000000000000000001",
        tx_hash="0xdead",
        block_number=tip - 50_000,
        bought_at=now - 7_200,
    )
    fresh = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xddd0000000000000000000000000000000000001",
        sender="0xccc0000000000000000000000000000000000001",
        tx_hash="0xbeef",
        block_number=tip - 100,
        bought_at=now - 30,
    )
    runner._pending_skip_transfers = [(stale, now - 60), (fresh, now - 10)]
    with patch("app.gmgn_portfolio.gmgn_circuit_open", return_value=False):
        n = runner._purge_stale_pending_transfers(cfg=cfg)
    assert n == 1
    assert len(runner._pending_skip_transfers) == 1
    assert runner._pending_skip_transfers[0][0].token == fresh.token


def test_purge_skips_buy_age_while_gmgn_circuit_open(tmp_path):
    """Circuit freeze must not let buy-age purge kill tip waits."""
    import time as _t

    from app.followup_logwatch import InboundTransfer

    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(enabled=True, alert_max_buy_age_sec=900)
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    tip = 100_000
    runner._last_known_tip = tip
    now = _t.time()
    waiting = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xbbb0000000000000000000000000000000000001",
        sender="0xccc0000000000000000000000000000000000001",
        tx_hash="0xdead",
        block_number=tip - 50,
        bought_at=now - 1_200,  # past max_age, would purge if circuit closed
    )
    runner._pending_skip_transfers = [(waiting, now - 10)]
    with patch("app.gmgn_portfolio.gmgn_circuit_open", return_value=True):
        n = runner._purge_stale_pending_transfers(cfg=cfg)
    assert n == 0
    assert len(runner._pending_skip_transfers) == 1


@pytest.mark.asyncio
async def test_sync_gmgn_overlays_tip_chain_block(tmp_path):
    """Tip token already in GMGN post_seed must keep chain block_number."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    tip_tok = "0xccc0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=10.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(enabled=True, max_deals=5)
    runner = FollowupRunner(store=store)
    post = [
        GmgnBuy(tip_tok, "TIP", "0xgmgn", 1_700_000_000),
    ]
    inserted = await runner._sync_wallet_gmgn_order(
        wallet,
        cfg,
        post_seed=post,
        tip_token=tip_tok,
        tip_symbol="TIP",
        tip_tx="0xchain",
        tip_block=99_999,
        tip_bought_at=1_700_000_050.0,
        tip_mcap=8_000.0,
        tip_bought_usd=40.0,
    )
    rows = {d["token"]: d for d in store.list_deals_for_wallet(wallet)}
    assert tip_tok in rows
    assert int(rows[tip_tok]["block_number"] or 0) == 99_999
    assert str(rows[tip_tok]["tx_hash"] or "") == "0xchain"
    assert inserted  # newly inserted tip


@pytest.mark.asyncio
async def test_live_tick_micro_drains_pending(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(enabled=True, logwatch_enabled=True, buys_only=False)
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    from app.followup_logwatch import InboundTransfer

    now = time.time()
    runner._pending_skip_transfers = [
        (
            InboundTransfer(
                wallet="0xaaa0000000000000000000000000000000000001",
                token="0xbbb0000000000000000000000000000000000002",
                sender="0xccc0000000000000000000000000000000000003",
                tx_hash="0xabc",
                block_number=99_900,
                bought_at=now - 10,
            ),
            now - 5,
        )
    ]
    drained: list[dict] = []

    async def fake_drain(*_a, **kwargs):
        drained.append(kwargs)
        return {"new_deals": 0, "alerts": 0}

    with patch.object(runner, "_live_tip_pass", new=AsyncMock()):
        with patch.object(
            runner, "_drain_pending_skip_transfers", side_effect=fake_drain
        ):
            with patch.object(
                runner, "_micro_retry_pending_mcap", new=AsyncMock()
            ):
                with patch.object(
                    runner, "_dispatch_outbox", new=AsyncMock(return_value=0)
                ):
                    await runner._live_tick(cfg)
    assert len(drained) == 1
    assert drained[0].get("max_batch") == 3
    assert float(drained[0].get("enrich_budget_sec") or 0) >= 2.0
    assert float(drained[0].get("enrich_budget_sec") or 0) <= 4.0


def test_queue_skip_enrich_drops_known_tokens(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    known_tok = "0xbbb0000000000000000000000000000000000002"
    fresh_tok = "0xddd0000000000000000000000000000000000004"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=known_tok,
                token_symbol="KNOWN",
                bought_tokens=1.0,
                bought_usd=10.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    from app.followup_logwatch import InboundTransfer

    now = time.time()
    known_tr = InboundTransfer(
        wallet=wallet,
        token=known_tok,
        sender="0xccc0000000000000000000000000000000000003",
        tx_hash="0xknown",
        block_number=99_900,
        bought_at=now - 10,
    )
    fresh_tr = InboundTransfer(
        wallet=wallet,
        token=fresh_tok,
        sender="0xccc0000000000000000000000000000000000003",
        tx_hash="0xfresh",
        block_number=99_950,
        bought_at=now - 5,
    )
    runner._pending_skip_transfers = [(known_tr, now - 20)]
    runner._queue_skip_enrich_transfers([known_tr, fresh_tr])
    tokens = {t.token.lower() for t, _ in runner._pending_skip_transfers}
    assert known_tok not in tokens
    assert fresh_tok in tokens


@pytest.mark.asyncio
async def test_drain_prioritizes_tip_proximal_over_fifo(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    cfg = FollowupConfig(enabled=True, buys_only=False)
    store.save_config(cfg)
    runner = FollowupRunner(store=store)
    tip = 100_000
    runner._last_known_tip = tip
    from app.followup_logwatch import InboundTransfer

    now = time.time()
    old_hist = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xbbb0000000000000000000000000000000000001",
        sender="0xccc0000000000000000000000000000000000001",
        tx_hash="0xold",
        block_number=tip - 2_000,
        bought_at=now - 500,
    )
    tip_miss = InboundTransfer(
        wallet="0xaaa0000000000000000000000000000000000001",
        token="0xddd0000000000000000000000000000000000002",
        sender="0xccc0000000000000000000000000000000000001",
        tx_hash="0xtip",
        block_number=tip - 20,
        bought_at=now - 15,
    )
    # Hist ghost queued earlier (would win pure FIFO).
    runner._pending_skip_transfers = [
        (old_hist, now - 200),
        (tip_miss, now - 5),
    ]
    seen: list[str] = []

    async def fake_process(batch, **_kwargs):
        seen.extend(t.token for t in batch)
        return {"new_deals": 0, "alerts": 0, "skipped": 0, "advanced": True}

    with patch(
        "app.gmgn_portfolio.gmgn_circuit_open", return_value=False
    ):
        with patch.object(
            runner, "_process_logwatch_transfers", side_effect=fake_process
        ):
            await runner._drain_pending_skip_transfers(
                cfg, rpc=MagicMock(), max_batch=1
            )
    assert seen == [tip_miss.token]
