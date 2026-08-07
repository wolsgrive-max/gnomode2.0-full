"""Resilience: pending-alert retry, notify rollback, cursor safety."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from app.followup import FollowupRunner, should_alert_deal
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


def test_unmark_notified_allows_retry(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    token = "0xbbb0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=token,
                token_symbol="T1",
                bought_tokens=1.0,
                bought_usd=50.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xtx1",
            )
        ],
        max_deals=5,
    )
    deal = store.record_deal(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=8_000.0,
        bought_usd=60.0,
        max_deals=5,
    )
    assert deal is not None
    assert store.mark_notified(deal.wallet, deal.token) is True
    assert store.mark_notified(deal.wallet, deal.token) is False
    store.unmark_notified(deal.wallet, deal.token)
    assert store.mark_notified(deal.wallet, deal.token) is True


def test_list_pending_alert_deals(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xbbb0000000000000000000000000000000000001",
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    d2 = store.record_deal(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=None,  # quote was down
        bought_usd=55.0,
        max_deals=5,
    )
    assert d2 is not None
    pending = store.list_pending_alert_deals(alert_on_deals=[2, 3], limit=10)
    assert len(pending) == 1
    assert pending[0].token == d2.token
    assert pending[0].mcap_at_buy is None


@pytest.mark.asyncio
async def test_deliver_deal_alert_enqueues_and_survives_tg_failure(tmp_path):
    """Transactional outbox: a TG failure must not drop the alert.

    The deal is claimed + enqueued atomically; a failed immediate send leaves
    the row ``pending`` so the dispatcher redelivers it once Telegram recovers.
    """
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xbbb0000000000000000000000000000000000001",
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    deal = store.record_deal(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=9_000.0,
        bought_usd=70.0,
        max_deals=5,
    )
    assert deal is not None
    runner = FollowupRunner(store=store)

    with patch(
        "app.followup.send_followup_deal",
        AsyncMock(side_effect=RuntimeError("tg down")),
    ):
        ok = await runner._deliver_deal_alert(
            "-1001",
            deal=deal,
            topic_id=None,
        )
    # Enqueued (not lost) even though the immediate send failed.
    assert ok is True
    stats = store.outbox_stats()
    assert stats["pending"] == 1
    assert stats["sent"] == 0
    # Deal is claimed, so it won't double-fire via the deal path.
    assert store.mark_notified(deal.wallet, deal.token) is False

    # Telegram recovers → dispatcher redelivers the pending row.
    import time as _t

    ok_send = AsyncMock(return_value=None)
    with patch("app.followup.send_followup_deal", ok_send):
        delivered = await runner._dispatch_outbox(
            store.load_config(), now=_t.time() + 10_000
        )
    assert delivered == 1
    assert ok_send.await_count == 1
    stats2 = store.outbox_stats()
    assert stats2["pending"] == 0
    assert stats2["sent"] == 1


@pytest.mark.asyncio
async def test_retry_pending_alerts_refills_mcap_and_sends(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            max_mcap_alert=20_000,
            alert_on_deals=[2, 3],
            telegram_chat_id="-1001",
            logwatch_enabled=False,
        )
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token="0xbbb0000000000000000000000000000000000001",
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    deal = store.record_deal(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        token_symbol="T2",
        mcap_at_buy=None,
        bought_usd=70.0,
        tx_hash="0xabc",
        max_deals=5,
    )
    assert deal is not None
    assert should_alert_deal(
        2, None, max_mcap_alert=20_000, alert_on_deals=[2, 3], bought_usd=70
    ) is False

    runner = FollowupRunner(store=store)
    sent = AsyncMock(return_value=False)

    with (
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch("app.followup.resolve_topic_id", return_value=None),
        patch("app.followup.send_followup_deal", sent),
        patch(
            "app.followup.estimate_token_quote",
            AsyncMock(return_value=(8_000.0, 0.01)),
        ),
    ):
        n = await runner._retry_pending_alerts(store.load_config(), rpc=None)

    assert n == 1
    assert sent.await_count == 1
    rows = store.list_deals_for_wallet(wallet)
    t2 = next(r for r in rows if r["token"] == deal.token)
    # mcap was filled in
    assert float(t2.get("mcap_at_buy") or 0) == 8_000.0


@pytest.mark.asyncio
async def test_logwatch_failure_triggers_legacy_and_sets_degraded(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            logwatch_enabled=True,
            logwatch_fail_threshold=1,
            safety_reconcile_sec=3600,
            prune_enabled=False,
            telegram_chat_id="",
        )
    )
    store.ingest_buyers(
        [
            BuyerRow(
                wallet="0xaaa0000000000000000000000000000000000001",
                token="0xbbb0000000000000000000000000000000000001",
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    runner = FollowupRunner(store=store)
    # Force live tip unhealthy via store cursors (no RPC probe).
    store.set_logwatch_live_cursor(1_000)
    runner._last_known_tip = 100_000
    runner._last_live_success_ts = 0.0
    legacy = AsyncMock()
    with (
        patch.object(runner, "_backfill_done", True),
        patch.object(
            runner, "_logwatch_pass", AsyncMock(side_effect=RuntimeError("fatal boom"))
        ),
        patch.object(runner, "_legacy_scan_pass", legacy),
        patch.object(runner, "_retry_pending_alerts", AsyncMock(return_value=0)),
        patch.object(runner, "_maybe_prune", AsyncMock(return_value=0)),
        patch.object(runner, "_balance_only_pass", AsyncMock()),
        patch.object(runner, "_maybe_chain_backfill", AsyncMock()),
    ):
        cfg = store.load_config()
        await runner.run_cycle(cfg)
        assert legacy.await_count == 0
        assert runner._maintenance_wake.is_set()
        await runner._maintenance_pass(cfg)

    assert legacy.await_count == 1
    st = runner.status()
    assert st.logwatch_degraded is True
    assert st.logwatch_fail_streak >= 1
    assert any(e.stage == "fallback" for e in st.log)


@pytest.mark.asyncio
async def test_logwatch_fail_with_healthy_live_skips_ops_and_legacy(tmp_path):
    """Hist hard-fail while live tip is fine must not spam TG or GMGN."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            logwatch_enabled=True,
            logwatch_fail_threshold=1,
            safety_reconcile_sec=3600,
            prune_enabled=False,
            telegram_chat_id="-1001",
        )
    )
    runner = FollowupRunner(store=store)
    store.set_logwatch_live_cursor(99_950)
    runner._last_known_tip = 100_000
    runner._last_live_success_ts = time.time()
    legacy = AsyncMock()
    ops = AsyncMock()
    with (
        patch.object(runner, "_backfill_done", True),
        patch.object(
            runner, "_logwatch_pass", AsyncMock(side_effect=RuntimeError("fatal boom"))
        ),
        patch.object(runner, "_legacy_scan_pass", legacy),
        patch.object(runner, "_retry_pending_alerts", AsyncMock(return_value=0)),
        patch.object(runner, "_maybe_prune", AsyncMock(return_value=0)),
        patch.object(runner, "_balance_only_pass", AsyncMock()),
        patch.object(runner, "_maybe_chain_backfill", AsyncMock()),
        patch.object(runner, "_ops_alert", ops),
    ):
        cfg = store.load_config()
        await runner.run_cycle(cfg)
        await runner._maintenance_pass(cfg)

    assert runner.status().logwatch_degraded is False
    assert legacy.await_count == 0
    ops.assert_not_awaited()
    assert any(
        "без DEGRADED" in (e.message or "") for e in runner.status().log
    )


@pytest.mark.asyncio
async def test_retryable_logwatch_exception_does_not_increment_streak(tmp_path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            logwatch_enabled=True,
            logwatch_fail_threshold=1,
            prune_enabled=False,
            telegram_chat_id="",
        )
    )
    runner = FollowupRunner(store=store)
    with (
        patch.object(runner, "_backfill_done", True),
        patch.object(
            runner,
            "_logwatch_pass",
            AsyncMock(side_effect=RuntimeError("503 Service Unavailable")),
        ),
        patch.object(runner, "_maybe_chain_backfill", AsyncMock()),
        patch.object(runner, "_dispatch_outbox", AsyncMock(return_value=0)),
        patch.object(runner, "_maybe_alert_cursor_lag", AsyncMock()),
    ):
        await runner.run_cycle(store.load_config())

    assert runner.status().logwatch_fail_streak == 0
    assert runner.status().logwatch_degraded is False


@pytest.mark.asyncio
async def test_logwatch_soft_fail_below_threshold_skips_legacy(tmp_path):
    """Single hard-fail below threshold must not DEGRADE or stampede GMGN."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(
        FollowupConfig(
            enabled=True,
            logwatch_enabled=True,
            logwatch_fail_threshold=3,
            safety_reconcile_sec=3600,
            prune_enabled=False,
            telegram_chat_id="",
        )
    )
    runner = FollowupRunner(store=store)
    runner._last_live_success_ts = time.time()
    runner._last_known_tip = 100_000
    store.set_logwatch_live_cursor(99_980)
    legacy = AsyncMock()
    with (
        patch.object(runner, "_backfill_done", True),
        patch.object(
            runner, "_logwatch_pass", AsyncMock(side_effect=RuntimeError("fatal boom"))
        ),
        patch.object(runner, "_legacy_scan_pass", legacy),
        patch.object(runner, "_retry_pending_alerts", AsyncMock(return_value=0)),
        patch.object(runner, "_maybe_prune", AsyncMock(return_value=0)),
        patch.object(runner, "_balance_only_pass", AsyncMock()),
        patch.object(runner, "_maybe_chain_backfill", AsyncMock()),
    ):
        cfg = store.load_config()
        await runner.run_cycle(cfg)
        await runner._maintenance_pass(cfg)

    assert legacy.await_count == 0
    st = runner.status()
    assert st.logwatch_degraded is False
    assert st.logwatch_fail_streak == 1
    assert any("hard-fail" in (e.message or "") for e in st.log)


@pytest.mark.asyncio
async def test_retryable_getlogs_error_is_soft_not_hard(tmp_path):
    """Alchemy 400/503 must not hard-fail hist (no DEGRADED streak)."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    store.save_config(FollowupConfig(enabled=True, logwatch_enabled=True))
    runner = FollowupRunner(store=store)
    cfg = store.load_config()
    with patch(
        "app.followup.fetch_inbound_transfers",
        AsyncMock(side_effect=RuntimeError("400 Bad Request")),
    ):
        res = await runner._logwatch_scan_window(
            cfg,
            rpc=AsyncMock(),
            watching=["0xaaa0000000000000000000000000000000000001"],
            from_block=1,
            to_block=10,
            fetch_timeout=5.0,
            label="hist",
            skip_enrich=True,
        )
    assert res is not None
    assert res["advanced"] is False
    assert res["new_deals"] == 0
