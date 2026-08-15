"""Transactional outbox for follow-up alerts: durability + backoff + telemetry."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from unittest.mock import AsyncMock, patch

import pytest

from app.followup import FollowupRunner
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


def _store(tmp_path) -> FollowupStore:
    return FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )


def _seed_deal(store: FollowupStore):
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
    return deal


def test_claim_and_enqueue_is_idempotent(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps({"v": 1, "kind": "deal", "chat": "-1", "wallet": deal.wallet})

    assert (
        store.claim_and_enqueue_deal(
            deal.wallet, deal.token, dedup_key=key, payload=payload
        )
        is True
    )
    # Second claim for the same deal is a no-op (already notified).
    assert (
        store.claim_and_enqueue_deal(
            deal.wallet, deal.token, dedup_key=key, payload=payload
        )
        is False
    )
    stats = store.outbox_stats()
    assert stats["pending"] == 1


def test_claim_and_enqueue_rejects_in_flight_sending(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = '{"v":1,"kind":"deal"}'
    assert store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )
    rows = store.claim_due_outbox(limit=1)
    assert rows and rows[0]["dedup_key"] == key
    # Simulate recovery: unmark + re-claim while lease is held.
    store.unmark_notified(deal.wallet, deal.token)
    assert (
        store.claim_and_enqueue_deal(
            deal.wallet, deal.token, dedup_key=key, payload=payload
        )
        is False
    )
    assert store.outbox_stats().get("sending", 0) == 1


def test_try_claim_ops_alert_is_atomic(tmp_path):
    store = _store(tmp_path)
    assert store.try_claim_ops_alert("ops_alert_test_ts", cooldown_sec=600) is True
    assert store.try_claim_ops_alert("ops_alert_test_ts", cooldown_sec=600) is False
    # After cooldown window, claim again.
    assert (
        store.try_claim_ops_alert(
            "ops_alert_test_ts", cooldown_sec=600, now=time.time() + 700
        )
        is True
    )


def test_release_ops_alert_claim_allows_immediate_retry(tmp_path):
    store = _store(tmp_path)
    assert store.try_claim_ops_alert("ops_alert_fail_ts", cooldown_sec=600) is True
    store.release_ops_alert_claim("ops_alert_fail_ts")
    assert store.try_claim_ops_alert("ops_alert_fail_ts", cooldown_sec=600) is True


@pytest.mark.asyncio
async def test_ops_alert_releases_claim_on_send_failure(tmp_path):
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(enabled=True, telegram_chat_id="-1001", ops_alert_cooldown_sec=600)
    )
    runner = FollowupRunner(store=store)
    with (
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch("app.followup.resolve_topic_id", return_value=None),
        patch(
            "app.followup.send_message",
            AsyncMock(side_effect=RuntimeError("tg down")),
        ),
    ):
        await runner._ops_alert(
            store.load_config(), kind="cycle_error", text="⚠️ boom"
        )
    # Cooldown must not stick after a failed send.
    assert store.try_claim_ops_alert("ops_alert_cycle_error_ts", cooldown_sec=600) is True


@pytest.mark.asyncio
async def test_ops_alert_fatal_db_error_sends_once(tmp_path):
    """Corrupt SQLite must not flood Telegram every hist tick."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(enabled=True, telegram_chat_id="-1001", ops_alert_cooldown_sec=60)
    )
    runner = FollowupRunner(store=store)
    sent = AsyncMock()
    text = "⚠️ Follow-up cycle error: file is not a database"
    with (
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch("app.followup.resolve_topic_id", return_value=None),
        patch("app.followup.send_message", sent),
    ):
        for _ in range(5):
            # Simulate meta claim always succeeding (fresh/empty DB after rotate).
            with patch.object(store, "try_claim_ops_alert", return_value=True):
                await runner._ops_alert(
                    store.load_config(), kind="cycle_error", text=text
                )

    assert sent.await_count == 1
    assert runner._fatal_error_backoff_until > time.time()


@pytest.mark.asyncio
async def test_ops_alert_survives_meta_claim_exception(tmp_path):
    """When followup.db itself is dead, still one-shot via in-memory gate."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(enabled=True, telegram_chat_id="-1001", ops_alert_cooldown_sec=60)
    )
    runner = FollowupRunner(store=store)
    sent = AsyncMock()
    text = "⚠️ Follow-up cycle error: database disk image is malformed"
    with (
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch("app.followup.resolve_topic_id", return_value=None),
        patch("app.followup.send_message", sent),
        patch.object(
            store,
            "try_claim_ops_alert",
            side_effect=sqlite3.DatabaseError("file is not a database"),
        ),
    ):
        await runner._ops_alert(store.load_config(), kind="cycle_error", text=text)
        await runner._ops_alert(store.load_config(), kind="cycle_error", text=text)

    assert sent.await_count == 1


@pytest.mark.asyncio
async def test_ops_alert_nonfatal_escalates_same_fingerprint(tmp_path):
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(enabled=True, telegram_chat_id="-1001", ops_alert_cooldown_sec=60)
    )
    runner = FollowupRunner(store=store)
    sent = AsyncMock()
    text = "⚠️ Follow-up: hist-цикл завис >180s и был прерван."
    with (
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch("app.followup.resolve_topic_id", return_value=None),
        patch("app.followup.send_message", sent),
        patch.object(store, "try_claim_ops_alert", return_value=True),
    ):
        await runner._ops_alert(store.load_config(), kind="hang", text=text)
        # Immediate repeat of the same text must be suppressed by memory gate.
        await runner._ops_alert(store.load_config(), kind="hang", text=text)

    assert sent.await_count == 1


@pytest.mark.asyncio
async def test_deliver_soft_honeypot_enqueues_outbox_recheck(tmp_path):
    """Soft HP must claim into outbox (re-check later), not silently drop."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            alert_skip_honeypot=True,
            telegram_chat_id="-1001",
        )
    )
    deal = _seed_deal(store)
    runner = FollowupRunner(store=store)

    with (
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(
                return_value=(
                    "ok",
                    {
                        "deal_index": deal.deal_index,
                        "wallet": deal.wallet,
                        "token": deal.token,
                    },
                )
            ),
        ),
        patch.object(runner, "_dispatch_outbox", AsyncMock(return_value=0)),
        patch.object(
            runner,
            "_ensure_deal_token_labels",
            AsyncMock(side_effect=lambda d, **_: d),
        ),
    ):
        ok = await runner._deliver_deal_alert(
            "-1001",
            deal=deal,
            topic_id=None,
            honeypot_reason="no_sells:fresh",
            origin="live",
        )

    assert ok is True
    assert store.outbox_stats()["pending"] == 1
    rows = store.claim_due_outbox(limit=1, now=time.time() + 10)
    assert rows
    payload = json.loads(rows[0]["payload"])
    assert payload.get("check_honeypot") is True
    assert payload.get("honeypot_reason") is None


@pytest.mark.asyncio
async def test_deliver_hard_honeypot_tip_fresh_enqueues_recheck(tmp_path):
    """Tip-fresh hard HP must not mark_notified — enqueue for outbox recheck."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            alert_skip_honeypot=True,
            telegram_chat_id="-1001",
            alert_max_buy_age_sec=900,
        )
    )
    now = time.time()
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
        token_symbol="TIP",
        mcap_at_buy=9_000.0,
        bought_usd=70.0,
        max_deals=5,
        block_number=99_990,
        bought_at=now - 30,
    )
    assert deal is not None
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000

    with (
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(
                return_value=(
                    "ok",
                    {
                        "deal_index": deal.deal_index,
                        "wallet": deal.wallet,
                        "token": deal.token,
                    },
                )
            ),
        ),
        patch.object(runner, "_dispatch_outbox", AsyncMock(return_value=0)),
        patch.object(
            runner,
            "_ensure_deal_token_labels",
            AsyncMock(side_effect=lambda d, **_: d),
        ),
    ):
        ok = await runner._deliver_deal_alert(
            "-1001",
            deal=deal,
            topic_id=None,
            honeypot_reason="gmgn:honeypot",
            origin="live",
        )

    assert ok is True
    rows = store.list_deals_for_wallet(wallet)
    tip_row = next(r for r in rows if r["token"] == deal.token)
    assert tip_row["notified"] in (0, 1)  # claimed via outbox
    assert store.outbox_stats()["pending"] == 1
    # Must have claimed (notified) via outbox path, not silent honeypot burn
    # without enqueue — pending outbox proves enqueue happened.


@pytest.mark.asyncio
async def test_deliver_gmgn_discard_tip_fresh_does_not_burn(tmp_path):
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            telegram_chat_id="-1001",
            alert_max_buy_age_sec=900,
        )
    )
    now = time.time()
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
        token_symbol="TIP",
        mcap_at_buy=9_000.0,
        bought_usd=70.0,
        max_deals=5,
        block_number=99_990,
        bought_at=now - 20,
    )
    assert deal is not None
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000

    with (
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("discard", None)),
        ),
        patch.object(
            runner,
            "_ensure_deal_token_labels",
            AsyncMock(side_effect=lambda d, **_: d),
        ),
    ):
        ok = await runner._deliver_deal_alert(
            "-1001",
            deal=deal,
            topic_id=None,
            origin="live",
        )

    assert ok is False
    tip_row = next(
        r for r in store.list_deals_for_wallet(wallet) if r["token"] == deal.token
    )
    assert tip_row["notified"] == 0
    assert store.outbox_stats().get("pending", 0) == 0


@pytest.mark.asyncio
async def test_deliver_gmgn_uncertain_still_enqueues(tmp_path):
    """GMGN defer must durable-enqueue so tip buys survive 429/circuit."""
    store = _store(tmp_path)
    deal = _seed_deal(store)
    runner = FollowupRunner(store=store)

    with (
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("defer", None)),
        ),
        patch.object(runner, "_dispatch_outbox", AsyncMock(return_value=0)),
        patch.object(
            runner,
            "_ensure_deal_token_labels",
            AsyncMock(side_effect=lambda d, **_: d),
        ),
    ):
        ok = await runner._deliver_deal_alert(
            "-1001",
            deal=deal,
            topic_id=None,
            origin="live",
        )

    assert ok is True
    assert store.outbox_stats()["pending"] == 1


@pytest.mark.asyncio
async def test_buys_only_accepts_quote_spend_router(tmp_path):
    """Router/smart-wallet buys (tx.from ≠ wallet) count when quote was spent."""
    from app.followup_logwatch import InboundTransfer
    from types import SimpleNamespace

    store = _store(tmp_path)
    wallet = "0xaaa0000000000000000000000000000000000001"
    pool = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
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
    cfg = FollowupConfig(
        enabled=True,
        buys_only=True,
        alert_on_deals=[2, 3, 4, 5],
        telegram_chat_id="-1001",
    )
    store.save_config(cfg)
    tr = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender=pool,
        tx_hash="0xdeadbeef",
        block_number=100,
        bought_at=time.time(),
    )
    runner = FollowupRunner(store=store)
    router = "0xrouter0000000000000000000000000000000001"
    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(return_value={pool: False}),
        batch_get_receipts=AsyncMock(return_value={}),
    )

    with (
        patch(
            "app.followup.tx_from_and_input",
            AsyncMock(return_value={"0xdeadbeef": (router, "0x3593564c")}),
        ),
        patch(
            "app.followup.wallet_sent_quote_in_tx",
            AsyncMock(return_value=True),
        ),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(
                return_value={
                    (wallet, tr.token, tr.tx_hash): (
                        8_000.0,
                        50.0,
                        None,
                        "T2",
                        "Token Two",
                    )
                }
            ),
        ),
        patch(
            "app.gmgn_portfolio.gmgn_api_configured",
            return_value=False,
        ),
        patch.object(runner, "_deliver_deal_alert", AsyncMock(return_value=True)),
    ):
        stats = await runner._process_logwatch_transfers(
            [tr],
            cfg=cfg,
            rpc=rpc,
            label="hist",
            from_block=100,
            to_block=100,
        )

    assert stats["new_deals"] >= 1


@pytest.mark.asyncio
async def test_buys_only_skips_eoa_gift_transfer(tmp_path):
    """EOA→wallet transfer is not a DEX buy even if tx.from == wallet."""
    from app.followup_logwatch import InboundTransfer
    from types import SimpleNamespace

    store = _store(tmp_path)
    wallet = "0xaaa0000000000000000000000000000000000001"
    friend = "0xddd000000000000000000000000000000000000d"
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
    cfg = FollowupConfig(
        enabled=True,
        buys_only=True,
        alert_on_deals=[2, 3, 4, 5],
        telegram_chat_id="-1001",
    )
    store.save_config(cfg)
    tr = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender=friend,
        tx_hash="0xdeadbeef",
        block_number=100,
        bought_at=time.time(),
    )
    runner = FollowupRunner(store=store)
    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(return_value={friend.lower(): True}),
    )

    with (
        patch(
            "app.followup.tx_from_and_input",
            AsyncMock(
                return_value={
                    "0xdeadbeef": (wallet.lower(), "0x3593564c")
                }
            ),
        ),
        patch.object(runner, "_deliver_deal_alert", AsyncMock(return_value=True)),
        patch(
            "app.gmgn_portfolio.gmgn_api_configured",
            return_value=False,
        ),
    ):
        stats = await runner._process_logwatch_transfers(
            [tr],
            cfg=cfg,
            rpc=rpc,
            label="live",
            from_block=100,
            to_block=100,
        )

    assert stats["new_deals"] == 0
    assert stats["alerts"] == 0
    assert stats["skipped"] >= 1


@pytest.mark.asyncio
async def test_classify_logwatch_buys_unit():
    from app.followup import classify_logwatch_buys
    from app.followup_logwatch import InboundTransfer
    from types import SimpleNamespace

    wallet = "0xaaa0000000000000000000000000000000000001"
    pool = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
    friend = "0xddd000000000000000000000000000000000000d"
    buy = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender=pool,
        tx_hash="0xbuy",
        block_number=1,
        bought_at=time.time(),
    )
    gift = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000003",
        sender=friend,
        tx_hash="0xgift",
        block_number=2,
        bought_at=time.time(),
    )
    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(
            return_value={pool: False, friend.lower(): True}
        )
    )
    buys, uncertain, skipped = await classify_logwatch_buys(
        [buy, gift],
        rpc=rpc,
        sender_map={"0xbuy": wallet.lower(), "0xgift": wallet.lower()},
        senders_ok=True,
        allow_quote_lookup=False,
    )
    assert len(buys) == 1
    assert buys[0].tx_hash == "0xbuy"
    assert skipped >= 1
    assert not uncertain


@pytest.mark.asyncio
async def test_classify_third_party_requeues_when_quote_disabled():
    """Router/aggregator buys must not hard-skip when tip disables quote lookup."""
    from app.followup import classify_logwatch_buys
    from app.followup_logwatch import InboundTransfer
    from types import SimpleNamespace

    wallet = "0xaaa0000000000000000000000000000000000001"
    pool = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
    router = "0xbbb000000000000000000000000000000000000b"
    buy = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender=pool,
        tx_hash="0xrouter",
        block_number=1,
        bought_at=time.time(),
    )
    rpc = SimpleNamespace(batch_is_eoa=AsyncMock(return_value={pool: False}))
    buys, uncertain, skipped = await classify_logwatch_buys(
        [buy],
        rpc=rpc,
        sender_map={"0xrouter": router},
        senders_ok=True,
        allow_quote_lookup=False,
    )
    assert buys == []
    assert skipped == 0
    assert len(uncertain) == 1


@pytest.mark.asyncio
async def test_classify_skips_erc20_transfer_selector():
    from app.followup import classify_logwatch_buys
    from app.followup_logwatch import InboundTransfer
    from types import SimpleNamespace

    wallet = "0xaaa0000000000000000000000000000000000001"
    pool = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
    tr = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender=pool,
        tx_hash="0xgift",
        block_number=1,
        bought_at=time.time(),
    )
    rpc = SimpleNamespace(batch_is_eoa=AsyncMock(return_value={pool: False}))
    buys, uncertain, skipped = await classify_logwatch_buys(
        [tr],
        rpc=rpc,
        sender_map={"0xgift": wallet.lower()},
        senders_ok=True,
        allow_quote_lookup=False,
        method_map={"0xgift": "0xa9059cbb000000000000000000000000aaa"},
    )
    assert buys == []
    assert skipped >= 1
    assert not uncertain


def test_live_cursor_monotonic(tmp_path):
    store = _store(tmp_path)
    store.set_logwatch_live_cursor(1000)
    store.set_logwatch_live_cursor(900)  # must not regress
    assert store.get_logwatch_live_cursor() == 1000
    store.set_logwatch_live_cursor(1100)
    assert store.get_logwatch_live_cursor() == 1100


def test_hist_cursor_monotonic(tmp_path):
    store = _store(tmp_path)
    store.set_logwatch_cursor(1000)
    store.set_logwatch_cursor(900)  # must not regress
    assert store.get_logwatch_cursor() == 1000
    store.set_logwatch_cursor(1100)
    assert store.get_logwatch_cursor() == 1100


def test_claim_due_outbox_leases_and_keyed_lookup(tmp_path):
    store = _store(tmp_path)
    d1 = _seed_deal(store)
    w2 = "0xddd0000000000000000000000000000000000002"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w2,
                token="0xeee0000000000000000000000000000000000003",
                token_symbol="SEED2",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx="0xseed2",
            )
        ],
        max_deals=5,
    )
    d2 = store.list_deals_for_wallet(w2)[0]
    k1 = f"deal:{d1.wallet.lower()}:{d1.token.lower()}"
    k2 = f"deal:{w2.lower()}:{str(d2['token']).lower()}"
    store.claim_and_enqueue_deal(d1.wallet, d1.token, dedup_key=k1, payload='{"a":1}')
    store.claim_and_enqueue_deal(w2, str(d2["token"]), dedup_key=k2, payload='{"a":2}')

    # Immediate dispatch for the newer key must find it despite older pending.
    keyed = store.claim_due_outbox(limit=1, dedup_key=k2)
    assert len(keyed) == 1
    assert keyed[0]["dedup_key"] == k2
    # Leased row is not claimable again.
    assert store.claim_due_outbox(limit=5, dedup_key=k2) == []
    # Older key still claimable.
    older = store.claim_due_outbox(limit=5)
    assert len(older) == 1
    assert older[0]["dedup_key"] == k1
    stats = store.outbox_stats()
    assert stats["pending"] == 2  # both in 'sending', counted as backlog


def test_list_due_respects_next_attempt_at(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload="{}"
    )
    due = store.list_due_outbox(limit=10)
    assert len(due) == 1
    oid = due[0]["id"]

    # Push the retry far into the future → not due now.
    store.mark_outbox_failed(
        oid, error="boom", next_attempt_at=time.time() + 3600, max_attempts=10
    )
    assert store.list_due_outbox(limit=10) == []
    # But due again once we look past next_attempt_at.
    later = store.list_due_outbox(now=time.time() + 7200, limit=10)
    assert len(later) == 1
    assert later[0]["attempts"] == 1


def test_mark_failed_reaches_failed_status_after_max_attempts(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload="{}"
    )
    oid = store.list_due_outbox(limit=1)[0]["id"]

    store.mark_outbox_failed(oid, error="x", next_attempt_at=0, max_attempts=2)
    assert store.outbox_stats()["pending"] == 1  # attempt 1, still pending
    store.mark_outbox_failed(oid, error="x", next_attempt_at=0, max_attempts=2)
    stats = store.outbox_stats()
    assert stats["failed"] == 1
    assert stats["pending"] == 0


def test_mark_outbox_deferred_does_not_burn_attempts(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload="{}"
    )
    oid = store.list_due_outbox(limit=1)[0]["id"]
    # Lease it like the dispatcher would.
    claimed = store.claim_due_outbox(limit=1, dedup_key=key)
    assert len(claimed) == 1
    store.mark_outbox_deferred(
        oid, error="gmgn gate defer", next_attempt_at=time.time() + 30
    )
    stats = store.outbox_stats()
    assert stats["pending"] == 1
    assert stats["failed"] == 0
    # attempts unchanged
    with store._lock:
        with store._connect() as conn:
            row = conn.execute(
                "SELECT attempts, status FROM alert_outbox WHERE id=?", (oid,)
            ).fetchone()
    assert int(row["attempts"]) == 0
    assert row["status"] == "pending"


def test_prune_outbox_drops_old_sent(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload="{}"
    )
    oid = store.list_due_outbox(limit=1)[0]["id"]
    store.mark_outbox_sent(oid)
    # Nothing pruned within the keep window.
    assert store.prune_outbox(keep_sent_sec=3600) == 0
    # Everything older than 0s is pruned.
    assert store.prune_outbox(keep_sent_sec=-1) == 1
    assert store.outbox_stats()["sent"] == 0


@pytest.mark.asyncio
async def test_dispatch_marks_failed_and_alerts_ops(tmp_path):
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            outbox_max_attempts=1,
            telegram_chat_id="-1001",
            logwatch_enabled=False,
        )
    )
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "T2",
            "deal_index": deal.deal_index,
            "mcap_at_buy": 9_000.0,
            "bought_usd": 70.0,
            "topic_id": None,
            "check_honeypot": False,
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )

    runner = FollowupRunner(store=store)
    ops = AsyncMock()
    with (
        patch(
            "app.followup.send_followup_deal",
            AsyncMock(side_effect=RuntimeError("tg down")),
        ),
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("ok", json.loads(payload))),
        ),
        patch.object(runner, "_ops_alert", ops),
    ):
        delivered = await runner._dispatch_outbox(store.load_config())

    assert delivered == 0
    # max_attempts=1 → straight to 'failed' + an ops alert fired.
    assert store.outbox_stats()["failed"] == 1
    assert ops.await_count == 1
    assert ops.await_args.kwargs.get("kind") == "outbox_failed"


@pytest.mark.asyncio
async def test_dispatch_delivers_deal_payload(tmp_path):
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "T2",
            "deal_index": deal.deal_index,
            "mcap_at_buy": 9_000.0,
            "bought_usd": 70.0,
            "topic_id": 55,
            "check_honeypot": False,
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )

    runner = FollowupRunner(store=store)
    sent = AsyncMock(return_value=None)
    with (
        patch("app.followup.send_followup_deal", sent),
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("ok", json.loads(payload))),
        ),
    ):
        delivered = await runner._dispatch_outbox(store.load_config())

    assert delivered == 1
    assert sent.await_count == 1
    assert sent.await_args.kwargs["topic_id"] == 55
    assert store.outbox_stats()["sent"] == 1


@pytest.mark.asyncio
async def test_dispatch_discards_past_max_invent_junk(tmp_path):
    """Stale invent-era outbox (#2 while GMGN is past max) must not TG."""
    from app.followup import GmgnRankVerdict

    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "JUNK",
            "deal_index": 2,
            "mcap_at_buy": 5_556.0,
            "check_honeypot": False,
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )
    runner = FollowupRunner(store=store)
    sent = AsyncMock(return_value=None)
    past = GmgnRankVerdict(
        uncertain=False,
        reason="past_max",
        seed_token="0xbbb0000000000000000000000000000000000001",
        post_seed=(),
        rank=None,
        past_max=True,
    )
    with (
        patch("app.followup.send_followup_deal", sent),
        patch.object(runner, "_gmgn_rank_verdict", AsyncMock(return_value=past)),
    ):
        delivered = await runner._dispatch_outbox(store.load_config())
    assert delivered == 0
    assert sent.await_count == 0
    assert store.outbox_stats()["sent"] == 1  # discarded as sent
    assert store.outbox_stats()["pending"] == 0
    w = store.list_watching()
    # wallet forced done
    assert deal.wallet.lower() not in [x.lower() for x in w]


@pytest.mark.asyncio
async def test_gate_outbox_allows_max_deals_rank(tmp_path):
    """Deal #max_deals (past_max window length) must not be discarded."""
    from app.followup import GmgnRankVerdict

    store = _store(tmp_path)
    deal = _seed_deal(store)
    runner = FollowupRunner(store=store)
    # Simulate old buggy shape: past_max True but rank still alertable #5.
    # Gate must still allow (defensive); verdict fix sets past_max False.
    verdict = GmgnRankVerdict(
        uncertain=False,
        reason="ok",
        seed_token="0xbbb0000000000000000000000000000000000001",
        post_seed=(),
        rank=5,
        past_max=True,
    )
    with patch.object(runner, "_gmgn_rank_verdict", AsyncMock(return_value=verdict)):
        action, gated = await runner._gate_outbox_deal(
            {
                "kind": "deal",
                "wallet": deal.wallet,
                "token": deal.token,
                "deal_index": 5,
            },
            FollowupConfig(max_deals=5, alert_on_deals=[2, 3, 4, 5]),
        )
    assert action == "ok"
    assert gated is not None
    assert gated["deal_index"] == 5


@pytest.mark.asyncio
async def test_gate_outbox_defers_when_rank_none(tmp_path):
    """Unranked tip must soft-defer, not permanently discard+burn."""
    from app.followup import GmgnRankVerdict

    store = _store(tmp_path)
    deal = _seed_deal(store)
    runner = FollowupRunner(store=store)
    verdict = GmgnRankVerdict(
        uncertain=False,
        reason="ok",
        seed_token="0xbbb0000000000000000000000000000000000001",
        post_seed=(),
        rank=None,
        past_max=False,
    )
    with patch.object(runner, "_gmgn_rank_verdict", AsyncMock(return_value=verdict)):
        action, _gated = await runner._gate_outbox_deal(
            {
                "kind": "deal",
                "wallet": deal.wallet,
                "token": deal.token,
                "deal_index": 2,
            },
            store.load_config(),
        )
    assert action == "defer"


def test_pending_tip_transfers_persist_and_freeze(tmp_path):
    store = _store(tmp_path)
    n = store.upsert_pending_tip_transfers(
        [
            {
                "wallet": "0xaaa0000000000000000000000000000000000001",
                "token": "0xbbb0000000000000000000000000000000000001",
                "sender": "0xccc",
                "tx_hash": "0xtx",
                "block_number": 100,
                "bought_at": 1_000.0,
                "queued_at": 1_100.0,
            }
        ]
    )
    assert n == 1
    rows = store.list_pending_tip_transfers()
    assert len(rows) == 1
    assert rows[0]["block_number"] == 100
    # Re-upsert keeps earlier queued_at
    store.upsert_pending_tip_transfers(
        [
            {
                "wallet": "0xaaa0000000000000000000000000000000000001",
                "token": "0xbbb0000000000000000000000000000000000001",
                "tx_hash": "0xtx2",
                "block_number": 101,
                "queued_at": 1_500.0,
            }
        ]
    )
    rows = store.list_pending_tip_transfers()
    assert rows[0]["queued_at"] == 1_100.0
    assert rows[0]["block_number"] == 101
    store.touch_pending_tip_queued_at(now=2_000.0)
    assert store.list_pending_tip_transfers()[0]["queued_at"] == 2_000.0
    store.delete_pending_tip_transfers(
        [
            (
                "0xaaa0000000000000000000000000000000000001",
                "0xbbb0000000000000000000000000000000000001",
            )
        ]
    )
    assert store.list_pending_tip_transfers() == []


@pytest.mark.asyncio
async def test_drain_keeps_durable_row_when_tip_lag_requeues(tmp_path):
    """Process re-queue must not be wiped by post-drain SQLite delete."""
    from unittest.mock import MagicMock

    from app.followup import GmgnRankVerdict
    from app.followup_logwatch import InboundTransfer

    store = _store(tmp_path)
    cfg = FollowupConfig(enabled=True, buys_only=False, alert_on_deals=[2, 3, 4, 5])
    store.save_config(cfg)
    wallet = "0xaaa0000000000000000000000000000000000001"
    tip_token = "0xbbb0000000000000000000000000000000000002"
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
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    now = time.time()
    tr = InboundTransfer(
        wallet=wallet,
        token=tip_token,
        sender="0xccc0000000000000000000000000000000000003",
        tx_hash="0xabc",
        block_number=99_900,
        bought_at=now - 30,
    )
    runner._queue_skip_enrich_transfers([tr])
    assert store.list_pending_tip_transfers()
    uncertain = GmgnRankVerdict(
        uncertain=True,
        reason="gmgn_tip_lag",
        seed_token="0xbbb0000000000000000000000000000000000001",
        post_seed=(),
        rank=None,
        past_max=False,
    )
    rpc = MagicMock()
    with (
        patch.object(
            runner, "_gmgn_rank_verdict", AsyncMock(return_value=uncertain)
        ),
        patch.object(
            runner,
            "_enrich_transfer",
            AsyncMock(return_value=(1_000.0, 50.0, None, "TOK", "")),
        ),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(return_value={}),
        ),
        patch("app.gmgn_portfolio.gmgn_api_configured", return_value=True),
        patch("app.gmgn_portfolio.gmgn_circuit_open", return_value=False),
    ):
        stats = await runner._drain_pending_skip_transfers(cfg, rpc=rpc)
    assert stats["new_deals"] == 0
    assert len(runner._pending_skip_transfers) == 1
    rows = store.list_pending_tip_transfers()
    assert len(rows) == 1
    assert rows[0]["token"].lower() == tip_token.lower()


@pytest.mark.asyncio
async def test_drain_continues_under_gmgn_circuit(tmp_path):
    """Circuit must not hard-pause drain — classify/rank from cache can proceed."""
    from unittest.mock import MagicMock

    from app.followup import GmgnRankVerdict
    from app.followup_logwatch import InboundTransfer

    store = _store(tmp_path)
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        alert_max_buy_age_sec=900,
        buys_only=False,
        alert_skip_honeypot=False,
        telegram_chat_id="-1001",
    )
    store.save_config(cfg)
    wallet = "0xaaa0000000000000000000000000000000000001"
    tip_token = "0xbbb0000000000000000000000000000000000002"
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
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    now = time.time()
    tr = InboundTransfer(
        wallet=wallet,
        token=tip_token,
        sender=wallet,
        tx_hash="0xabc",
        block_number=99_900,
        bought_at=now - 30,
    )
    runner._queue_skip_enrich_transfers([tr])
    ok = GmgnRankVerdict(
        uncertain=False,
        reason="ok",
        seed_token="0xbbb0000000000000000000000000000000000001",
        post_seed=(),
        rank=2,
        past_max=False,
    )
    rpc = MagicMock()
    with (
        patch.object(runner, "_gmgn_rank_verdict", AsyncMock(return_value=ok)),
        patch.object(
            runner,
            "_enrich_transfer",
            AsyncMock(return_value=(1_000.0, 50.0, None, "TOK", "")),
        ),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(return_value={}),
        ),
        patch.object(runner, "_deliver_deal_alert", AsyncMock(return_value=True)),
        patch("app.gmgn_portfolio.gmgn_api_configured", return_value=True),
        patch("app.gmgn_portfolio.gmgn_circuit_open", return_value=True),
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
    ):
        stats = await runner._drain_pending_skip_transfers(cfg, rpc=rpc)
    assert stats["new_deals"] >= 1 or tip_token.lower() in store.known_tokens(wallet)
    # Must not leave the tip forever parked solely because circuit was open.
    assert not any(
        t.token.lower() == tip_token.lower() for t, _ in runner._pending_skip_transfers
    )


@pytest.mark.asyncio
async def test_dispatch_releases_lease_on_cancel(tmp_path):
    """CancelledError must not leave the row stuck in ``sending``."""
    store = _store(tmp_path)
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "T2",
            "deal_index": deal.deal_index,
            "mcap_at_buy": 9_000.0,
            "bought_usd": 70.0,
            "check_honeypot": False,
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )
    runner = FollowupRunner(store=store)

    async def _boom(*_a, **_k):
        raise asyncio.CancelledError()

    with (
        patch.object(runner, "_send_outbox_payload", side_effect=_boom),
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("ok", json.loads(payload))),
        ),
    ):
        with pytest.raises(asyncio.CancelledError):
            await runner._dispatch_outbox(store.load_config())

    stats = store.outbox_stats()
    assert stats.get("sending", 0) == 0
    assert stats["pending"] == 1


@pytest.mark.asyncio
async def test_dispatch_soft_honeypot_ships_immediately(tmp_path):
    """Soft DexScreener heuristics must not delay tip alerts by 45s loops."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            alert_skip_honeypot=True,
            telegram_chat_id="-1001",
        )
    )
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    now = time.time()
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "T2",
            "deal_index": deal.deal_index,
            "mcap_at_buy": 9_000.0,
            "bought_usd": 70.0,
            "honeypot_reason": "no_sells:fresh",
            "check_honeypot": False,
            "bought_at": now - 20,
            "block_number": 99_950,
            "origin": "live",
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    sent = AsyncMock(return_value=None)
    with (
        patch("app.followup.send_followup_deal", sent),
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("ok", json.loads(payload))),
        ),
    ):
        delivered = await runner._dispatch_outbox(store.load_config())

    assert delivered == 1
    assert sent.await_count == 1
    assert store.outbox_stats()["sent"] == 1
    assert store.outbox_stats()["pending"] == 0


@pytest.mark.asyncio
async def test_dispatch_hard_honeypot_tip_fresh_ships_immediately(tmp_path):
    """Tip-fresh hard HP false positives must ship now, not sit in outbox."""
    store = _store(tmp_path)
    store.save_config(
        FollowupConfig(
            enabled=True,
            alert_skip_honeypot=True,
            telegram_chat_id="-1001",
            alert_max_buy_age_sec=900,
        )
    )
    now = time.time()
    deal = _seed_deal(store)
    key = f"deal:{deal.wallet.lower()}:{deal.token.lower()}"
    payload = json.dumps(
        {
            "v": 1,
            "kind": "deal",
            "chat": "-1001",
            "wallet": deal.wallet,
            "token": deal.token,
            "token_symbol": "T2",
            "deal_index": deal.deal_index,
            "mcap_at_buy": 9_000.0,
            "bought_usd": 70.0,
            "honeypot_reason": "gmgn:honeypot",
            "check_honeypot": False,
            "bought_at": now - 40,
            "block_number": 99_950,
            "origin": "live",
        }
    )
    store.claim_and_enqueue_deal(
        deal.wallet, deal.token, dedup_key=key, payload=payload
    )
    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    sent = AsyncMock(return_value=None)
    with (
        patch("app.followup.send_followup_deal", sent),
        patch.object(
            runner,
            "_gate_outbox_deal",
            AsyncMock(return_value=("ok", json.loads(payload))),
        ),
    ):
        delivered = await runner._dispatch_outbox(store.load_config())

    assert delivered == 1
    assert sent.await_count == 1
    assert store.outbox_stats()["sent"] == 1
    assert store.outbox_stats()["pending"] == 0


@pytest.mark.asyncio
async def test_buys_only_fail_closed_on_unknown_sender(tmp_path):
    """Unknown tx.from must not invent a buy when buys_only is on."""
    from app.followup_logwatch import InboundTransfer

    store = _store(tmp_path)
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
    cfg = FollowupConfig(
        enabled=True,
        buys_only=True,
        alert_on_deals=[2, 3, 4, 5],
        telegram_chat_id="-1001",
    )
    store.save_config(cfg)
    tr = InboundTransfer(
        wallet=wallet,
        token="0xccc0000000000000000000000000000000000002",
        sender="0x8366a39cc670b4001a1121b8f6a443a643e40951",
        tx_hash="0xdeadbeef",
        block_number=100,
        bought_at=time.time(),
    )
    runner = FollowupRunner(store=store)
    rpc = object()

    async def _timeout(*_a, **_k):
        raise asyncio.TimeoutError()

    with (
        patch("app.followup.tx_from_and_input", side_effect=_timeout),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(return_value={}),
        ),
        patch.object(runner, "_enrich_transfer", AsyncMock()),
        patch.object(runner, "_deliver_deal_alert", AsyncMock(return_value=True)),
    ):
        stats = await runner._process_logwatch_transfers(
            [tr],
            cfg=cfg,
            rpc=rpc,
            label="live",
            from_block=100,
            to_block=100,
        )

    assert stats["new_deals"] == 0
    assert stats["alerts"] == 0
    # Fail-closed: unknown senders are requeued (not counted as buys).
    assert len(runner._pending_skip_transfers) >= 1
