"""Logwatch must not invent fake deal #2 when GMGN already has many uniques."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.followup import FollowupRunner, post_seed_unique_buys
from app.followup_logwatch import InboundTransfer
from app.followup_store import FollowupStore
from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult
from app.models import BuyerRow, FollowupConfig


def test_post_seed_unique_buys_skips_pre_seed():
    seed = "0xseed000000000000000000000000000000000001"
    buys = [
        GmgnBuy("0xold", "OLD", "", 10),
        GmgnBuy(seed, "SEED", "", 100),
        GmgnBuy("0xa", "A", "", 110),
        GmgnBuy("0xb", "B", "", 120),
    ]
    seed_buy, post = post_seed_unique_buys(buys, seed)
    assert seed_buy is not None
    assert [b.token for b in post] == ["0xa", "0xb"]


@pytest.mark.asyncio
async def test_logwatch_no_false_deal2_when_gmgn_has_8_uniques(tmp_path, monkeypatch):
    """Seed + 7 GMGN post-seed uniques → late transfer must NOT alert as #2."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    late = "0xccc0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        max_mcap_alert=50_000.0,
        buys_only=False,
        telegram_chat_id="",
        alert_max_buy_age_sec=86_400,
        alert_max_block_lag=2_000,
    )
    store.save_config(cfg)

    # seed + 7 post-seed (8 unique). Late tip token is the 7th post-seed (#8).
    gmgn_buys = [GmgnBuy(seed.lower(), "SEED", "0xseed", 100)]
    for i in range(7):
        tok = f"0x{i+1:040x}"
        gmgn_buys.append(GmgnBuy(tok, f"T{i}", f"0xtx{i}", 200 + i * 10))
    # Override last post-seed with the late token we "discover" on logwatch.
    gmgn_buys[-1] = GmgnBuy(late.lower(), "LATE", "0xlate", 260)

    async def fake_gmgn(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=gmgn_buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", fake_gmgn)
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_api_configured", lambda: True
    )
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_circuit_open", lambda: False
    )

    runner = FollowupRunner(store=store)
    runner._last_known_tip = 100_000
    delivered: list[int] = []

    async def capture_alert(chat, *, deal, topic_id=None, honeypot_reason=None, **_k):
        delivered.append(int(deal.deal_index))
        return True

    transfers = [
        InboundTransfer(
            wallet=wallet,
            token=late.lower(),
            sender=wallet,
            tx_hash="0xlate",
            block_number=99_900,
            bought_at=__import__("time").time() - 30,
        )
    ]

    async def fake_fetch(*_a, **_k):
        return transfers

    async def fake_enrich(*_a, **_k):
        return {
            (wallet, late.lower(), "0xlate"): (
                9_000.0,
                50.0,
                None,
                "LATE",
                "Late Token",
            )
        }

    with (
        patch("app.followup.fetch_inbound_transfers", side_effect=fake_fetch),
        patch.object(runner, "_prefetch_transfer_enrichment", side_effect=fake_enrich),
        patch.object(runner, "_deliver_deal_alert", side_effect=capture_alert),
    ):
        res = await runner._logwatch_scan_window(
            cfg,
            rpc=MagicMock(),
            watching=[wallet],
            from_block=99_800,
            to_block=100_000,
            fetch_timeout=5.0,
            label="live",
            skip_enrich=False,
        )

    assert res is not None
    assert delivered == []  # must NOT telegram as fake #2
    _seen, deal_count, status = store.get_wallet_scan_meta(wallet)
    assert status == "done"
    assert deal_count >= 5
    # Late token beyond max_deals window is not kept as alertable #2.
    tokens = {d["token"] for d in store.list_deals_for_wallet(wallet)}
    assert late.lower() not in tokens or all(
        int(d["deal_index"]) != 2 or d["token"] != late.lower()
        for d in store.list_deals_for_wallet(wallet)
    )


@pytest.mark.asyncio
async def test_logwatch_uncertain_gmgn_does_not_invent_deal2(tmp_path, monkeypatch):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        max_mcap_alert=50_000.0,
        buys_only=False,
    )
    store.save_config(cfg)

    async def rate_limited(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=[], ok=False, rate_limited=True)

    monkeypatch.setattr("app.followup.fetch_unique_buys", rate_limited)
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_api_configured", lambda: True
    )
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_circuit_open", lambda: False
    )

    runner = FollowupRunner(store=store)
    delivered: list[int] = []

    async def capture_alert(*_a, **_k):
        delivered.append(1)
        return True

    transfers = [
        InboundTransfer(
            wallet=wallet,
            token=tip.lower(),
            sender=wallet,
            tx_hash="0xtip",
            block_number=99_900,
            bought_at=__import__("time").time() - 10,
        )
    ]

    with (
        patch(
            "app.followup.fetch_inbound_transfers",
            AsyncMock(return_value=transfers),
        ),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(
                return_value={
                    (wallet, tip.lower(), "0xtip"): (
                        9_000.0,
                        50.0,
                        None,
                        "TIP",
                        "Tip",
                    )
                }
            ),
        ),
        patch.object(runner, "_deliver_deal_alert", side_effect=capture_alert),
    ):
        await runner._logwatch_scan_window(
            cfg,
            rpc=MagicMock(),
            watching=[wallet],
            from_block=99_800,
            to_block=100_000,
            fetch_timeout=5.0,
            label="live",
        )

    assert delivered == []
    assert tip.lower() not in store.known_tokens(wallet)
    _seen, deal_count, status = store.get_wallet_scan_meta(wallet)
    assert deal_count == 1
    assert status == "watching"


@pytest.mark.asyncio
async def test_seed_miss_with_many_uniques_marks_past_max(tmp_path, monkeypatch):
    """Wrong seed + 100 GMGN uniques must not invent Dora-style local #4."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    stale_seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=stale_seed,
                token_symbol="STALE",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        max_mcap_alert=50_000.0,
        buys_only=False,
    )
    store.save_config(cfg)

    buys = [
        GmgnBuy(f"0x{i:040x}", f"T{i}", "", float(1_000 + i))
        for i in range(1, 21)
    ]
    # tip appears late in GMGN history — not a follow-up #2…#5
    buys.append(GmgnBuy(tip, "DORA", "", 2_000.0))

    async def many_uniques(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", many_uniques)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: False)

    runner = FollowupRunner(store=store)
    delivered: list[int] = []

    async def capture_alert(*_a, **_k):
        delivered.append(1)
        return True

    transfers = [
        InboundTransfer(
            wallet=wallet,
            token=tip.lower(),
            sender=wallet,
            tx_hash="0xtip",
            block_number=99_900,
            bought_at=__import__("time").time() - 10,
        )
    ]
    with (
        patch(
            "app.followup.fetch_inbound_transfers",
            AsyncMock(return_value=transfers),
        ),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(
                return_value={
                    (wallet, tip.lower(), "0xtip"): (
                        9_000.0,
                        50.0,
                        None,
                        "DORA",
                        "Dora",
                    )
                }
            ),
        ),
        patch.object(runner, "_deliver_deal_alert", side_effect=capture_alert),
    ):
        await runner._logwatch_scan_window(
            cfg,
            rpc=MagicMock(),
            watching=[wallet],
            from_block=99_800,
            to_block=100_000,
            fetch_timeout=5.0,
            label="live",
        )

    assert delivered == []
    _seen, deal_count, status = store.get_wallet_scan_meta(wallet)
    assert status == "done"
    assert tip.lower() not in store.known_tokens(wallet) or all(
        int(d["deal_index"]) != 4 or d["token"] != tip.lower()
        for d in store.list_deals_for_wallet(wallet)
    )


@pytest.mark.asyncio
async def test_stale_seed_reanchors_tip_rank_and_alerts(tmp_path, monkeypatch):
    """Stale local seed + tip at GMGN absolute #4 must alert as deal #4."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    stale_seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc00000000000000000000000000000000000cc"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=stale_seed,
                token_symbol="STALE",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        max_mcap_alert=50_000.0,
        buys_only=False,
        alert_skip_honeypot=False,
        telegram_chat_id="-1001",
    )
    store.save_config(cfg)

    buys = [
        GmgnBuy("0x1110000000000000000000000000000000000001", "A", "", 100.0),
        GmgnBuy("0x2220000000000000000000000000000000000002", "B", "", 110.0),
        GmgnBuy("0x3330000000000000000000000000000000000003", "C", "", 120.0),
        GmgnBuy(tip, "TIP", "0xtip", 130.0),
    ]

    async def fake_gmgn(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", fake_gmgn)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: False)

    runner = FollowupRunner(store=store)
    alerted: list[int] = []

    async def capture(_chat, *, deal, **_k):
        alerted.append(int(deal.deal_index))
        return True

    transfers = [
        InboundTransfer(
            wallet=wallet,
            token=tip.lower(),
            sender=wallet,
            tx_hash="0xtip",
            block_number=99_900,
            bought_at=__import__("time").time() - 5,
        )
    ]
    with (
        patch(
            "app.followup.fetch_inbound_transfers",
            AsyncMock(return_value=transfers),
        ),
        patch("app.followup.telegram_configured", return_value=True),
        patch("app.followup.resolve_chat_id", return_value="-1001"),
        patch.object(
            runner,
            "_prefetch_transfer_enrichment",
            AsyncMock(
                return_value={
                    (wallet, tip.lower(), "0xtip"): (
                        9_000.0,
                        50.0,
                        None,
                        "TIP",
                        "Tip",
                    )
                }
            ),
        ),
        patch.object(runner, "_deliver_deal_alert", side_effect=capture),
    ):
        await runner._logwatch_scan_window(
            cfg,
            rpc=MagicMock(),
            watching=[wallet],
            from_block=99_800,
            to_block=100_000,
            fetch_timeout=5.0,
            label="live",
        )

    assert 4 in alerted
    assert tip.lower() in store.known_tokens(wallet)
    rows = {d["token"]: int(d["deal_index"]) for d in store.list_deals_for_wallet(wallet)}
    assert rows.get(tip.lower()) == 4


@pytest.mark.asyncio
async def test_repair_marks_done_when_gmgn_past_max(tmp_path, monkeypatch):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        gmgn_repair_batch=8,
    )
    store.save_config(cfg)

    gmgn_buys = [GmgnBuy(seed.lower(), "SEED", "0xseed", 100)]
    for i in range(6):
        gmgn_buys.append(
            GmgnBuy(f"0x{i+1:040x}", f"T{i}", f"0xtx{i}", 200 + i * 10)
        )

    async def fake_gmgn(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=gmgn_buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", fake_gmgn)
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_api_configured", lambda: True
    )
    monkeypatch.setattr(
        "app.gmgn_portfolio.gmgn_circuit_open", lambda: False
    )

    runner = FollowupRunner(store=store)
    await runner._repair_undercounted_wallets(cfg, rpc=MagicMock())
    _seen, deal_count, status = store.get_wallet_scan_meta(wallet)
    assert status == "done"
    assert deal_count >= 5


@pytest.mark.asyncio
async def test_verdict_last_slot_not_past_max(tmp_path, monkeypatch):
    """Tip that fills #max_deals must have past_max=False so TG can fire."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc0000000000000000000000000000000000005"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(enabled=True, max_deals=5, alert_on_deals=[2, 3, 4, 5])
    # seed + 4 post-seed; tip is the 4th post-seed → deal #5
    gmgn_buys = [GmgnBuy(seed.lower(), "SEED", "0xseed", 100.0)]
    for i in range(3):
        gmgn_buys.append(
            GmgnBuy(f"0x{i+1:040x}", f"T{i}", f"0xt{i}", 200.0 + i)
        )
    gmgn_buys.append(GmgnBuy(tip.lower(), "TIP", "0xtip", 210.0))

    async def fake_gmgn(_wallet: str, **_kwargs):
        return UniqueBuysResult(buys=gmgn_buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", fake_gmgn)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: False)

    runner = FollowupRunner(store=store)
    verdict = await runner._gmgn_rank_verdict(wallet, tip, cfg)
    assert verdict.uncertain is False
    assert verdict.rank == 5
    assert verdict.past_max is False
    assert verdict.reason == "ok"


@pytest.mark.asyncio
async def test_circuit_ranks_from_cache_without_network(tmp_path, monkeypatch):
    """Open GMGN circuit must still rank from a recent ok cache hit."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc00000000000000000000000000000000000cc"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(enabled=True, max_deals=5, alert_on_deals=[2, 3, 4, 5])
    buys = [
        GmgnBuy(seed.lower(), "SEED", "0xseed", 100.0),
        GmgnBuy("0x1110000000000000000000000000000000000001", "A", "", 110.0),
        GmgnBuy(tip.lower(), "TIP", "0xtip", 120.0),
    ]
    fetch_calls = {"n": 0}

    async def fake_gmgn(_wallet: str, **_kwargs):
        fetch_calls["n"] += 1
        return UniqueBuysResult(buys=buys, ok=True, rate_limited=False)

    monkeypatch.setattr("app.followup.fetch_unique_buys", fake_gmgn)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: False)

    runner = FollowupRunner(store=store)
    warm = await runner._fetch_unique_buys_cached(wallet, cfg=cfg)
    assert warm.ok
    assert fetch_calls["n"] == 1

    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: True)
    verdict = await runner._gmgn_rank_verdict(wallet, tip, cfg)
    assert verdict.uncertain is False
    assert verdict.rank == 3
    assert verdict.reason == "ok"
    assert fetch_calls["n"] == 1  # no network under circuit


@pytest.mark.asyncio
async def test_circuit_without_cache_is_uncertain_hold(tmp_path, monkeypatch):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    tip = "0xccc00000000000000000000000000000000000cc"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(enabled=True, max_deals=5)

    async def boom(_wallet: str, **_kwargs):
        raise AssertionError("must not fetch under circuit without cache")

    monkeypatch.setattr("app.followup.fetch_unique_buys", boom)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_api_configured", lambda: True)
    monkeypatch.setattr("app.gmgn_portfolio.gmgn_circuit_open", lambda: True)

    runner = FollowupRunner(store=store)
    verdict = await runner._gmgn_rank_verdict(wallet, tip, cfg)
    assert verdict.uncertain is True
    assert verdict.reason == "gmgn_circuit"


@pytest.mark.asyncio
async def test_gmgn_sync_does_not_burn_fresh_alertable_siblings(tmp_path, monkeypatch):
    """Earlier GMGN uniques inserted while syncing a later tip must stay alertable."""
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    wallet = "0xaaa0000000000000000000000000000000000001"
    seed = "0xbbb0000000000000000000000000000000000001"
    kep = "0xccc00000000000000000000000000000000000aa"
    jam = "0xddd00000000000000000000000000000000000bb"
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=wallet,
                token=seed,
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=8_000.0,
                buys_count=1,
                first_tx="0xseed",
            )
        ],
        max_deals=5,
    )
    cfg = FollowupConfig(
        enabled=True,
        max_deals=5,
        alert_on_deals=[2, 3, 4, 5],
        alert_max_buy_age_sec=900,
    )
    now = __import__("time").time()
    post = [
        GmgnBuy(kep, "KEP", "0xkep", int(now - 120), 20.0),
        GmgnBuy(jam, "JAM", "0xjam", int(now - 30), 20.0),
    ]
    runner = FollowupRunner(store=store)
    await runner._sync_wallet_gmgn_order(
        wallet,
        cfg,
        post_seed=post,
        tip_token=jam,
        tip_symbol="JAM",
        tip_tx="0xjam",
        tip_block=99_900,
        tip_bought_at=now - 30.0,
        tip_mcap=40_000.0,
        tip_bought_usd=20.0,
    )
    rows = {d["token"]: d for d in store.list_deals_for_wallet(wallet)}
    assert kep.lower() in rows
    assert int(rows[kep.lower()]["deal_index"]) == 2
    assert not bool(rows[kep.lower()]["notified"]), "fresh #2 sibling must not be burned"
    assert jam.lower() in rows

