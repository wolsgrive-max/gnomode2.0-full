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
