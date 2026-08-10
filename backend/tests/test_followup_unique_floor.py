"""Follow-up deals floor Blockscout unique undercount (Relay / permit2)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import wallet_metrics as wm
from app.followup_store import FollowupStore
from app.models import BuyerRow, ParseRequest


def _buyer(
    wallet: str,
    token: str,
    *,
    symbol: str = "",
    tx: str = "0x" + "ab" * 32,
    block: int = 1,
    tokens_traded_7d: int | None = 1,
    bought_usd: float = 1.0,
    mcap: float = 1000.0,
) -> BuyerRow:
    return BuyerRow(
        wallet=wallet,
        token=token,
        token_symbol=symbol,
        bought_tokens=100.0,
        bought_usd=bought_usd,
        mcap_at_first_buy=mcap,
        buys_count=1,
        first_tx=tx,
        first_block=block,
        tokens_traded_7d=tokens_traded_7d,
    )


@pytest.fixture()
def fu_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FollowupStore:
    db = tmp_path / "followup.db"
    monkeypatch.setattr("app.followup_store.settings.followup_db_path", str(db))
    store = FollowupStore(db_path=str(db))
    monkeypatch.setattr("app.followup_store.followup_store", store)
    return store


def test_floor_counts_prior_deal_plus_current_token(fu_store: FollowupStore) -> None:
    wallet = "0xa67d7eb4dc68fa6ce8e34ef8cadaf075b9893fbb"
    pandu = "0x621bbed44acaaa4803bfd4d418513ed265878e4d"
    mancer = "0x783f13d8459121ffc8d5ec0151740cb1a2797b9f"
    fu_store.ingest_buyers(
        [_buyer(wallet, pandu, symbol="Pandu")],
        max_deals=5,
    )
    assert wm.followup_unique_floor(wallet, mancer) == 2
    assert wm.followup_unique_floor(wallet, pandu) == 1
    assert wm.apply_followup_unique_floor(1, wallet, mancer) == 2
    assert wm.apply_followup_unique_floor(None, wallet, mancer) == 2


def test_renumber_bumps_tokens_traded_and_busts_cache(
    fu_store: FollowupStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app import wallet_unique_cache as uc

    uc_path = tmp_path / "wallet_unique.db"
    monkeypatch.setattr("app.wallet_unique_cache.settings.unique_cache_ttl_sec", 3600)
    uc.reset_for_tests(uc_path)

    wallet = "0xa67d7eb4dc68fa6ce8e34ef8cadaf075b9893fbb"
    pandu = "0x621bbed44acaaa4803bfd4d418513ed265878e4d"
    mancer = "0x783f13d8459121ffc8d5ec0151740cb1a2797b9f"
    uc.put_exact(wallet, 720, 1, exact=True)
    wm._tokens7d_cache[f"{wm._TOKENS7D_CACHE_VER}:h720:{wallet}"] = (1, 0.0)

    fu_store.ingest_buyers(
        [_buyer(wallet, pandu, symbol="Pandu", tx="0x" + "11" * 32)],
        max_deals=5,
    )
    assert (
        fu_store.record_deal(
            wallet=wallet,
            token=mancer,
            token_symbol="MANCER",
            mcap_at_buy=2000.0,
            bought_usd=3.0,
            tx_hash="0x" + "22" * 32,
            block_number=2,
        )
        is not None
    )
    _seen, deal_count, _status = fu_store.get_wallet_scan_meta(wallet)
    assert deal_count == 2
    row = fu_store.list_wallets()[0]
    assert int(row.tokens_traded_7d or 0) >= 2
    assert uc.get_exact(wallet, 720) is None
    assert f"{wm._TOKENS7D_CACHE_VER}:h720:{wallet}" not in wm._tokens7d_cache
    uc.reset_for_tests(None)
    wm._tokens7d_cache.clear()


@pytest.mark.asyncio
async def test_enrich_rejects_second_buy_via_followup_floor(
    fu_store: FollowupStore,
) -> None:
    wallet = "0xa67d7eb4dc68fa6ce8e34ef8cadaf075b9893fbb"
    pandu = "0x621bbed44acaaa4803bfd4d418513ed265878e4d"
    mancer = "0x783f13d8459121ffc8d5ec0151740cb1a2797b9f"
    fu_store.ingest_buyers(
        [_buyer(wallet, pandu, symbol="Pandu", block=10)],
        max_deals=5,
    )

    async def fake_batch(wallets, **_kw):
        return {w.lower(): 1 for w in wallets}

    buyer = _buyer(
        wallet,
        mancer,
        symbol="MANCER",
        tx="0x" + "cd" * 32,
        block=20,
        bought_usd=2.91,
        mcap=2691.0,
    )
    req = ParseRequest(
        tokens=[mancer],
        min_tokens_traded_7d=1.0,
        max_tokens_traded_7d=1.0,
    )
    with patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch):
        kept = await wm.enrich_and_filter_buyers(
            rpc=AsyncMock(),
            token=mancer,
            buyers=[buyer],
            req=req,
            start_block=1,
            end_block=100,
        )
    assert kept == []


@pytest.mark.asyncio
async def test_enrich_rejects_bs0_floor1_when_gmgn_multi(
    fu_store: FollowupStore,
) -> None:
    """WOOF-class miss: BS=0 → FU floor=1 must not skip GMGN max-guard.

    Relay wallets often show inbound unique=0; floor only knows the current
    tip token. Without GMGN cross-check, max=1 falsely admits multi-traders.
    """
    import time

    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult

    wallet = "0xe209e0047731aa494289e1af9a0d03da19c5ef08"
    woof = "0x2cbb100a11620337a588765804d6ea4e4617e6f1"
    other = "0x7f28abd3569c1522ae7c8b691d6683c6d1c06a63"
    now_ts = int(time.time())

    async def fake_batch(wallets, **_kw):
        return {w.lower(): 0 for w in wallets}

    async def fake_gmgn(wallet_addr, **_kw):
        del wallet_addr, _kw
        return UniqueBuysResult(
            buys=[
                GmgnBuy(
                    token=other, symbol="POG", tx_hash="0x1", timestamp=now_ts
                ),
                GmgnBuy(
                    token=woof, symbol="WOOF", tx_hash="0x2", timestamp=now_ts
                ),
            ],
            ok=True,
            rate_limited=False,
        )

    buyer = _buyer(
        wallet,
        woof,
        symbol="WOOF",
        tx="0x" + "dd" * 32,
        block=30,
        bought_usd=19.43,
        mcap=4074.55,
        tokens_traded_7d=None,
    )
    req = ParseRequest(
        tokens=[woof],
        min_tokens_traded_7d=1.0,
        max_tokens_traded_7d=1.0,
    )
    # Floor alone: no prior deals → FU=1 (current token). Must still reject.
    assert wm.followup_unique_floor(wallet, woof) == 1
    with (
        patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch),
        patch("app.gmgn_portfolio.fetch_unique_buys", new=fake_gmgn),
    ):
        kept = await wm.enrich_and_filter_buyers(
            rpc=AsyncMock(),
            token=woof,
            buyers=[buyer],
            req=req,
            start_block=1,
            end_block=100,
        )
    assert kept == []


@pytest.mark.asyncio
async def test_enrich_keeps_true_unique_after_max_guard(
    fu_store: FollowupStore,
) -> None:
    """BS=0 → FU=1 + GMGN=1 still admits a real early buyer."""
    import time

    from app.gmgn_portfolio import GmgnBuy, UniqueBuysResult

    wallet = "0x1111111111111111111111111111111111111111"
    token = "0x2222222222222222222222222222222222222222"
    now_ts = int(time.time())

    async def fake_batch(wallets, **_kw):
        return {w.lower(): 0 for w in wallets}

    async def fake_gmgn(wallet_addr, **_kw):
        del wallet_addr, _kw
        return UniqueBuysResult(
            buys=[
                GmgnBuy(
                    token=token, symbol="ONE", tx_hash="0x1", timestamp=now_ts
                ),
            ],
            ok=True,
            rate_limited=False,
        )

    buyer = _buyer(wallet, token, symbol="ONE", tokens_traded_7d=None)
    req = ParseRequest(
        tokens=[token],
        min_tokens_traded_7d=1.0,
        max_tokens_traded_7d=1.0,
    )
    with (
        patch("app.wallet_metrics.batch_tokens_traded_7d", new=fake_batch),
        patch("app.gmgn_portfolio.fetch_unique_buys", new=fake_gmgn),
    ):
        kept = await wm.enrich_and_filter_buyers(
            rpc=AsyncMock(),
            token=token,
            buyers=[buyer],
            req=req,
            start_block=1,
            end_block=100,
        )
    assert len(kept) == 1
    assert kept[0].tokens_traded_7d == 1
