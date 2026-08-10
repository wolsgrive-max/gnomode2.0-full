"""Batch enrichment: parallel + time-boxed so alerts are not serialized."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from app.followup import FollowupRunner
from app.followup_logwatch import InboundTransfer
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


def _store(tmp_path) -> FollowupStore:
    return FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )


def _watching(store: FollowupStore, wallets: list[str]) -> None:
    store.ingest_buyers(
        [
            BuyerRow(
                wallet=w,
                token=f"0xseed{i:036x}",
                token_symbol="SEED",
                bought_tokens=1.0,
                bought_usd=40.0,
                mcap_at_first_buy=5_000.0,
                buys_count=1,
                first_tx=f"0xseedtx{i}",
            )
            for i, w in enumerate(wallets)
        ],
        max_deals=5,
    )


def _transfers(n: int) -> list[InboundTransfer]:
    return [
        InboundTransfer(
            wallet=f"0xaaa{i:037x}",
            token=f"0xbbb{i:037x}",
            sender=f"0xaaa{i:037x}",
            tx_hash=f"0xtx{i:062x}",
            block_number=1000 + i,
            bought_at=float(1_700_000_000 + i),
        )
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_prefetch_enriches_batch_in_parallel(tmp_path):
    """6 deals × ~0.3s of replay must finish in ~0.3s, not ~1.8s."""
    store = _store(tmp_path)
    trs = _transfers(6)
    _watching(store, [t.wallet for t in trs])
    cfg = FollowupConfig(
        enabled=True,
        buys_only=False,
        logwatch_enrich_concurrency=6,
        logwatch_enrich_timeout_sec=5,
    )
    runner = FollowupRunner(store=store)

    async def slow_entry(token, tx, *, rpc=None):
        await asyncio.sleep(0.3)
        raise RuntimeError("no pool")

    started = time.time()
    with (
        patch("app.replay.estimate_entry_at_tx", slow_entry),
        patch(
            "app.followup.estimate_token_quote",
            AsyncMock(return_value=(7_000.0, 0.001)),
        ),
        patch(
            "app.replay.estimate_onchain_spot_mcap",
            AsyncMock(return_value=None),
        ),
        patch("app.security.honeypot_reason_for_token", AsyncMock(return_value=None)),
    ):
        rpc = AsyncMock()
        rpc.token_meta = AsyncMock(return_value={"symbol": "", "name": ""})
        out = await runner._prefetch_transfer_enrichment(
            trs, cfg=cfg, rpc=rpc, sender_map={}
        )
    elapsed = time.time() - started

    assert len(out) == 6
    # All six ran concurrently: well under the 6 × 0.3s = 1.8s serial cost.
    assert elapsed < 1.2, f"enrichment serialized: {elapsed:.2f}s"
    mcap, bought, hp, symbol, name = out[
        (trs[0].wallet, trs[0].token, trs[0].tx_hash)
    ]
    assert mcap == 7_000.0
    assert hp is None
    assert symbol == ""
    assert name == ""


@pytest.mark.asyncio
async def test_enrich_is_time_boxed(tmp_path):
    """A hanging replay must not block the deal — quote fallback still applies."""
    store = _store(tmp_path)
    tr = _transfers(1)[0]
    cfg = FollowupConfig(
        enabled=True,
        buys_only=False,
        logwatch_enrich_timeout_sec=2,
    )
    runner = FollowupRunner(store=store)

    async def hanging(token, tx, *, rpc=None):
        await asyncio.sleep(60)

    started = time.time()
    with (
        patch("app.replay.estimate_entry_at_tx", hanging),
        patch(
            "app.followup.estimate_token_quote",
            AsyncMock(return_value=(1_234.0, 0.5)),
        ),
        patch(
            "app.replay.estimate_onchain_spot_mcap",
            AsyncMock(return_value=None),
        ),
        patch("app.security.honeypot_reason_for_token", AsyncMock(return_value=None)),
    ):
        rpc = AsyncMock()
        rpc.token_meta = AsyncMock(return_value={"symbol": "X", "name": "Token X"})
        mcap, bought, hp, symbol, name = await runner._enrich_transfer(
            tr, cfg=cfg, rpc=rpc, budget_sec=3.0
        )
    elapsed = time.time() - started

    # Gave up on the replay after ~2s and fell back to the spot quote.
    assert mcap == 1_234.0
    assert symbol == "X"
    assert elapsed < 5.0, f"timeout not enforced: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_prefetch_skips_known_tokens(tmp_path):
    """Already-known tokens must never pay replay cost."""
    store = _store(tmp_path)
    trs = _transfers(2)
    _watching(store, [t.wallet for t in trs])
    # Record the first transfer's token as an existing deal.
    store.record_deal(
        wallet=trs[0].wallet,
        token=trs[0].token,
        token_symbol="T",
        mcap_at_buy=1_000.0,
        bought_usd=10.0,
        max_deals=5,
    )
    cfg = FollowupConfig(enabled=True, buys_only=False)
    runner = FollowupRunner(store=store)

    calls: list[str] = []

    async def track(token, tx, *, rpc=None):
        calls.append(token)
        raise RuntimeError("no pool")

    with (
        patch("app.replay.estimate_entry_at_tx", track),
        patch(
            "app.followup.estimate_token_quote", AsyncMock(return_value=(5_000.0, 0.1))
        ),
        patch("app.security.honeypot_reason_for_token", AsyncMock(return_value=None)),
    ):
        out = await runner._prefetch_transfer_enrichment(
            trs, cfg=cfg, rpc=None, sender_map={}
        )

    assert len(out) == 1
    assert trs[0].token not in calls
    assert (trs[1].wallet, trs[1].token, trs[1].tx_hash) in out
