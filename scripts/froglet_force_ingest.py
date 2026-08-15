#!/usr/bin/env python3
"""Force-ingest known FROGLET early buyers into follow-up."""

from __future__ import annotations

import asyncio
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("froglet_ingest")

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN = [
    "0x0e507839ecdf7a6eacfdce67427c4b6975328659",
    "0x91a54dfd4c346cb6a81cbc1357da673161568dbb",
    "0x14de114921829c059ca4934d5ff2c226452b93c4",
    "0x952b61bd0185533e926154f0e4e98452ee1f1186",
]


async def main() -> None:
    from app.chain import RpcClient
    from app.followup import followup_runner
    from app.models import BuyerRow, ParseRequest
    from app.replay import parse_token
    from app.watch_store import watch_store
    from app.wallet_metrics import enrich_and_filter_buyers

    cfg = watch_store.load_config()
    n = watch_store.unparse_tokens([F])
    logger.info("unparsed FROGLET rows=%s", n)

    rpc = RpcClient()
    res = await parse_token(
        rpc,
        F,
        mcap_threshold=float(cfg.wallet.mcap_threshold or 30_000),
        exclude_honeypots=False,
        wallet_filters=None,
    )
    logger.info(
        "raw parse error=%s buyers=%s symbol=%s",
        res.error,
        len(res.buyers),
        res.symbol,
    )
    buys1 = [b for b in res.buyers if b.buys_count == 1]
    logger.info("buys_count=1 → %s", len(buys1))
    addrs = {b.wallet.lower() for b in buys1}
    for w in KNOWN:
        logger.info("known %s in_early=%s", w[:12], w.lower() in addrs)

    # Re-apply wallet filters like watch
    wallet_req = ParseRequest(
        tokens=[F],
        mcap_threshold=cfg.wallet.mcap_threshold,
        exclude_honeypots=cfg.wallet.exclude_honeypots,
        min_wallet_balance_eth=cfg.wallet.min_wallet_balance_eth,
        max_wallet_balance_eth=cfg.wallet.max_wallet_balance_eth,
        min_hold_time_minutes=cfg.wallet.min_hold_time_minutes,
        max_hold_time_minutes=cfg.wallet.max_hold_time_minutes,
        min_tokens_traded_7d=cfg.wallet.min_tokens_traded_7d,
        max_tokens_traded_7d=cfg.wallet.max_tokens_traded_7d,
        tokens_unique_period=cfg.wallet.tokens_unique_period,
    )
    filtered = await enrich_and_filter_buyers(
        rpc,
        token=F,
        buyers=buys1,
        req=wallet_req,
        start_block=0,
        end_block=0,
    )
    logger.info("after filters %s", len(filtered))
    for b in filtered:
        logger.info(
            "PASS %s bal=%s unique=%s mcap=%.0f",
            b.wallet,
            b.wallet_balance_eth,
            b.tokens_traded_7d,
            b.mcap_at_first_buy,
        )

    # Prefer filtered; if known wallets filtered out, still ingest them from raw
    # buys1 (user asked to track missed wallets).
    by_w = {b.wallet.lower(): b for b in buys1}
    to_ingest: list[BuyerRow] = list(filtered)
    have = {b.wallet.lower() for b in to_ingest}
    for w in KNOWN:
        key = w.lower()
        if key in have:
            continue
        row = by_w.get(key)
        if row is None:
            logger.warning("known %s not in early buyers — skip", w[:12])
            continue
        logger.info("force-include known miss %s", w[:12])
        to_ingest.append(row)

    if not to_ingest:
        logger.warning("nothing to ingest")
        return

    watch_store.apply_qualify_updates(
        ath_updates={F: (150_000.0, res.symbol or "FROGLET")},
        held=[],
        expired=[],
        candidates=[F],
        now=time.time(),
    )
    n_ins = await followup_runner.ingest_from_watch(to_ingest)
    logger.info("ingested_deals=%s wallets=%s", n_ins, len(to_ingest))
    for b in to_ingest:
        logger.info("  watching %s", b.wallet)


if __name__ == "__main__":
    asyncio.run(main())
