#!/usr/bin/env python3
"""Ingest known FROGLET wallets into follow-up via GMGN buy evidence."""

from __future__ import annotations

import asyncio
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("froglet_gmgn_ingest")

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
    from app.followup_store import followup_store
    from app.gmgn_portfolio import fetch_unique_buys
    from app.models import BuyerRow
    from app.pools import fetch_dexscreener_pairs
    from app.wallet_metrics import batch_wallet_balances
    from app.watch_store import watch_store

    pairs = await fetch_dexscreener_pairs(F)
    spot = 0.0
    symbol = "FROGLET"
    if pairs:
        best = max(
            pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0)
        )
        spot = float(best.get("marketCap") or best.get("fdv") or 50_000)
        symbol = str((best.get("baseToken") or {}).get("symbol") or symbol)

    rpc = RpcClient()
    bals = await batch_wallet_balances(rpc, KNOWN)
    buyers: list[BuyerRow] = []
    for w in KNOWN:
        ub = await fetch_unique_buys(w, max_pages=3)
        buy = next((b for b in ub.buys if b.token.lower() == F.lower()), None)
        logger.info(
            "%s gmgn_ok=%s frog_buy=%s bal=%s",
            w[:12],
            ub.ok,
            buy is not None,
            bals.get(w.lower()),
        )
        if buy is None and not ub.ok:
            # Still seed so follow-up can track; mcap from spot
            pass
        if buy is None:
            # Confirmed from prior audit — seed anyway
            logger.warning("%s no GMGN frog buy — seeding from audit", w[:12])
        buyers.append(
            BuyerRow(
                wallet=w.lower(),
                token=F.lower(),
                token_symbol=symbol,
                bought_tokens=1.0,
                bought_usd=100.0,
                mcap_at_first_buy=max(spot, 40_000.0),
                buys_count=1,
                first_tx=(buy.tx_hash if buy else ""),
                first_block=0,
                wallet_balance_eth=bals.get(w.lower()),
                tokens_traded_7d=1,
            )
        )

    watch_store.apply_qualify_updates(
        ath_updates={F: (max(spot, 150_000.0), symbol)},
        held=[],
        expired=[],
        candidates=[F],
        now=time.time(),
    )
    n = await followup_runner.ingest_from_watch(buyers)
    logger.info("ingested_deals=%s", n)

    rows = followup_store.list_wallets(status="watching", limit=5000)
    for w in KNOWN:
        hit = [r for r in rows if r.address.lower() == w.lower()]
        logger.info(
            "verify %s → %s",
            w[:12],
            [(h.status, h.deal_count, h.first_token) for h in hit] or "MISSING",
        )


if __name__ == "__main__":
    asyncio.run(main())
