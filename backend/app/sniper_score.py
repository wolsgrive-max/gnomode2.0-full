"""RayBot-style sniper scoring: 1 token = 1 trade_count."""

from __future__ import annotations

import logging

from .config import settings
from .database import Database, get_db
from .launchpads.types import SniperHit

logger = logging.getLogger(__name__)


async def record_sniper_trade(
    wallet: str,
    token: str,
    *,
    hit: SniperHit | None = None,
    min_buy_usd: float | None = None,
    db: Database | None = None,
) -> bool:
    """
    Record a sniper buy. Increments trade_count only for a new wallet+token pair.
    Returns True if the pair was new and counted.
    """
    store = db or get_db()
    threshold = settings.min_buy_usd if min_buy_usd is None else min_buy_usd
    amount = float(hit.amount_usd) if hit else 0.0
    if threshold > 0 and amount > 0 and amount < threshold:
        logger.debug(
            "Skip sniper trade below min_buy_usd: %s %s $%.2f",
            wallet[:10],
            token[:10],
            amount,
        )
        return False
    # If amount unknown (curve Transfer scan), still count the pair.
    return await store.ainsert_trade(
        wallet=wallet,
        token=token,
        mcap_at_trade=hit.mcap_at_trade if hit else None,
        amount_usd=amount or None,
        tx_hash=hit.tx if hit else None,
        block=hit.block if hit else None,
    )


async def record_sniper_hits(
    token: str,
    hits: list[SniperHit],
    *,
    min_buy_usd: float | None = None,
    db: Database | None = None,
) -> int:
    n = 0
    for hit in hits:
        if await record_sniper_trade(
            hit.wallet, token, hit=hit, min_buy_usd=min_buy_usd, db=db
        ):
            n += 1
    return n
