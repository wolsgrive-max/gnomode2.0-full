"""Discover first-N curve buyers after a migration and store as snipers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .chain import RpcClient
from .config import settings
from .database import get_db
from .launchpads.adapters import get_adapter
from .launchpads.types import MigrationEvent, SniperHit
from .mcap_checker import _fetch_mcap_batch
from .sniper_score import record_sniper_hits
from .wallet_metrics import batch_tokens_traded_7d

logger = logging.getLogger(__name__)

# Dedup in-flight discovery per token
_inflight: set[str] = set()


async def _filter_one_token_early_buyers(
    hits: list[SniperHit],
    *,
    max_first_mcap: float,
) -> list[SniperHit]:
    """Keep only wallets with first-buy mcap ≤ cap and exactly 1 token in 7d."""
    under_mcap: list[SniperHit] = []
    for h in hits:
        mcap = float(h.mcap_at_trade or 0)
        if mcap <= 0 or mcap > max_first_mcap:
            continue
        under_mcap.append(h)
    if not under_mcap:
        return []

    counts = await batch_tokens_traded_7d(
        [h.wallet for h in under_mcap],
        enough=None,
        too_many=1,
    )
    kept: list[SniperHit] = []
    for h in under_mcap:
        n = counts.get(h.wallet.lower())
        if n is not None and int(n) == 1:
            kept.append(h)
    return kept


async def discover_snipers_for_migration(
    event: MigrationEvent,
    *,
    rpc: RpcClient | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Scan adapter snipers and persist with RayBot 1-token=1-trade rule.

    Only wallets with first-buy mcap ≤ sniper_max_first_mcap and exactly one
    distinct ERC-20 in the last 7 days are tracked.
    Safe to call in a background task after ``store_migration``.
    """
    token = event.token.lower()
    if token in _inflight:
        return {"ok": False, "snipers": 0, "message": "already_running"}
    _inflight.add(token)
    client = rpc or RpcClient(concurrency=2)
    n_limit = limit if limit is not None else settings.sniper_limit
    max_mcap = float(settings.sniper_max_first_mcap)
    try:
        adapter = get_adapter(event.launchpad_id)
        hits = await adapter.scan_snipers(client, event, limit=n_limit)
        if not hits:
            logger.info(
                "No snipers for %s [%s]", event.token[:12], event.launchpad_id
            )
            return {"ok": True, "snipers": 0, "message": "no_hits"}

        # Spot mcap for first_mcap annotation (best-effort).
        mcap_map = await _fetch_mcap_batch([event.token])
        info = mcap_map.get(token) or {}
        mcap = float(info.get("mcap") or 0) or None
        liq = info.get("liquidity_usd")
        if mcap is not None or liq is not None:
            try:
                await get_db().aupdate_token_market(
                    event.token, mcap_usd=mcap, liquidity_usd=liq
                )
            except Exception:  # noqa: BLE001
                logger.debug("update_token_market failed", exc_info=True)

        for hit in hits:
            if hit.mcap_at_trade <= 0 and mcap:
                hit.mcap_at_trade = mcap

        filtered = await _filter_one_token_early_buyers(hits, max_first_mcap=max_mcap)
        if not filtered:
            logger.info(
                "Snipers for %s [%s]: %d hits → 0 after mcap≤%.0f + 1-token filter",
                event.token[:12],
                event.launchpad_id,
                len(hits),
                max_mcap,
            )
            return {
                "ok": True,
                "snipers": 0,
                "hits": len(hits),
                "message": "filtered_out",
            }

        recorded = await record_sniper_hits(
            event.token, filtered, min_buy_usd=0.0
        )
        logger.info(
            "Snipers for %s [%s]: %d hits → %d kept (mcap≤%.0f, 1 token) → %d new",
            event.token[:12],
            event.launchpad_id,
            len(hits),
            len(filtered),
            max_mcap,
            recorded,
        )
        return {
            "ok": True,
            "snipers": recorded,
            "hits": len(hits),
            "kept": len(filtered),
            "message": f"recorded {recorded}",
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("discover_snipers failed for %s", event.token[:12])
        return {"ok": False, "snipers": 0, "message": str(exc)}
    finally:
        _inflight.discard(token)


def spawn_sniper_discovery(event: MigrationEvent) -> None:
    """Fire-and-forget background discovery (does not block migration store)."""

    async def _run() -> None:
        await discover_snipers_for_migration(event)

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_run())
    except RuntimeError:
        logger.warning("No running loop — sniper discovery skipped for %s", event.token[:12])
