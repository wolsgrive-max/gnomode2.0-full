"""Periodic MCAP tracker: discover pre-50k tokens, trigger analysis at target."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .config import settings
from .constants import DEXSCREENER_CHAIN
from .database import get_db

logger = logging.getLogger(__name__)

STABLE_WINDOW_HOURS = 4
DEAD_WINDOW_HOURS = 24
# Cap work per tick so watch / API stay responsive with large trackers.
_CHECK_BATCH = 120
_ANALYZE_PER_TICK = 2
_FETCH_CONCURRENCY = 4

_check_lock = asyncio.Lock()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


async def _fetch_mcap_batch(addresses: list[str]) -> dict[str, dict[str, float | None]]:
    """
    Fetch current mcap (+ optional price/liq) from DexScreener in batches of 30.

    Returns ``{address.lower(): {"mcap", "price_usd", "liquidity_usd"}}``.
    """
    result: dict[str, dict[str, float | None]] = {}
    batch_size = 30
    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

    async with httpx.AsyncClient(timeout=15.0) as client:

        async def one(batch: list[str]) -> None:
            url = (
                f"https://api.dexscreener.com/tokens/v1/{DEXSCREENER_CHAIN}/"
                f"{','.join(batch)}"
            )
            async with sem:
                try:
                    resp = await client.get(url)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("DexScreener batch error: %s", exc)
                    return
            if resp.status_code != 200:
                logger.debug("DexScreener HTTP %s for batch", resp.status_code)
                return
            pairs = resp.json()
            if not isinstance(pairs, list):
                return
            by_token: dict[str, list[dict[str, Any]]] = {}
            for p in pairs:
                for side in ("baseToken", "quoteToken"):
                    a = str((p.get(side) or {}).get("address") or "").lower()
                    if a:
                        by_token.setdefault(a, []).append(p)
            for addr, addr_pairs in by_token.items():
                best = max(
                    addr_pairs,
                    key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
                )
                mcap = best.get("marketCap")
                if mcap is None:
                    mcap = best.get("fdv") or 0
                liq = (best.get("liquidity") or {}).get("usd")
                price = best.get("priceUsd")
                try:
                    price_f = float(price) if price is not None else None
                except (TypeError, ValueError):
                    price_f = None
                try:
                    liq_f = float(liq) if liq is not None else None
                except (TypeError, ValueError):
                    liq_f = None
                result[addr] = {
                    "mcap": float(mcap or 0),
                    "price_usd": price_f,
                    "liquidity_usd": liq_f,
                }

        batches = [
            addresses[i : i + batch_size] for i in range(0, len(addresses), batch_size)
        ]
        await asyncio.gather(*[one(b) for b in batches])
    return result


def _detect_trend(
    snapshots: list[dict[str, Any]],
    *,
    growth_pct: float | None = None,
    dead_pct: float | None = None,
) -> str:
    growth_pct = (
        settings.mcap_tracker_min_growth_pct if growth_pct is None else growth_pct
    )
    dead_pct = settings.mcap_tracker_dead_pct if dead_pct is None else dead_pct
    now = datetime.now(timezone.utc)
    four_hours_ago = now - timedelta(hours=STABLE_WINDOW_HOURS)
    day_ago = now - timedelta(hours=DEAD_WINDOW_HOURS)

    recent = [
        s for s in snapshots if _parse_ts(str(s["checked_at"])) >= four_hours_ago
    ]
    day_hist = [
        s for s in snapshots if _parse_ts(str(s["checked_at"])) >= day_ago
    ]

    if not recent:
        return "unknown"

    if len(day_hist) >= 2:
        first = float(day_hist[0]["mcap"] or 0)
        last_mcap = float(day_hist[-1]["mcap"] or 0)
        if first > 0 and ((last_mcap - first) / first) * 100 <= -dead_pct:
            return "dead"

    if len(recent) >= 2:
        first = float(recent[0]["mcap"] or 0)
        last_mcap = float(recent[-1]["mcap"] or 0)
        if first > 0:
            change = ((last_mcap - first) / first) * 100
            if change >= growth_pct:
                return "growing"
            if change <= -30:
                return "falling"
            if abs(change) <= 15:
                return "stable"

    return "unknown"


async def check_mcap_tracker(*, limit: int | None = None, analyze_limit: int | None = None) -> dict[str, int]:
    """Main check: refresh mcaps (bounded), analyze a few at target, cleanup.

    Designed to finish quickly even with 10k+ tracked tokens — process a slice
    each tick; the background loop covers the rest over time.
    """
    stats = {"pending": 0, "checked": 0, "analyzed": 0, "deleted": 0}
    if _check_lock.locked():
        logger.info("MCAP Tracker: skip overlapping run")
        return stats

    async with _check_lock:
        db = get_db()
        target = settings.mcap_tracker_target
        batch_limit = limit if limit is not None else _CHECK_BATCH
        max_analyze = (
            analyze_limit if analyze_limit is not None else _ANALYZE_PER_TICK
        )

        pending = await db.aget_mcap_tracker_pending(limit=batch_limit)
        stats["pending"] = len(pending)
        if not pending:
            stats["deleted"] = await db.acleanup_mcap_tracker(
                max_age_days=settings.mcap_tracker_max_age_days
            )
            return stats

        addresses = [str(t["address"]) for t in pending]
        mcap_map = await _fetch_mcap_batch(addresses)
        now_iso = _utc_now_iso()
        since_iso = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # First pass: compute tentative updates (trend uses previous snapshots).
        # Fetch prior snapshots once per token in a compact batch update after.
        updates: list[dict[str, Any]] = []

        for token in pending:
            addr = str(token["address"])
            info = mcap_map.get(addr.lower()) or {}
            current_mcap = float(info.get("mcap") or 0)
            if current_mcap <= 0:
                continue
            peak = max(float(token.get("peak_mcap") or 0), current_mcap)
            updates.append(
                {
                    "address": addr,
                    "mcap": current_mcap,
                    "price_usd": info.get("price_usd"),
                    "liquidity_usd": info.get("liquidity_usd"),
                    "checked_at": now_iso,
                    "peak_mcap": peak,
                    "prev_trend": token.get("trend"),
                    "trend_since": token.get("trend_since"),
                    "target": target,
                }
            )

        hit_target: list[dict[str, Any]] = []
        if updates:
            hit_target = await db.aapply_mcap_check_batch(
                updates,
                since_iso=since_iso,
                growth_pct=settings.mcap_tracker_min_growth_pct,
                dead_pct=settings.mcap_tracker_dead_pct,
            )
            stats["checked"] = len(updates)

        analyzed = 0
        for row in hit_target:
            if analyzed >= max_analyze:
                break
            addr = str(row["address"])
            current_mcap = float(row["mcap"])
            logger.info(
                "MCAP Tracker: %s reached %.0f — running analysis",
                addr[:10],
                current_mcap,
            )
            try:
                from .migration_pipeline import parse_token_migration

                result = await parse_token_migration(addr)
                if result.get("ok"):
                    await db.aupdate_mcap_target_reached(addr, now_iso)
                    await db.adelete_mcap_tracker(addr)
                    analyzed += 1
                    logger.info(
                        "MCAP Tracker: %s stored (mcap=%.0f)", addr[:10], current_mcap
                    )
                else:
                    logger.debug(
                        "MCAP Tracker: %s analysis failed: %s",
                        addr[:10],
                        result.get("message"),
                    )
            except Exception:  # noqa: BLE001
                logger.exception("MCAP Tracker: analysis failed for %s", addr[:10])
        stats["analyzed"] = analyzed

        deleted = await db.acleanup_mcap_tracker(
            max_age_days=settings.mcap_tracker_max_age_days
        )
        stats["deleted"] = int(deleted or 0)
        if deleted:
            logger.info("MCAP Tracker: cleaned up %d dead/old tokens", deleted)
        logger.info(
            "MCAP Tracker tick: checked=%d analyzed=%d deleted=%d (batch=%d)",
            stats["checked"],
            stats["analyzed"],
            stats["deleted"],
            batch_limit,
        )
        return stats


async def add_token_to_tracker(
    token_address: str,
    symbol: str = "",
    name: str = "",
    launchpad_id: str = "",
    dex: str = "",
    pool_id: str = "",
    first_seen_mcap: float = 0.0,
) -> bool:
    """Add a token to the mcap tracker. False if duplicate / already migrated."""
    db = get_db()
    if await db.aget_token(token_address):
        return False
    if await db.aget_mcap_tracker_one(token_address):
        return False

    now_iso = _utc_now_iso()
    await db.ainsert_mcap_tracker(
        address=token_address,
        symbol=symbol,
        name=name,
        launchpad_id=launchpad_id,
        dex=dex,
        pool_id=pool_id,
        first_seen_mcap=first_seen_mcap,
    )
    if first_seen_mcap > 0:
        await db.ainsert_mcap_snapshot(
            token_address=token_address,
            mcap=first_seen_mcap,
            price_usd=None,
            liquidity_usd=None,
            checked_at=now_iso,
        )
    logger.info(
        "MCAP Tracker: added %s (%s) mcap=%.0f",
        token_address[:10],
        symbol,
        first_seen_mcap,
    )
    return True


async def analyze_already_above(token: str, mcap: float) -> None:
    """Token already ≥ target at discovery — analyze once and store."""
    from .migration_pipeline import parse_token_migration

    db = get_db()
    if await db.aget_token(token):
        return
    try:
        result = await parse_token_migration(token)
        if result.get("ok"):
            logger.info("MCAP: %s already at %.0f — analyzed", token[:10], mcap)
    except Exception:  # noqa: BLE001
        logger.exception("MCAP: analysis failed for already-above %s", token)
