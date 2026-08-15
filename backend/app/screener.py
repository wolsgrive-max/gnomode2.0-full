"""Robinhood token screener over the in-memory 24h token index.

Discovery of new tokens (Uniswap V2/V3/V4 pools created in the last 24h) lives in
``token_index``; this module applies user filters/sorting and the honeypot gate
to the cached, DexScreener-enriched rows.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from .constants import DEXSCREENER_API
from .models import ScreenedToken, ScreenRequest, ScreenSortBy, ScreenSortOrder
from .security import assess_tokens_honeypot
from .token_index import token_index

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]
TokensCb = Callable[[list[ScreenedToken]], Awaitable[None]]

_DS_TIMEOUT = httpx.Timeout(12.0, connect=8.0)
# RH meme mints are almost always 1e9; used when supply is unknown.
_RH_DEFAULT_SUPPLY = 1_000_000_000.0
# price×1e9 vs DexScreener mcap/fdv — beyond this, trust DS (low-supply tokens).
_MCAP_SUPPLY_DIVERGENCE = 10.0


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _ds_reported_mcap(pair: dict[str, Any]) -> float:
    raw = pair.get("marketCap")
    if raw is None or _f(raw) <= 0:
        raw = pair.get("fdv")
    return _f(raw)


def _dex_market_cap(pair: dict[str, Any], *, supply: float | None = None) -> float:
    """Market cap from DexScreener pair.

    Prefer ``priceUsd × supply`` when supply is known. For the default 1e9 RH
    assumption, reject the result when it diverges wildly from DS
    ``marketCap``/``fdv`` — otherwise low-supply tokens (e.g. supply≈500)
    inflate to billions and falsely pass the ATH≥50k gate.
    """
    price_usd = _f(pair.get("priceUsd"))
    ds = _ds_reported_mcap(pair)
    if supply is not None and float(supply) > 0:
        if price_usd > 0:
            return price_usd * float(supply)
        return ds
    if price_usd > 0:
        assumed = price_usd * _RH_DEFAULT_SUPPLY
        if ds > 0:
            hi = max(assumed, ds)
            lo = min(assumed, ds)
            if lo > 0 and hi / lo > _MCAP_SUPPLY_DIVERGENCE:
                return ds
        return assumed
    return ds


def _in_range(value: float | None, lo: float | None, hi: float | None) -> bool:
    if lo is None and hi is None:
        return True
    if value is None:
        return False
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


async def _fetch_dex_pairs(
    client: httpx.AsyncClient, addresses: list[str]
) -> list[dict[str, Any]]:
    url = f"{DEXSCREENER_API}/tokens/v1/robinhood/{','.join(addresses)}"
    delay = 1.0
    for attempt in range(5):
        try:
            resp = await client.get(url, timeout=_DS_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    pairs = data.get("pairs")
                    return pairs if isinstance(pairs, list) else []
                return []
            if resp.status_code in (429, 502, 503):
                ra = resp.headers.get("Retry-After")
                try:
                    ra_s = float(ra) if ra else 0.0
                except (TypeError, ValueError):
                    ra_s = 0.0
                sleep_for = max(delay, min(ra_s, 60.0))
                logger.warning(
                    "DexScreener HTTP %s; backing off %.1fs (try %s)",
                    resp.status_code,
                    sleep_for,
                    attempt + 1,
                )
                await asyncio.sleep(sleep_for)
                delay = min(delay * 2, 30.0)
                continue
            logger.warning("DexScreener tokens/v1 %s: %s", resp.status_code, resp.text[:200])
            return []
        except Exception as exc:  # noqa: BLE001
            logger.warning("DexScreener enrich error (try %s): %r", attempt + 1, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
    return []


def _best_pair_for_token(token: str, pairs: list[dict[str, Any]]) -> dict[str, Any] | None:
    t = token.lower()
    best: dict[str, Any] | None = None
    best_liq = -1.0
    for p in pairs:
        chain = str(p.get("chainId") or "").lower()
        if chain and chain not in ("robinhood", "4663"):
            continue
        base = str((p.get("baseToken") or {}).get("address") or "").lower()
        quote = str((p.get("quoteToken") or {}).get("address") or "").lower()
        if t not in (base, quote):
            continue
        liq = _f((p.get("liquidity") or {}).get("usd"))
        # Prefer RH pairs; score ties by liquidity
        score = liq if chain in ("robinhood", "4663") else liq - 1_000_000_000_000.0
        if score > best_liq:
            best_liq = score
            best = p
    return best


def _pair_to_screened(
    token_addr: str, meta: dict[str, Any], pair: dict[str, Any]
) -> ScreenedToken:
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    t = token_addr.lower()
    if str(base.get("address") or "").lower() == t:
        symbol = str(base.get("symbol") or meta.get("symbol") or "")
        name = str(base.get("name") or meta.get("name") or "")
    else:
        symbol = str(quote.get("symbol") or meta.get("symbol") or "")
        name = str(quote.get("name") or meta.get("name") or "")

    created = pair.get("pairCreatedAt")
    created_ms = int(created) if created is not None else None
    age_h: float | None = None
    if created_ms is not None and created_ms > 0:
        age_h = max(0.0, (time.time() * 1000 - created_ms) / 3_600_000)

    pair_addr = str(pair.get("pairAddress") or "")
    chain = str(pair.get("chainId") or "robinhood")
    ds_url = str(pair.get("url") or f"https://dexscreener.com/{chain}/{pair_addr}")

    txns = (pair.get("txns") or {}).get("h24") or {}
    buys = int(_f(txns.get("buys")))
    sells = int(_f(txns.get("sells")))
    traders = buys + sells

    return ScreenedToken(
        address=token_addr,
        symbol=symbol,
        name=name,
        pair_address=pair_addr,
        dex_id=str(pair.get("dexId") or ""),
        price_usd=_f(pair.get("priceUsd")),
        liquidity_usd=_f((pair.get("liquidity") or {}).get("usd")),
        market_cap=_dex_market_cap(pair),
        traders_24h=traders,
        buys_24h=buys,
        sells_24h=sells,
        pair_created_at_ms=created_ms,
        pair_age_hours=age_h,
        url=ds_url,
        gmgn_url=f"https://gmgn.ai/robinhood/token/{token_addr}",
    )


def _passes_primary(row: ScreenedToken, req: ScreenRequest) -> bool:
    if not _in_range(row.liquidity_usd, req.min_liq, req.max_liq):
        return False
    if not _in_range(row.market_cap, req.min_mcap, req.max_mcap):
        return False
    # Peak seen in the 24h index; None/0 on the request disables the gate.
    min_ath = req.min_ath_mcap
    if min_ath is not None and min_ath > 0:
        if not _in_range(float(row.ath_mcap or 0.0), min_ath, None):
            return False
    if not _in_range(float(row.traders_24h), req.min_traders, req.max_traders):
        return False
    if req.min_pair_age_hours is not None or req.max_pair_age_hours is not None:
        if row.pair_age_hours is None:
            return False
        if not _in_range(row.pair_age_hours, req.min_pair_age_hours, req.max_pair_age_hours):
            return False
    return True


def _sort_key(row: ScreenedToken, sort_by: ScreenSortBy) -> float:
    if sort_by == ScreenSortBy.market_cap:
        return row.market_cap
    if sort_by == ScreenSortBy.traders:
        return float(row.traders_24h)
    if sort_by == ScreenSortBy.pair_age:
        return row.pair_age_hours if row.pair_age_hours is not None else -1.0
    return row.liquidity_usd


def _sorted_rows(rows: list[ScreenedToken], req: ScreenRequest) -> list[ScreenedToken]:
    reverse = req.sort_order == ScreenSortOrder.desc
    out = sorted(rows, key=lambda r: _sort_key(r, req.sort_by), reverse=reverse)
    return out[: req.max_results]


async def _filter_honeypots(rows: list[ScreenedToken]) -> list[ScreenedToken]:
    """Filter honeypots via GMGN security on the returned slice only."""
    if not rows:
        return []
    verdicts = await assess_tokens_honeypot(
        [(r.address, r.buys_24h, r.sells_24h) for r in rows]
    )
    kept: list[ScreenedToken] = []
    for row in rows:
        reason = verdicts.get(row.address.lower())
        if reason:
            logger.info(
                "Screener skip honeypot %s (%s): %s",
                row.symbol or row.address[:10],
                row.address[:12],
                reason,
            )
            continue
        kept.append(row)
    return kept


async def screen_tokens(
    req: ScreenRequest,
    on_progress: ProgressCb | None = None,
    on_tokens: TokensCb | None = None,
) -> list[ScreenedToken]:
    async def prog(stage: str, message: str, percent: float) -> None:
        if on_progress:
            await on_progress(stage, message, percent)

    async def emit(rows: list[ScreenedToken]) -> None:
        if on_tokens:
            await on_tokens(_sorted_rows(rows, req))

    await prog("index", "Preparing 24h token index…", 0.02)
    # Cold start blocks here (scan + enrich); warm starts return instantly.
    # Soft timeout: public RPC wall-timeouts must not crash Watch (Хвать).
    try:
        await asyncio.wait_for(token_index.ensure_ready(on_progress=prog), timeout=120.0)
    except asyncio.TimeoutError:
        logger.warning("token_index.ensure_ready timed out — screening warm pool")
    except Exception as exc:  # noqa: BLE001
        logger.warning("token_index.ensure_ready soft-fail: %s", exc)

    pool = token_index.get_tokens()
    await prog("filter", f"Filtering {len(pool)} tokens from last 24h…", 0.9)

    matched = [r for r in pool if _passes_primary(r, req)]
    rows_out = _sorted_rows(matched, req)
    await emit(rows_out)

    if req.exclude_honeypots and matched:
        scan_n = min(len(matched), req.max_results + 200)
        ranked = sorted(
            matched,
            key=lambda r: _sort_key(r, req.sort_by),
            reverse=(req.sort_order == ScreenSortOrder.desc),
        )[:scan_n]
        await prog("security", f"Honeypot check (GMGN) for {len(ranked)} tokens…", 0.94)
        try:
            checked = await asyncio.wait_for(_filter_honeypots(ranked), timeout=45.0)
        except TimeoutError:
            logger.warning("Honeypot filter timed out — keeping candidates without GMGN filter")
            checked = ranked
        rows_out = _sorted_rows(checked, req)
        await emit(rows_out)

    await prog("done", f"Done — {len(rows_out)} tokens", 1.0)
    return rows_out
