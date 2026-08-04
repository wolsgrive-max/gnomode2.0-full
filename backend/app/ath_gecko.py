"""Peak market-cap via GeckoTerminal OHLCV (Robinhood).

DexScreener has no ATH field; sparse DS samples miss pumps. Gecko candle
highs × total supply give a usable peak for the 24h token index / screener.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from .chain import http_client
from .constants import DEXSCREENER_API

logger = logging.getLogger(__name__)

_GECKO_BASE = "https://api.geckoterminal.com/api/v2"
_NETWORK = "robinhood"
_DEFAULT_SUPPLY = 1_000_000_000.0
_CACHE_TTL_S = 20 * 60.0
_MAX_PAGES = 3
_CACHE: dict[str, tuple[float, float]] = {}  # token -> (ts, ath_mcap)
_POOL_CACHE: dict[str, tuple[float, str]] = {}  # token -> (ts, pool)
_sem = asyncio.Semaphore(1)


@dataclass(frozen=True)
class GeckoAthResult:
    token: str
    ath_mcap: float
    peak_price: float = 0.0
    supply: float = 0.0
    pool: str = ""
    candles: int = 0
    error: str | None = None


def _f(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


async def resolve_pool_address(token: str, *, hint: str | None = None) -> str | None:
    """Best-effort pool / pair address for Gecko OHLCV."""
    key = token.strip().lower()
    if hint and str(hint).strip():
        return str(hint).strip()
    now = time.time()
    hit = _POOL_CACHE.get(key)
    if hit and now - hit[0] < _CACHE_TTL_S:
        return hit[1] or None

    # Prefer in-memory index.
    try:
        from .token_index import token_index

        entry = token_index._tokens.get(key)
        if entry is not None:
            if entry.dex == "uniswap_v4" and entry.pool_id:
                pool = entry.pool_id
                _POOL_CACHE[key] = (now, pool)
                return pool
            if entry.pool_address:
                _POOL_CACHE[key] = (now, entry.pool_address)
                return entry.pool_address
            screened = entry.screened
            if screened and screened.pair_address:
                _POOL_CACHE[key] = (now, screened.pair_address)
                return screened.pair_address
    except Exception:  # noqa: BLE001
        pass

    # DexScreener pairAddress
    try:
        resp = await http_client().get(
            f"{DEXSCREENER_API}/tokens/v1/robinhood/{token}",
            timeout=12.0,
        )
        if resp.status_code == 200:
            data = resp.json()
            pairs = data if isinstance(data, list) else (data.get("pairs") or [])
            best = None
            best_liq = -1.0
            for p in pairs or []:
                chain = str(p.get("chainId") or "").lower()
                if chain and chain not in ("robinhood", "4663"):
                    continue
                base = str((p.get("baseToken") or {}).get("address") or "").lower()
                quote = str((p.get("quoteToken") or {}).get("address") or "").lower()
                if key not in (base, quote):
                    continue
                liq = _f((p.get("liquidity") or {}).get("usd"))
                if liq > best_liq:
                    best_liq = liq
                    best = str(p.get("pairAddress") or "")
            if best:
                _POOL_CACHE[key] = (now, best)
                return best
    except Exception as exc:  # noqa: BLE001
        logger.debug("DS pool resolve %s: %s", key[:10], exc)

    # GMGN biggest_pool_address
    try:
        resp = await http_client().get(
            f"https://gmgn.ai/api/v1/token_info/robinhood/{key}",
            headers={
                "Origin": "https://gmgn.ai",
                "Referer": "https://gmgn.ai/",
                "Accept": "application/json",
            },
            timeout=12.0,
        )
        if resp.status_code == 200:
            data = (resp.json() or {}).get("data") or {}
            pool = str(data.get("biggest_pool_address") or "").strip()
            if pool:
                _POOL_CACHE[key] = (now, pool)
                return pool
            supply = _f(data.get("total_supply") or data.get("circulating_supply"))
            if supply > 0:
                # stash supply via side-channel on cache miss path later
                pass
    except Exception as exc:  # noqa: BLE001
        logger.debug("GMGN pool resolve %s: %s", key[:10], exc)

    _POOL_CACHE[key] = (now, "")
    return None


async def resolve_supply(token: str) -> float:
    key = token.strip().lower()
    try:
        resp = await http_client().get(
            f"https://gmgn.ai/api/v1/token_info/robinhood/{key}",
            headers={
                "Origin": "https://gmgn.ai",
                "Referer": "https://gmgn.ai/",
                "Accept": "application/json",
            },
            timeout=12.0,
        )
        if resp.status_code == 200:
            data = (resp.json() or {}).get("data") or {}
            supply = _f(data.get("total_supply") or data.get("circulating_supply"))
            if supply > 0:
                return supply
    except Exception as exc:  # noqa: BLE001
        logger.debug("GMGN supply %s: %s", key[:10], exc)
    return _DEFAULT_SUPPLY


async def _fetch_ohlcv_pages(pool: str, *, max_pages: int = _MAX_PAGES) -> list[list[Any]]:
    """Newest-first minute candles: [ts, o, h, l, c, vol]."""
    pool = pool.strip()
    if not pool:
        return []
    url = f"{_GECKO_BASE}/networks/{_NETWORK}/pools/{pool}/ohlcv/minute"
    out: list[list[Any]] = []
    before: int | None = None
    client = http_client()
    for _ in range(max_pages):
        params: dict[str, Any] = {
            "aggregate": 1,
            "limit": 1000,
            "currency": "usd",
        }
        if before is not None:
            params["before_timestamp"] = before
        delay = 2.0
        data = None
        for _attempt in range(5):
            try:
                resp = await client.get(url, params=params, timeout=20.0)
                if resp.status_code in (429, 502, 503):
                    ra = resp.headers.get("Retry-After")
                    try:
                        ra_s = float(ra) if ra else 0.0
                    except (TypeError, ValueError):
                        ra_s = 0.0
                    # Never trust a tiny Retry-After — stampede if we sleep 0–200ms.
                    sleep_for = max(delay, min(ra_s, 90.0))
                    logger.warning(
                        "Gecko OHLCV HTTP %s; backing off %.1fs",
                        resp.status_code,
                        sleep_for,
                    )
                    await asyncio.sleep(sleep_for)
                    delay = min(delay * 2, 60.0)
                    continue
                if resp.status_code != 200:
                    logger.debug("Gecko OHLCV %s: %s", resp.status_code, resp.text[:120])
                    return out
                data = resp.json()
                break
            except Exception as exc:  # noqa: BLE001
                logger.debug("Gecko OHLCV error: %s", exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
        if data is None:
            break
        ohlcv = (
            ((data.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or []
        )
        if not isinstance(ohlcv, list) or not ohlcv:
            break
        for row in ohlcv:
            if isinstance(row, (list, tuple)) and len(row) >= 5:
                out.append(list(row))
        try:
            before = int(ohlcv[-1][0])
        except (TypeError, ValueError, IndexError):
            break
        if len(ohlcv) < 1000:
            break
        await asyncio.sleep(1.0)
    return out


async def fetch_ohlcv_peak_price(pool: str) -> tuple[float, int]:
    """Return (max USD high, candle count) over recent OHLCV pages."""
    ohlcv = await _fetch_ohlcv_pages(pool)
    peak = 0.0
    for row in ohlcv:
        high = _f(row[2])
        if high > peak:
            peak = high
    return peak, len(ohlcv)


# token -> (fetched_at, candles newest-first)
_OHLCV_CACHE: dict[str, tuple[float, list[list[Any]]]] = {}
_OHLCV_TTL_S = 10 * 60.0


async def fetch_price_near_ts(token: str, ts: float, *, pool: str | None = None) -> float | None:
    """USD close of the minute candle covering ``ts`` (or nearest older)."""
    if ts <= 0:
        return None
    key = token.strip().lower()
    now = time.time()
    hit = _OHLCV_CACHE.get(key)
    candles: list[list[Any]]
    if hit and now - hit[0] < _OHLCV_TTL_S:
        candles = hit[1]
    else:
        async with _sem:
            pool_addr = await resolve_pool_address(token, hint=pool)
            if not pool_addr:
                return None
            candles = await _fetch_ohlcv_pages(pool_addr, max_pages=2)
            _OHLCV_CACHE[key] = (now, candles)
    if not candles:
        return None
    target = int(ts)
    best_close = None
    best_delta = None
    for row in candles:
        try:
            cts = int(row[0])
        except (TypeError, ValueError, IndexError):
            continue
        # Candle covers [cts, cts+60); prefer containing / nearest older.
        if cts <= target < cts + 60:
            px = _f(row[4])
            return px if px > 0 else None
        delta = target - cts
        if delta < 0:
            continue
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_close = _f(row[4])
    if best_close and best_close > 0 and best_delta is not None and best_delta < 3600:
        return best_close
    return None


async def fetch_token_ath_mcap(
    token: str,
    *,
    pool: str | None = None,
    supply: float | None = None,
    use_cache: bool = True,
) -> GeckoAthResult:
    """Peak mcap = max OHLCV high × supply for ``token``."""
    key = token.strip().lower()
    now = time.time()
    if use_cache:
        hit = _CACHE.get(key)
        if hit and now - hit[0] < _CACHE_TTL_S:
            return GeckoAthResult(token=key, ath_mcap=hit[1], pool=pool or "")

    async with _sem:
        pool_addr = await resolve_pool_address(token, hint=pool)
        if not pool_addr:
            return GeckoAthResult(token=key, ath_mcap=0.0, error="no pool")
        sup = float(supply) if supply and supply > 0 else await resolve_supply(token)
        peak_px, n = await fetch_ohlcv_peak_price(pool_addr)
        if peak_px <= 0:
            return GeckoAthResult(
                token=key,
                ath_mcap=0.0,
                supply=sup,
                pool=pool_addr,
                candles=n,
                error="no ohlcv",
            )
        ath = peak_px * sup
        _CACHE[key] = (now, ath)
        return GeckoAthResult(
            token=key,
            ath_mcap=ath,
            peak_price=peak_px,
            supply=sup,
            pool=pool_addr,
            candles=n,
        )
