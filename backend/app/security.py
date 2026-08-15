"""Combined honeypot checks for Robinhood Chain.

Primary gate: GMGN token security (same data shown on gmgn.ai) — fast & accurate.
Fallback: DexScreener buy/sell heuristics when GMGN returns unknown.
On-chain sim is available as an optional deep check but is not used for screener.
"""

from __future__ import annotations

import logging
from typing import Any

from .gmgn import GmgnSecurity, check_token_security, check_tokens_security
from .pools import fetch_dexscreener_pairs

logger = logging.getLogger(__name__)

_MIN_BUYS_NO_SELLS = 3
_ASYMMETRY_MIN_BUYS = 20
_ASYMMETRY_RATIO = 20.0


def dexscreener_honeypot_reason(
    *,
    buys_24h: int | None,
    sells_24h: int | None,
) -> str | None:
    """Only block on strong DexScreener signals (used when GMGN is unknown)."""
    if buys_24h is None or sells_24h is None:
        return None
    buys = int(buys_24h)
    sells = int(sells_24h)
    if buys >= _MIN_BUYS_NO_SELLS and sells == 0:
        return f"no_sells (buys={buys})"
    if sells > 0 and buys >= _ASYMMETRY_MIN_BUYS and (buys / sells) >= _ASYMMETRY_RATIO:
        return f"buy_sell_asymmetry ({buys}/{sells})"
    return None


def pair_txns_24h(pair: dict[str, Any] | None) -> tuple[int, int]:
    if not pair:
        return 0, 0
    txns = (pair.get("txns") or {}).get("h24") or {}
    try:
        buys = int(float(txns.get("buys") or 0))
    except (TypeError, ValueError):
        buys = 0
    try:
        sells = int(float(txns.get("sells") or 0))
    except (TypeError, ValueError):
        sells = 0
    return buys, sells


def best_rh_pair(pairs: list[dict[str, Any]], token: str) -> dict[str, Any] | None:
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
        try:
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
        except (TypeError, ValueError):
            liq = 0.0
        if liq > best_liq:
            best_liq = liq
            best = p
    return best


def _gmgn_block_reason(sec: GmgnSecurity) -> str | None:
    if sec.blocked:
        return sec.reason or "gmgn:honeypot"
    return None


async def assess_honeypot(
    address: str,
    *,
    buys_24h: int | None = None,
    sells_24h: int | None = None,
    pair: dict[str, Any] | None = None,
    gmgn: GmgnSecurity | None = None,
    **_ignored: Any,
) -> str | None:
    """Return block reason, or None if token is kept."""
    if pair is not None and (buys_24h is None or sells_24h is None):
        buys_24h, sells_24h = pair_txns_24h(pair)

    sec = gmgn if gmgn is not None else await check_token_security(address)
    blocked = _gmgn_block_reason(sec)
    if blocked:
        return blocked

    # Confident not-honeypot from GMGN → keep
    if sec.is_honeypot is False:
        return None

    # Unknown on GMGN → light DexScreener fallback only
    return dexscreener_honeypot_reason(buys_24h=buys_24h, sells_24h=sells_24h)


async def assess_tokens_honeypot(
    items: list[tuple[str, int, int]],
) -> dict[str, str | None]:
    """Batch assess (address, buys_24h, sells_24h) → reason|None."""
    if not items:
        return {}

    addrs = [a for a, _, _ in items]
    gmgn_map = await check_tokens_security(addrs)

    out: dict[str, str | None] = {}
    for addr, buys, sells in items:
        key = addr.lower()
        sec = gmgn_map.get(key) or GmgnSecurity(address=key, is_honeypot=None)
        blocked = _gmgn_block_reason(sec)
        if blocked:
            out[key] = blocked
            continue
        if sec.is_honeypot is False:
            out[key] = None
            continue
        out[key] = dexscreener_honeypot_reason(buys_24h=buys, sells_24h=sells)
    return out


async def honeypot_reason_for_token(address: str) -> str | None:
    """Full check for a single token (GMGN first, DexScreener fallback)."""
    # GMGN first — faster and avoids DexScreener 429 on the alert critical path.
    sec = await check_token_security(address)
    blocked = _gmgn_block_reason(sec)
    if blocked:
        return blocked
    if sec.is_honeypot is False:
        return None
    pairs = await fetch_dexscreener_pairs(address)
    pair = best_rh_pair(pairs, address)
    buys, sells = pair_txns_24h(pair)
    return dexscreener_honeypot_reason(buys_24h=buys, sells_24h=sells)
