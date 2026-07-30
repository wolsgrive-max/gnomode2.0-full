"""GoPlus Token Security checks for Robinhood Chain (4663)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from .chain import http_client
from .constants import CHAIN_ID

logger = logging.getLogger(__name__)

GOPLUS_TOKEN_SECURITY = f"https://api.gopluslabs.io/api/v1/token_security/{CHAIN_ID}"

# RH endpoint effectively returns one address per call — keep batches small + concurrent.
_CONCURRENCY = 6
_CACHE_TTL_S = 600.0
_MAX_SELL_TAX = 0.10  # GoPlus tax is a ratio: 0.1 == 10%

_cache: dict[str, tuple[float, "TokenSecurity"]] = {}
_sem = asyncio.Semaphore(_CONCURRENCY)


@dataclass(frozen=True)
class TokenSecurity:
    address: str
    is_honeypot: bool
    reason: str | None = None
    raw: dict[str, Any] | None = None

    @property
    def blocked(self) -> bool:
        return self.is_honeypot


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _tax_ratio(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    # Some payloads use percent (e.g. 50); normalize to ratio.
    if n > 1.0:
        n = n / 100.0
    return n


def classify_security(address: str, info: dict[str, Any] | None) -> TokenSecurity:
    """Decide if a GoPlus payload means honeypot / unsellable."""
    addr = address.lower()
    if not info:
        return TokenSecurity(address=addr, is_honeypot=False, reason=None, raw=None)

    if _truthy(info.get("is_honeypot")):
        return TokenSecurity(addr, True, "is_honeypot", info)
    if _truthy(info.get("cannot_sell_all")):
        return TokenSecurity(addr, True, "cannot_sell_all", info)
    if _truthy(info.get("cannot_buy")):
        return TokenSecurity(addr, True, "cannot_buy", info)
    if _truthy(info.get("is_blacklisted")):
        return TokenSecurity(addr, True, "is_blacklisted", info)
    if _truthy(info.get("is_in_dest_blacklist")):
        return TokenSecurity(addr, True, "is_in_dest_blacklist", info)
    if _truthy(info.get("selfdestruct")) or _truthy(info.get("is_self_destruct")):
        return TokenSecurity(addr, True, "selfdestruct", info)
    if _truthy(info.get("owner_change_balance")):
        return TokenSecurity(addr, True, "owner_change_balance", info)
    if _truthy(info.get("can_take_back_ownership")):
        return TokenSecurity(addr, True, "can_take_back_ownership", info)
    if _truthy(info.get("is_anti_whale")) and _truthy(info.get("anti_whale_modifiable")):
        return TokenSecurity(addr, True, "anti_whale_modifiable", info)
    if _truthy(info.get("trading_cooldown")):
        return TokenSecurity(addr, True, "trading_cooldown", info)
    if _truthy(info.get("transfer_pausable")):
        return TokenSecurity(addr, True, "transfer_pausable", info)

    sell_tax = _tax_ratio(info.get("sell_tax"))
    if sell_tax is not None and sell_tax >= _MAX_SELL_TAX:
        return TokenSecurity(addr, True, f"sell_tax={sell_tax:.0%}", info)

    buy_tax = _tax_ratio(info.get("buy_tax"))
    if buy_tax is not None and buy_tax >= _MAX_SELL_TAX:
        return TokenSecurity(addr, True, f"buy_tax={buy_tax:.0%}", info)

    return TokenSecurity(addr, False, None, info)


async def _fetch_one(address: str) -> TokenSecurity:
    key = address.lower()
    now = time.time()
    cached = _cache.get(key)
    if cached and now - cached[0] < _CACHE_TTL_S:
        return cached[1]

    async with _sem:
        try:
            resp = await http_client().get(
                GOPLUS_TOKEN_SECURITY,
                params={"contract_addresses": key},
                timeout=12.0,
            )
            if resp.status_code != 200:
                logger.warning("GoPlus %s for %s: %s", resp.status_code, key[:12], resp.text[:160])
                result = TokenSecurity(key, False, None, None)
                _cache[key] = (now, result)
                return result
            data = resp.json()
            result_map = data.get("result") if isinstance(data, dict) else None
            info = None
            if isinstance(result_map, dict):
                info = result_map.get(key) or next(iter(result_map.values()), None)
            result = classify_security(key, info if isinstance(info, dict) else None)
            _cache[key] = (now, result)
            return result
        except Exception as exc:  # noqa: BLE001
            logger.warning("GoPlus error for %s: %r", key[:12], exc)
            result = TokenSecurity(key, False, None, None)
            _cache[key] = (now, result)
            return result


async def check_token_security(address: str) -> TokenSecurity:
    return await _fetch_one(address)


async def check_tokens_security(addresses: list[str]) -> dict[str, TokenSecurity]:
    """Check many addresses concurrently (deduped). Fail-open on API errors."""
    uniq = list(dict.fromkeys(a.lower() for a in addresses if a))
    if not uniq:
        return {}
    results = await asyncio.gather(*(_fetch_one(a) for a in uniq))
    return {r.address: r for r in results}


async def honeypot_reason(address: str) -> str | None:
    """Return human-readable block reason, or None if allowed / unknown."""
    sec = await check_token_security(address)
    if not sec.blocked:
        return None
    return sec.reason or "honeypot"
