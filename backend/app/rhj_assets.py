"""Robinhood stock-token (RHJ) blacklist via public assets API."""

from __future__ import annotations

import logging
import time
from typing import Any

from .chain import http_client
from .database import get_db

logger = logging.getLogger(__name__)

# Public catalog of tokenized equities on Robinhood Chain (best-effort).
_RHJ_URLS = (
    "https://api.robinhood.com/rhj/assets/",
    "https://robinhood.com/api/rhj/assets/",
)

_CACHE_TTL = 6 * 3600.0
_cached_at = 0.0
_cached_addrs: set[str] = set()


def _extract_addresses(payload: Any) -> set[str]:
    out: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                kl = str(k).lower()
                if kl in (
                    "address",
                    "contract_address",
                    "contractaddress",
                    "token_address",
                    "tokenaddress",
                ) and isinstance(v, str) and v.startswith("0x") and len(v) >= 42:
                    out.add(v.lower())
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return out


async def fetch_rhj_addresses() -> set[str]:
    global _cached_at, _cached_addrs
    now = time.time()
    if _cached_addrs and now - _cached_at < _CACHE_TTL:
        return _cached_addrs

    client = http_client()
    for url in _RHJ_URLS:
        try:
            resp = await client.get(url, timeout=20.0)
            if resp.status_code != 200:
                logger.debug("RHJ %s → %s", url, resp.status_code)
                continue
            data = resp.json()
            addrs = _extract_addresses(data)
            if addrs:
                _cached_addrs = addrs
                _cached_at = now
                logger.info("RHJ assets: %d addresses from %s", len(addrs), url)
                return addrs
        except Exception as exc:  # noqa: BLE001
            logger.debug("RHJ fetch %s failed: %s", url, exc)
    return _cached_addrs


async def sync_rhj_blacklist() -> int:
    """Pull RHJ assets into SQLite blacklist. Returns count upserted."""
    addrs = await fetch_rhj_addresses()
    if not addrs:
        return 0
    db = get_db()
    n = 0
    for a in addrs:
        await db.aadd_blacklist(a, reason="rhj_stock_token", source="rhj")
        n += 1
    return n


async def is_rhj_token(address: str) -> bool:
    addrs = await fetch_rhj_addresses()
    if address.lower() in addrs:
        return True
    return await get_db().ais_blacklisted(address)
