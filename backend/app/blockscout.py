"""Blockscout API helpers (metadata + transfer fallback)."""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator

from .chain import http_client
from .config import settings
from .constants import BLOCKSCOUT_BASE, BLOCKSCOUT_PRO_BASE

logger = logging.getLogger(__name__)


def _base_url() -> str:
    if settings.blockscout_api_key:
        return BLOCKSCOUT_PRO_BASE
    return f"{BLOCKSCOUT_BASE}/api/v2"


def _headers() -> dict[str, str]:
    if settings.blockscout_api_key:
        return {"apikey": settings.blockscout_api_key}
    return {}


def blockscout_api_base() -> str:
    return _base_url()


def blockscout_headers() -> dict[str, str]:
    return _headers()


async def fetch_address_info(address: str) -> dict[str, Any] | None:
    url = f"{_base_url()}/addresses/{address}"
    try:
        resp = await http_client().get(url, headers=_headers())
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Blockscout address info failed: %s", exc)
        return None


async def fetch_token_info(token: str) -> dict[str, Any] | None:
    url = f"{_base_url()}/tokens/{token}"
    try:
        resp = await http_client().get(url, headers=_headers())
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Blockscout token info failed: %s", exc)
        return None


async def iter_token_transfers(token: str, max_pages: int = 50) -> AsyncIterator[dict[str, Any]]:
    """Paginate ERC-20 transfers for a token (newest first on Blockscout)."""
    url = f"{_base_url()}/tokens/{token}/transfers"
    params: dict[str, Any] = {}
    client = http_client()
    page = 0
    while page < max_pages:
        page += 1
        try:
            resp = await client.get(url, params=params, headers=_headers())
            if resp.status_code != 200:
                logger.warning("Blockscout transfers %s: %s", resp.status_code, resp.text[:200])
                return
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Blockscout transfers error: %s", exc)
            return

        items = data.get("items") or []
        for item in items:
            yield item

        next_params = data.get("next_page_params")
        if not next_params:
            return
        params = next_params
