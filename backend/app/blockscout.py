"""Blockscout API helpers (metadata + transfer fallback)."""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator

from .chain import http_client
from .config import settings
from .constants import BLOCKSCOUT_BASE, BLOCKSCOUT_PRO_BASE

logger = logging.getLogger(__name__)

# When Pro returns 401/402 (bad key / out of credits), flip to public explorer
# for the rest of the process lifetime.
_pro_disabled: bool = False


def _public_base() -> str:
    return f"{BLOCKSCOUT_BASE}/api/v2"


def _use_pro() -> bool:
    return bool(settings.blockscout_api_key) and not _pro_disabled


def disable_blockscout_pro(reason: str) -> None:
    global _pro_disabled
    if _pro_disabled:
        return
    _pro_disabled = True
    logger.warning(
        "Blockscout Pro disabled (%s) — falling back to %s",
        reason,
        _public_base(),
    )


def _base_url() -> str:
    if _use_pro():
        return BLOCKSCOUT_PRO_BASE
    return _public_base()


def _headers() -> dict[str, str]:
    # Pro docs: Authorization Bearer … OR apikey query param (not a custom header).
    if _use_pro():
        return {"Authorization": f"Bearer {settings.blockscout_api_key}"}
    return {}


def blockscout_api_base() -> str:
    return _base_url()


def blockscout_headers() -> dict[str, str]:
    return _headers()


def blockscout_auth_params() -> dict[str, str]:
    """Extra query params for Pro (apikey=) — safe no-op on public."""
    if _use_pro():
        return {"apikey": settings.blockscout_api_key}
    return {}


async def _get_json(
    path: str, *, params: dict[str, Any] | None = None
) -> tuple[int, Any] | None:
    """GET ``path`` under current base; on Pro 401/402 flip to public and retry once."""
    client = http_client()
    params = dict(params or {})
    for _ in range(2):
        url = f"{_base_url()}{path}"
        req_params = {**blockscout_auth_params(), **params}
        try:
            resp = await client.get(url, params=req_params, headers=_headers())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Blockscout GET %s failed: %s", path, exc)
            return None
        if resp.status_code in (401, 402) and _use_pro():
            disable_blockscout_pro(f"HTTP {resp.status_code}")
            continue
        try:
            return resp.status_code, resp.json()
        except Exception:  # noqa: BLE001
            return resp.status_code, None
    return None


async def fetch_address_info(address: str) -> dict[str, Any] | None:
    got = await _get_json(f"/addresses/{address}")
    if got is None or got[0] != 200 or not isinstance(got[1], dict):
        return None
    return got[1]


async def fetch_token_info(token: str) -> dict[str, Any] | None:
    got = await _get_json(f"/tokens/{token}")
    if got is None or got[0] != 200 or not isinstance(got[1], dict):
        return None
    return got[1]


async def iter_token_transfers(token: str) -> AsyncIterator[dict[str, Any]]:
    """Paginate ERC-20 transfers for a token (newest first on Blockscout)."""
    params: dict[str, Any] = {}
    while True:
        got = await _get_json(f"/tokens/{token}/transfers", params=params)
        if got is None or got[0] != 200 or not isinstance(got[1], dict):
            if got is not None:
                logger.warning("Blockscout transfers %s", got[0])
            return
        data = got[1]
        items = data.get("items") or []
        for item in items:
            yield item
        next_params = data.get("next_page_params")
        if not next_params:
            return
        params = next_params


def _transfer_block_number(item: dict[str, Any]) -> int:
    raw = item.get("block_number")
    if raw is None:
        raw = item.get("blockNumber")
    if raw is None:
        return 0
    try:
        if isinstance(raw, str) and raw.startswith(("0x", "0X")):
            return int(raw, 16)
        return int(raw)
    except (TypeError, ValueError):
        return 0


async def iter_address_token_transfers(
    wallet: str,
    *,
    max_pages: int = 8,
    direction: str | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Paginate ERC-20 transfers for a wallet (newest first).

    ``direction``: ``\"to\"`` = inbound only, ``\"from\"`` = outbound, ``None`` = all.
    """
    params: dict[str, Any] = {}
    if direction:
        params["filter"] = direction
    for _ in range(max(1, max_pages)):
        got = await _get_json(f"/addresses/{wallet}/token-transfers", params=params)
        if got is None:
            return
        status, data = got
        if status == 404:
            return
        if status != 200 or not isinstance(data, dict):
            logger.warning("Blockscout wallet transfers %s", status)
            return
        items = data.get("items") or []
        for item in items:
            yield item
        next_params = data.get("next_page_params")
        if not next_params or not items:
            return
        params = dict(next_params)
        if direction and "filter" not in params:
            params["filter"] = direction


async def scan_address_token_transfers(
    wallet: str,
    *,
    max_pages: int = 8,
    after_block: int = 0,
    direction: str | None = "to",
) -> tuple[list[dict[str, Any]], int, bool]:
    """Fetch newest-first wallet transfers with catch-up semantics.

    Returns ``(items, max_block_seen, caught_up)``.

    Items are only those with ``block > after_block`` (when ``after_block > 0``).
    ``caught_up`` is True when the scan reached ``after_block`` or exhausted
    Blockscout pages. If ``max_pages`` runs out first, ``caught_up`` is False —
    the caller must not advance a watermark past unread history (sells/noise
    otherwise bury earlier buys forever).
    """
    params: dict[str, Any] = {}
    if direction:
        params["filter"] = direction
    items_out: list[dict[str, Any]] = []
    max_block_seen = 0
    caught_up = False
    pages = max(1, max_pages)

    for _ in range(pages):
        got = await _get_json(f"/addresses/{wallet}/token-transfers", params=params)
        if got is None:
            # Transient failure — do not claim catch-up (retry next cycle).
            return items_out, max_block_seen, False
        status, data = got
        if status == 404:
            return items_out, max_block_seen, True
        if status != 200 or not isinstance(data, dict):
            logger.warning("Blockscout wallet transfers %s", status)
            return items_out, max_block_seen, False
        items = data.get("items") or []
        next_params = data.get("next_page_params")
        if not items:
            caught_up = True
            break
        page_hit_wm = False
        for item in items:
            block = _transfer_block_number(item)
            if block > max_block_seen:
                max_block_seen = block
            if after_block > 0:
                if block <= 0:
                    continue
                if block <= after_block:
                    page_hit_wm = True
                    caught_up = True
                    break
            items_out.append(item)
        if page_hit_wm:
            break
        if not next_params:
            caught_up = True
            break
        params = dict(next_params)
        if direction and "filter" not in params:
            params["filter"] = direction
    return items_out, max_block_seen, caught_up
