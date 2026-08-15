"""Blockscout API helpers (metadata + transfer fallback).

All GETs share a process-wide pace + concurrency cap so follow-up scans,
wallet metrics, and buy-gate lookups do not stampede the public explorer
into 429 storms. Prefer accuracy (retry + Retry-After) over speed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator

from .chain import http_client
from .config import settings
from .constants import BLOCKSCOUT_BASE, BLOCKSCOUT_PRO_BASE

logger = logging.getLogger(__name__)

# When Pro returns 401/402 (bad key / out of credits), flip to public explorer
# for the rest of the process lifetime.
_pro_disabled: bool = False

# Global Blockscout pacing (shared limiter). Keep conservative on the public
# explorer; Pro key unlocks a modestly higher ceiling for watch unique enrich.
# ~2–3 req/s with concurrency 2 avoided transfer 429 storms in production.
_BS_CONCURRENCY = 2
_BS_MIN_INTERVAL = 0.40  # seconds between request *starts*
_BS_CONCURRENCY_PRO = 4
_BS_MIN_INTERVAL_PRO = 0.22
_BS_MAX_ATTEMPTS = 8
_bs_sem_public = asyncio.Semaphore(_BS_CONCURRENCY)
_bs_sem_pro = asyncio.Semaphore(_BS_CONCURRENCY_PRO)
_bs_pace_lock = asyncio.Lock()
_bs_next_ok = 0.0


def _public_base() -> str:
    return f"{BLOCKSCOUT_BASE}/api/v2"


def _use_pro() -> bool:
    return bool(settings.blockscout_api_key) and not _pro_disabled


def _bs_sem() -> asyncio.Semaphore:
    return _bs_sem_pro if _use_pro() else _bs_sem_public


def _bs_min_interval() -> float:
    return _BS_MIN_INTERVAL_PRO if _use_pro() else _BS_MIN_INTERVAL


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


async def _pace_blockscout() -> None:
    """Wait until the shared Blockscout token-bucket allows another request."""
    global _bs_next_ok
    interval = _bs_min_interval()
    async with _bs_pace_lock:
        now = time.time()
        wait = _bs_next_ok - now
        if wait > 0:
            await asyncio.sleep(wait)
        _bs_next_ok = time.time() + interval


def _retry_after_seconds(resp: Any, fallback: float, *, cap: float = 60.0) -> float:
    ra = getattr(getattr(resp, "headers", None), "get", lambda _k: None)("Retry-After")
    if ra is None:
        return fallback
    try:
        # Prefer the larger of Retry-After and our backoff — tiny RA values
        # (0 / 0.2s) would otherwise stampede right back into 429.
        return max(fallback, min(float(ra), cap))
    except (TypeError, ValueError):
        return fallback


async def _get_json(
    path: str, *, params: dict[str, Any] | None = None
) -> tuple[int, Any] | None:
    """Paced GET under current base; retries 429/5xx; Pro 401/402 → public."""
    client = http_client()
    params = dict(params or {})
    delay = 0.75
    # Extra attempts when Pro flips mid-loop (don't burn the retry budget).
    attempts = 0
    max_attempts = _BS_MAX_ATTEMPTS + 2
    while attempts < max_attempts:
        attempts += 1
        url = f"{_base_url()}{path}"
        req_params = {**blockscout_auth_params(), **params}
        resp = None
        async with _bs_sem():
            await _pace_blockscout()
            try:
                resp = await client.get(url, params=req_params, headers=_headers())
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Blockscout GET %s error attempt %s: %s", path, attempts, exc
                )
                await asyncio.sleep(delay)
                delay = min(delay * 1.7, 45.0)
                continue

        if resp.status_code in (401, 402, 403) and _use_pro():
            disable_blockscout_pro(f"HTTP {resp.status_code}")
            continue

        if resp.status_code in (429, 502, 503):
            sleep_for = _retry_after_seconds(resp, delay, cap=90.0)
            logger.warning(
                "Blockscout %s → HTTP %s; backing off %.1fs (attempt %s)",
                path,
                resp.status_code,
                sleep_for,
                attempts,
            )
            # Push the shared pace forward so other callers don't stampede.
            global _bs_next_ok
            async with _bs_pace_lock:
                _bs_next_ok = max(_bs_next_ok, time.time() + sleep_for)
            await asyncio.sleep(sleep_for)
            delay = min(delay * 1.8, 45.0)
            continue

        try:
            return resp.status_code, resp.json()
        except Exception:  # noqa: BLE001
            return resp.status_code, None

    logger.warning("Blockscout GET %s exhausted retries", path)
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
