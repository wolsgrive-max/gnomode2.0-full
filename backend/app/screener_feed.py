"""Token feed for watch/Хвать: local screener or remote truegnomode.

truegnomode exposes screened tokens via async jobs:
  POST {base}/api/screen  → {job_id, status, ...}
  GET  {base}/api/screen/{job_id} → poll until done/error; results[] = ScreenedToken

We do not rewrite truegnomode screener internals — only consume its output.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urljoin

import httpx

from .config import settings
from .models import ScreenRequest, ScreenedToken
from .screener import screen_tokens as screen_tokens_local

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]


def truegnomode_base_url() -> str:
    """Configured truegnomode API root, or empty for local screener."""
    return (settings.truegnomode_screener_url or "").strip().rstrip("/")


def using_remote_screener() -> bool:
    return bool(truegnomode_base_url())


def _core_screen_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Map a truegnomode ScreenedToken JSON object onto our ScreenedToken fields."""
    keys = (
        "address",
        "symbol",
        "name",
        "pair_address",
        "dex_id",
        "price_usd",
        "liquidity_usd",
        "market_cap",
        "ath_mcap",
        "traders_24h",
        "buys_24h",
        "sells_24h",
        "pair_created_at_ms",
        "pair_age_hours",
        "url",
        "gmgn_url",
    )
    out: dict[str, Any] = {k: row[k] for k in keys if k in row}
    # Donor screen mirrors observed peak onto ath_mcap; if ath is missing,
    # fall back to dexscreener_observed_peak_mcap_usd / market_cap.
    ath = out.get("ath_mcap")
    if ath is None or float(ath or 0) <= 0:
        peak = row.get("dexscreener_observed_peak_mcap_usd")
        if peak is not None and float(peak or 0) > 0:
            out["ath_mcap"] = float(peak)
        elif out.get("market_cap") is not None:
            out["ath_mcap"] = float(out["market_cap"] or 0)
    return out


def map_remote_results(raw_rows: list[dict[str, Any]]) -> list[ScreenedToken]:
    tokens: list[ScreenedToken] = []
    for row in raw_rows:
        if not isinstance(row, dict) or not row.get("address"):
            continue
        try:
            tokens.append(ScreenedToken.model_validate(_core_screen_fields(row)))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Skip malformed truegnomode token %s: %s",
                str(row.get("address", ""))[:12],
                type(exc).__name__,
            )
    return tokens


def _remote_screen_payload(
    req: ScreenRequest,
    *,
    force_enrich_addresses: list[str] | None = None,
) -> dict[str, Any]:
    """Build POST /api/screen body compatible with truegnomode donor defaults."""
    payload = req.model_dump(exclude_none=True)
    # Production truegnomode path — never ask for legacy LITE quality screen.
    payload["screen_pipeline_mode"] = "donor"
    if force_enrich_addresses:
        payload["force_enrich_addresses"] = [
            a for a in force_enrich_addresses if a and str(a).strip()
        ]
    return payload


async def fetch_truegnomode_screen(
    req: ScreenRequest,
    *,
    base_url: str | None = None,
    on_progress: ProgressCb | None = None,
    force_enrich_addresses: list[str] | None = None,
    timeout_sec: float | None = None,
    poll_interval_sec: float | None = None,
) -> list[ScreenedToken]:
    """Start a truegnomode screen job and return mapped ScreenedToken rows."""
    base = (base_url or truegnomode_base_url()).rstrip("/")
    if not base:
        raise ValueError("truegnomode_screener_url is empty")

    timeout = float(
        timeout_sec
        if timeout_sec is not None
        else settings.truegnomode_screen_timeout_sec
    )
    poll = float(
        poll_interval_sec
        if poll_interval_sec is not None
        else settings.truegnomode_poll_interval_sec
    )
    poll = max(0.1, poll)

    async def prog(stage: str, message: str, percent: float) -> None:
        if on_progress is not None:
            await on_progress(stage, message, percent)

    screen_url = urljoin(base + "/", "api/screen")
    payload = _remote_screen_payload(req, force_enrich_addresses=force_enrich_addresses)
    await prog("screen", f"truegnomode: POST /api/screen ({base})", 0.05)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=60.0)) as client:
        last_exc: Exception | None = None
        started = None
        for attempt in range(1, 4):
            try:
                started = await client.post(screen_url, json=payload)
                break
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_exc = exc
                logger.warning(
                    "truegnomode screen start attempt %s failed: %s",
                    attempt,
                    type(exc).__name__,
                )
                await asyncio.sleep(min(2.0 * attempt, 6.0))
        if started is None:
            assert last_exc is not None
            raise RuntimeError(
                f"truegnomode unreachable at {base}: {type(last_exc).__name__}"
            ) from last_exc

        if started.status_code >= 400:
            raise RuntimeError(
                f"truegnomode POST /api/screen HTTP {started.status_code}: "
                f"{started.text[:300]}"
            )
        body = started.json()
        job_id = str(body.get("job_id") or "").strip()
        if not job_id:
            raise RuntimeError("truegnomode screen response missing job_id")

        status_url = urljoin(base + "/", f"api/screen/{job_id}")
        deadline = asyncio.get_running_loop().time() + timeout
        last_results: list[dict[str, Any]] = []

        while True:
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(
                    f"truegnomode screen job {job_id} timed out after {timeout:.0f}s"
                )
            try:
                resp = await client.get(status_url)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                logger.warning(
                    "truegnomode poll %s transport: %s",
                    job_id,
                    type(exc).__name__,
                )
                await asyncio.sleep(poll)
                continue

            if resp.status_code >= 400:
                raise RuntimeError(
                    f"truegnomode GET /api/screen/{job_id} HTTP {resp.status_code}"
                )
            job = resp.json()
            status = str(job.get("status") or "")
            progress = job.get("progress") or {}
            stage = str(progress.get("stage") or status or "screen")
            message = str(progress.get("message") or f"truegnomode job {status}")
            # truegnomode stores percent as 0–100 in JobProgress.
            raw_pct = progress.get("percent")
            try:
                pct = float(raw_pct) if raw_pct is not None else 0.0
            except (TypeError, ValueError):
                pct = 0.0
            if pct > 1.0:
                pct = pct / 100.0
            await prog(stage, message, max(0.05, min(pct, 0.99)))

            raw_rows = job.get("results")
            if isinstance(raw_rows, list):
                last_results = [r for r in raw_rows if isinstance(r, dict)]

            if status in ("done", "error"):
                if status == "error":
                    err = job.get("error") or "unknown error"
                    raise RuntimeError(f"truegnomode screen job failed: {err}")
                tokens = map_remote_results(last_results)
                await prog("done", f"truegnomode: {len(tokens)} tokens", 1.0)
                logger.info(
                    "truegnomode screen job %s done — %s tokens from %s",
                    job_id,
                    len(tokens),
                    base,
                )
                return tokens

            await asyncio.sleep(poll)


async def fetch_screened_tokens(
    req: ScreenRequest,
    *,
    on_progress: ProgressCb | None = None,
    force_enrich_addresses: list[str] | None = None,
) -> list[ScreenedToken]:
    """Watch token feed: remote truegnomode when configured, else local screener."""
    if using_remote_screener():
        return await fetch_truegnomode_screen(
            req,
            on_progress=on_progress,
            force_enrich_addresses=force_enrich_addresses,
        )
    return await screen_tokens_local(req, on_progress=on_progress)
