"""Cold-start first-wave readiness for watch/Хвать."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from app.token_index import TokenEntry, TokenIndex, _COLD_READY_NEW


@pytest.mark.asyncio
async def test_cold_refresh_marks_ready_after_first_wave(monkeypatch):
    idx = TokenIndex()
    # Seed more tokens than the first-wave cap.
    for i in range(_COLD_READY_NEW + 50):
        addr = f"0x{'%040x' % i}"
        idx._tokens[addr] = TokenEntry(
            address=addr,
            dex="uniswap_v2",
            quote_address="0x" + "11" * 20,
            created_block=1_000_000 + i,
            pool_address="0x" + "22" * 20,
        )

    async def fake_scan(*, full: bool, on_progress=None):  # noqa: ANN001
        return []

    calls: list[dict] = []

    async def fake_enrich(
        *,
        stale_limit=None,
        new_limit=None,
        concurrency=None,
        on_progress=None,
        progress_label="Enriching new tokens",
    ):  # noqa: ANN001
        calls.append(
            {
                "stale_limit": stale_limit,
                "new_limit": new_limit,
                "concurrency": concurrency,
                "progress_label": progress_label,
            }
        )
        # Mimic first-wave enrich marking newest N as screened.
        if new_limit is not None:
            pending = [e for e in idx._tokens.values() if e.screened is None]
            pending.sort(key=lambda e: -e.created_block)
            for e in pending[:new_limit]:
                from app.models import ScreenedToken

                e.screened = ScreenedToken(
                    address=e.address,
                    symbol="T",
                    name="T",
                    liquidity_usd=1000,
                    market_cap=5000,
                    ath_mcap=5000,
                    traders=1,
                    pair_age_hours=1,
                    pair_address=e.pool_address or e.address,
                    dex=e.dex,
                    quote_address=e.quote_address,
                )
                e.enriched_at = 1.0
            # First wave should flip ready before tail runs.
            assert idx.cold_started is False

    monkeypatch.setattr(idx, "scan_new_pools", fake_scan)
    monkeypatch.setattr(idx, "enrich_pending", fake_enrich)
    monkeypatch.setattr(idx, "_apply_gecko_peaks", AsyncMock(return_value=0))
    monkeypatch.setattr(idx, "_prune", lambda: None)

    await idx.refresh(full=True)

    assert idx.cold_started is True
    assert idx.building is False
    assert calls
    assert calls[0]["new_limit"] == _COLD_READY_NEW
    # Tail scheduled as background task.
    assert idx._cold_tail_busy()
    await asyncio.sleep(0.05)
    # Wait briefly for scheduled tail to finish (second enrich call).
    for _ in range(50):
        if not idx._cold_tail_busy():
            break
        await asyncio.sleep(0.02)
    assert not idx._cold_tail_busy()
    assert any(c.get("progress_label") == "Cold remaining enrich" for c in calls)
