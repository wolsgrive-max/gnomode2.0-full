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


@pytest.mark.asyncio
async def test_incremental_enrich_caps_with_stride(monkeypatch):
    """Unlimited newest-first enrich starves mid-age liquid tokens under V4 spam."""
    from app.token_index import _INCREMENTAL_NEW_LIMIT

    idx = TokenIndex()
    idx.cold_started = True
    for i in range(_INCREMENTAL_NEW_LIMIT + 200):
        addr = f"0x{'%040x' % (i + 1)}"
        idx._tokens[addr] = TokenEntry(
            address=addr,
            dex="uniswap_v4",
            quote_address="0x" + "11" * 20,
            created_block=2_000_000 + i,
            pool_id="0x" + f"{i:064x}",
        )

    calls: list[dict] = []

    async def fake_scan(*, full: bool, on_progress=None):  # noqa: ANN001
        return []

    async def fake_enrich(
        *,
        stale_limit=None,
        new_limit=None,
        concurrency=None,
        on_progress=None,
        progress_label="Enriching new tokens",
    ):  # noqa: ANN001
        calls.append({"stale_limit": stale_limit, "new_limit": new_limit})

    monkeypatch.setattr(idx, "scan_new_pools", fake_scan)
    monkeypatch.setattr(idx, "enrich_pending", fake_enrich)
    monkeypatch.setattr(idx, "_apply_gecko_peaks", AsyncMock(return_value=0))
    monkeypatch.setattr(idx, "_prune", lambda: None)
    monkeypatch.setattr(idx, "_parse_active", lambda: False)

    await idx.refresh(full=False)

    assert calls
    assert calls[0]["new_limit"] == _INCREMENTAL_NEW_LIMIT
    assert calls[0]["stale_limit"] is not None


@pytest.mark.asyncio
async def test_parse_busy_still_enriches_busy_slice(monkeypatch):
    from app.token_index import _BUSY_NEW_LIMIT

    idx = TokenIndex()
    idx.cold_started = True
    calls: list[dict] = []
    scans: list[bool] = []

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
                "progress_label": progress_label,
            }
        )

    async def fake_scan(*, full: bool, on_progress=None):  # noqa: ANN001
        scans.append(full)
        return []

    monkeypatch.setattr(idx, "scan_new_pools", fake_scan)
    monkeypatch.setattr(idx, "enrich_pending", fake_enrich)
    monkeypatch.setattr(idx, "_apply_gecko_peaks", AsyncMock(return_value=0))
    monkeypatch.setattr(idx, "_prune", lambda: None)
    monkeypatch.setattr(idx, "_parse_active", lambda: True)

    await idx.refresh(full=False)

    assert scans == [False]
    assert len(calls) == 1
    assert calls[0]["new_limit"] == _BUSY_NEW_LIMIT
    assert calls[0]["stale_limit"] == 0
    assert "Busy-slice" in calls[0]["progress_label"]


@pytest.mark.asyncio
async def test_enrich_pending_stride_includes_older_never_enriched(monkeypatch):
    """Newest + oldest + mid stride — mid-age tokens get a turn."""
    from app.token_index import _INCREMENTAL_NEW_LIMIT

    idx = TokenIndex()
    # 0 = oldest created_block, N = newest
    n = _INCREMENTAL_NEW_LIMIT * 4
    for i in range(n):
        addr = f"0x{'%040x' % (i + 1)}"
        idx._tokens[addr] = TokenEntry(
            address=addr,
            dex="uniswap_v4",
            quote_address="0x" + "11" * 20,
            created_block=1000 + i,
            pool_id="0x" + f"{i:064x}",
        )

    async def fake_pairs(client, addrs):  # noqa: ANN001
        return []

    monkeypatch.setattr("app.screener._fetch_dex_pairs", fake_pairs)

    await idx.enrich_pending(stale_limit=0, new_limit=_INCREMENTAL_NEW_LIMIT)

    enriched = [e for e in idx._tokens.values() if e.screened is not None]
    assert len(enriched) == _INCREMENTAL_NEW_LIMIT
    blocks = {e.created_block for e in enriched}
    # Newest third should be present.
    newest = 1000 + n - 1
    assert newest in blocks
    # Oldest third must be present (FIFO backlog drain).
    oldest = 1000
    assert oldest in blocks
    # Mid-age band must get stride hits.
    mid = 1000 + n // 2
    mid_hits = [b for b in blocks if abs(b - mid) < n // 4]
    assert mid_hits, f"expected mid-age hits near {mid}, got {sorted(blocks)[:15]}…"


def test_select_never_enriched_covers_newest_oldest_mid():
    from app.token_index import _select_never_enriched

    entries = [
        TokenEntry(
            address=f"0x{'%040x' % (i + 1)}",
            dex="uniswap_v4",
            quote_address="0x" + "11" * 20,
            created_block=10_000 + i,
            pool_id="0x" + f"{i:064x}",
        )
        for i in range(90)
    ]
    # Newest-first (as enrich_pending sorts).
    entries.sort(key=lambda e: -e.created_block)
    picked = _select_never_enriched(entries, 30)
    assert len(picked) == 30
    blocks = {e.created_block for e in picked}
    assert 10_000 + 89 in blocks  # newest
    assert 10_000 in blocks  # oldest
    assert any(10_000 + 30 <= b <= 10_000 + 60 for b in blocks)


@pytest.mark.asyncio
async def test_parse_busy_still_scans_factories(monkeypatch):
    """Busy parse must still discover new pools (enrich cannot invent them)."""
    from app.token_index import _BUSY_NEW_LIMIT

    idx = TokenIndex()
    idx.cold_started = True
    scans: list[bool] = []
    calls: list[dict] = []

    async def fake_scan(*, full: bool, on_progress=None):  # noqa: ANN001
        scans.append(full)
        return []

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
                "progress_label": progress_label,
            }
        )

    monkeypatch.setattr(idx, "scan_new_pools", fake_scan)
    monkeypatch.setattr(idx, "enrich_pending", fake_enrich)
    monkeypatch.setattr(idx, "_apply_gecko_peaks", AsyncMock(return_value=0))
    monkeypatch.setattr(idx, "_prune", lambda: None)
    monkeypatch.setattr(idx, "_parse_active", lambda: True)

    await idx.refresh(full=False)

    assert scans == [False]
    assert len(calls) == 1
    assert calls[0]["new_limit"] == _BUSY_NEW_LIMIT
    assert calls[0]["stale_limit"] == 0


@pytest.mark.asyncio
async def test_cold_tail_busy_skips_incremental(monkeypatch):
    """While cold tail runs, skip scan/DS enrich but still allow Gecko."""
    idx = TokenIndex()
    idx.cold_started = True
    gecko_calls: list[int] = []

    async def boom_scan(*, full: bool, on_progress=None):  # noqa: ANN001
        raise AssertionError("scan must wait for cold tail")

    async def fake_gecko(addrs, *, limit=16):  # noqa: ANN001
        gecko_calls.append(limit)
        return 0

    monkeypatch.setattr(idx, "scan_new_pools", boom_scan)
    monkeypatch.setattr(idx, "_cold_tail_busy", lambda: True)
    monkeypatch.setattr(idx, "_parse_active", lambda: False)
    monkeypatch.setattr(idx, "_gecko_refresh_candidates", lambda: ["0xabc"])
    monkeypatch.setattr(idx, "_apply_gecko_peaks", fake_gecko)

    await idx.refresh(full=False)
    assert gecko_calls == [8]
