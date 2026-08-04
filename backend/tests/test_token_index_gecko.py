"""Gecko ATH bumps entry.ath_mcap in the token index."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, patch

import pytest

from app.ath_gecko import GeckoAthResult
from app.models import ScreenedToken
from app.token_index import TokenEntry, TokenIndex


def _entry(
    addr: str,
    *,
    block: int,
    ath: float = 2_600.0,
    gecko_at: float = 0.0,
    pair_age_h: float | None = 2.0,
    first_seen: float | None = None,
) -> TokenEntry:
    return TokenEntry(
        address=addr,
        dex="uniswap_v3",
        quote_address="0xquote",
        created_block=block,
        pool_address="0xpool",
        ath_mcap=ath,
        gecko_ath_at=gecko_at,
        first_seen=first_seen if first_seen is not None else time.time(),
        screened=ScreenedToken(
            address=addr,
            symbol=addr[-3:].upper(),
            market_cap=ath,
            ath_mcap=ath,
            pair_age_hours=pair_age_h,
            liquidity_usd=1_000.0,
        ),
    )


@pytest.mark.asyncio
async def test_apply_gecko_peaks_bumps_ath():
    idx = TokenIndex()
    entry = TokenEntry(
        address="0xTok",
        dex="uniswap_v3",
        quote_address="0xquote",
        created_block=10,
        pool_address="0xpool",
        ath_mcap=10_000.0,
        screened=ScreenedToken(
            address="0xTok",
            symbol="TOK",
            market_cap=10_000.0,
            ath_mcap=10_000.0,
        ),
    )
    idx._tokens["0xtok"] = entry

    fake = GeckoAthResult(token="0xtok", ath_mcap=77_000.0, pool="0xpool")
    with patch(
        "app.ath_gecko.fetch_token_ath_mcap",
        new=AsyncMock(return_value=fake),
    ):
        n = await idx._apply_gecko_peaks(["0xTok"], limit=10)

    assert n == 1
    assert entry.ath_mcap == 77_000.0
    assert entry.gecko_ath_at > 0
    assert entry.screened is not None
    assert entry.screened.ath_mcap == 77_000.0


@pytest.mark.asyncio
async def test_young_never_probed_beats_old_never_probed():
    """PCC-class miss: young dump must win Gecko budget over stale queue."""
    idx = TokenIndex()
    young = _entry("0xYoungPcc", block=9_999_999, ath=2_600.0, gecko_at=0.0, pair_age_h=1.5)
    olds = [
        _entry(f"0xOld{i:02d}", block=i, ath=100_000.0, gecko_at=0.0, pair_age_h=72.0)
        for i in range(20)
    ]
    idx._tokens[young.address.lower()] = young
    for e in olds:
        idx._tokens[e.address.lower()] = e

    probed: list[str] = []

    async def fake_fetch(token: str, pool=None):
        probed.append(token.lower())
        return GeckoAthResult(token=token.lower(), ath_mcap=523_000.0, pool=pool or "")

    addrs = [e.address for e in olds] + [young.address]
    with patch("app.ath_gecko.fetch_token_ath_mcap", new=AsyncMock(side_effect=fake_fetch)):
        with patch("app.token_index.asyncio.sleep", new=AsyncMock()):
            n = await idx._apply_gecko_peaks(addrs, limit=4)

    assert n == 4
    assert probed[0] == young.address.lower()
    assert young.ath_mcap == 523_000.0
    assert young.gecko_ath_at > 0


@pytest.mark.asyncio
async def test_young_failure_not_stamped_allows_retry():
    idx = TokenIndex()
    young = _entry("0xYoungFail", block=100, gecko_at=0.0, pair_age_h=3.0)
    idx._tokens[young.address.lower()] = young

    with patch(
        "app.ath_gecko.fetch_token_ath_mcap",
        new=AsyncMock(side_effect=RuntimeError("429")),
    ):
        with patch("app.token_index.asyncio.sleep", new=AsyncMock()):
            await idx._apply_gecko_peaks([young.address], limit=1)

    assert young.gecko_ath_at == 0.0


def test_gecko_refresh_candidates_includes_young_under_cap():
    idx = TokenIndex()
    never = _entry("0xNever", block=1, gecko_at=0.0, pair_age_h=48.0)
    young_probed = _entry(
        "0xYoungRetry",
        block=2,
        gecko_at=time.time() - 200.0,
        pair_age_h=5.0,
    )
    old_probed = _entry(
        "0xOldProbed",
        block=3,
        gecko_at=time.time() - 200.0,
        pair_age_h=72.0,
    )
    idx._tokens[never.address.lower()] = never
    idx._tokens[young_probed.address.lower()] = young_probed
    idx._tokens[old_probed.address.lower()] = old_probed

    keys = set(idx._gecko_refresh_candidates())
    assert never.address.lower() in keys
    assert young_probed.address.lower() in keys
    assert old_probed.address.lower() not in keys
