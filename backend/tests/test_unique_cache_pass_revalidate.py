"""Cached unique counts ≤ too_many must not admit wallets without revalidation."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app import wallet_metrics as wm
from app import wallet_unique_cache as uc


@pytest.mark.asyncio
async def test_stale_unique1_cache_revalidated_when_too_many(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "wallet_unique.db"
    monkeypatch.setattr(
        "app.wallet_unique_cache.settings.unique_cache_ttl_sec", 6 * 3600
    )
    uc.reset_for_tests(path)
    wm._tokens7d_cache.clear()
    wallet = "0xcc4bc7882d5ec4a3716e589ab3d816b61ae47a1a"
    # Poison: exact=1 cached (true right after first buy).
    uc.put_exact(wallet, 720, 1, exact=True, now=time.time())
    wm._tokens7d_cache[f"{wm._TOKENS7D_CACHE_VER}:h720:{wallet}"] = (
        1,
        time.time(),
    )

    async def fake_one(*_a, **_k):
        return 2, False  # early-exit too_many

    with patch.object(wm, "_tokens_traded_7d_one", new=AsyncMock(side_effect=fake_one)):
        out = await wm.batch_tokens_traded_7d(
            [wallet], lookback_hours=720.0, enough=1, too_many=1
        )
    assert out[wallet] == 2
    uc.reset_for_tests(None)
    wm._tokens7d_cache.clear()


@pytest.mark.asyncio
async def test_cached_reject_above_too_many_trusted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "wallet_unique.db"
    monkeypatch.setattr(
        "app.wallet_unique_cache.settings.unique_cache_ttl_sec", 6 * 3600
    )
    uc.reset_for_tests(path)
    wm._tokens7d_cache.clear()
    wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    uc.put_exact(wallet, 720, 9, exact=True, now=time.time())

    with patch.object(
        wm, "_tokens_traded_7d_one", new=AsyncMock(side_effect=AssertionError("no BS"))
    ):
        out = await wm.batch_tokens_traded_7d(
            [wallet], lookback_hours=720.0, too_many=1
        )
    assert out[wallet] == 9
    uc.reset_for_tests(None)
    wm._tokens7d_cache.clear()
