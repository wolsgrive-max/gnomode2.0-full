"""Tests for migration validation filters."""

from __future__ import annotations

from app.migration_validate import is_plausible_token_address
from app.launchpads.types import MigrationEvent
from app.migration_validate import verify_migration_event
import pytest


def test_rejects_quote_and_zero():
    assert not is_plausible_token_address("0x0000000000000000000000000000000000000000")
    assert not is_plausible_token_address(
        "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
    )  # WETH
    assert is_plausible_token_address("0x1111111111111111111111111111111111111111")


@pytest.mark.asyncio
async def test_verify_skips_without_rpc(monkeypatch):
    """Without RPC mocks, bad address fails fast."""
    from app.chain import RpcClient

    ev = MigrationEvent(
        token="0x0000000000000000000000000000000000000000",
        launchpad_id="bags",
        dex="uniswap_v4",
        block=1,
        tx="0x" + "11" * 32,
        source="scan",
    )
    ok, reason, _ = await verify_migration_event(RpcClient(concurrency=1), ev)
    assert not ok
    assert reason == "bad_address"
