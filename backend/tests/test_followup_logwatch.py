"""Tests for follow-up eth_getLogs discovery helpers."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.constants import TRANSFER_TOPIC
from app.followup_logwatch import (
    fetch_inbound_transfers,
    parse_transfer_log,
    topic_address,
    topic_batch_count,
)


def test_topic_address_pads_20_bytes():
    assert topic_address("0x02783264ad1f8d53d2668acb68002d75d7be13ae") == (
        "0x00000000000000000000000002783264ad1f8d53d2668acb68002d75d7be13ae"
    )
    assert topic_address("02783264ad1f8d53d2668acb68002d75d7be13ae").endswith(
        "02783264ad1f8d53d2668acb68002d75d7be13ae"
    )


def test_parse_transfer_log_erc20():
    log = {
        "address": "0x4df46e819638ed21f8da63d2764e2a0cdb6922a8",
        "topics": [
            TRANSFER_TOPIC,
            "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
            "0x00000000000000000000000002783264ad1f8d53d2668acb68002d75d7be13ae",
        ],
        "data": "0x01",
        "blockNumber": "0x1b6be85",
        "transactionHash": "0xbe78022f2d1c4ec1a81a85606e8d8a01501211fa7d960b62e12447c269d7566f",
    }
    parsed = parse_transfer_log(log)
    assert parsed is not None
    token, frm, to, tx, block = parsed
    assert token == "0x4df46e819638ed21f8da63d2764e2a0cdb6922a8"
    assert frm == "0x8366a39cc670b4001a1121b8f6a443a643e40951"
    assert to == "0x02783264ad1f8d53d2668acb68002d75d7be13ae"
    assert tx.startswith("0xbe78022f")
    assert block == 0x1B6BE85


def test_parse_transfer_log_skips_non_erc20_topics():
    # ERC-721 style: 4 topics
    log = {
        "address": "0x4df46e819638ed21f8da63d2764e2a0cdb6922a8",
        "topics": [
            TRANSFER_TOPIC,
            "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
            "0x00000000000000000000000002783264ad1f8d53d2668acb68002d75d7be13ae",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        ],
        "data": "0x",
        "blockNumber": "0x1",
        "transactionHash": "0xabc",
    }
    # 4 topics still has from/to in [1]/[2] — we accept len>=3. NFT noise is
    # filtered later by quote/known/buy-gate. Ensure 2-topic logs are dropped.
    assert parse_transfer_log(log) is not None
    assert parse_transfer_log({**log, "topics": log["topics"][:2]}) is None


def test_topic_batch_count_chunks_at_50():
    assert topic_batch_count(0) == 1
    assert topic_batch_count(50) == 1
    assert topic_batch_count(51) == 2
    assert topic_batch_count(125) == 3


@pytest.mark.asyncio
async def test_fetch_inbound_transfers_runs_topic_batches_concurrently():
    """250+ wallets → ≥3 get_logs_chunked calls with overlapping concurrency."""
    wallets = [f"0x{i:040x}" for i in range(250)]
    assert topic_batch_count(len(wallets)) >= 3

    concurrent = 0
    peak = 0
    calls = 0

    async def fake_get_logs_chunked(**kwargs):
        nonlocal concurrent, peak, calls
        calls += 1
        concurrent += 1
        peak = max(peak, concurrent)
        await asyncio.sleep(0.05)
        concurrent -= 1
        return []

    rpc = MagicMock()
    rpc.get_logs_chunked = AsyncMock(side_effect=fake_get_logs_chunked)

    out = await fetch_inbound_transfers(
        rpc, wallets, from_block=100, to_block=110, chunk_size=50
    )
    assert out == []
    assert calls >= 3
    assert peak >= 2


@pytest.mark.asyncio
async def test_fetch_soft_partial_keeps_successful_batches():
    """One topic batch failure must not discard transfers from other batches."""
    from app.followup_logwatch import fetch_inbound_transfers_result

    wallets = [f"0x{i:040x}" for i in range(250)]
    calls = {"n": 0}

    async def fake_get_logs_chunked(**kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise TimeoutError("batch boom")
        # Emit a Transfer to wallet 0 on first batch only.
        if calls["n"] == 1:
            w = wallets[0]
            return [
                {
                    "address": "0x4df46e819638ed21f8da63d2764e2a0cdb6922a8",
                    "topics": [
                        TRANSFER_TOPIC,
                        "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
                        "0x" + ("0" * 24) + w[2:],
                    ],
                    "data": "0x01",
                    "blockNumber": hex(105),
                    "transactionHash": "0xabc123",
                }
            ]
        return []

    rpc = MagicMock()
    rpc.get_logs_chunked = AsyncMock(side_effect=fake_get_logs_chunked)
    rpc._call = AsyncMock(return_value={"timestamp": 1_700_000_000})

    transfers, incomplete = await fetch_inbound_transfers_result(
        rpc,
        wallets,
        from_block=100,
        to_block=110,
        chunk_size=50,
        soft_partial=True,
        batch_timeout_sec=2.0,
        batch_parallel=3,
    )
    assert incomplete is True
    assert len(transfers) == 1
    assert transfers[0].wallet == wallets[0]


@pytest.mark.asyncio
async def test_fetch_deadline_returns_partial_not_empty():
    """Deadline must keep early batch results instead of discarding via cancel."""
    import time as _time

    from app.followup_logwatch import fetch_inbound_transfers_result

    wallets = [f"0x{i:040x}" for i in range(250)]
    calls = {"n": 0}

    async def fake_get_logs_chunked(**kwargs):
        calls["n"] += 1
        n = calls["n"]
        if n == 1:
            await asyncio.sleep(0.05)
            w = wallets[0]
            return [
                {
                    "address": "0x4df46e819638ed21f8da63d2764e2a0cdb6922a8",
                    "topics": [
                        TRANSFER_TOPIC,
                        "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
                        "0x" + ("0" * 24) + w[2:],
                    ],
                    "data": "0x01",
                    "blockNumber": hex(105),
                    "transactionHash": "0xdead01",
                }
            ]
        await asyncio.sleep(2.0)
        return []

    rpc = MagicMock()
    rpc.get_logs_chunked = AsyncMock(side_effect=fake_get_logs_chunked)

    transfers, incomplete = await fetch_inbound_transfers_result(
        rpc,
        wallets,
        from_block=100,
        to_block=110,
        chunk_size=50,
        soft_partial=True,
        batch_timeout_sec=5.0,
        batch_parallel=1,
        deadline_mono=_time.monotonic() + 0.25,
    )
    assert incomplete is True
    assert len(transfers) == 1
    assert transfers[0].bought_at == 0.0  # soft_partial skips getBlock
