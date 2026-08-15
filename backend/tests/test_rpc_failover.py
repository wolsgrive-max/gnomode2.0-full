"""RPC pool / Alchemy failover helpers."""

from __future__ import annotations

import pytest

from app.chain import (
    RpcClient,
    _redact_exc,
    _redact_rpc_url,
    _should_failover,
    alchemy_rpc_url,
    resolve_rpc_urls,
)


def test_resolve_rpc_urls_alchemy_first():
    urls = resolve_rpc_urls(
        primary="https://rpc.mainnet.chain.robinhood.com",
        alchemy_key="testkey123",
        extras="",
    )
    assert urls[0] == "https://robinhood-mainnet.g.alchemy.com/v2/testkey123"
    assert "https://rpc.mainnet.chain.robinhood.com" in urls
    assert len(urls) == 2  # alchemy + public (primary == public, deduped)


def test_resolve_rpc_urls_without_alchemy():
    urls = resolve_rpc_urls(
        primary="https://rpc.mainnet.chain.robinhood.com",
        alchemy_key="",
        extras="https://example.rpc/extra",
    )
    assert urls[0] == "https://rpc.mainnet.chain.robinhood.com"
    assert "https://example.rpc/extra" in urls
    assert alchemy_rpc_url("") is None


def test_redact_rpc_url_hides_key():
    assert (
        _redact_rpc_url("https://robinhood-mainnet.g.alchemy.com/v2/sekrit")
        == "https://robinhood-mainnet.g.alchemy.com/v2/***"
    )
    assert _redact_rpc_url("https://rpc.mainnet.chain.robinhood.com").endswith(
        "robinhood.com"
    )


def test_redact_exc_strips_alchemy_key():
    msg = (
        "Client error '400 Bad Request' for url "
        "'https://robinhood-mainnet.g.alchemy.com/v2/alch_SECRET_KEY'"
    )
    assert "alch_SECRET" not in _redact_exc(RuntimeError(msg))
    assert "/v2/***" in _redact_exc(RuntimeError(msg))


def test_should_failover_on_400():
    assert _should_failover(RuntimeError("400 Bad Request")) is True


def test_prefer_non_alchemy_for_get_logs():
    client = RpcClient(
        rpc_urls=[
            "https://robinhood-mainnet.g.alchemy.com/v2/fake",
            "https://rpc.mainnet.chain.robinhood.com",
        ],
        concurrency=2,
    )
    assert client._is_alchemy_url()
    assert client._prefer_non_alchemy() is True
    assert not client._is_alchemy_url()
    assert "robinhood.com" in client.rpc_url


def test_get_logs_url_order_addressed_vs_addressless():
    """Factory (addressed) → Alchemy first; Transfer OR (no address) → public first."""
    client = RpcClient(
        rpc_urls=[
            "https://robinhood-mainnet.g.alchemy.com/v2/fake",
            "https://rpc.mainnet.chain.robinhood.com",
        ],
        concurrency=2,
    )
    alchemy = "https://robinhood-mainnet.g.alchemy.com/v2/fake"
    public = "https://rpc.mainnet.chain.robinhood.com"
    addressed = sorted(
        client.rpc_urls,
        key=lambda u: (0 if client._is_alchemy_url(u) else 1, u),
    )
    addressless = sorted(
        client.rpc_urls,
        key=lambda u: (1 if client._is_alchemy_url(u) else 0, u),
    )
    assert addressed[0] == alchemy
    assert addressless[0] == public
    assert client._is_alchemy_url(addressless[-1])


def test_rpc_clients_reuse_shared_w3_session():
    """Avoid Unclosed client session storms from per-tick RpcClient()."""
    from app import chain as chain_mod

    before = len(chain_mod._w3_by_url)
    a = RpcClient(
        rpc_urls=["https://rpc.mainnet.chain.robinhood.com"],
        concurrency=1,
        sem_scope="test_reuse_a",
    )
    b = RpcClient(
        rpc_urls=["https://rpc.mainnet.chain.robinhood.com"],
        concurrency=1,
        sem_scope="test_reuse_b",
    )
    assert a.w3 is b.w3
    assert len(chain_mod._w3_by_url) >= before


@pytest.mark.asyncio
async def test_rpc_client_rotates_on_failover():
    client = RpcClient(
        rpc_urls=[
            "https://broken.invalid/rpc",
            "https://rpc.mainnet.chain.robinhood.com",
        ],
        concurrency=2,
    )
    assert client.rpc_url == "https://broken.invalid/rpc"
    # Force rotate as if the first endpoint failed.
    assert client._rotate_url(RuntimeError("403 Forbidden")) is True
    assert client.rpc_url == "https://rpc.mainnet.chain.robinhood.com"
    # Round-robin back to the first URL.
    assert client._rotate_url(RuntimeError("connection reset")) is True
    assert client.rpc_url == "https://broken.invalid/rpc"


@pytest.mark.asyncio
async def test_block_number_bypasses_getlogs_semaphore():
    """eth_blockNumber must not wait behind saturated getLogs slots."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    client = RpcClient(
        rpc_urls=["https://rpc.mainnet.chain.robinhood.com"],
        concurrency=1,
        sem_scope="test_bn_light",
    )
    # Saturate the scoped getLogs/_call semaphore.
    await client._sem.acquire()
    try:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value={"result": "0x2a"})
        with patch("app.chain.http_client") as http:
            http.return_value.post = AsyncMock(return_value=mock_resp)
            tip = await asyncio.wait_for(client.block_number(), timeout=2.0)
        assert tip == 42
        http.return_value.post.assert_awaited()
    finally:
        client._sem.release()


@pytest.mark.asyncio
async def test_jsonrpc_batch_bypasses_getlogs_semaphore():
    """tx_from batches must not stall behind getLogs (alert starvation)."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    client = RpcClient(
        rpc_urls=["https://rpc.mainnet.chain.robinhood.com"],
        concurrency=1,
        sem_scope="test_batch_light",
    )
    await client._sem.acquire()
    try:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(
            return_value=[{"jsonrpc": "2.0", "id": 1, "result": {"from": "0xabc"}}]
        )
        with patch("app.chain.http_client") as http:
            http.return_value.post = AsyncMock(return_value=mock_resp)
            # Force known rpc_id so batch id matches mock.
            client._rpc_id = 0
            out = await asyncio.wait_for(
                client._jsonrpc_batch([("eth_getTransactionByHash", ["0x1"])]),
                timeout=2.0,
            )
        assert out == [{"from": "0xabc"}]
        http.return_value.post.assert_awaited()
    finally:
        client._sem.release()
