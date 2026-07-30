"""Async-friendly Web3 RPC helpers with concurrency, batching + retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from eth_abi import decode as abi_decode
from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.contract.async_contract import AsyncContract

from .config import settings
from .constants import (
    ERC20_ABI,
    UNI_V2_FACTORY,
    UNI_V2_FACTORY_ABI,
    UNI_V2_PAIR_ABI,
    UNI_V3_FACTORY,
    UNI_V3_FACTORY_ABI,
    UNI_V3_POOL_ABI,
    ZERO,
)

logger = logging.getLogger(__name__)

# Shared HTTP client for DexScreener / Blockscout / CoinGecko
_http: httpx.AsyncClient | None = None

# One process-wide semaphore per RPC URL so JobStore + TokenIndex + honeypot
# sim don't each open their own pool and stampede the public endpoint into 429s.
_rpc_sems: dict[str, asyncio.Semaphore] = {}


def _shared_sem(rpc_url: str, concurrency: int) -> asyncio.Semaphore:
    # Created lazily; first caller wins on the limit (intentionally sticky).
    sem = _rpc_sems.get(rpc_url)
    if sem is None:
        sem = asyncio.Semaphore(max(1, concurrency))
        _rpc_sems[rpc_url] = sem
    return sem


def _is_retryable(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if any(
        x in msg
        for x in (
            "429",
            "rate",
            "timeout",
            "too many",
            "503",
            "502",
            "connection",
            "server error",
            "temporarily",
        )
    ):
        return True
    # httpx raises HTTPStatusError with .response
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) in (429, 502, 503):
        return True
    return False


def _retry_delay(exc: BaseException, attempt: int, base: float = 0.5) -> float:
    """Exponential backoff; honour Retry-After on 429 when present."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        ra = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
        if ra:
            try:
                return min(float(ra), 30.0)
            except ValueError:
                pass
    # Cap grows with attempt: 0.5 → 1 → 2 → 4 → 8 → 6
    return min(base * (2**attempt), 6.0)


class CallRevert(Exception):
    """eth_call reverted; `.data` holds hex revert payload when available."""

    def __init__(self, message: str, data: str | None = None):
        super().__init__(message)
        self.data = data


def http_client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
            headers={"User-Agent": "gnomode/1.0"},
        )
    return _http


def checksum(addr: str) -> str:
    return AsyncWeb3.to_checksum_address(addr)


class RpcClient:
    def __init__(self, rpc_url: str | None = None, concurrency: int | None = None):
        self.rpc_url = rpc_url or settings.rpc_url
        self.w3 = AsyncWeb3(
            AsyncHTTPProvider(
                self.rpc_url,
                request_kwargs={"timeout": 45},
            )
        )
        # Share the gate across all RpcClient instances for this URL.
        self._sem = _shared_sem(self.rpc_url, concurrency or settings.rpc_concurrency)
        self._chunk = settings.log_chunk_size
        self._code_cache: dict[str, bool] = {}
        self._rpc_id = 0

    async def _call(self, coro_factory, retries: int = 5):
        last_exc: Exception | None = None
        for attempt in range(retries):
            async with self._sem:
                try:
                    return await coro_factory()
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if not _is_retryable(exc) or attempt == retries - 1:
                        raise
                    delay = _retry_delay(exc, attempt)
                    logger.warning(
                        "RPC retry %s/%s in %.1fs: %s",
                        attempt + 1, retries, delay, exc,
                    )
            await asyncio.sleep(delay)
        assert last_exc
        raise last_exc

    async def _jsonrpc_batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Fire a JSON-RPC batch; returns results in order (None on per-item error)."""
        if not calls:
            return []
        payload = []
        for method, params in calls:
            self._rpc_id += 1
            payload.append(
                {"jsonrpc": "2.0", "id": self._rpc_id, "method": method, "params": params}
            )

        async def do_post():
            async with self._sem:
                r = await http_client().post(self.rpc_url, json=payload)
                r.raise_for_status()
                return r.json()

        data = None
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                data = await do_post()
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if not _is_retryable(exc) or attempt == 7:
                    raise
                delay = _retry_delay(exc, attempt)
                logger.warning("batch RPC retry in %.1fs: %s", delay, exc)
                await asyncio.sleep(delay)
        if data is None:
            assert last_exc
            raise last_exc

        if isinstance(data, dict):
            data = [data]
        by_id = {item.get("id"): item for item in (data or []) if isinstance(item, dict)}
        out: list[Any] = []
        # payload ids are sequential from start_id
        start_id = payload[0]["id"]
        for i in range(len(calls)):
            item = by_id.get(start_id + i, {})
            if "error" in item:
                out.append(None)
            else:
                out.append(item.get("result"))
        return out

    async def block_number(self) -> int:
        return await self._call(lambda: self.w3.eth.block_number)

    def erc20(self, address: str) -> AsyncContract:
        return self.w3.eth.contract(address=checksum(address), abi=ERC20_ABI)

    def v2_factory(self) -> AsyncContract:
        return self.w3.eth.contract(address=checksum(UNI_V2_FACTORY), abi=UNI_V2_FACTORY_ABI)

    def v2_pair(self, address: str) -> AsyncContract:
        return self.w3.eth.contract(address=checksum(address), abi=UNI_V2_PAIR_ABI)

    def v3_factory(self) -> AsyncContract:
        return self.w3.eth.contract(address=checksum(UNI_V3_FACTORY), abi=UNI_V3_FACTORY_ABI)

    def v3_pool(self, address: str) -> AsyncContract:
        return self.w3.eth.contract(address=checksum(address), abi=UNI_V3_POOL_ABI)

    async def token_meta(self, address: str) -> dict[str, Any]:
        c = self.erc20(address)

        async def safe(fn, default):
            try:
                return await self._call(fn)
            except Exception:  # noqa: BLE001
                return default

        decimals, supply, symbol, name = await asyncio.gather(
            safe(lambda: c.functions.decimals().call(), 18),
            safe(lambda: c.functions.totalSupply().call(), 0),
            safe(lambda: c.functions.symbol().call(), ""),
            safe(lambda: c.functions.name().call(), ""),
        )
        return {
            "address": checksum(address),
            "decimals": int(decimals),
            "total_supply_raw": int(supply),
            "symbol": str(symbol),
            "name": str(name),
        }

    async def get_v2_pair(self, token_a: str, token_b: str) -> str | None:
        factory = self.v2_factory()
        pair = await self._call(
            lambda: factory.functions.getPair(checksum(token_a), checksum(token_b)).call()
        )
        if not pair or pair.lower() == ZERO.lower():
            return None
        return checksum(pair)

    async def get_v3_pool(self, token_a: str, token_b: str, fee: int) -> str | None:
        factory = self.v3_factory()
        pool = await self._call(
            lambda: factory.functions.getPool(
                checksum(token_a), checksum(token_b), fee
            ).call()
        )
        if not pool or pool.lower() == ZERO.lower():
            return None
        return checksum(pool)

    async def get_logs(
        self,
        *,
        address: str | list[str],
        topics: list[Any],
        from_block: int,
        to_block: int,
    ) -> list[Any]:
        params: dict[str, Any] = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "topics": topics,
        }
        if isinstance(address, list):
            params["address"] = [checksum(a) for a in address]
        else:
            params["address"] = checksum(address)

        return await self._call(lambda: self.w3.eth.get_logs(params))

    async def get_logs_chunked(
        self,
        *,
        address: str | list[str],
        topics: list[Any],
        from_block: int,
        to_block: int,
        chunk_size: int | None = None,
        on_progress=None,
        parallel: int = 2,
    ) -> list[Any]:
        """Fetch logs in parallel chunk windows (kept small to avoid RPC 429s)."""
        chunk = chunk_size or self._chunk
        ranges: list[tuple[int, int]] = []
        start = from_block
        while start <= to_block:
            end = min(start + chunk - 1, to_block)
            ranges.append((start, end))
            start = end + 1
        if not ranges:
            return []

        logs: list[Any] = []
        total = len(ranges)
        done = 0
        sem = asyncio.Semaphore(max(1, parallel))

        async def one(a: int, b: int) -> list[Any]:
            nonlocal chunk
            local_chunk = b - a + 1
            cur = a
            out: list[Any] = []
            while cur <= b:
                end = min(cur + local_chunk - 1, b)
                try:
                    async with sem:
                        part = await self.get_logs(
                            address=address, topics=topics, from_block=cur, to_block=end
                        )
                    out.extend(part)
                    cur = end + 1
                except Exception as exc:  # noqa: BLE001
                    msg = str(exc).lower()
                    if local_chunk > 500 and any(
                        x in msg for x in ("limit", "range", "too large", "response size", "query")
                    ):
                        local_chunk = max(local_chunk // 2, 500)
                        continue
                    raise
            return out

        # Process in waves for progress
        wave = max(parallel, 1)
        for i in range(0, len(ranges), wave):
            batch = ranges[i : i + wave]
            parts = await asyncio.gather(*[one(a, b) for a, b in batch])
            for part in parts:
                logs.extend(part)
            done += len(batch)
            if on_progress:
                await on_progress(min(done / total, 1.0), batch[0][0], batch[-1][1])
        return logs

    async def is_eoa(self, address: str, cache: dict[str, bool] | None = None) -> bool:
        key = address.lower()
        store = cache if cache is not None else self._code_cache
        if key in store:
            return store[key]
        try:
            code = await self._call(lambda: self.w3.eth.get_code(checksum(address)))
            empty = (
                code is None
                or code == b""
                or (isinstance(code, (bytes, bytearray)) and len(code) == 0)
                or (isinstance(code, str) and code in ("0x", "0x0", ""))
            )
            result = bool(empty)
        except Exception:  # noqa: BLE001
            result = True
        store[key] = result
        return result

    async def batch_is_eoa(self, addresses: list[str], cache: dict[str, bool] | None = None) -> dict[str, bool]:
        """Batch eth_getCode for many addresses."""
        store = cache if cache is not None else self._code_cache
        uniq: list[str] = []
        seen: set[str] = set()
        for a in addresses:
            k = a.lower()
            if k in seen or k in store:
                continue
            seen.add(k)
            uniq.append(a)
        # chunk batches of 40
        for i in range(0, len(uniq), 40):
            batch = uniq[i : i + 40]
            calls = [("eth_getCode", [checksum(a), "latest"]) for a in batch]
            try:
                results = await self._jsonrpc_batch(calls)
            except Exception as exc:  # noqa: BLE001
                logger.warning("batch getCode failed, falling back: %s", exc)
                await asyncio.gather(*[self.is_eoa(a, store) for a in batch])
                continue
            for addr, code in zip(batch, results, strict=False):
                if code is None:
                    store[addr.lower()] = True
                    continue
                if isinstance(code, str):
                    empty = code in ("0x", "0x0", "")
                else:
                    empty = not code
                store[addr.lower()] = empty
        return {a.lower(): store.get(a.lower(), True) for a in addresses}

    async def batch_get_receipts(self, tx_hashes: list[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        uniq = list(dict.fromkeys(tx_hashes))
        for i in range(0, len(uniq), 25):
            batch = uniq[i : i + 25]
            calls = [("eth_getTransactionReceipt", [h]) for h in batch]
            try:
                results = await self._jsonrpc_batch(calls)
            except Exception as exc:  # noqa: BLE001
                logger.warning("batch receipts failed: %s", exc)
                continue
            for h, receipt in zip(batch, results, strict=False):
                if receipt:
                    out[h.lower()] = receipt
        return out

    async def eth_call_raw(
        self,
        tx: dict[str, Any],
        block: str = "latest",
    ) -> str:
        """Raw eth_call. On revert raises CallRevert with hex data when present."""

        def _extract_revert_data(payload: Any) -> str | None:
            if payload is None:
                return None
            if isinstance(payload, str):
                if payload.startswith("0x") and len(payload) >= 2:
                    return payload
                # Sometimes error is a JSON string
                try:
                    import json

                    return _extract_revert_data(json.loads(payload))
                except Exception:  # noqa: BLE001
                    return None
            if isinstance(payload, dict):
                for key in ("data", "result"):
                    if key in payload:
                        found = _extract_revert_data(payload[key])
                        if found:
                            return found
                err = payload.get("error")
                if err is not None:
                    return _extract_revert_data(err)
            return None

        async def do_call():
            async with self._sem:
                r = await http_client().post(
                    self.rpc_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_call",
                        "params": [tx, block],
                    },
                )
                r.raise_for_status()
                return r.json()

        data = await self._call(do_call)
        if isinstance(data, dict) and "error" in data:
            err = data["error"]
            msg = str(err.get("message") if isinstance(err, dict) else err)
            revert = _extract_revert_data(err)
            if revert is None:
                # web3-style: error.data may be nested
                revert = _extract_revert_data(data)
            raise CallRevert(msg, revert)
        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, str):
            raise CallRevert("empty eth_call result", None)
        return result


def topic_address(topic: str | bytes) -> str:
    if isinstance(topic, bytes):
        h = topic.hex()
    else:
        h = topic[2:] if topic.startswith("0x") else topic
    return checksum("0x" + h[-40:])


def decode_uint256(data: str | bytes, index: int = 0) -> int:
    raw = data if isinstance(data, bytes) else bytes.fromhex(data[2:] if data.startswith("0x") else data)
    word = raw[index * 32 : (index + 1) * 32]
    return int.from_bytes(word, "big")


def decode_int256(data: str | bytes, index: int = 0) -> int:
    raw = data if isinstance(data, bytes) else bytes.fromhex(data[2:] if data.startswith("0x") else data)
    word = raw[index * 32 : (index + 1) * 32]
    (val,) = abi_decode(["int256"], word)
    return int(val)
