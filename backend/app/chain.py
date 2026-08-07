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
    UNI_V4_STATE_VIEW,
    UNI_V4_STATE_VIEW_ABI,
    ZERO,
)

logger = logging.getLogger(__name__)

# Shared HTTP client for DexScreener / Blockscout / CoinGecko
_http: httpx.AsyncClient | None = None

# One process-wide semaphore per RPC URL so JobStore + TokenIndex + honeypot
# sim don't each open their own pool and stampede the public endpoint into 429s.
_rpc_sems: dict[str, asyncio.Semaphore] = {}

# Shared AsyncWeb3 per URL. Creating a new AsyncHTTPProvider every RpcClient()
# (hist/live/maint ticks) leaked aiohttp ClientSessions → "Unclosed client
# session" storms and intermittent getLogs / block_number failures.
_w3_by_url: dict[str, AsyncWeb3] = {}

_PUBLIC_ROBINHOOD_RPC = "https://rpc.mainnet.chain.robinhood.com"
_ALCHEMY_ROBINHOOD_TMPL = "https://robinhood-mainnet.g.alchemy.com/v2/{key}"


def alchemy_rpc_url(api_key: str | None = None) -> str | None:
    key = (api_key if api_key is not None else settings.alchemy_api_key or "").strip()
    if not key:
        return None
    return _ALCHEMY_ROBINHOOD_TMPL.format(key=key)


def resolve_rpc_urls(
    *,
    primary: str | None = None,
    alchemy_key: str | None = None,
    extras: str | None = None,
) -> list[str]:
    """Ordered RPC pool: Alchemy (if keyed) → primary → extras → public.

    Deduplicates while preserving preference order so failover is predictable.
    """
    out: list[str] = []
    seen: set[str] = set()

    def _add(url: str | None) -> None:
        u = (url or "").strip()
        if not u or u in seen:
            return
        seen.add(u)
        out.append(u)

    _add(alchemy_rpc_url(alchemy_key))
    _add(primary if primary is not None else settings.rpc_url)
    raw_extras = extras if extras is not None else settings.rpc_urls
    for part in str(raw_extras or "").split(","):
        _add(part)
    _add(_PUBLIC_ROBINHOOD_RPC)
    return out


def _shared_sem(rpc_url: str, concurrency: int) -> asyncio.Semaphore:
    # Created lazily; first caller wins on the limit (intentionally sticky).
    sem = _rpc_sems.get(rpc_url)
    if sem is None:
        sem = asyncio.Semaphore(max(1, concurrency))
        _rpc_sems[rpc_url] = sem
    return sem


def _sem_key(rpc_url: str, *, scope: str) -> str:
    return f"{scope}:{rpc_url}"


def _scoped_sem(rpc_url: str, concurrency: int, *, scope: str) -> asyncio.Semaphore:
    """Semaphore isolated by scope so a stuck token_index getLogs cannot
    starve the follow-up logwatch loop (and vice versa)."""
    key = _sem_key(rpc_url, scope=scope)
    sem = _rpc_sems.get(key)
    if sem is None:
        sem = asyncio.Semaphore(max(1, concurrency))
        _rpc_sems[key] = sem
    return sem


def reset_rpc_semaphores(*, scope: str | None = None) -> int:
    """Drop cached semaphores so a cancelled cycle cannot starve the next.

    Zombie RPC tasks may still hold the old Semaphore objects; new callers get
    fresh ones. Returns how many entries were cleared.
    """
    if scope is None:
        n = len(_rpc_sems)
        _rpc_sems.clear()
        return n
    prefix = f"{scope}:"
    keys = [k for k in list(_rpc_sems) if k == scope or str(k).startswith(prefix)]
    # Also clear legacy shared keys that are bare URLs (no scope prefix) when
    # resetting the shared pool.
    if scope == "shared":
        keys = [k for k in list(_rpc_sems) if ":" not in str(k) or str(k).startswith("shared:")]
    for k in keys:
        _rpc_sems.pop(k, None)
    return len(keys)


def reset_followup_rpc_pressure() -> int:
    """Clear followup + live + shared sems after hung getLogs/block_number.

    Hist/live often starve while ``shared`` (token_index etc.) holds slots;
    resetting only ``followup`` left the process wedged on shared wall-timeouts.
    """
    cleared = 0
    for scope in ("followup", "followup_live", "shared"):
        cleared += reset_rpc_semaphores(scope=scope)
    return cleared


def _is_retryable(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if any(
        x in msg
        for x in (
            "429",
            "400",
            "bad request",
            "rate",
            "timeout",
            "too many",
            "503",
            "502",
            "connection",
            "server error",
            "temporarily",
            "403",
            "401",
            "forbidden",
            "unauthorized",
            "bad gateway",
            "gateway timeout",
        )
    ):
        return True
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None) in (
        400,
        401,
        403,
        429,
        502,
        503,
        504,
    ):
        return True
    return False


def _should_failover(exc: BaseException) -> bool:
    """Hard endpoint problems → try the next URL in the pool immediately."""
    msg = str(exc).lower()
    if any(
        x in msg
        for x in (
            "400",
            "bad request",
            "403",
            "401",
            "forbidden",
            "unauthorized",
            "connection",
            "timeout",
            "502",
            "503",
            "504",
            "bad gateway",
            "name or service not known",
            "nodename nor servname",
        )
    ):
        return True
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) if resp is not None else None
    return code in (400, 401, 403, 502, 503, 504)


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
    return min(base * (2**attempt), 12.0)


def _redact_rpc_url(url: str) -> str:
    """Hide API keys in logs (…/v2/<secret>)."""
    u = url or ""
    marker = "/v2/"
    if marker in u:
        head, _tail = u.split(marker, 1)
        return f"{head}{marker}***"
    return u


def _redact_exc(exc: BaseException) -> str:
    """Strip API keys that httpx embeds in exception URLs."""
    import re

    text = str(exc)
    return re.sub(
        r"(https?://[^/\s]+/v2/)[A-Za-z0-9_\-]+",
        r"\1***",
        text,
    )


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
    def __init__(
        self,
        rpc_url: str | None = None,
        concurrency: int | None = None,
        *,
        rpc_urls: list[str] | None = None,
        sem_scope: str = "shared",
    ):
        if rpc_urls is not None:
            urls = [u.strip() for u in rpc_urls if u and u.strip()]
        elif rpc_url:
            # Explicit single-ish URL — still append public fallback via resolver.
            urls = resolve_rpc_urls(primary=rpc_url, alchemy_key="")
        else:
            urls = resolve_rpc_urls()
        if not urls:
            urls = [_PUBLIC_ROBINHOOD_RPC]
        self._urls = urls
        self._url_index = 0
        self._concurrency = concurrency or settings.rpc_concurrency
        # "shared" = legacy process-wide pool; named scopes isolate hot paths
        # (followup must not wait behind a cancelled token_index getLogs).
        self._sem_scope = (sem_scope or "shared").strip() or "shared"
        self._chunk = settings.log_chunk_size
        self._code_cache: dict[str, bool] = {}
        self._rpc_id = 0
        self._bind_url(self._urls[0])

    def _bind_url(self, url: str) -> None:
        self.rpc_url = url
        self.w3 = self._w3_for(url)
        if self._sem_scope == "shared":
            self._sem = _shared_sem(self.rpc_url, self._concurrency)
        else:
            self._sem = _scoped_sem(
                self.rpc_url, self._concurrency, scope=self._sem_scope
            )

    def _w3_for(self, url: str) -> AsyncWeb3:
        hit = _w3_by_url.get(url)
        if hit is not None:
            return hit
        w3 = AsyncWeb3(
            AsyncHTTPProvider(
                url,
                request_kwargs={"timeout": 45},
            )
        )
        _w3_by_url[url] = w3
        return w3

    @property
    def rpc_urls(self) -> list[str]:
        return list(self._urls)

    def active_rpc_label(self) -> str:
        return _redact_rpc_url(self.rpc_url)

    def _rotate_url(self, reason: BaseException) -> bool:
        """Switch to the next URL in the pool. Returns False if only one URL."""
        if len(self._urls) <= 1:
            return False
        prev = self.rpc_url
        self._url_index = (self._url_index + 1) % len(self._urls)
        nxt = self._urls[self._url_index]
        self._bind_url(nxt)
        logger.warning(
            "RPC failover %s → %s (%s)",
            _redact_rpc_url(prev),
            _redact_rpc_url(nxt),
            _redact_exc(reason),
        )
        return True

    def _is_alchemy_url(self, url: str | None = None) -> bool:
        return "alchemy.com" in (url or self.rpc_url or "").lower()

    def _prefer_non_alchemy(self) -> bool:
        """Bind to a non-Alchemy URL if present (Alchemy rejects some getLogs shapes)."""
        if not self._is_alchemy_url():
            return False
        for i, u in enumerate(self._urls):
            if not self._is_alchemy_url(u):
                prev = self.rpc_url
                self._url_index = i
                self._bind_url(u)
                logger.info(
                    "RPC prefer non-Alchemy for getLogs: %s → %s",
                    _redact_rpc_url(prev),
                    _redact_rpc_url(u),
                )
                return True
        return False

    async def _call(self, coro_factory, retries: int = 8):
        last_exc: Exception | None = None
        urls_tried = 0
        max_url_rounds = max(1, len(self._urls))
        # Hard wall so a single hung eth_call cannot pin a semaphore slot
        # until the 180s cycle watchdog (and starve block_number forever).
        call_timeout = 20.0
        while urls_tried < max_url_rounds:
            urls_tried += 1
            for attempt in range(retries):
                delay = 0.5
                try:
                    await asyncio.wait_for(self._sem.acquire(), timeout=8.0)
                except TimeoutError as exc:
                    last_exc = exc
                    logger.warning(
                        "RPC sem busy on %s scope=%s — failover/retry",
                        self.active_rpc_label(),
                        self._sem_scope,
                    )
                    break
                try:
                    try:
                        return await asyncio.wait_for(
                            coro_factory(), timeout=call_timeout
                        )
                    except TimeoutError as exc:
                        last_exc = exc
                        logger.warning(
                            "RPC wall-timeout %.0fs on %s scope=%s",
                            call_timeout,
                            self.active_rpc_label(),
                            self._sem_scope,
                        )
                        break
                    except CallRevert:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc
                        if not _is_retryable(exc):
                            raise
                        # Hard endpoint errors → next URL immediately (no backoff burn).
                        if _should_failover(exc):
                            break
                        if attempt == retries - 1:
                            break
                        delay = _retry_delay(exc, attempt)
                        logger.warning(
                            "RPC retry %s/%s on %s in %.1fs: %s",
                            attempt + 1,
                            retries,
                            self.active_rpc_label(),
                            delay,
                            _redact_exc(exc),
                        )
                finally:
                    self._sem.release()
                await asyncio.sleep(delay)
            if not self._rotate_url(last_exc or RuntimeError("rpc failed")):
                break
        assert last_exc
        raise last_exc

    async def _jsonrpc_batch(self, calls: list[tuple[str, list[Any]]]) -> list[Any]:
        """Fire a JSON-RPC batch; returns results in order (None on per-item error)."""
        if not calls:
            return []

        async def do_post():
            payload = []
            for method, params in calls:
                self._rpc_id += 1
                payload.append(
                    {
                        "jsonrpc": "2.0",
                        "id": self._rpc_id,
                        "method": method,
                        "params": params,
                    }
                )
            start_id = payload[0]["id"]

            # Sem is already held by _call — do NOT acquire again (deadlock
            # when concurrency slots are saturated by concurrent batch calls).
            r = await http_client().post(self.rpc_url, json=payload)
            r.raise_for_status()
            data = r.json()

            if isinstance(data, dict):
                data = [data]
            by_id = {
                item.get("id"): item for item in (data or []) if isinstance(item, dict)
            }
            out: list[Any] = []
            for i in range(len(calls)):
                item = by_id.get(start_id + i, {})
                if "error" in item:
                    out.append(None)
                else:
                    out.append(item.get("result"))
            return out

        return await self._call(do_post, retries=6)

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

    def v4_state_view(self) -> AsyncContract:
        return self.w3.eth.contract(
            address=checksum(UNI_V4_STATE_VIEW),
            abi=UNI_V4_STATE_VIEW_ABI,
        )

    async def get_v4_slot0(self, pool_id: str) -> tuple[int, int, int, int]:
        """Read current Uniswap V4 price state directly from StateView."""
        raw = (pool_id or "").strip()
        if raw.startswith("0x"):
            raw = raw[2:]
        if len(raw) != 64:
            raise ValueError("V4 pool_id must be bytes32")
        result = await self._call(
            lambda: self.v4_state_view().functions.getSlot0(bytes.fromhex(raw)).call()
        )
        return int(result[0]), int(result[1]), int(result[2]), int(result[3])

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

        def _txt(v: Any) -> str:
            if isinstance(v, (bytes, bytearray)):
                return bytes(v).decode("utf-8", errors="ignore").rstrip("\x00").strip()
            return str(v or "").strip()

        return {
            "address": checksum(address),
            "decimals": int(decimals),
            "total_supply_raw": int(supply),
            "symbol": _txt(symbol),
            "name": _txt(name),
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
        address: str | list[str] | None = None,
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
        elif address:
            params["address"] = checksum(address)

        # Isolated per-URL attempts (do NOT mutate self.rpc_url / self.w3).
        # Alchemy frequently returns HTTP 400 for eth_getLogs (especially
        # address-less or large windows); try non-Alchemy endpoints first.
        # Parallel token_index / logwatch callers share this client — mutating
        # the active URL under them caused public↔Alchemy oscillation.
        urls = sorted(
            self._urls,
            key=lambda u: (1 if self._is_alchemy_url(u) else 0, u),
        )
        last_exc: Exception | None = None
        for url in urls:
            w3 = self._w3_for(url)
            if self._sem_scope == "shared":
                sem = _shared_sem(url, self._concurrency)
            else:
                sem = _scoped_sem(url, self._concurrency, scope=self._sem_scope)
            attempts = 2 if self._is_alchemy_url(url) else 3
            for attempt in range(attempts):
                delay = 0.5
                try:
                    await asyncio.wait_for(sem.acquire(), timeout=8.0)
                except TimeoutError as exc:
                    last_exc = exc
                    logger.warning(
                        "get_logs sem busy on %s scope=%s — skip",
                        _redact_rpc_url(url),
                        self._sem_scope,
                    )
                    break
                try:
                    try:
                        return await asyncio.wait_for(
                            w3.eth.get_logs(params),
                            timeout=15.0,
                        )
                    except TimeoutError as exc:
                        last_exc = exc
                        logger.warning(
                            "get_logs wall-timeout 15s on %s [%s,%s]",
                            _redact_rpc_url(url),
                            from_block,
                            to_block,
                        )
                        break
                    except CallRevert:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc
                        if not _is_retryable(exc):
                            raise
                        # Hard 400/403 on this endpoint → next URL immediately.
                        if _should_failover(exc):
                            logger.warning(
                                "get_logs skip %s: %s",
                                _redact_rpc_url(url),
                                _redact_exc(exc),
                            )
                            break
                        if attempt >= attempts - 1:
                            break
                        delay = _retry_delay(exc, attempt)
                        logger.warning(
                            "get_logs retry %s/%s on %s in %.1fs: %s",
                            attempt + 1,
                            attempts,
                            _redact_rpc_url(url),
                            delay,
                            _redact_exc(exc),
                        )
                finally:
                    sem.release()
                await asyncio.sleep(delay)
        assert last_exc
        raise last_exc

    async def get_logs_chunked(
        self,
        *,
        address: str | list[str] | None = None,
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
                        x in msg
                        for x in ("limit", "range", "too large", "response size", "query")
                    ):
                        local_chunk = max(local_chunk // 2, 500)
                        continue
                    raise
            return out

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

    async def batch_is_eoa(
        self, addresses: list[str], cache: dict[str, bool] | None = None
    ) -> dict[str, bool]:
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

        def _norm(h: str) -> str:
            s = str(h).lower()
            return s if s.startswith("0x") else f"0x{s}"

        uniq = list(dict.fromkeys(_norm(h) for h in tx_hashes))
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
    raw = (
        data
        if isinstance(data, bytes)
        else bytes.fromhex(data[2:] if data.startswith("0x") else data)
    )
    word = raw[index * 32 : (index + 1) * 32]
    return int.from_bytes(word, "big")


def decode_int256(data: str | bytes, index: int = 0) -> int:
    raw = (
        data
        if isinstance(data, bytes)
        else bytes.fromhex(data[2:] if data.startswith("0x") else data)
    )
    word = raw[index * 32 : (index + 1) * 32]
    (val,) = abi_decode(["int256"], word)
    return int(val)
