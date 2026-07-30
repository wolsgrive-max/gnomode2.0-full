"""Realtime migration bus via eth_subscribe (+ HTTP gap-fill)."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed

from .chain import RpcClient, checksum
from .config import settings
from .constants import (
    BAGS_MIGRATED_TOPIC,
    BAGS_V4_HOOK,
    FLAP_LAUNCHED_TO_DEX_TOPIC,
    FLAP_LAUNCHPAD,
    FLAP_VAULT_PORTAL,
    HOODFUN_GRADUATED_TOPIC,
    HOODFUN_LAUNCHPAD,
    HOODFUN_LAUNCHPAD_LEGACY,
    QUOTE_TOKENS,
    UNI_V3_FACTORY,
    UNI_V4_POOL_MANAGER,
    V3_POOL_CREATED_TOPIC,
    V4_INITIALIZE_TOPIC,
    ZERO,
)
from .launchpads.bags import parse_bags_migrated_log
from .launchpads.base import data_word_addr, topic_to_addr
from .launchpads.flap import parse_flap_launched_log
from .launchpads.hoodfun import parse_hoodfun_graduated_log
from .launchpads.registry import classify_pool
from .launchpads.types import MigrationEvent

logger = logging.getLogger(__name__)

Handler = Callable[[MigrationEvent], Awaitable[None]]

_KNOWN_QUOTES = {a.lower() for a in QUOTE_TOKENS} | {ZERO.lower()}


def _http_to_wss(url: str) -> str:
    if url.startswith("https://"):
        return "wss://" + url[len("https://") :]
    if url.startswith("http://"):
        return "ws://" + url[len("http://") :]
    if url.startswith("wss://") or url.startswith("ws://"):
        return url
    return url


def _non_quote_token(a: str, b: str) -> str | None:
    a_q = a.lower() in _KNOWN_QUOTES
    b_q = b.lower() in _KNOWN_QUOTES
    if a_q == b_q:
        return None
    addr = b if a_q else a
    # Skip zero-address / null tokens
    if addr.lower().startswith("0x00000000000000000000000000000000000000"):
        return None
    return addr


def _norm_topic(t: Any) -> str:
    if isinstance(t, (bytes, bytearray)):
        h = t.hex()
    else:
        h = str(t)
    if not h.startswith("0x"):
        h = "0x" + h
    return h.lower()


def parse_v4_initialize_log(log: dict[str, Any]) -> MigrationEvent | None:
    """Parse Uniswap V4 Initialize on Robinhood.

    RH indexes currency0/currency1 (4 topics). Data = fee, tickSpacing, hooks,
    sqrtPriceX96, tick. Older layouts with currencies in data are still accepted
    as a fallback when only 2 topics are present.
    """
    topics = log.get("topics") or []
    if len(topics) < 2:
        return None
    data = log.get("data") or "0x"
    if len(topics) >= 4:
        currency0 = topic_to_addr(topics[2])
        currency1 = topic_to_addr(topics[3])
        hooks = data_word_addr(data, 2)
    else:
        currency0 = data_word_addr(data, 0)
        currency1 = data_word_addr(data, 1)
        hooks = data_word_addr(data, 4)
    token = _non_quote_token(currency0, currency1)
    if not token:
        return None
    pool_id = _norm_topic(topics[1])
    spec = classify_pool(hooks=hooks, dex="uniswap_v4")
    if hooks and hooks.lower() == BAGS_V4_HOOK.lower():
        launchpad_id = "bags"
    else:
        launchpad_id = spec.id
    tx = log.get("transactionHash")
    tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
    if not tx_hex.startswith("0x"):
        tx_hex = "0x" + tx_hex
    return MigrationEvent(
        token=checksum(token),
        launchpad_id=launchpad_id,
        dex="uniswap_v4",
        block=int(log["blockNumber"], 16)
        if isinstance(log["blockNumber"], str)
        else int(log["blockNumber"]),
        tx=tx_hex,
        pool_id=pool_id,
        hooks=checksum(hooks) if hooks and not hooks.lower().startswith("0x00000000000000000000000000000000000000") else None,
        source="ws",
    )


def parse_v3_pool_created_log(log: dict[str, Any]) -> MigrationEvent | None:
    topics = log.get("topics") or []
    if len(topics) < 3:
        return None
    token0 = topic_to_addr(topics[1])
    token1 = topic_to_addr(topics[2])
    token = _non_quote_token(token0, token1)
    if not token:
        return None
    pool = data_word_addr(log.get("data") or "0x", 1)
    spec = classify_pool(dex="uniswap_v3")
    tx = log.get("transactionHash")
    tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
    if not tx_hex.startswith("0x"):
        tx_hex = "0x" + tx_hex
    return MigrationEvent(
        token=checksum(token),
        launchpad_id=spec.id,
        dex="uniswap_v3",
        block=int(log["blockNumber"], 16)
        if isinstance(log["blockNumber"], str)
        else int(log["blockNumber"]),
        tx=tx_hex,
        pool=checksum(pool),
        source="ws",
    )


def parse_subscription_log(log: dict[str, Any]) -> MigrationEvent | None:
    """Normalize hex fields from eth_subscription payload then parse."""
    # eth_subscribe returns hex strings for blockNumber/logIndex etc.
    norm = dict(log)
    if isinstance(norm.get("blockNumber"), str):
        norm["blockNumber"] = int(norm["blockNumber"], 16)
    if isinstance(norm.get("logIndex"), str):
        norm["logIndex"] = int(norm["logIndex"], 16)
    topics = norm.get("topics") or []
    if not topics:
        return None
    t0 = _norm_topic(topics[0])
    addr = str(norm.get("address") or "").lower()

    if t0 == BAGS_MIGRATED_TOPIC.lower():
        return parse_bags_migrated_log(norm)
    if t0 == HOODFUN_GRADUATED_TOPIC.lower():
        return parse_hoodfun_graduated_log(norm)
    if t0 == FLAP_LAUNCHED_TO_DEX_TOPIC.lower():
        return parse_flap_launched_log(norm)
    if t0 == V4_INITIALIZE_TOPIC.lower():
        ev = parse_v4_initialize_log(norm)
        if not ev:
            return None
        # Prefer Bags classification when hooks match
        if ev.hooks and ev.hooks.lower() == BAGS_V4_HOOK.lower():
            ev.launchpad_id = "bags"
            return ev
        # Without pre-migration discovery, ignore non-Bags V4 initializes
        if not settings.discover_pre_migration:
            return None
        return ev
    if t0 == V3_POOL_CREATED_TOPIC.lower() and addr == UNI_V3_FACTORY.lower():
        if not settings.discover_pre_migration:
            return None
        return parse_v3_pool_created_log(norm)
    return None



class WsMigrationBus:
    def __init__(self, on_event: Handler | None = None) -> None:
        self._on_event = on_event
        self._queue: asyncio.Queue[MigrationEvent] = asyncio.Queue(maxsize=2000)
        self._seen: set[str] = set()
        self._seen_max = 50_000
        self.last_seen_block: int = 0
        self._rpc_id = 0
        self._running = False
        self._wss = settings.wss_rpc_url or _http_to_wss(settings.rpc_url)

    def set_handler(self, handler: Handler) -> None:
        self._on_event = handler

    def _dedupe_key(self, ev: MigrationEvent) -> str:
        return f"{ev.token.lower()}:{ev.tx.lower()}:{ev.launchpad_id}"

    async def emit(self, ev: MigrationEvent) -> None:
        key = self._dedupe_key(ev)
        if key in self._seen:
            return
        self._seen.add(key)
        if len(self._seen) > self._seen_max:
            # Drop oldest-ish by clearing half (set has no order — full clear ok)
            self._seen.clear()
            self._seen.add(key)
        if ev.block:
            self.last_seen_block = max(self.last_seen_block, ev.block)
        try:
            self._queue.put_nowait(ev)
        except asyncio.QueueFull:
            logger.warning("Migration queue full — dropping %s", ev.token[:12])

    async def _worker(self, worker_id: int = 0) -> None:
        while self._running:
            ev = await self._queue.get()
            try:
                if self._on_event:
                    await self._on_event(ev)
            except Exception:  # noqa: BLE001
                logger.exception("Worker %d handler failed for %s", worker_id, ev.token)
            finally:
                self._queue.task_done()

    async def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    async def _subscribe(self, ws: Any, params: list[Any]) -> str:
        req_id = await self._next_id()
        await ws.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "eth_subscribe",
                    "params": params,
                }
            )
        )
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("id") == req_id:
                sub_id = msg.get("result")
                if not sub_id:
                    raise RuntimeError(f"subscribe failed: {msg}")
                return str(sub_id)
            # Interleaved notification — handle if present
            if msg.get("method") == "eth_subscription":
                await self._handle_notice(msg)

    async def _handle_notice(self, msg: dict[str, Any]) -> None:
        params = msg.get("params") or {}
        result = params.get("result")
        if not isinstance(result, dict):
            return
        ev = parse_subscription_log(result)
        if ev:
            # Skip unknown V3/V4 from live feed unless pre-migration discovery is on
            if not settings.discover_pre_migration and ev.launchpad_id in ("unknown_v3", "unknown_v4"):
                return
            await self.emit(ev)

    async def _subscribe_all(self, ws: Any) -> None:
        # V4 Initialize on PoolManager — only for Bags hook detection
        await self._subscribe(
            ws,
            [
                "logs",
                {
                    "address": UNI_V4_POOL_MANAGER,
                    "topics": [V4_INITIALIZE_TOPIC],
                },
            ],
        )
        # V3 PoolCreated — only when pre-migration discovery is enabled
        if settings.discover_pre_migration:
            await self._subscribe(
                ws,
                [
                    "logs",
                    {
                        "address": UNI_V3_FACTORY,
                        "topics": [V3_POOL_CREATED_TOPIC],
                    },
                ],
            )
        # Bags Migrated (any curve contract — filter by topic only)
        await self._subscribe(
            ws,
            [
                "logs",
                {"topics": [BAGS_MIGRATED_TOPIC]},
            ],
        )
        # hood.fun Graduated
        for pad in (HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY):
            await self._subscribe(
                ws,
                [
                    "logs",
                    {
                        "address": pad,
                        "topics": [HOODFUN_GRADUATED_TOPIC],
                    },
                ],
            )
        # Flap LaunchedToDEX
        for pad in (FLAP_LAUNCHPAD, FLAP_VAULT_PORTAL):
            await self._subscribe(
                ws,
                [
                    "logs",
                    {
                        "address": pad,
                        "topics": [FLAP_LAUNCHED_TO_DEX_TOPIC],
                    },
                ],
            )
        logger.info("WsMigrationBus subscribed on %s", self._wss.split("?")[0])

    async def gap_fill(self, from_block: int | None = None) -> int:
        """HTTP getLogs catch-up since last_seen_block."""
        rpc = RpcClient(concurrency=2)
        tip = await rpc.block_number()
        start = from_block
        if start is None:
            start = self.last_seen_block + 1 if self.last_seen_block else max(1, tip - 50_000)
        if start > tip:
            return 0
        count = 0

        async def ingest(logs: list[Any], parser, *, allow_unknown: bool = False) -> None:
            nonlocal count
            for lg in logs:
                # Convert AttributeDict-like to dict with int block
                raw = {
                    "address": lg.get("address"),
                    "topics": list(lg.get("topics") or []),
                    "data": lg.get("data"),
                    "blockNumber": int(lg["blockNumber"]),
                    "transactionHash": lg.get("transactionHash"),
                    "logIndex": int(lg.get("logIndex") or 0),
                }
                ev = parser(raw)
                if ev:
                    if not allow_unknown and ev.launchpad_id in ("unknown_v3", "unknown_v4"):
                        continue
                    ev.source = "gap"
                    await self.emit(ev)
                    count += 1

        v4_logs = await rpc.get_logs_chunked(
            address=UNI_V4_POOL_MANAGER,
            topics=[V4_INITIALIZE_TOPIC],
            from_block=start,
            to_block=tip,
        )

        def _v4_graduated_only(raw: dict[str, Any]) -> MigrationEvent | None:
            ev = parse_v4_initialize_log(raw)
            if not ev:
                return None
            if ev.hooks and ev.hooks.lower() == BAGS_V4_HOOK.lower():
                ev.launchpad_id = "bags"
                return ev
            if settings.discover_pre_migration:
                return ev
            return None

        await ingest(
            v4_logs,
            _v4_graduated_only,
            allow_unknown=settings.discover_pre_migration,
        )


        if settings.discover_pre_migration:
            v3_logs = await rpc.get_logs_chunked(
                address=UNI_V3_FACTORY,
                topics=[V3_POOL_CREATED_TOPIC],
                from_block=start,
                to_block=tip,
            )
            await ingest(v3_logs, parse_v3_pool_created_log, allow_unknown=True)

        bags_logs = await rpc.get_logs_chunked(
            address=[],  # topic-only not supported with empty — skip if RPC rejects
            topics=[BAGS_MIGRATED_TOPIC],
            from_block=start,
            to_block=tip,
        ) if False else []  # noqa: SIM222 — placeholder; use address-less via raw
        del bags_logs

        # Bags Migrated: no fixed address — use eth_getLogs without address via batch
        try:
            bags_raw = await rpc._call(
                lambda: rpc.w3.eth.get_logs(
                    {
                        "fromBlock": start,
                        "toBlock": tip,
                        "topics": [BAGS_MIGRATED_TOPIC],
                    }
                )
            )
            await ingest(bags_raw or [], parse_bags_migrated_log)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Bags Migrated gap-fill skipped: %s", exc)

        for pad in (HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY):
            try:
                hood_logs = await rpc.get_logs_chunked(
                    address=pad,
                    topics=[HOODFUN_GRADUATED_TOPIC],
                    from_block=start,
                    to_block=tip,
                )
                await ingest(hood_logs, parse_hoodfun_graduated_log)
            except Exception as exc:  # noqa: BLE001
                logger.debug("hood.fun gap-fill %s: %s", pad[:10], exc)

        for pad in (FLAP_LAUNCHPAD, FLAP_VAULT_PORTAL):
            try:
                flap_logs = await rpc.get_logs_chunked(
                    address=pad,
                    topics=[FLAP_LAUNCHED_TO_DEX_TOPIC],
                    from_block=start,
                    to_block=tip,
                )
                await ingest(flap_logs, parse_flap_launched_log)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Flap gap-fill %s: %s", pad[:10], exc)

        self.last_seen_block = tip
        logger.info("Gap-fill %s→%s emitted ~%d events", start, tip, count)
        return count

    async def _ws_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    self._wss,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    await self._subscribe_all(ws)
                    backoff = 1.0
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("method") == "eth_subscription":
                            await self._handle_notice(msg)
            except ConnectionClosed as exc:
                logger.warning("WS closed: %s — reconnect in %.1fs", exc, backoff)
            except Exception as exc:  # noqa: BLE001
                logger.warning("WS error: %s — reconnect in %.1fs", exc, backoff)
            if not self._running:
                break
            # Gap-fill on reconnect
            try:
                if self.last_seen_block:
                    await self.gap_fill(self.last_seen_block + 1)
            except Exception:  # noqa: BLE001
                logger.exception("Gap-fill after reconnect failed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    async def run(self) -> None:
        if not settings.migration_bus_enabled:
            logger.info("Migration bus disabled")
            return
        self._running = True
        _WORKERS = 5
        workers = [asyncio.create_task(self._worker(i)) for i in range(_WORKERS)]
        try:
            try:
                await self.gap_fill()
            except Exception:  # noqa: BLE001
                logger.exception("Initial gap-fill failed")
            await self._ws_loop()
        finally:
            self._running = False
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    def stop(self) -> None:
        self._running = False

    def status(self) -> dict[str, object]:
        wss = self._wss.split("?")[0] if self._wss else ""
        return {
            "enabled": bool(settings.migration_bus_enabled),
            "running": self._running,
            "last_seen_block": self.last_seen_block,
            "queue_size": self._queue.qsize(),
            "wss_url": wss,
        }


# Module singleton; handler wired at startup
migration_bus = WsMigrationBus()
