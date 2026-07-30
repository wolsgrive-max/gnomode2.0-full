"""In-memory index of NEW tokens on Robinhood Chain.

A "new token" is any token whose Uniswap V3/V4 pool was created within the last
24h. Tokens are discovered from on-chain factory events (V3 ``PoolCreated`` +
V4 ``Initialize``) — NOT from the Blockscout ``/tokens`` catalog, whose cursor
pagination breaks after ~350 priced tokens.

The index lives entirely in process memory and is kept fresh by a background
refresh loop; the screener reads from it instead of hitting the chain per query.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import httpx

from .chain import RpcClient, checksum
from .constants import (
    BLOCKS_PER_SECOND,
    QUOTE_TOKENS,
    UNI_V3_FACTORY,
    UNI_V4_POOL_MANAGER,
    V3_POOL_CREATED_TOPIC,
    V4_INITIALIZE_TOPIC,
    WINDOW_24H_BLOCKS,
    ZERO,
)
from .models import ScreenedToken

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]

_KNOWN_QUOTES = {a.lower() for a in QUOTE_TOKENS} | {ZERO.lower()}

_ENRICH_BATCH = 30
# Politeness: the index shares the RPC endpoint + DexScreener with the wallet
# parser, so it runs on its own small connection pool and low concurrency and
# never re-enriches the whole set in one burst.
_ENRICH_CONCURRENCY = 3
_INDEX_RPC_CONCURRENCY = 2
_REFRESH_INTERVAL_S = 120
_ENRICH_TTL_S = 15 * 60  # metrics considered fresh for 15 min
# Max stale tokens re-enriched per incremental cycle (new tokens are always
# enriched in full). ~30 batches keeps a steady, low background load while the
# whole set still gets refreshed within a few cycles — no data is dropped.
_REFRESH_SLICE = 30 * _ENRICH_BATCH
# While a parse job is active, only enrich brand-new tokens (few) and skip the
# stale-refresh so the parser gets the RPC/HTTP budget.
_BUSY_SLICE = 0


def _topic_addr(topic: Any) -> str:
    h = topic.hex() if isinstance(topic, (bytes, bytearray)) else str(topic)
    if h.startswith("0x"):
        h = h[2:]
    return "0x" + h[-40:].lower()


def _data_word_addr(data: Any, index: int) -> str:
    h = data.hex() if isinstance(data, (bytes, bytearray)) else str(data)
    if h.startswith("0x"):
        h = h[2:]
    word = h[index * 64 : (index + 1) * 64]
    return "0x" + word[-40:].lower()


@dataclass
class TokenEntry:
    address: str  # checksummed, for display
    dex: str  # "uniswap_v3" | "uniswap_v4"
    quote_address: str
    created_block: int
    pool_address: str = ""
    pool_id: str | None = None
    screened: ScreenedToken | None = None
    enriched_at: float = 0.0
    first_seen: float = field(default_factory=time.time)


class TokenIndex:
    def __init__(self) -> None:
        self._tokens: dict[str, TokenEntry] = {}
        self._rpc: RpcClient | None = None
        self._http: httpx.AsyncClient | None = None
        self.last_tip: int = 0
        self.last_refresh_ts: float = 0.0
        self.last_scan_ts: float = 0.0
        self.building: bool = False
        self.cold_started: bool = False
        self._refreshing: bool = False

    # ---------------------------------------------------------------- helpers

    def _rpc_client(self) -> RpcClient:
        # Own client with low concurrency so background scans stay polite and
        # don't trigger RPC 429s that would starve the wallet parser.
        if self._rpc is None:
            self._rpc = RpcClient(concurrency=_INDEX_RPC_CONCURRENCY)
        return self._rpc

    def _http_client(self) -> httpx.AsyncClient:
        # Dedicated pool so DexScreener enrichment never drains the shared
        # connection pool used by the parser's RPC batch calls.
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(12.0, connect=8.0),
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
                headers={"User-Agent": "gnomode-index/1.0"},
            )
        return self._http

    def _consider(
        self,
        a: str,
        b: str,
        *,
        dex: str,
        pool: str,
        pool_id: str | None,
        block: int,
    ) -> str | None:
        """Register a pool's non-quote token. Returns key if newly added."""
        a_q = a in _KNOWN_QUOTES
        b_q = b in _KNOWN_QUOTES
        if a_q == b_q:
            # both quotes or both non-quote (token<->token) — not screenable
            return None
        token, quote = (b, a) if a_q else (a, b)
        key = token
        entry = self._tokens.get(key)
        if entry is None:
            self._tokens[key] = TokenEntry(
                address=checksum(token),
                dex=dex,
                quote_address=checksum(quote) if quote != ZERO else ZERO,
                created_block=block,
                pool_address=checksum(pool) if pool and pool != ZERO else "",
                pool_id=pool_id,
            )
            return key
        if block and (entry.created_block == 0 or block < entry.created_block):
            entry.created_block = block
        return None

    def _minimal(self, entry: TokenEntry) -> ScreenedToken:
        """Placeholder row for tokens DexScreener does not (yet) index."""
        age_h: float | None = None
        if self.last_tip and entry.created_block:
            age_h = max(
                0.0,
                (self.last_tip - entry.created_block) / BLOCKS_PER_SECOND / 3600.0,
            )
        return ScreenedToken(
            address=entry.address,
            dex_id=entry.dex,
            pair_age_hours=age_h,
            gmgn_url=f"https://gmgn.ai/robinhood/token/{entry.address}",
        )

    def _maybe_track_mcap(self, entry: TokenEntry) -> None:
        """Fire-and-forget: track sub-target tokens / analyze those already above."""
        from .config import settings

        if not settings.mcap_tracker_enabled:
            return
        screened = entry.screened
        if not screened or not screened.market_cap or screened.market_cap <= 0:
            return
        target = settings.mcap_tracker_target
        mcap = float(screened.market_cap)
        if mcap < target:
            from .mcap_checker import add_token_to_tracker

            asyncio.ensure_future(
                add_token_to_tracker(
                    token_address=entry.address,
                    symbol=screened.symbol or "",
                    name=screened.name or "",
                    dex=entry.dex or screened.dex_id or "",
                    pool_id=entry.pool_id or "",
                    first_seen_mcap=mcap,
                )
            )
        else:
            from .mcap_checker import analyze_already_above

            asyncio.ensure_future(analyze_already_above(entry.address, mcap))

    def _prune(self) -> None:
        if not self.last_tip:
            return
        cutoff = self.last_tip - WINDOW_24H_BLOCKS
        stale = [
            k
            for k, e in self._tokens.items()
            if e.created_block and e.created_block < cutoff
        ]
        for k in stale:
            del self._tokens[k]

    # ------------------------------------------------------------------ scan

    async def scan_new_pools(
        self, *, full: bool, on_progress: ProgressCb | None = None
    ) -> list[str]:
        rpc = self._rpc_client()
        tip = await rpc.block_number()
        if full or self.last_tip == 0:
            from_block = max(1, tip - WINDOW_24H_BLOCKS)
        else:
            from_block = max(1, self.last_tip + 1, tip - WINDOW_24H_BLOCKS)

        if from_block > tip:
            self.last_tip = tip
            self.last_scan_ts = time.time()
            return []

        if on_progress:
            await on_progress(
                "scan",
                f"Scanning new V3/V4 pools (blocks {from_block}→{tip})…",
                0.05,
            )

        v3_logs, v4_logs = await asyncio.gather(
            rpc.get_logs_chunked(
                address=UNI_V3_FACTORY,
                topics=[V3_POOL_CREATED_TOPIC],
                from_block=from_block,
                to_block=tip,
            ),
            rpc.get_logs_chunked(
                address=UNI_V4_POOL_MANAGER,
                topics=[V4_INITIALIZE_TOPIC],
                from_block=from_block,
                to_block=tip,
            ),
        )

        new_keys: list[str] = []
        for log in v3_logs:
            topics = log["topics"]
            if len(topics) < 3:
                continue
            token0 = _topic_addr(topics[1])
            token1 = _topic_addr(topics[2])
            # V3 PoolCreated data = (int24 tickSpacing, address pool)
            pool = _data_word_addr(log["data"], 1)
            key = self._consider(
                token0,
                token1,
                dex="uniswap_v3",
                pool=pool,
                pool_id=None,
                block=int(log["blockNumber"]),
            )
            if key:
                new_keys.append(key)

        for log in v4_logs:
            topics = log["topics"]
            if len(topics) < 2:
                continue
            # RH V4 Initialize: currency0/currency1 are indexed (topics[2], topics[3]).
            if len(topics) >= 4:
                currency0 = _topic_addr(topics[2])
                currency1 = _topic_addr(topics[3])
            else:
                currency0 = _data_word_addr(log["data"], 0)
                currency1 = _data_word_addr(log["data"], 1)
            pool_id = topics[1]
            pool_id_hex = (
                pool_id.hex() if isinstance(pool_id, (bytes, bytearray)) else str(pool_id)
            )
            if not pool_id_hex.startswith("0x"):
                pool_id_hex = "0x" + pool_id_hex
            key = self._consider(
                currency0,
                currency1,
                dex="uniswap_v4",
                pool="",
                pool_id=pool_id_hex.lower(),
                block=int(log["blockNumber"]),
            )
            if key:
                new_keys.append(key)

        self.last_tip = tip
        self.last_scan_ts = time.time()
        logger.info(
            "Token index scan %s→%s: V3=%d V4=%d logs, +%d new tokens (total %d)",
            from_block,
            tip,
            len(v3_logs),
            len(v4_logs),
            len(new_keys),
            len(self._tokens),
        )
        return new_keys

    # --------------------------------------------------------------- enrich

    async def enrich_pending(
        self,
        *,
        stale_limit: int | None = None,
        on_progress: ProgressCb | None = None,
    ) -> None:
        # Lazy import avoids a circular dependency (screener imports token_index).
        from .screener import _best_pair_for_token, _fetch_dex_pairs, _pair_to_screened

        now = time.time()
        # New tokens are ALWAYS enriched in full (never dropped). Stale tokens
        # are refreshed oldest-first, capped per cycle so the whole set still
        # gets refreshed over a few cycles without a heavy burst.
        new_tokens = [e for e in self._tokens.values() if e.screened is None]
        stale = [
            e
            for e in self._tokens.values()
            if e.screened is not None and (now - e.enriched_at) > _ENRICH_TTL_S
        ]
        new_tokens.sort(key=lambda e: -e.created_block)
        stale.sort(key=lambda e: e.enriched_at)  # oldest metrics first
        if stale_limit is not None:
            stale = stale[:stale_limit]

        pending = new_tokens + stale
        if not pending:
            return

        batches = [
            pending[i : i + _ENRICH_BATCH]
            for i in range(0, len(pending), _ENRICH_BATCH)
        ]
        sem = asyncio.Semaphore(_ENRICH_CONCURRENCY)
        client = self._http_client()
        total = len(batches)
        done = 0

        async def run_batch(batch: list[TokenEntry]) -> None:
            nonlocal done
            addrs = [e.address for e in batch]
            async with sem:
                pairs = await _fetch_dex_pairs(client, addrs)
            by_token: dict[str, list[dict[str, Any]]] = {a.lower(): [] for a in addrs}
            for p in pairs:
                for side in ("baseToken", "quoteToken"):
                    a = str((p.get(side) or {}).get("address") or "").lower()
                    if a in by_token:
                        by_token[a].append(p)
            stamp = time.time()
            for entry in batch:
                key = entry.address.lower()
                best = _best_pair_for_token(entry.address, by_token.get(key, []))
                if best:
                    entry.screened = _pair_to_screened(entry.address, {}, best)
                elif entry.screened is None:
                    entry.screened = self._minimal(entry)
                entry.enriched_at = stamp
                self._maybe_track_mcap(entry)
            done += 1
            if on_progress and (done == total or done % 5 == 0):
                await on_progress(
                    "enrich",
                    f"Enriching new tokens {done}/{total} batches…",
                    min(0.95, 0.1 + 0.85 * done / max(total, 1)),
                )

        await asyncio.gather(*(run_batch(b) for b in batches))

    # -------------------------------------------------------------- refresh

    @staticmethod
    def _parse_active() -> bool:
        try:
            from .jobs import jobs
            from .watch import watch_runner

            return jobs.has_active() or bool(getattr(watch_runner, "running", False))
        except Exception:  # noqa: BLE001
            return False

    async def refresh(
        self, *, full: bool = False, on_progress: ProgressCb | None = None
    ) -> None:
        if self._refreshing:
            return
        self._refreshing = True
        try:
            cold = full or not self.cold_started
            if not self.cold_started:
                self.building = True
            parse_busy = self._parse_active()
            if parse_busy and not cold:
                # Yield the entire RPC budget to the wallet parser / watch —
                # skip getLogs scan and stale enrichment while they run.
                logger.info("parse/watch active — skipping index scan/enrich this cycle")
                return
            await self.scan_new_pools(full=cold, on_progress=on_progress)
            self._prune()
            # Mid-cold: background builds yield to *manual* parse jobs so they
            # aren't starved by index RPC. Never defer when ensure_ready/screen
            # is driving this refresh (on_progress set) — watch itself waits on
            # enrich, so skipping it returns an empty screener pool.
            # Leave cold_started=False so the next idle cycle finishes enrich.
            if (
                cold
                and on_progress is None
                and self._parse_active()
            ):
                logger.info("parse/watch active mid-cold — deferring enrich")
                self.building = False
                return
            if cold:
                # One-time full enrichment — complete coverage.
                stale_limit: int | None = None
            else:
                stale_limit = _REFRESH_SLICE
            await self.enrich_pending(stale_limit=stale_limit, on_progress=on_progress)
            self.cold_started = True
            self.building = False
            self.last_refresh_ts = time.time()
        finally:
            self._refreshing = False

    async def ensure_ready(self, on_progress: ProgressCb | None = None) -> None:
        """Block until the first cold build completes (used by a screen job)."""
        if self.cold_started:
            return
        # Wait out an in-flight background build; if it deferred enrich we
        # finish the cold start ourselves (must not return with empty pool).
        while self._refreshing:
            await asyncio.sleep(0.5)
            if self.cold_started:
                return
        if self.cold_started:
            return
        await self.refresh(full=True, on_progress=on_progress)

    async def run_refresh_loop(self) -> None:
        try:
            await self.refresh(full=True)
        except Exception:  # noqa: BLE001
            logger.exception("Initial token index build failed")
        while True:
            await asyncio.sleep(_REFRESH_INTERVAL_S)
            try:
                await self.refresh(full=False)
            except Exception:  # noqa: BLE001
                logger.exception("Token index refresh failed")

    # ---------------------------------------------------------------- reads

    def get_tokens(self) -> list[ScreenedToken]:
        return [e.screened for e in self._tokens.values() if e.screened is not None]

    def status(self) -> dict[str, Any]:
        enriched = sum(1 for e in self._tokens.values() if e.screened is not None)
        return {
            "tokens_24h": len(self._tokens),
            "enriched": enriched,
            "building": self.building,
            "cold_started": self.cold_started,
            "refreshing": self._refreshing,
            "last_tip": self.last_tip,
            "last_scan_ts": self.last_scan_ts,
            "last_refresh_ts": self.last_refresh_ts,
            "window_hours": 24,
        }


token_index = TokenIndex()
