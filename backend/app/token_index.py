"""In-memory index of NEW tokens on Robinhood Chain.

A "new token" is any token whose Uniswap V2/V3/V4 pool was created within the
last 24h. Tokens are discovered from on-chain factory events (V2
``PairCreated`` + V3 ``PoolCreated`` + V4 ``Initialize``) — NOT from the
Blockscout ``/tokens`` catalog, whose cursor pagination breaks after ~350
priced tokens.

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
    PAIR_CREATED_TOPIC,
    QUOTE_TOKENS,
    UNI_V2_FACTORY,
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
_ENRICH_CONCURRENCY = 2
# Cold first-wave: keep low — DexScreener + Gecko share public rate limits.
_COLD_ENRICH_CONCURRENCY = 2
_INDEX_RPC_CONCURRENCY = 2
# Factory PairCreated/PoolCreated scans: 100k default chunks wall-time out on
# public Robinhood RPC and saturate the scoped sem (Watch cycle crashes).
_INDEX_LOG_CHUNK = 8_000
_INDEX_LOG_PARALLEL = 1
_REFRESH_INTERVAL_S = 120
_ENRICH_TTL_S = 15 * 60  # metrics considered fresh for 15 min
# Max stale tokens re-enriched per incremental cycle (new tokens are always
# enriched in full). ~30 batches keeps a steady, low background load while the
# whole set still gets refreshed within a few cycles — no data is dropped.
_REFRESH_SLICE = 30 * _ENRICH_BATCH
# Newest never-enriched tokens to enrich before declaring cold_started.
# Enough for screener max_results + ATH hold; rest continues in-background.
_COLD_READY_NEW = 1_200
# Incremental cycles must CAP never-enriched work. Unlimited newest-first
# enrich is starved by endless V4 spam — mid-age liquid tokens (ATH≥gate)
# stay screened=None forever and never reach watch/Хвать.
_INCREMENTAL_NEW_LIMIT = 300
# While a manual parse job is active: skip getLogs + stale refresh, but still
# enrich a small newest+stride slice so the ATH queue does not freeze.
_BUSY_NEW_LIMIT = 90
# Hot-set: frequent DexScreener ATH samples for liquid indexed tokens.
_HOT_ENRICH_INTERVAL_S = 120
_HOT_ENRICH_CAP = 120
_HOT_MIN_LIQ_USD = 4_000.0
# Gecko OHLCV peaks (rate-limited); run after DS enrich on new/hot tokens.
# Keep small (Gecko 429s), but young never-probed must win the queue —
# otherwise a $500k wick dumps to $2k DS spot before we ever OHLCV-probe.
_GECKO_BATCH_LIMIT = 24
_GECKO_RETRY_S = 20 * 60.0
# Young tokens still inside the watch max-age window: retry failed/zero ATH
# probes quickly so a brief pump is not permanently missed after a dump.
_GECKO_YOUNG_RETRY_S = 3 * 60.0
_GECKO_YOUNG_AGE_H = 24.0


def _entry_is_young(entry: "TokenEntry", now: float | None = None) -> bool:
    """True while the pool is still inside the watch-style age window."""
    ts = time.time() if now is None else now
    if entry.screened is not None and entry.screened.pair_age_hours is not None:
        return float(entry.screened.pair_age_hours) <= _GECKO_YOUNG_AGE_H
    if entry.first_seen > 0:
        return (ts - entry.first_seen) <= _GECKO_YOUNG_AGE_H * 3600.0
    return False


def _select_never_enriched(
    new_tokens: list["TokenEntry"], new_limit: int
) -> list["TokenEntry"]:
    """Pick a never-enriched slice that covers newest, oldest, and mid-age.

    ``new_tokens`` must be sorted newest-first (``-created_block``).
    """
    if new_limit <= 0:
        return []
    if len(new_tokens) <= new_limit:
        return list(new_tokens)
    n_new = max(1, new_limit // 3)
    n_old = max(1, new_limit // 3)
    n_mid = max(0, new_limit - n_new - n_old)
    head = new_tokens[:n_new]
    oldest = new_tokens[-n_old:] if n_old else []
    # Middle band: everything not already taken as newest/oldest.
    rest_end = len(new_tokens) - n_old if n_old else len(new_tokens)
    rest = new_tokens[n_new:rest_end]
    mid: list[TokenEntry] = []
    if n_mid > 0 and rest:
        step = max(1, len(rest) // n_mid)
        mid = rest[::step][:n_mid]
    chosen: list[TokenEntry] = []
    seen: set[str] = set()
    for e in head + oldest + mid:
        key = e.address.lower()
        if key in seen:
            continue
        seen.add(key)
        chosen.append(e)
        if len(chosen) >= new_limit:
            break
    # Fill leftover slots newest-first if dedupe shrank the set.
    if len(chosen) < new_limit:
        for e in new_tokens:
            key = e.address.lower()
            if key in seen:
                continue
            seen.add(key)
            chosen.append(e)
            if len(chosen) >= new_limit:
                break
    return chosen


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
    dex: str  # "uniswap_v2" | "uniswap_v3" | "uniswap_v4"
    quote_address: str
    created_block: int
    pool_address: str = ""
    pool_id: str | None = None
    screened: ScreenedToken | None = None
    enriched_at: float = 0.0
    first_seen: float = field(default_factory=time.time)
    # Peak market_cap seen while this entry is alive in the index (DS + Gecko).
    ath_mcap: float = 0.0
    # Last successful Gecko ATH probe (0 = never).
    gecko_ath_at: float = 0.0


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
        self._hot_addresses: set[str] = set()
        self._hot_enriching: bool = False
        self.last_hot_enrich_ts: float = 0.0
        self._cold_tail_task: asyncio.Task[None] | None = None

    # ---------------------------------------------------------------- helpers

    def _rpc_client(self) -> RpcClient:
        # Own client with low concurrency so background scans stay polite and
        # don't trigger RPC 429s that would starve the wallet parser.
        if self._rpc is None:
            self._rpc = RpcClient(
                concurrency=_INDEX_RPC_CONCURRENCY, sem_scope="token_index"
            )
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
            ath_mcap=entry.ath_mcap,
            gmgn_url=f"https://gmgn.ai/robinhood/token/{entry.address}",
        )

    def _apply_ath(self, entry: TokenEntry, row: ScreenedToken) -> ScreenedToken:
        """Bump entry ATH from current mcap and mirror it onto the screened row."""
        # Drop absurd peaks from the old price×1e9 bug (billions on low-supply
        # tokens); real RH meme ATH almost never reaches $1B.
        prev = entry.ath_mcap
        if prev >= 1_000_000_000.0 and row.market_cap > 0 and prev > row.market_cap * 50:
            prev = 0.0
        peak = max(prev, row.ath_mcap, row.market_cap)
        entry.ath_mcap = peak
        if row.ath_mcap != peak:
            return row.model_copy(update={"ath_mcap": peak})
        return row

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
                f"Scanning new V2/V3/V4 pools (blocks {from_block}→{tip})…",
                0.05,
            )

        # Sequential factories + small chunks: a 3-way gather × parallel=2
        # against concurrency=2 caused sem-busy + 15s/20s wall-timeouts that
        # crashed Watch via ensure_ready.
        async def _factory_logs(address: str, topic: str) -> list[Any]:
            try:
                return await rpc.get_logs_chunked(
                    address=address,
                    topics=[topic],
                    from_block=from_block,
                    to_block=tip,
                    chunk_size=_INDEX_LOG_CHUNK,
                    parallel=_INDEX_LOG_PARALLEL,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "token_index factory scan soft-fail %s…%s %s: %s",
                    from_block,
                    tip,
                    address[:10],
                    type(exc).__name__,
                )
                return []

        v2_logs = await _factory_logs(UNI_V2_FACTORY, PAIR_CREATED_TOPIC)
        v3_logs = await _factory_logs(UNI_V3_FACTORY, V3_POOL_CREATED_TOPIC)
        v4_logs = await _factory_logs(UNI_V4_POOL_MANAGER, V4_INITIALIZE_TOPIC)

        new_keys: list[str] = []
        for log in v2_logs:
            topics = log["topics"]
            if len(topics) < 3:
                continue
            token0 = _topic_addr(topics[1])
            token1 = _topic_addr(topics[2])
            # PairCreated data = (address pair, uint256)
            pool = _data_word_addr(log["data"], 0)
            key = self._consider(
                token0,
                token1,
                dex="uniswap_v2",
                pool=pool,
                pool_id=None,
                block=int(log["blockNumber"]),
            )
            if key:
                new_keys.append(key)

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
            # Initialize(bytes32 indexed id, address indexed currency0,
            #            address indexed currency1, ...) — currencies in topics.
            if len(topics) < 4:
                continue
            currency0 = _topic_addr(topics[2])
            currency1 = _topic_addr(topics[3])
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
            "Token index scan %s→%s: V2=%d V3=%d V4=%d logs, +%d new tokens (total %d)",
            from_block,
            tip,
            len(v2_logs),
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
        new_limit: int | None = None,
        concurrency: int | None = None,
        on_progress: ProgressCb | None = None,
        progress_label: str = "Enriching new tokens",
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
        if new_limit is not None and len(new_tokens) > new_limit:
            # Cover the whole 24h window under continuous V4 create spam:
            # newest + oldest + stride through the middle. Newest-only leaves
            # mid-age liquid dumps (MEATSPIN) at screened=None forever; pure
            # stride can reshuffle and re-miss the same addresses as the
            # front of the queue grows.
            new_tokens = _select_never_enriched(new_tokens, new_limit)
        elif new_limit is not None:
            new_tokens = new_tokens[: max(0, new_limit)]
        if stale_limit is not None:
            stale = stale[:stale_limit]

        pending = new_tokens + stale
        if not pending:
            return

        batches = [
            pending[i : i + _ENRICH_BATCH]
            for i in range(0, len(pending), _ENRICH_BATCH)
        ]
        workers = max(1, concurrency or _ENRICH_CONCURRENCY)
        sem = asyncio.Semaphore(workers)
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
                    row = _pair_to_screened(entry.address, {}, best)
                    entry.screened = self._apply_ath(entry, row)
                elif entry.screened is None:
                    entry.screened = self._apply_ath(entry, self._minimal(entry))
                else:
                    # Stale refresh missed DexScreener — keep row, still sync ath field.
                    entry.screened = self._apply_ath(entry, entry.screened)
                entry.enriched_at = stamp
            done += 1
            if on_progress and (done == total or done % 5 == 0):
                await on_progress(
                    "enrich",
                    f"{progress_label} {done}/{total} batches…",
                    min(0.95, 0.1 + 0.85 * done / max(total, 1)),
                )

        await asyncio.gather(*(run_batch(b) for b in batches))

    async def force_enrich_addresses(
        self,
        addresses: list[str],
        *,
        on_progress: ProgressCb | None = None,
    ) -> dict[str, ScreenedToken]:
        """Force DexScreener refresh for addresses, ignoring enrich TTL.

        In-index entries are updated in place (ATH peaks preserved). Addresses
        missing from the 24h index are still fetched and returned so callers
        (watch catch-up) can bump ATH hold peaks without re-adding stale tokens.
        """
        from .screener import _best_pair_for_token, _fetch_dex_pairs, _pair_to_screened

        keys = sorted({a.strip().lower() for a in addresses if a and str(a).strip()})
        if not keys:
            return {}

        batches = [
            keys[i : i + _ENRICH_BATCH] for i in range(0, len(keys), _ENRICH_BATCH)
        ]
        sem = asyncio.Semaphore(_ENRICH_CONCURRENCY)
        client = self._http_client()
        total = len(batches)
        done = 0
        out: dict[str, ScreenedToken] = {}

        async def run_batch(batch: list[str]) -> None:
            nonlocal done
            async with sem:
                pairs = await _fetch_dex_pairs(client, batch)
            by_token: dict[str, list[dict[str, Any]]] = {a: [] for a in batch}
            for p in pairs:
                for side in ("baseToken", "quoteToken"):
                    a = str((p.get(side) or {}).get("address") or "").lower()
                    if a in by_token:
                        by_token[a].append(p)
            stamp = time.time()
            for addr in batch:
                entry = self._tokens.get(addr)
                best = _best_pair_for_token(
                    entry.address if entry else addr, by_token.get(addr, [])
                )
                if entry is not None:
                    if best:
                        row = _pair_to_screened(entry.address, {}, best)
                        entry.screened = self._apply_ath(entry, row)
                    elif entry.screened is None:
                        entry.screened = self._apply_ath(entry, self._minimal(entry))
                    else:
                        entry.screened = self._apply_ath(entry, entry.screened)
                    entry.enriched_at = stamp
                    out[addr] = entry.screened
                elif best:
                    row = _pair_to_screened(addr, {}, best)
                    peak = max(row.ath_mcap, row.market_cap)
                    if row.ath_mcap != peak:
                        row = row.model_copy(update={"ath_mcap": peak})
                    out[addr] = row
            done += 1
            if on_progress and (done == total or done % 5 == 0):
                await on_progress(
                    "enrich",
                    f"Catch-up hold enrich {done}/{total} batches…",
                    min(0.95, 0.1 + 0.85 * done / max(total, 1)),
                )

        await asyncio.gather(*(run_batch(b) for b in batches))
        return out

    # -------------------------------------------------------------- refresh

    @staticmethod
    def _parse_active() -> bool:
        try:
            from .jobs import jobs

            return jobs.has_active()
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
            # Cold tail still draining screened=None — skip competing scan/DS
            # enrich (DexScreener budget), but keep a Gecko slice so liquid
            # under-gate dumps (MEATSPIN) are not stuck at DS-spot ATH for the
            # whole multi-minute tail.
            if self._cold_tail_busy() and not cold:
                logger.info("cold tail busy — gecko-only slice this cycle")
                busy_gecko = self._gecko_refresh_candidates()
                if busy_gecko:
                    await self._apply_gecko_peaks(
                        busy_gecko,
                        limit=min(8, _GECKO_BATCH_LIMIT),
                    )
                return
            if parse_busy and not cold:
                # Still scan factories so pools created during a multi-hour
                # parse enter `_tokens` (enrich alone cannot discover them).
                # Skip stale re-enrich; keep a small newest/oldest/mid slice so
                # mid-age ATH tokens are not frozen for the whole parse. Also
                # keep a small Gecko slice (liq-first) — without it, dumped
                # pumps stay at DS-spot ATH and never pass watch's min_ath.
                logger.info(
                    "parse job active — scan + busy-slice enrich (skip stale)"
                )
                await self.scan_new_pools(full=False, on_progress=on_progress)
                self._prune()
                await self.enrich_pending(
                    stale_limit=0,
                    new_limit=_BUSY_NEW_LIMIT,
                    progress_label="Busy-slice enrich",
                )
                busy_gecko = self._gecko_refresh_candidates()
                if busy_gecko:
                    await self._apply_gecko_peaks(
                        busy_gecko,
                        limit=min(8, _GECKO_BATCH_LIMIT),
                    )
                return
            await self.scan_new_pools(full=cold, on_progress=on_progress)
            self._prune()
            # Never-probed + young-under-age (force-probe / young retry).
            # Without the young path, a dumped short pump falls out of the hot
            # set (liq < $4k) and never re-enters Gecko after the first miss.
            gecko_candidates = self._gecko_refresh_candidates()
            if cold:
                # Unblock watch/Хвать ASAP: newest slice first, then finish
                # the rest in a background tail so ensure_ready can return.
                await self.enrich_pending(
                    stale_limit=0,
                    new_limit=_COLD_READY_NEW,
                    concurrency=_COLD_ENRICH_CONCURRENCY,
                    on_progress=on_progress,
                    progress_label="Cold first-wave enrich",
                )
                enriched = sum(1 for e in self._tokens.values() if e.screened is not None)
                self.cold_started = True
                self.building = False
                self.last_refresh_ts = time.time()
                logger.info(
                    "Cold index ready after first wave (%d/%d enriched)",
                    enriched,
                    len(self._tokens),
                )
                if on_progress:
                    await on_progress(
                        "index",
                        f"Index ready ({enriched}/{len(self._tokens)}); finishing enrich…",
                        0.55,
                    )
                self._schedule_cold_tail(gecko_candidates)
            else:
                # Cap + stride (inside enrich_pending): pure newest-only never
                # reaches mid-age liquid pools under V4 create spam.
                await self.enrich_pending(
                    stale_limit=_REFRESH_SLICE,
                    new_limit=_INCREMENTAL_NEW_LIMIT,
                    on_progress=on_progress,
                )
                if gecko_candidates:
                    await self._apply_gecko_peaks(
                        gecko_candidates,
                        limit=_GECKO_BATCH_LIMIT,
                    )
                self.cold_started = True
                self.building = False
                self.last_refresh_ts = time.time()
        finally:
            self._refreshing = False

    def _cold_tail_busy(self) -> bool:
        t = self._cold_tail_task
        return t is not None and not t.done()

    def _schedule_cold_tail(self, gecko_candidates: list[str]) -> None:
        if self._cold_tail_busy():
            return
        self._cold_tail_task = asyncio.create_task(
            self._run_cold_tail(gecko_candidates),
            name="token-index-cold-tail",
        )

    async def _run_cold_tail(self, gecko_candidates: list[str]) -> None:
        try:
            await self.enrich_pending(
                stale_limit=0,
                progress_label="Cold remaining enrich",
            )
            # Recompute after enrich — first-wave snapshot misses tokens that
            # only gained screened/liq in the tail (MEATSPIN-class).
            pending_gecko = self._gecko_refresh_candidates()
            if not pending_gecko:
                pending_gecko = [
                    k
                    for k in gecko_candidates
                    if (e := self._tokens.get(k)) is not None and e.gecko_ath_at <= 0.0
                ]
            if pending_gecko:
                # Keep Gecko light: hot-enrich loop densifies ATH later.
                # Ranking (needs_gate / oldest-young) picks liquid dumps first.
                await self._apply_gecko_peaks(
                    pending_gecko,
                    limit=min(32, max(_GECKO_BATCH_LIMIT, 20)),
                )
            self.last_refresh_ts = time.time()
            logger.info(
                "Cold index tail done (%d/%d enriched)",
                sum(1 for e in self._tokens.values() if e.screened is not None),
                len(self._tokens),
            )
        except Exception:  # noqa: BLE001
            logger.exception("Cold index tail failed")

    async def ensure_ready(self, on_progress: ProgressCb | None = None) -> None:
        """Block until the first cold wave completes (used by a screen job)."""
        if self.cold_started:
            return
        if not self._refreshing:
            await self.refresh(full=True, on_progress=on_progress)
            return
        # Background cold build in progress — wait, keep UI progress alive.
        while not self.cold_started:
            if not self._refreshing:
                break
            if on_progress:
                enriched = sum(
                    1 for e in self._tokens.values() if e.screened is not None
                )
                total = max(len(self._tokens), 1)
                await on_progress(
                    "index",
                    f"Ждём индекс: {enriched}/{total} обогащено…",
                    min(0.85, 0.05 + 0.8 * enriched / total),
                )
            await asyncio.sleep(1.0)
        if not self.cold_started:
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

    def set_hot_addresses(self, addresses: list[str] | set[str]) -> None:
        """Optional explicit hot-set (merged with liquid indexed tokens)."""
        self._hot_addresses = {
            a.strip().lower() for a in addresses if a and str(a).strip()
        }

    def _hot_candidates(self) -> list[str]:
        """Liquid indexed tokens (+ optional explicit hot-set), capped for DS."""
        out: list[str] = []
        seen: set[str] = set()
        for addr in self._hot_addresses:
            if addr in seen:
                continue
            seen.add(addr)
            out.append(addr)
            if len(out) >= _HOT_ENRICH_CAP:
                return out
        liquid = [
            e
            for e in self._tokens.values()
            if e.screened is not None
            and e.screened.liquidity_usd >= _HOT_MIN_LIQ_USD
        ]
        liquid.sort(key=lambda e: e.screened.market_cap if e.screened else 0.0, reverse=True)
        for e in liquid:
            key = e.address.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
            if len(out) >= _HOT_ENRICH_CAP:
                break
        return out

    def _gecko_refresh_candidates(self) -> list[str]:
        """Never-probed tokens plus young ones still under the age cap."""
        now = time.time()
        out: list[str] = []
        for key, entry in self._tokens.items():
            if entry.gecko_ath_at <= 0.0 or _entry_is_young(entry, now):
                out.append(key)
        return out

    async def _apply_gecko_peaks(
        self, addresses: list[str], *, limit: int = _GECKO_BATCH_LIMIT
    ) -> int:
        """Bump ``ath_mcap`` from GeckoTerminal OHLCV for a subset of tokens."""
        from .ath_gecko import fetch_token_ath_mcap

        now = time.time()
        # Sort: young never-probed first, then young retries, then old
        # never-probed, then old retries. Within young+never, prefer *liquid*
        # pools over newest-created — V4 spam would otherwise monopolize the
        # Gecko budget while a mid-age dumped pump (MEATSPIN: DS spot ~16k,
        # real ATH ~148k) sits at gecko_ath_at=0 forever and fails ATH≥50k.
        ranked: list[tuple[int, float, float, str]] = []
        for raw in addresses:
            key = raw.strip().lower()
            entry = self._tokens.get(key)
            if entry is None:
                continue
            age = now - entry.gecko_ath_at if entry.gecko_ath_at > 0 else 1e12
            young = _entry_is_young(entry, now)
            retry_after = _GECKO_YOUNG_RETRY_S if young else _GECKO_RETRY_S
            if entry.gecko_ath_at > 0 and age < retry_after:
                continue
            never = entry.gecko_ath_at <= 0.0
            if young and never:
                tier = 0
            elif young:
                tier = 1
            elif never:
                tier = 2
            else:
                tier = 3
            liq = float(entry.screened.liquidity_usd) if entry.screened else 0.0
            age_h = (
                float(entry.screened.pair_age_hours)
                if entry.screened is not None and entry.screened.pair_age_hours is not None
                else 0.0
            )
            if never:
                # Liquid + DS-spot still under the typical watch ATH gate must
                # win over already-passing tokens and V4 dust (MEATSPIN class).
                # Within that set, oldest-young first — about to leave the 24h
                # window — then higher liq.
                needs_gate = (
                    0
                    if (
                        entry.screened is not None
                        and liq >= _HOT_MIN_LIQ_USD
                        and float(entry.ath_mcap or 0.0) < 50_000.0
                    )
                    else 1
                )
                ranked.append(
                    (
                        tier,
                        needs_gate,
                        -age_h,
                        -liq,
                        -float(entry.created_block or 0),
                        key,
                    )
                )
            else:
                ranked.append((tier, 0, -age, -liq, 0.0, key))
        ranked.sort()
        # Young fills the whole batch first; only spill leftover slots to older
        # tokens. A deep young queue must not lose slots to stale never-probed.
        lim = max(0, limit)
        young_keys = [k for t, *_rest, k in ranked if t <= 1]
        other_keys = [k for t, *_rest, k in ranked if t >= 2]
        keys = young_keys[:lim]
        if len(keys) < lim:
            keys.extend(other_keys[: lim - len(keys)])
        if not keys:
            return 0

        async def one(key: str) -> None:
            entry = self._tokens.get(key)
            if entry is None:
                return
            pool_hint = entry.pool_address or entry.pool_id
            young = _entry_is_young(entry, now)
            try:
                result = await fetch_token_ath_mcap(
                    entry.address,
                    pool=pool_hint or None,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("Gecko ATH failed for %s: %s", key, exc)
                # Do not stamp probe time on hard failure for young tokens —
                # allow a quick retry while age≤24h.
                if not young:
                    entry.gecko_ath_at = time.time()
                return
            if result.ath_mcap <= 0:
                # Zero/miss: for young tokens leave gecko_ath_at unset (or stale)
                # so the next cycle can catch a short-lived pump via minute candles.
                if not young:
                    entry.gecko_ath_at = time.time()
                return
            entry.gecko_ath_at = time.time()
            prev = entry.ath_mcap
            if (
                prev >= 1_000_000_000.0
                and result.ath_mcap > 0
                and prev > result.ath_mcap * 50
            ):
                prev = 0.0
            peak = max(prev, result.ath_mcap)
            entry.ath_mcap = peak
            if entry.screened is not None:
                entry.screened = self._apply_ath(entry, entry.screened)

        for key in keys:
            await one(key)
            await asyncio.sleep(1.25)
        logger.info(
            "Gecko ATH probed %d tokens (young_first=%d)",
            len(keys),
            sum(1 for k in keys if (e := self._tokens.get(k)) and _entry_is_young(e)),
        )
        return len(keys)

    async def refresh_hot(self) -> int:
        """Force-enrich the hot-set for denser ATH sampling (DS + Gecko)."""
        if (
            self._hot_enriching
            or self._refreshing
            or self._cold_tail_busy()
            or self._parse_active()
            or not self.cold_started
        ):
            return 0
        addrs = self._hot_candidates()
        if not addrs:
            return 0
        self._hot_enriching = True
        try:
            enriched = await self.force_enrich_addresses(addrs)
            await self._apply_gecko_peaks(addrs, limit=_GECKO_BATCH_LIMIT)
            self.last_hot_enrich_ts = time.time()
            logger.info("Hot ATH enrich: %d/%d tokens", len(enriched), len(addrs))
            return len(enriched)
        finally:
            self._hot_enriching = False

    async def run_hot_enrich_loop(self) -> None:
        """Background loop: denser DexScreener + Gecko samples for liquid tokens."""
        while True:
            await asyncio.sleep(_HOT_ENRICH_INTERVAL_S)
            try:
                await self.refresh_hot()
            except Exception:  # noqa: BLE001
                logger.exception("Hot token enrich failed")

    # ---------------------------------------------------------------- reads

    def get_tokens(self) -> list[ScreenedToken]:
        out: list[ScreenedToken] = []
        for e in self._tokens.values():
            if e.screened is None:
                continue
            e.screened = self._apply_ath(e, e.screened)
            out.append(e.screened)
        return out

    def get_token(self, address: str) -> ScreenedToken | None:
        entry = self._tokens.get(address.lower())
        if entry is None or entry.screened is None:
            return None
        entry.screened = self._apply_ath(entry, entry.screened)
        return entry.screened

    def known_addresses(self) -> set[str]:
        return set(self._tokens.keys())

    def mcap_peaks(
        self, addresses: list[str] | None = None
    ) -> dict[str, tuple[float, str]]:
        """Return ``(ath_mcap, symbol)`` for indexed tokens."""
        keys = (
            {a.lower() for a in addresses}
            if addresses is not None
            else set(self._tokens.keys())
        )
        out: dict[str, tuple[float, str]] = {}
        for key in keys:
            entry = self._tokens.get(key)
            if entry is None:
                continue
            row = entry.screened
            if row is not None:
                row = self._apply_ath(entry, row)
                entry.screened = row
                peak = max(entry.ath_mcap, row.ath_mcap, row.market_cap)
                out[key] = (peak, row.symbol or "")
            elif entry.ath_mcap > 0:
                out[key] = (entry.ath_mcap, "")
        return out

    def status(self) -> dict[str, Any]:
        enriched = sum(1 for e in self._tokens.values() if e.screened is not None)
        return {
            "tokens_24h": len(self._tokens),
            "enriched": enriched,
            "building": self.building,
            "cold_started": self.cold_started,
            "refreshing": self._refreshing,
            "cold_tail": self._cold_tail_busy(),
            "last_tip": self.last_tip,
            "last_scan_ts": self.last_scan_ts,
            "last_refresh_ts": self.last_refresh_ts,
            "window_hours": 24,
        }


token_index = TokenIndex()
