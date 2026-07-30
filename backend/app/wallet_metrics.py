"""Wallet-level metrics for parsed buyers: native balance, token hold time,
distinct tokens traded in the last 7 days — plus range filtering.

Balance comes from batched ``eth_getBalance``; hold time is derived from the
token's Transfer logs (first outgoing transfer after the first buy, or "still
holding" up to the chain tip); 7d traded tokens are distinct non-quote token
contracts from Blockscout token-transfers where the wallet sold (from=wallet)
or bought via a contract (to=wallet, from.is_contract) — EOA airdrops ignored.

Speed / completeness:
- No stage budgets that abandon wallets mid-fetch.
- Cheap filters first (balance → hold → tokens 7d); expensive work only on
  survivors.
- Hold scan walks logs chronologically (parallel prefetch), drops wallets
  early once max hold is exceeded without a sell, and stops once every
  wallet's first sell is known (or the range ends).
- Tokens-7d stops early when min (pass) or max (fail) is already decided.
- Bounded retries + shared Blockscout pacing (no infinite spin on 429).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from .blockscout import blockscout_api_base, blockscout_headers
from .chain import RpcClient, checksum, http_client, topic_address
from .config import settings
from .constants import BLOCKS_PER_SECOND, QUOTE_TOKENS, TRANSFER_TOPIC
from .models import BuyerRow, ParseRequest

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]

# Blockscout: Pro key → higher throughput; free tier stays conservative.
# Keep concurrency modest so paced waiters don't look "stuck".
_BLOCKSCOUT_CONCURRENCY = 6 if settings.blockscout_api_key else 4
_BS_MIN_INTERVAL = 0.06 if settings.blockscout_api_key else 0.1  # ~16 / ~10 req/s
_MAX_TRANSFER_PAGES = 12
_BALANCE_CHUNK_PARALLEL = 3
_HOLD_PREFETCH = 2  # parallel getLogs windows while scanning hold time
_MAX_METRIC_ATTEMPTS = 8
_MAX_HOLD_SECONDS = 300  # 5 min timeout for hold-time prefetch

_BALANCE_TTL = 120.0
_balance_cache: dict[str, tuple[float, float]] = {}

_TOKENS7D_TTL = 600.0
_TOKENS7D_CACHE_VER = "v4"
_tokens7d_cache: dict[str, tuple[int, float]] = {}

_HOLDING_TTL = 180.0
_hold_cache: dict[tuple[str, str], tuple[int, int | None, float]] = {}

_CACHE_MAX = 50_000
_CACHE_PRUNE_AGE = 3600.0

_bs_sem = asyncio.Semaphore(_BLOCKSCOUT_CONCURRENCY)
_bs_pace_lock = asyncio.Lock()
_bs_next_ok = 0.0
_bs_interval = _BS_MIN_INTERVAL  # adaptive; grows on 429, shrinks on success


def _prune_cache(cache: dict, now: float) -> None:
    if len(cache) <= _CACHE_MAX:
        return
    stale = [k for k, v in cache.items() if now - v[-1] > _CACHE_PRUNE_AGE]
    for k in stale:
        cache.pop(k, None)


def balance_filter_active(req: ParseRequest) -> bool:
    return req.min_wallet_balance_eth is not None or req.max_wallet_balance_eth is not None


def hold_time_filter_active(req: ParseRequest) -> bool:
    return req.min_hold_time_minutes is not None or req.max_hold_time_minutes is not None


def tokens_7d_filter_active(req: ParseRequest) -> bool:
    return req.min_tokens_traded_7d is not None or req.max_tokens_traded_7d is not None


def _passes(value: float | None, lo: float | None, hi: float | None) -> bool:
    """Range check. ``None`` fails when any bound is set."""
    if lo is None and hi is None:
        return True
    if value is None:
        return False
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


async def batch_wallet_balances(
    rpc: RpcClient, wallets: list[str]
) -> dict[str, float | None]:
    """Native ETH balance per wallet via batched eth_getBalance."""
    now = time.time()
    out: dict[str, float | None] = {}
    misses: list[str] = []
    for w in dict.fromkeys(w.lower() for w in wallets):
        cached = _balance_cache.get(w)
        if cached is not None and now - cached[1] < _BALANCE_TTL:
            out[w] = cached[0]
        else:
            out[w] = None
            misses.append(w)

    def store(w: str, wei: int) -> None:
        val = wei / 1e18
        out[w] = val
        _balance_cache[w] = (val, time.time())

    async def one_chunk(batch: list[str]) -> None:
        remaining = list(batch)
        delay = 0.35
        for round_i in range(1, _MAX_METRIC_ATTEMPTS + 1):
            if not remaining:
                return
            calls = [("eth_getBalance", [checksum(w), "latest"]) for w in remaining]
            try:
                results = await rpc._jsonrpc_batch(calls)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "balance batch round %s failed (%d): %s",
                    round_i, len(remaining), exc,
                )
                results = []
            for w, raw in zip(remaining, results, strict=False):
                if raw is None:
                    continue
                try:
                    store(w, int(str(raw), 16))
                except (TypeError, ValueError):
                    continue
            remaining = [w for w in remaining if out.get(w) is None]
            if not remaining:
                return
            if round_i >= 3:
                still: list[str] = []
                for w in remaining:
                    try:
                        wei = await rpc._call(
                            lambda addr=checksum(w): rpc.w3.eth.get_balance(addr)
                        )
                        store(w, int(wei))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("eth_getBalance %s: %s", w[:10], exc)
                        still.append(w)
                remaining = still
                if not remaining:
                    return
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 8.0)

    if misses:
        chunks = [misses[i : i + 40] for i in range(0, len(misses), 40)]
        sem = asyncio.Semaphore(_BALANCE_CHUNK_PARALLEL)

        async def run_chunk(batch: list[str]) -> None:
            async with sem:
                await one_chunk(batch)

        await asyncio.gather(*[run_chunk(c) for c in chunks])
        unresolved = sum(1 for w in misses if out.get(w) is None)
        if unresolved:
            logger.warning("eth_getBalance unresolved for %d/%d", unresolved, len(misses))
    _prune_cache(_balance_cache, time.time())
    return out


def _hold_minutes(from_b: int, to_b: int) -> float:
    return max(to_b - from_b, 0) / BLOCKS_PER_SECOND / 60.0


async def _fetch_logs_range(
    rpc: RpcClient,
    *,
    token: str,
    from_block: int,
    to_block: int,
) -> list:
    """One adaptive getLogs window (shrinks on range-too-large)."""
    local = max(to_block - from_block + 1, 1)
    cur = from_block
    out: list = []
    while cur <= to_block:
        end = min(cur + local - 1, to_block)
        try:
            part = await rpc.get_logs(
                address=token,
                topics=[TRANSFER_TOPIC],
                from_block=cur,
                to_block=end,
            )
            out.extend(part)
            cur = end + 1
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if local > 500 and any(
                x in msg for x in ("limit", "range", "too large", "response size", "query")
            ):
                local = max(local // 2, 500)
                continue
            raise
    return out


def _blocks_for_minutes(minutes: float) -> int:
    return max(int(minutes * 60 * BLOCKS_PER_SECOND), 1)


async def compute_hold_times(
    rpc: RpcClient,
    *,
    token: str,
    buyers: list[BuyerRow],
    start_block: int,
    end_block: int,
    min_minutes: float | None = None,
    max_minutes: float | None = None,
    on_progress: ProgressCb | None = None,
) -> dict[str, float | None]:
    """Hold time in minutes per wallet (lowercased key).

    Walks Transfer logs chronologically (with parallel prefetch) and stops as
    soon as every tracked wallet has a first outbound transfer — or, when
    ``max_minutes`` is set, as soon as hold without a sell already exceeds max
    (no need to walk all the way to tip for long holders that already fail).
    Wallets whose max possible hold (buy → tip) is already below
    ``min_minutes`` are failed without a scan.
    """
    tkey = token.lower()
    now = time.time()
    first_block = {b.wallet.lower(): b.first_block for b in buyers}
    out: dict[str, float | None] = {w: None for w in first_block}
    if not first_block:
        return out

    need_scan: dict[str, int] = {}
    for wallet, fb in first_block.items():
        # Impossible to satisfy min hold even if still holding at tip.
        if min_minutes is not None and _hold_minutes(fb, end_block) < min_minutes:
            out[wallet] = _hold_minutes(fb, end_block)
            continue
        ent = _hold_cache.get((tkey, wallet))
        if ent is not None and ent[0] == fb:
            first_out = ent[1]
            if first_out is not None:
                out[wallet] = _hold_minutes(fb, first_out)
                continue
            if now - ent[2] < _HOLDING_TTL:
                out[wallet] = _hold_minutes(fb, end_block)
                continue
        need_scan[wallet] = fb

    if not need_scan:
        return out

    from_block = max(min(need_scan.values()), start_block)
    span = max(end_block - from_block, 1)
    first_out_block: dict[str, int] = {}
    # Wallets failed on max without a real sell (hold lower-bound only).
    max_failed: set[str] = set()
    pending = set(need_scan)
    chunk = max(settings.log_chunk_size, 20_000)
    cur = from_block
    delay = 0.8
    attempts_left = _MAX_METRIC_ATTEMPTS
    max_blocks = _blocks_for_minutes(max_minutes) if max_minutes is not None else None
    last_prog = 0.0
    _hold_start = time.monotonic()

    async def _heartbeat(upto: int) -> None:
        nonlocal last_prog
        if not on_progress:
            return
        frac = min(max((upto - from_block) / span, 0.0), 1.0)
        # Throttle UI updates (~every 3% of the range).
        if frac - last_prog < 0.03 and upto < end_block and pending:
            return
        last_prog = frac
        # Keep message stable so the job log refreshes in-place (not flood).
        await on_progress(
            "filter",
            f"Hold time: scanning transfers for {len(need_scan)} wallets…",
            0.90 + 0.05 * frac,
        )

    def _drop_past_max(upto: int) -> None:
        if max_blocks is None:
            return
        for wallet in list(pending):
            fb = need_scan[wallet]
            cutoff = fb + max_blocks
            if upto < cutoff:
                continue
            out[wallet] = _hold_minutes(fb, cutoff)
            max_failed.add(wallet)
            pending.discard(wallet)

    while cur <= end_block and pending:
        if time.monotonic() - _hold_start > _MAX_HOLD_SECONDS:
            logger.warning("Hold-time prefetch time limit hit, falling back for %d wallets", len(pending))
            for wallet in pending:
                fb = need_scan[wallet]
                out[wallet] = _hold_minutes(fb, end_block)
                _hold_cache[(tkey, wallet)] = (fb, None, time.time())
            pending.clear()
            break
        ranges: list[tuple[int, int]] = []
        c = cur
        for _ in range(_HOLD_PREFETCH):
            if c > end_block:
                break
            # With max set, never fetch past the furthest fail-cutoff.
            if max_blocks is not None and pending:
                furthest = max(need_scan[w] + max_blocks for w in pending)
                if c > furthest:
                    break
            end = min(c + chunk - 1, end_block)
            if max_blocks is not None and pending:
                furthest = max(need_scan[w] + max_blocks for w in pending)
                end = min(end, furthest)
            ranges.append((c, end))
            c = end + 1
        if not ranges:
            # Soft-cap stopped us with pending wallets → they exceed max.
            if max_blocks is not None:
                _drop_past_max(max(need_scan[w] + max_blocks for w in pending))
            break

        try:
            parts = await asyncio.gather(
                *[
                    _fetch_logs_range(rpc, token=token, from_block=a, to_block=b)
                    for a, b in ranges
                ]
            )
        except Exception as exc:  # noqa: BLE001
            attempts_left -= 1
            logger.warning(
                "hold-time prefetch %s-%s failed (%s left): %s",
                ranges[0][0], ranges[-1][1], attempts_left, exc,
            )
            if attempts_left <= 0:
                stamp = time.time()
                for wallet in pending:
                    fb = need_scan[wallet]
                    out[wallet] = _hold_minutes(fb, end_block)
                    _hold_cache[(tkey, wallet)] = (fb, None, stamp)
                return out
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 12.0)
            continue

        total_logs = 0
        for (_a, b), logs in zip(ranges, parts, strict=True):
            total_logs += len(logs)
            logs.sort(key=lambda lg: int(lg["blockNumber"]))
            for log in logs:
                if not pending:
                    break
                topics = log.get("topics") or []
                if len(topics) < 3:
                    continue
                frm = topic_address(topics[1]).lower()
                if frm not in pending:
                    continue
                fb = need_scan[frm]
                block = int(log["blockNumber"])
                if block < fb:
                    continue
                prev = first_out_block.get(frm)
                if prev is None or block < prev:
                    first_out_block[frm] = block
                    pending.discard(frm)
            _drop_past_max(b)
            await _heartbeat(b)
            if not pending:
                break

        cur = ranges[-1][1] + 1
        if total_logs < 2000 * len(ranges) and chunk < 200_000:
            chunk = min(chunk * 2, 200_000)

    stamp = time.time()
    for wallet, fb in need_scan.items():
        if wallet in max_failed:
            # Already set out[]; cache as still-holding so a later run can
            # re-check if tip advanced / max tightened.
            _hold_cache[(tkey, wallet)] = (fb, None, stamp)
            continue
        first_out = first_out_block.get(wallet)
        out[wallet] = _hold_minutes(fb, first_out if first_out is not None else end_block)
        _hold_cache[(tkey, wallet)] = (fb, first_out, stamp)
    _prune_cache(_hold_cache, stamp)
    return out


def _parse_ts(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _bs_get(url: str, params: dict[str, object]):
    """Paced Blockscout GET with 429/5xx retries and adaptive interval.

    Pace scheduling happens *before* taking the concurrency semaphore so
    waiters don't pin worker slots idle (which made the UI look frozen).
    """
    global _bs_next_ok, _bs_interval
    resp = None
    delay = 0.5
    for attempt in range(_MAX_METRIC_ATTEMPTS):
        async with _bs_pace_lock:
            now = time.time()
            wait = _bs_next_ok - now
            _bs_next_ok = max(_bs_next_ok, now) + _bs_interval
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            async with _bs_sem:
                resp = await http_client().get(
                    url, params=params, headers=blockscout_headers()
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Blockscout GET error attempt %s: %s", attempt + 1, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 12.0)
            continue
        if resp.status_code in (429, 502, 503):
            async with _bs_pace_lock:
                _bs_interval = min(_bs_interval * 1.5, 0.5)
            ra = resp.headers.get("Retry-After")
            try:
                sleep_for = min(float(ra), 20.0) if ra else delay
            except ValueError:
                sleep_for = delay
            await asyncio.sleep(sleep_for)
            delay = min(delay * 1.6, 12.0)
            continue
        if resp.status_code == 200:
            async with _bs_pace_lock:
                _bs_interval = max(_BS_MIN_INTERVAL, _bs_interval * 0.92)
        break
    return resp


def _addr_hash(node: object) -> str:
    if isinstance(node, dict):
        return str(node.get("hash") or node.get("address_hash") or "").lower()
    return str(node or "").lower()


def _is_contract(node: object) -> bool:
    return isinstance(node, dict) and bool(node.get("is_contract"))


async def _tokens_traded_7d_one(
    wallet: str,
    cutoff: datetime,
    *,
    enough: int | None = None,
    too_many: int | None = None,
) -> tuple[int, bool] | None:
    """Distinct non-quote tokens traded in 7d.

    Returns ``(count, exact)``. May stop early when:
    - ``enough`` is met (min-only pass) → ``exact=False``
    - ``too_many`` is exceeded (max fail) → ``exact=False``
    Partial results are not cached as full counts.
    """
    wallet_l = wallet.lower()
    url = f"{blockscout_api_base()}/addresses/{wallet}/token-transfers"
    # Prefer ERC-20 only — skips NFT noise and usually fewer pages.
    use_type_filter = True
    params: dict[str, object] = {"type": "ERC-20"}
    tokens: set[str] = set()
    try:
        for _ in range(_MAX_TRANSFER_PAGES):
            resp = await _bs_get(url, params)
            if (
                use_type_filter
                and resp is not None
                and resp.status_code in (400, 422)
            ):
                # Older Blockscout builds may not accept type= — retry bare.
                use_type_filter = False
                params = {k: v for k, v in params.items() if k != "type"}
                resp = await _bs_get(url, params)
            if resp is None or resp.status_code not in (200, 404):
                return None
            if resp.status_code == 404:
                return 0, True
            data = resp.json()
            items = data.get("items") or []
            reached_cutoff = False
            for item in items:
                ts = _parse_ts(item.get("timestamp"))
                if ts is not None and ts < cutoff:
                    reached_cutoff = True
                    break
                tok = item.get("token") or {}
                addr = str(tok.get("address") or tok.get("address_hash") or "").lower()
                if not addr or addr in QUOTE_TOKENS:
                    continue
                frm = item.get("from")
                to = item.get("to")
                frm_h = _addr_hash(frm)
                to_h = _addr_hash(to)
                if frm_h == wallet_l:
                    tokens.add(addr)
                elif to_h == wallet_l and _is_contract(frm):
                    tokens.add(addr)
                if enough is not None and too_many is None and len(tokens) >= enough:
                    return len(tokens), False
                if too_many is not None and len(tokens) > too_many:
                    return len(tokens), False
            next_params = data.get("next_page_params")
            if reached_cutoff or not next_params or not items:
                break
            params = dict(next_params)
            if use_type_filter:
                params["type"] = "ERC-20"
    except Exception as exc:  # noqa: BLE001
        logger.warning("tokens-7d lookup failed for %s: %s", wallet, exc)
        return None
    return len(tokens), True


async def batch_tokens_traded_7d(
    wallets: list[str],
    on_progress: ProgressCb | None = None,
    *,
    enough: int | None = None,
    too_many: int | None = None,
) -> dict[str, int | None]:
    """Distinct ERC-20 tokens traded in 7d; paced + bounded retries."""
    now = time.time()
    out: dict[str, int | None] = {}
    misses: list[str] = []
    for w in dict.fromkeys(w.lower() for w in wallets):
        cached = _tokens7d_cache.get(f"{_TOKENS7D_CACHE_VER}:{w}")
        if cached is not None and now - cached[1] < _TOKENS7D_TTL:
            out[w] = cached[0]
        else:
            out[w] = None
            misses.append(w)

    if not misses:
        return out

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    done = 0
    total = len(misses)
    lock = asyncio.Lock()

    async def one(wallet: str) -> None:
        nonlocal done
        delay = 0.6
        for attempt in range(_MAX_METRIC_ATTEMPTS):
            result = await _tokens_traded_7d_one(
                wallet, cutoff, enough=enough, too_many=too_many
            )
            if result is not None:
                count, exact = result
                out[wallet] = count
                # Never cache early-exit lower/upper bounds — a later tighter
                # filter would otherwise reuse an incomplete count.
                if exact:
                    _tokens7d_cache[f"{_TOKENS7D_CACHE_VER}:{wallet}"] = (
                        count, time.time()
                    )
                break
            if attempt + 1 < _MAX_METRIC_ATTEMPTS:
                logger.warning(
                    "tokens-7d retry %s/%s for %s in %.1fs",
                    attempt + 1, _MAX_METRIC_ATTEMPTS, wallet[:12], delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 1.6, 10.0)
        async with lock:
            done += 1
            if on_progress and (done == total or done % 5 == 0 or done <= 3):
                await on_progress(
                    "wallets",
                    f"Tokens 7d {done}/{total} wallets…",
                    0.9 + 0.08 * done / max(total, 1),
                )

    await asyncio.wait_for(asyncio.gather(*[one(w) for w in misses]), timeout=300)
    unresolved = sum(1 for w in misses if out.get(w) is None)
    if unresolved:
        logger.warning("tokens-7d unresolved for %d/%d after retries", unresolved, total)
    _prune_cache(_tokens7d_cache, time.time())
    return out


async def enrich_and_filter_buyers(
    rpc: RpcClient,
    *,
    token: str,
    buyers: list[BuyerRow],
    req: ParseRequest,
    start_block: int,
    end_block: int,
    on_progress: ProgressCb | None = None,
) -> list[BuyerRow]:
    """Filter buyers with metrics — expensive work only on surviving wallets.

    Order: balance (cheap RPC) → hold (shared log scan, early-stop) →
    tokens 7d (Blockscout, early-exit on min/max) → fill missing display
    balances for the final shortlist.
    """
    if not buyers:
        return buyers

    async def prog(stage: str, message: str, percent: float) -> None:
        if on_progress:
            await on_progress(stage, message, percent)

    kept = list(buyers)
    want_bal = balance_filter_active(req)
    want_hold = hold_time_filter_active(req)
    want_t7 = tokens_7d_filter_active(req)
    initial = len(kept)

    if not (want_bal or want_hold or want_t7):
        await prog(
            "wallets",
            f"No wallet filters — keeping all {initial} early buyers",
            0.95,
        )
    else:
        await prog(
            "wallets",
            f"Wallet filters on {initial} buyers "
            f"(balance={want_bal}, hold={want_hold}, tokens7d={want_t7})",
            0.86,
        )

    # 1) Balance first — cheapest and often the strongest cut.
    if want_bal and kept:
        before = len(kept)
        await prog("filter", f"Balance: fetching {before} wallets…", 0.88)
        balances = await batch_wallet_balances(rpc, [b.wallet for b in kept])
        survivors: list[BuyerRow] = []
        unknown = 0
        for row in kept:
            row.wallet_balance_eth = balances.get(row.wallet.lower())
            if row.wallet_balance_eth is None:
                unknown += 1
            if _passes(
                row.wallet_balance_eth,
                req.min_wallet_balance_eth,
                req.max_wallet_balance_eth,
            ):
                survivors.append(row)
        kept = survivors
        await prog(
            "filter",
            f"Balance: {before} → {len(kept)} kept"
            + (f" ({unknown} unknown dropped)" if unknown else ""),
            0.89,
        )

    # 2) Hold time — one chronological Transfer scan, stop when all sold.
    if want_hold and kept:
        before = len(kept)
        await prog("filter", f"Hold time: scanning transfers for {before} wallets…", 0.90)
        hold_times = await compute_hold_times(
            rpc,
            token=token,
            buyers=kept,
            start_block=start_block,
            end_block=end_block,
            min_minutes=req.min_hold_time_minutes,
            max_minutes=req.max_hold_time_minutes,
            on_progress=on_progress,
        )
        survivors = []
        for row in kept:
            row.hold_time_minutes = hold_times.get(row.wallet.lower())
            if _passes(
                row.hold_time_minutes, req.min_hold_time_minutes, req.max_hold_time_minutes
            ):
                survivors.append(row)
        kept = survivors
        await prog("filter", f"Hold time: {before} → {len(kept)} kept", 0.91)

    # 3) Tokens 7d — most expensive per wallet; run on the shortlist only.
    if want_t7 and kept:
        before = len(kept)
        enough: int | None = None
        too_many: int | None = None
        # Early-exit for pass is only safe when max is unset (otherwise need
        # the full count to know we are not above max).
        if req.min_tokens_traded_7d is not None and req.max_tokens_traded_7d is None:
            enough = max(int(req.min_tokens_traded_7d), 0)
        if req.max_tokens_traded_7d is not None:
            too_many = max(int(req.max_tokens_traded_7d), 0)
        await prog("filter", f"Tokens 7d: counting for {before} wallets…", 0.92)
        tokens_7d = await batch_tokens_traded_7d(
            [b.wallet for b in kept],
            on_progress=on_progress,
            enough=enough,
            too_many=too_many,
        )
        survivors = []
        unknown = 0
        for row in kept:
            row.tokens_traded_7d = tokens_7d.get(row.wallet.lower())
            if row.tokens_traded_7d is None:
                unknown += 1
            if _passes(
                None if row.tokens_traded_7d is None else float(row.tokens_traded_7d),
                req.min_tokens_traded_7d,
                req.max_tokens_traded_7d,
            ):
                survivors.append(row)
        kept = survivors
        await prog(
            "filter",
            f"Tokens 7d: {before} → {len(kept)} kept"
            + (f" ({unknown} unknown dropped)" if unknown else ""),
            0.96,
        )

    # Display balances for the final shortlist when we skipped the balance stage.
    if kept and not want_bal:
        need = [b.wallet for b in kept if b.wallet_balance_eth is None]
        if need:
            await prog("wallets", f"Filling display balances ({len(need)})…", 0.98)
            balances = await batch_wallet_balances(rpc, need)
            for row in kept:
                if row.wallet_balance_eth is None:
                    row.wallet_balance_eth = balances.get(row.wallet.lower())

    if want_bal or want_hold or want_t7:
        await prog(
            "filter",
            f"Filters done: {initial} → {len(kept)} wallets",
            0.99,
        )

    return kept
