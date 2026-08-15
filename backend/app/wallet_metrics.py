"""Wallet-level metrics for parsed buyers: native balance, token hold time,
distinct tokens bought in the lookback window — plus range filtering.

Balance comes from batched ``eth_getBalance``; hold time is derived from the
token's Transfer logs (first outgoing transfer after the first buy, or "still
holding" up to the chain tip); unique-token count is distinct non-quote tokens
**bought** via contract (DEX/router/pool → wallet). Outbound sells/sends and
EOA gifts («скинули») are ignored.

Speed / completeness:
- No stage budgets that abandon wallets mid-fetch.
- Cheap filters first (balance → hold → tokens 7d); expensive work only on
  survivors.
- Hold scan walks logs chronologically and stops once every wallet's first
  sell is known (or the range ends).
- Tokens-7d stops early when min (pass) or max (fail) is already decided.
- Bounded retries + shared Blockscout pacing (no infinite spin on 429).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from .blockscout import _get_json, blockscout_api_base
from .buy_gate import is_dex_buy_transfer, is_wallet_initiated_buy
from .chain import RpcClient, checksum, topic_address
from .config import settings
from .constants import BLOCKS_PER_SECOND, QUOTE_TOKENS, TRANSFER_TOPIC
from .models import BuyerRow, ParseRequest
from . import wallet_unique_cache as unique_cache

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]

_MAX_TRANSFER_PAGES = 12
_BALANCE_CHUNK_PARALLEL = 2
_MAX_METRIC_ATTEMPTS = 8

_BALANCE_TTL = 120.0
_balance_cache: dict[str, tuple[float, float]] = {}

_TOKENS7D_TTL = 600.0
_TOKENS7D_CACHE_VER = "v10"  # SPCX removed from QUOTE_TOKENS
_tokens7d_cache: dict[str, tuple[int, float]] = {}


def followup_unique_floor(wallet: str, token: str) -> int:
    """Lower bound on distinct buys from follow-up deals.

    Relay / permit2 routes often never Transfer the bought token to the EOA
    (tokens stay on intermediate contracts), so Blockscout inbound unique
    undercounts. Follow-up already recorded prior watch/GMGN buys — treat the
    current parse token as +1 when it is not yet in deals.
    """
    try:
        from .followup_store import followup_store

        known = followup_store.known_tokens(wallet)
    except Exception as exc:  # noqa: BLE001
        logger.debug("followup unique floor skipped: %s", exc)
        return 0
    token_l = (token or "").strip().lower()
    n = len(known)
    if token_l and token_l not in known:
        n += 1
    return n


def apply_followup_unique_floor(
    count: int | None, wallet: str, token: str
) -> int | None:
    """Raise ``count`` to at least the follow-up floor; keep ``None`` if floor=0."""
    floor = followup_unique_floor(wallet, token)
    if floor <= 0:
        return count
    if count is None:
        return floor
    return max(int(count), floor)

_HOLDING_TTL = 180.0
_hold_cache: dict[tuple[str, str], tuple[int, int | None, float]] = {}

_CACHE_MAX = 50_000
_CACHE_PRUNE_AGE = 3600.0

# Process-wide: at most N tokens run unique/Blockscout enrich at once.
# Created lazily so tests can monkeypatch PARSE_UNIQUE_CONCURRENCY.
_unique_enrich_sem: asyncio.Semaphore | None = None
_unique_enrich_sem_n: int | None = None
_unique_enrich_sem_lock = asyncio.Lock()


async def _unique_enrich_semaphore() -> asyncio.Semaphore:
    """Shared unique-enrich gate (resized if constant changes in tests)."""
    global _unique_enrich_sem, _unique_enrich_sem_n
    from .watch_qualify import PARSE_UNIQUE_CONCURRENCY

    n = max(1, int(PARSE_UNIQUE_CONCURRENCY))
    async with _unique_enrich_sem_lock:
        if _unique_enrich_sem is None or _unique_enrich_sem_n != n:
            _unique_enrich_sem = asyncio.Semaphore(n)
            _unique_enrich_sem_n = n
        return _unique_enrich_sem


def reset_unique_enrich_semaphore_for_tests() -> None:
    """Drop cached sem so the next acquire picks up a monkeypatched concurrency."""
    global _unique_enrich_sem, _unique_enrich_sem_n
    _unique_enrich_sem = None
    _unique_enrich_sem_n = None


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


async def _gmgn_unique_buys_in_window(
    wallet: str,
    *,
    lookback_hours: float,
) -> int | None:
    """Count distinct non-quote GMGN buys in the lookback window.

    Used as fail-soft when Blockscout undercounts (exact 0 / None) and as a
    max-filter guard when a low BS count would PASS early-buyer (Relay wallets
    with no inbound Transfer still show unique=0/1 on BS while GMGN has many).
    Returns ``None`` when the OpenAPI fetch fails / rate-limits.
    """
    from .gmgn_portfolio import fetch_unique_buys

    try:
        result = await fetch_unique_buys(wallet, max_pages=3)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "GMGN unique fail-soft lookup failed for %s: %s",
            wallet[:12],
            type(exc).__name__,
        )
        return None
    if not result.ok or result.rate_limited:
        return None
    cutoff_ts = time.time() - max(float(lookback_hours or 168.0), 1.0) * 3600.0
    tokens: set[str] = set()
    for buy in result.buys:
        tok = str(buy.token or "").lower()
        if not tok or tok in QUOTE_TOKENS:
            continue
        ts = int(buy.timestamp or 0)
        if ts > 0 and ts < cutoff_ts:
            continue
        tokens.add(tok)
    return len(tokens)


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


async def compute_hold_times(
    rpc: RpcClient,
    *,
    token: str,
    buyers: list[BuyerRow],
    start_block: int,
    end_block: int,
    min_minutes: float | None = None,
) -> dict[str, float | None]:
    """Hold time in minutes per wallet (lowercased key).

    Walks Transfer logs chronologically and stops as soon as every tracked
    wallet has a first outbound transfer. Wallets whose max possible hold
    (buy → tip) is already below ``min_minutes`` are failed without a scan.
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
    scanned_first_out: dict[str, int] = {}
    pending = set(need_scan)
    chunk = max(settings.log_chunk_size, 20_000)
    cur = from_block
    delay = 0.8
    attempts_left = _MAX_METRIC_ATTEMPTS

    while cur <= end_block and pending:
        end = min(cur + chunk - 1, end_block)
        try:
            logs = await _fetch_logs_range(
                rpc, token=token, from_block=cur, to_block=end
            )
        except Exception as exc:  # noqa: BLE001
            attempts_left -= 1
            logger.warning(
                "hold-time chunk %s-%s failed (%s left): %s",
                cur, end, attempts_left, exc,
            )
            if attempts_left <= 0:
                for wallet in pending:
                    fb = need_scan[wallet]
                    out[wallet] = _hold_minutes(fb, end_block)
                    _hold_cache[(tkey, wallet)] = (fb, None, time.time())
                return out
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 12.0)
            continue

        # Sort within the chunk so first_out is the earliest block.
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
            prev = scanned_first_out.get(frm)
            if prev is None or block < prev:
                scanned_first_out[frm] = block
                pending.discard(frm)

        cur = end + 1
        # Adaptive: grow chunk after successes, keep RPC load bounded.
        if len(logs) < 2000 and chunk < 200_000:
            chunk = min(chunk * 2, 200_000)

    stamp = time.time()
    for wallet, fb in need_scan.items():
        first_out = scanned_first_out.get(wallet)
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


class _BsResp:
    """Minimal response stand-in for callers that expect ``.status_code`` / ``.json()``."""

    __slots__ = ("status_code", "_data")

    def __init__(self, status_code: int, data: object) -> None:
        self.status_code = status_code
        self._data = data

    def json(self) -> object:
        return self._data


async def _bs_get(url: str, params: dict[str, object]):
    """Blockscout GET via shared process-wide pace (see ``blockscout._get_json``)."""
    base = blockscout_api_base()
    if url.startswith(base):
        path = url[len(base) :]
    else:
        marker = "/api/v2"
        idx = url.find(marker)
        path = url[idx + len(marker) :] if idx >= 0 else url
    if not path.startswith("/"):
        path = f"/{path}"
    got = await _get_json(path, params=dict(params))
    if got is None:
        return None
    return _BsResp(got[0], got[1])


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
    """Distinct non-quote tokens **bought by this wallet** since ``cutoff``.

    A transfer counts only when ``tx.from == wallet`` (wallet-initiated swap).
    Airdrops / third-party multicalls that credit the wallet are ignored.

    Cheap multi-trader reject: accumulate sync ``is_dex_buy_transfer`` hits in
    ``approx``; when ``too_many`` is set and ``len(approx) > too_many``, run the
    full buy-gate only until confirmed buys exceed ``too_many``. In the pass
    band (``len(approx) <= too_many``), every approx token gets a full gate.

    Returns ``(count, exact)``. May stop early when:
    - ``enough`` is met (min-only pass) → ``exact=False``
    - ``too_many`` is exceeded (max fail) → ``exact=False``
    Partial results are not cached as full counts.
    """
    wallet_l = wallet.lower()
    url = f"{blockscout_api_base()}/addresses/{wallet}/token-transfers"
    # Inbound-only pages: sells/outbound noise must not fill the window.
    params: dict[str, object] = {"filter": "to"}
    tokens: set[str] = set()
    approx: set[str] = set()
    pending: dict[str, dict] = {}

    async def _confirm_pending(*, stop_when_approx_ok: bool) -> tuple[int, bool] | None:
        """Full buy-gate on pending items. Return early-exit or None."""
        for addr in list(pending):
            if stop_when_approx_ok and too_many is not None and len(approx) <= too_many:
                break
            item = pending.pop(addr)
            if await is_wallet_initiated_buy(item, wallet_l):
                tokens.add(addr)
                if enough is not None and too_many is None and len(tokens) >= enough:
                    return len(tokens), False
                if too_many is not None and len(tokens) > too_many:
                    return len(tokens), False
            else:
                approx.discard(addr)
        return None

    try:
        for _ in range(_MAX_TRANSFER_PAGES):
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
                if not addr or addr in QUOTE_TOKENS or addr in approx or addr in tokens:
                    continue
                if not is_dex_buy_transfer(item, wallet_l):
                    continue
                approx.add(addr)
                pending[addr] = item
                # Multi-trader: confirm only until real buys exceed too_many.
                if too_many is not None and len(approx) > too_many:
                    early = await _confirm_pending(stop_when_approx_ok=True)
                    if early is not None:
                        return early
                # Min-only: confirm once cheap hits cover the floor.
                elif enough is not None and too_many is None and len(approx) >= enough:
                    early = await _confirm_pending(stop_when_approx_ok=False)
                    if early is not None:
                        return early
            next_params = data.get("next_page_params")
            if reached_cutoff or not next_params or not items:
                break
            params = dict(next_params)
            if "filter" not in params:
                params["filter"] = "to"
        # Pass band (or no max): full buy-gate for remaining approx tokens.
        early = await _confirm_pending(stop_when_approx_ok=False)
        if early is not None:
            return early
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
    lookback_hours: float = 168.0,
) -> dict[str, int | None]:
    """Distinct ERC-20 tokens traded in ``lookback_hours``; paced + bounded retries."""
    now = time.time()
    hours = max(float(lookback_hours or 168.0), 1.0)
    period_hours = max(int(hours), 1)
    cache_prefix = f"{_TOKENS7D_CACHE_VER}:h{period_hours}:"
    out: dict[str, int | None] = {}
    misses: list[str] = []
    unique_cache.maybe_seed()

    def _trust_cached(count: int) -> bool:
        # Stale low counts falsely PASS max filters (unique=1 after 2nd buy).
        if too_many is not None and int(count) <= int(too_many):
            return False
        return True

    for w in dict.fromkeys(w.lower() for w in wallets):
        cached = _tokens7d_cache.get(f"{cache_prefix}{w}")
        if cached is not None and now - cached[1] < _TOKENS7D_TTL:
            if _trust_cached(cached[0]):
                out[w] = cached[0]
                continue
        durable = unique_cache.get_exact(w, period_hours, now=now)
        if durable is not None and _trust_cached(durable):
            out[w] = durable
            _tokens7d_cache[f"{cache_prefix}{w}"] = (durable, now)
            continue
        out[w] = None
        misses.append(w)

    if not misses:
        return out

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    done = 0
    total = len(misses)
    lock = asyncio.Lock()
    label = f"{hours:g}h"
    from .watch_qualify import PARSE_UNIQUE_WALLET_FANOUT

    fanout = max(1, int(PARSE_UNIQUE_WALLET_FANOUT))
    wallet_sem = asyncio.Semaphore(fanout)

    async def one(wallet: str) -> None:
        nonlocal done
        async with wallet_sem:
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
                        stamp = time.time()
                        _tokens7d_cache[f"{cache_prefix}{wallet}"] = (count, stamp)
                        await asyncio.to_thread(
                            unique_cache.put_exact,
                            wallet,
                            period_hours,
                            count,
                            exact=True,
                            now=stamp,
                        )
                    break
                if attempt + 1 < _MAX_METRIC_ATTEMPTS:
                    logger.warning(
                        "tokens-%s retry %s/%s for %s in %.1fs",
                        label, attempt + 1, _MAX_METRIC_ATTEMPTS, wallet[:12], delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 1.6, 10.0)
        async with lock:
            done += 1
            if on_progress and (done == total or done % 5 == 0 or done <= 3):
                await on_progress(
                    "wallets",
                    f"Tokens {label} {done}/{total} wallets…",
                    0.9 + 0.08 * done / max(total, 1),
                )

    await asyncio.gather(*[one(w) for w in misses])
    unresolved = sum(1 for w in misses if out.get(w) is None)
    if unresolved:
        logger.warning(
            "tokens-%s unresolved for %d/%d after retries", label, unresolved, total
        )
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
    out_meta: dict | None = None,
) -> list[BuyerRow]:
    """Filter buyers with metrics — expensive work only on surviving wallets.

    Order: balance (cheap RPC) → hold (shared log scan, early-stop) →
    tokens 7d (Blockscout, early-exit on min/max) → fill missing display
    balances for the final shortlist.

    ``out_meta`` (optional) receives ``unique_partial`` / ``unique_skipped`` so
    watch can stamp hold for young requeue without raising the unique wall.
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

    # Drop smart-contract "buyers" (relay / helper hops). V2 used to attribute
    # pool→contract before EOA resolve; contracts must never enter Хвать TG.
    if kept and hasattr(rpc, "batch_is_eoa"):
        try:
            eoa_map = await rpc.batch_is_eoa([b.wallet for b in kept])
        except Exception as exc:  # noqa: BLE001
            logger.warning("EOA filter skipped: %s", exc)
            eoa_map = {}
        if isinstance(eoa_map, dict) and eoa_map:
            before_c = len(kept)
            # Explicit False drops; missing key keeps (fail-open on partial maps).
            kept = [
                b
                for b in kept
                if eoa_map.get(b.wallet.lower(), True) is not False
            ]
            dropped = before_c - len(kept)
            if dropped:
                await prog(
                    "filter",
                    f"Contracts: dropped {dropped} non-EOA hop(s) "
                    f"({before_c} → {len(kept)})",
                    0.855,
                )
            if not kept:
                return []

    if not (want_bal or want_hold or want_t7):
        await prog(
            "wallets",
            f"No wallet filters — keeping all {len(kept)} early buyers",
            0.95,
        )
    else:
        await prog(
            "wallets",
            f"Wallet filters on {len(kept)} buyers "
            f"(balance={want_bal}, hold={want_hold}, tokens7d={want_t7})",
            0.86,
        )

        # 1) Balance first — cheapest and often the strongest cut.
        # Fail-open on unknown balance (RPC miss) — mirror unique fail-open so
        # transient eth_getBalance flakes do not wipe early buyers.
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
                    survivors.append(row)
                    continue
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
                + (f" ({unknown} unknown kept)" if unknown else ""),
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

    # 3) Unique tokens in lookback — most expensive; run on the shortlist only.
    # Process in batches and stop once we have enough passers (Хвать does not
    # need every unique=1 wallet on a 300-buyer token).
    # Process-wide unique slot: parse ×N otherwise stampede Blockscout and the
    # per-token wall trips with fewer wallets examined.
    if want_t7 and kept:
        from .watch_qualify import (
            PARSE_MAX_PASSING_BUYERS,
            PARSE_UNIQUE_BATCH,
            unique_lookup_batch_size,
            unique_wall_sec,
        )

        before = len(kept)
        enough: int | None = None
        too_many: int | None = None
        # Early-exit for pass is only safe when max is unset (otherwise need
        # the full count to know we are not above max).
        if req.min_tokens_traded_7d is not None and req.max_tokens_traded_7d is None:
            enough = max(int(req.min_tokens_traded_7d), 0)
        if req.max_tokens_traded_7d is not None:
            too_many = max(int(req.max_tokens_traded_7d), 0)
        from .models import tokens_unique_period_hours

        lookback_h = tokens_unique_period_hours(
            getattr(req, "tokens_unique_period", None)
        )
        period_label = getattr(
            getattr(req, "tokens_unique_period", None),
            "value",
            None,
        ) or f"{lookback_h:g}h"
        pass_cap = max(1, int(PARSE_MAX_PASSING_BUYERS))
        max_batch = max(1, int(PARSE_UNIQUE_BATCH))
        wall_sec = unique_wall_sec(before)

        await prog(
            "filter",
            f"Tokens {period_label}: waiting unique slot "
            f"(up to {before} wallets, cap {pass_cap}, wall {wall_sec:.0f}s)…",
            0.915,
        )
        unique_sem = await _unique_enrich_semaphore()
        wait_t0 = time.time()
        async with unique_sem:
            waited = time.time() - wait_t0
            if waited >= 0.5:
                logger.info(
                    "unique slot waited %.1fs token=%s",
                    waited,
                    token[:12],
                )
            # Wall starts only after we own the slot — queue wait must not burn it.
            wall_deadline = time.time() + wall_sec
            await prog(
                "filter",
                f"Tokens {period_label}: counting up to {before} wallets "
                f"(adaptive batches ≤{max_batch}, stop at {pass_cap} pass "
                f"or {wall_sec:.0f}s)…",
                0.92,
            )
            survivors = []
            unknown = 0
            soft_rescued = 0
            examined = 0
            idx = 0
            wall_hit = False
            while idx < len(kept) and len(survivors) < pass_cap:
                if time.time() >= wall_deadline:
                    wall_hit = True
                    break
                batch_n = unique_lookup_batch_size(
                    pass_cap=pass_cap,
                    n_survivors=len(survivors),
                    max_batch=max_batch,
                )
                if batch_n <= 0:
                    break
                batch = kept[idx : idx + batch_n]
                idx += len(batch)
                tokens_7d = await batch_tokens_traded_7d(
                    [b.wallet for b in batch],
                    on_progress=on_progress,
                    enough=enough,
                    too_many=too_many,
                    lookback_hours=lookback_h,
                )
                # Decide the whole fetched batch even if wall expired mid-decide —
                # RPC already paid; dropping the tail silently hid valid wallets.
                # With a max unique bound, unknown counts must not reach TG
                # (MANCER: wall + fail-open → "30d tokens —" multi-traders).
                # Min-only stays fail-open so indexer flakes do not erase uniques.
                fail_closed_unknown = too_many is not None
                for row in batch:
                    if len(survivors) >= pass_cap:
                        break
                    examined += 1
                    raw_n = tokens_7d.get(row.wallet.lower())
                    # Follow-up prior deals beat BS undercount (Relay/permit2:
                    # token never lands on the EOA → inbound unique misses).
                    floored = apply_followup_unique_floor(raw_n, row.wallet, token)
                    if (
                        floored is not None
                        and raw_n is not None
                        and int(floored) > int(raw_n)
                    ):
                        logger.info(
                            "unique floor: BS=%s → FU=%s wallet=%s token=%s",
                            raw_n,
                            floored,
                            row.wallet[:12],
                            token[:12],
                        )
                    elif floored is not None and raw_n is None:
                        logger.info(
                            "unique floor: BS=None → FU=%s wallet=%s token=%s",
                            floored,
                            row.wallet[:12],
                            token[:12],
                        )
                    row.tokens_traded_7d = floored
                    if row.tokens_traded_7d is None:
                        gmgn_n = await _gmgn_unique_buys_in_window(
                            row.wallet, lookback_hours=lookback_h
                        )
                        if gmgn_n is not None and _passes(
                            float(gmgn_n),
                            req.min_tokens_traded_7d,
                            req.max_tokens_traded_7d,
                        ):
                            row.tokens_traded_7d = gmgn_n
                            soft_rescued += 1
                            survivors.append(row)
                            logger.info(
                                "unique fail-soft: BS=None GMGN=%s wallet=%s",
                                gmgn_n,
                                row.wallet[:12],
                            )
                            continue
                        if fail_closed_unknown:
                            unknown += 1
                            continue
                        # Min-only: keep candidate so one-trade wallets are not
                        # dropped when unique lookup temporarily fails.
                        unknown += 1
                        survivors.append(row)
                        continue
                    if _passes(
                        float(row.tokens_traded_7d),
                        req.min_tokens_traded_7d,
                        req.max_tokens_traded_7d,
                    ):
                        # Max filter: BS undercount (Relay / no inbound Transfer)
                        # + FU floor=1 (current token only) falsely PASSes
                        # early-buyer. WOOF: BS=0 → FU=1 while GMGN had ~60.
                        # Cross-check GMGN whenever a low/unknown BS count would
                        # admit the wallet.
                        if too_many is not None and (
                            raw_n is None or int(raw_n) <= int(too_many)
                        ):
                            gmgn_n = await _gmgn_unique_buys_in_window(
                                row.wallet, lookback_hours=lookback_h
                            )
                            if gmgn_n is not None:
                                merged = max(
                                    int(row.tokens_traded_7d), int(gmgn_n)
                                )
                                if merged != int(row.tokens_traded_7d):
                                    logger.info(
                                        "unique max-guard: BS=%s FU=%s "
                                        "GMGN=%s → %s wallet=%s",
                                        raw_n,
                                        floored,
                                        gmgn_n,
                                        merged,
                                        row.wallet[:12],
                                    )
                                row.tokens_traded_7d = merged
                                if not _passes(
                                    float(merged),
                                    req.min_tokens_traded_7d,
                                    req.max_tokens_traded_7d,
                                ):
                                    continue
                        survivors.append(row)
                        continue
                    # Fail-soft: exact Blockscout 0 can miss a real buy (indexer lag).
                    # Only rescue the min-floor case — never soft-pass overshoot > max.
                    if (
                        int(row.tokens_traded_7d) == 0
                        and raw_n is not None
                        and int(raw_n) == 0
                        and req.min_tokens_traded_7d is not None
                        and float(req.min_tokens_traded_7d) > 0
                    ):
                        gmgn_n = await _gmgn_unique_buys_in_window(
                            row.wallet, lookback_hours=lookback_h
                        )
                        if gmgn_n is not None and _passes(
                            float(gmgn_n),
                            req.min_tokens_traded_7d,
                            req.max_tokens_traded_7d,
                        ):
                            row.tokens_traded_7d = gmgn_n
                            soft_rescued += 1
                            survivors.append(row)
                            logger.info(
                                "unique fail-soft: BS=0 GMGN=%s wallet=%s",
                                gmgn_n,
                                row.wallet[:12],
                            )
                            continue
                if time.time() >= wall_deadline:
                    wall_hit = True
                    break

            # Wall left a tail never fetched.
            # Max set → fail-closed (unexamined stay out; young requeue via meta).
            # Min-only → fail-open (same as historical BS-None path).
            if len(survivors) < pass_cap and idx < len(kept):
                wall_hit = True
                if too_many is None:
                    for row in kept[idx:]:
                        if len(survivors) >= pass_cap:
                            break
                        unknown += 1
                        survivors.append(row)
                else:
                    unknown += max(0, len(kept) - idx)

        skipped = max(0, before - len(survivors))
        kept = survivors
        soft_note = f", soft_rescue={soft_rescued}" if soft_rescued else ""
        wall_note = ", wall_stop" if wall_hit else ""
        open_note = f", open/unknown={unknown}" if unknown else ""
        if out_meta is not None:
            out_meta["unique_partial"] = bool(wall_hit and examined < before)
            out_meta["unique_skipped"] = int(skipped)
            out_meta["unique_wall"] = bool(wall_hit)
        if wall_hit or examined < before:
            logger.info(
                "unique capped: kept=%s examined=%s (cap=%s wall=%s open=%s) token=%s",
                len(kept),
                examined,
                pass_cap,
                wall_hit,
                unknown,
                token[:12],
            )
        await prog(
            "filter",
            f"Tokens 7d: {before} → {len(kept)} kept"
            + open_note
            + soft_note
            + wall_note,
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
