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
from .buy_gate import is_wallet_initiated_buy
from .chain import RpcClient, checksum, topic_address
from .config import settings
from .constants import BLOCKS_PER_SECOND, QUOTE_TOKENS, TRANSFER_TOPIC
from .models import BuyerRow, ParseRequest

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]

_MAX_TRANSFER_PAGES = 12
_BALANCE_CHUNK_PARALLEL = 2
_MAX_METRIC_ATTEMPTS = 8

_BALANCE_TTL = 120.0
_balance_cache: dict[str, tuple[float, float]] = {}

_TOKENS7D_TTL = 600.0
_TOKENS7D_CACHE_VER = "v7"  # fail-open on unknown + sender retries / quote-spend
_tokens7d_cache: dict[str, tuple[int, float]] = {}

_HOLDING_TTL = 180.0
_hold_cache: dict[tuple[str, str], tuple[int, int | None, float]] = {}

_CACHE_MAX = 50_000
_CACHE_PRUNE_AGE = 3600.0


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
                if not addr or addr in QUOTE_TOKENS or addr in tokens:
                    continue
                if not await is_wallet_initiated_buy(item, wallet_l):
                    continue
                tokens.add(addr)
                if enough is not None and too_many is None and len(tokens) >= enough:
                    return len(tokens), False
                if too_many is not None and len(tokens) > too_many:
                    return len(tokens), False
            next_params = data.get("next_page_params")
            if reached_cutoff or not next_params or not items:
                break
            params = dict(next_params)
            if "filter" not in params:
                params["filter"] = "to"
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
    cache_prefix = f"{_TOKENS7D_CACHE_VER}:h{int(hours)}:"
    out: dict[str, int | None] = {}
    misses: list[str] = []
    for w in dict.fromkeys(w.lower() for w in wallets):
        cached = _tokens7d_cache.get(f"{cache_prefix}{w}")
        if cached is not None and now - cached[1] < _TOKENS7D_TTL:
            out[w] = cached[0]
        else:
            out[w] = None
            misses.append(w)

    if not misses:
        return out

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    done = 0
    total = len(misses)
    lock = asyncio.Lock()
    label = f"{hours:g}h"

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
                    _tokens7d_cache[f"{cache_prefix}{wallet}"] = (
                        count, time.time()
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
        from .models import tokens_unique_period_hours

        lookback_h = tokens_unique_period_hours(
            getattr(req, "tokens_unique_period", None)
        )
        period_label = getattr(
            getattr(req, "tokens_unique_period", None),
            "value",
            None,
        ) or f"{lookback_h:g}h"
        await prog(
            "filter",
            f"Tokens {period_label}: counting for {before} wallets…",
            0.92,
        )
        tokens_7d = await batch_tokens_traded_7d(
            [b.wallet for b in kept],
            on_progress=on_progress,
            enough=enough,
            too_many=too_many,
            lookback_hours=lookback_h,
        )
        survivors = []
        unknown = 0
        for row in kept:
            row.tokens_traded_7d = tokens_7d.get(row.wallet.lower())
            if row.tokens_traded_7d is None:
                # Blockscout flake: keep candidate (fail-open) so one-trade wallets
                # are not dropped when unique lookup temporarily fails.
                unknown += 1
                survivors.append(row)
                continue
            if _passes(
                float(row.tokens_traded_7d),
                req.min_tokens_traded_7d,
                req.max_tokens_traded_7d,
            ):
                survivors.append(row)
        kept = survivors
        await prog(
            "filter",
            f"Tokens 7d: {before} → {len(kept)} kept"
            + (f" ({unknown} unknown kept)" if unknown else ""),
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
