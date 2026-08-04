"""GMGN OpenAPI portfolio client (wallet activity / buys).

Uses the same exist-auth flow as ``gmgn-cli``:
  GET https://openapi.gmgn.ai/v1/user/wallet_activity
  Headers: X-APIKEY
  Query: chain, wallet_address, timestamp, client_id, optional type/limit/cursor

Docs key from gmgn-cli README works for robinhood reads when no paid key is
set — use ``GMGN_API_KEY`` / ``GMGN_API_KEYS`` from https://gmgn.ai/ai for
production rate limits.

Follow-up *scan* only calls OpenAPI when at least one paid key is configured
(docs key is reserved for ``/api/followup/verify``). Paid keys rotate
round-robin with **per-key** pace + concurrency + 429 circuit so N keys
≈ N× effective RPS.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import settings

logger = logging.getLogger(__name__)

_HOST = "https://openapi.gmgn.ai"
# Public docs / CLI example key (exist-auth queries only).
_DOCS_API_KEY = "gmgn_solbscbaseethmonadtron"
_CHAIN = "robinhood"

# Per paid key: concurrency + start-to-start spacing.
# CRITICAL (Walter): accuracy > fake ≤15s. Bursting ~12 rps 429s the shared
# IP ceiling → empty "success" cycles. Prefer ~2.5 rps global, 90s cool,
# no sibling-key stampede on one wallet. RATE_LIMIT_BANNED → whole-pool cool.
# Adaptive interval still grows after 429 / decays on 200. HTTP 402 → disable.
_PAID_KEY_CONCURRENCY = 1
_PAID_MIN_INTERVAL = 0.80  # per-key floor (~1.25 rps/key)
_POOL_MIN_INTERVAL = 0.40  # ~2.5 rps global across the pool
_DOCS_CONCURRENCY = 1
_CIRCUIT_COOLDOWN_SEC = 90.0
_CIRCUIT_COOLDOWN_DOCS_SEC = 180.0
_QUOTA_DISABLE_SEC = 3600.0  # HTTP 402 / payment
_IP_BAN_COOL_SEC = 300.0  # RATE_LIMIT_BANNED
_SIBLING_SOFT_COOL_SEC = 12.0  # soft-cool other keys after soft 429
# Paid path: no cross-cycle activity cache — it produced false ~0.3s epochs
# that never saw new buys. Docs key still caches briefly.
_ACTIVITY_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_ACTIVITY_CACHE_TTL_SEC = 0.0
_ACTIVITY_CACHE_TTL_DOCS_SEC = 20.0
_ACTIVITY_CACHE_MAX = 400
_MAX_KEY_RETRIES = 0  # on 429: cool + back off, do not burn next key

_pool_pace_lock = asyncio.Lock()
_pool_next_ok = 0.0
_pool_interval = _POOL_MIN_INTERVAL  # adaptive: grows after 429, decays on 200

@dataclass
class _KeySlot:
    """One API key with its own limiter + circuit."""

    key: str
    sem: asyncio.Semaphore
    pace_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    next_ok: float = 0.0
    circuit_until: float = 0.0
    label: str = ""

    def available(self) -> bool:
        return time.time() >= self.circuit_until

    def mask(self) -> str:
        k = self.key
        if len(k) <= 12:
            return k[:4] + "…"
        return f"{k[:10]}…{k[-4:]}"


@dataclass(frozen=True)
class GmgnBuy:
    token: str
    symbol: str
    tx_hash: str
    timestamp: int
    cost_usd: float | None = None
    event_type: str = "buy"


@dataclass(frozen=True)
class ActivityFetchResult:
    """Outcome of one wallet_activity fetch (distinguishes empty vs 429)."""

    rows: list[dict[str, Any]]
    ok: bool = True
    rate_limited: bool = False


@dataclass(frozen=True)
class UniqueBuysResult:
    """Parsed unique buys plus fetch health for follow-up fallback decisions."""

    buys: list[GmgnBuy]
    ok: bool = True
    rate_limited: bool = False


_slots: list[_KeySlot] | None = None
_slots_sig: str | None = None
_rr_lock = asyncio.Lock()
_rr_idx = 0


def _mask_key(key: str) -> str:
    k = (key or "").strip()
    if len(k) <= 12:
        return k[:4] + "…"
    return f"{k[:10]}…{k[-4:]}"


def _collect_paid_keys() -> list[str]:
    """Gather unique paid keys from env (order preserved).

    Sources (all optional, merged):
      - ``GMGN_API_KEYS`` comma-separated
      - ``GMGN_API_KEY``, ``GMGN_API_KEY_2``, ``GMGN_API_KEY_3``
    """
    raw: list[str] = []
    csv = (getattr(settings, "gmgn_api_keys", None) or "").strip()
    if csv:
        raw.extend(p.strip() for p in csv.split(",") if p.strip())
    for attr in ("gmgn_api_key", "gmgn_api_key_2", "gmgn_api_key_3"):
        v = (getattr(settings, attr, None) or "").strip()
        if v:
            raw.append(v)
    seen: set[str] = set()
    out: list[str] = []
    for k in raw:
        if k == _DOCS_API_KEY or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _ensure_slots() -> list[_KeySlot]:
    """Rebuild key slots when env key set changes (e.g. after restart)."""
    global _slots, _slots_sig
    keys = _collect_paid_keys()
    sig = "|".join(keys) if keys else f"docs:{_DOCS_API_KEY}"
    if _slots is not None and _slots_sig == sig:
        return _slots
    if keys:
        _slots = [
            _KeySlot(
                key=k,
                sem=asyncio.Semaphore(_PAID_KEY_CONCURRENCY),
                label=_mask_key(k),
            )
            for k in keys
        ]
        logger.info(
            "GMGN OpenAPI key pool: %d key(s) [%s] (per-key ~%.0f req/s, conc=%d)",
            len(_slots),
            ", ".join(s.label for s in _slots),
            1.0 / _PAID_MIN_INTERVAL,
            _PAID_KEY_CONCURRENCY,
        )
    else:
        _slots = [
            _KeySlot(
                key=_DOCS_API_KEY,
                sem=asyncio.Semaphore(_DOCS_CONCURRENCY),
                label="docs",
            )
        ]
        logger.info("GMGN OpenAPI using docs key (rate-limited)")
    _slots_sig = sig
    return _slots


def gmgn_api_configured() -> bool:
    """True when at least one explicit paid key is set (not docs fallback)."""
    return bool(_collect_paid_keys())


def gmgn_key_pool_size() -> int:
    """Number of paid keys in the rotation pool (0 = docs only)."""
    return len(_collect_paid_keys())


def _activity_cache_ttl() -> float:
    return (
        _ACTIVITY_CACHE_TTL_SEC
        if gmgn_api_configured()
        else _ACTIVITY_CACHE_TTL_DOCS_SEC
    )


def gmgn_circuit_open() -> bool:
    """True only when *every* slot is in cooldown (no key can serve)."""
    slots = _ensure_slots()
    return all(not s.available() for s in slots)


async def _pick_slot() -> _KeySlot | None:
    """Round-robin among non-circuit keys; fall back to soonest-ready."""
    global _rr_idx
    slots = _ensure_slots()
    if not slots:
        return None
    async with _rr_lock:
        n = len(slots)
        for _ in range(n):
            slot = slots[_rr_idx % n]
            _rr_idx = (_rr_idx + 1) % n
            if slot.available():
                return slot
        # All cooling — pick the one that opens soonest (caller may still fail).
        return min(slots, key=lambda s: s.circuit_until)


async def _pace_slot(slot: _KeySlot) -> None:
    """Global pool + per-key start-to-start spacing (shared ceiling)."""
    global _pool_next_ok
    if not gmgn_api_configured():
        return
    async with _pool_pace_lock:
        now = time.time()
        wait = _pool_next_ok - now
        if wait > 0:
            await asyncio.sleep(wait)
        _pool_next_ok = time.time() + _pool_interval
    async with slot.pace_lock:
        now = time.time()
        wait = slot.next_ok - now
        if wait > 0:
            await asyncio.sleep(wait)
        slot.next_ok = time.time() + _PAID_MIN_INTERVAL


def _note_success() -> None:
    """Decay adaptive pool interval toward the steady target after OK replies."""
    global _pool_interval
    target = _POOL_MIN_INTERVAL
    if _pool_interval > target:
        _pool_interval = max(target, _pool_interval * 0.92)


def _trip_slot(
    slot: _KeySlot,
    status: int,
    *,
    retry_after: float | None = None,
    ban: bool = False,
) -> None:
    global _pool_next_ok, _pool_interval
    if status == 402:
        cool = _QUOTA_DISABLE_SEC
    elif ban:
        cool = _IP_BAN_COOL_SEC
    else:
        cool = (
            _CIRCUIT_COOLDOWN_SEC
            if gmgn_api_configured()
            else _CIRCUIT_COOLDOWN_DOCS_SEC
        )
        if retry_after is not None and retry_after > 0:
            cool = max(cool, min(float(retry_after), 180.0))
    until = time.time() + cool
    slot.circuit_until = until
    slot.next_ok = max(slot.next_ok, until)
    if ban:
        # IP banned — pause every key so we stop spinning empty cycles.
        _pool_interval = min(1.0, max(_pool_interval, 0.50))
        _pool_next_ok = max(_pool_next_ok, until)
        for other in _ensure_slots():
            other.circuit_until = max(other.circuit_until, until)
            other.next_ok = max(other.next_ok, until)
    elif status == 429:
        _pool_interval = min(
            1.0, max(_pool_interval * 1.5, _POOL_MIN_INTERVAL * 1.5)
        )
        _pool_next_ok = max(_pool_next_ok, time.time() + min(cool, 30.0))
        if gmgn_api_configured():
            sib_until = time.time() + _SIBLING_SOFT_COOL_SEC
            for other in _ensure_slots():
                if other is slot:
                    continue
                other.circuit_until = max(other.circuit_until, sib_until)
                other.next_ok = max(other.next_ok, sib_until)
    logger.warning(
        "GMGN OpenAPI circuit open %ss on key %s after HTTP %s "
        "(pool=%d, docs=%s, pool_iv=%.3fs, ban=%s)",
        int(cool),
        slot.label or slot.mask(),
        status,
        gmgn_key_pool_size(),
        not gmgn_api_configured(),
        _pool_interval,
        ban,
    )


def _cache_get(key: str) -> list[dict[str, Any]] | None:
    ttl = _activity_cache_ttl()
    if ttl <= 0:
        return None
    ent = _ACTIVITY_CACHE.get(key)
    if not ent:
        return None
    ts, rows = ent
    if time.time() - ts > ttl:
        _ACTIVITY_CACHE.pop(key, None)
        return None
    return rows


def _cache_put(key: str, rows: list[dict[str, Any]]) -> None:
    if _activity_cache_ttl() <= 0:
        return
    if len(_ACTIVITY_CACHE) >= _ACTIVITY_CACHE_MAX:
        oldest = sorted(_ACTIVITY_CACHE.items(), key=lambda kv: kv[1][0])
        for k, _ in oldest[: max(1, _ACTIVITY_CACHE_MAX // 5)]:
            _ACTIVITY_CACHE.pop(k, None)
    _ACTIVITY_CACHE[key] = (time.time(), rows)


async def wait_for_gmgn_capacity(*, timeout: float = 45.0) -> bool:
    """Block until at least one paid key is off circuit (or timeout)."""
    if not gmgn_api_configured():
        return True
    deadline = time.time() + max(0.0, timeout)
    while time.time() < deadline:
        if not gmgn_circuit_open():
            return True
        slots = _ensure_slots()
        soonest = min((s.circuit_until for s in slots), default=time.time())
        sleep_for = min(1.0, max(0.05, soonest - time.time()))
        await asyncio.sleep(sleep_for)
    return not gmgn_circuit_open()


async def fetch_wallet_activity_result(
    wallet: str,
    *,
    chain: str = _CHAIN,
    event_types: list[str] | None = None,
    limit: int = 50,
    max_pages: int = 20,
) -> ActivityFetchResult:
    """Paginated wallet activity with explicit rate-limit / error status."""
    wallet_l = wallet.strip().lower()
    if not wallet_l:
        return ActivityFetchResult(rows=[], ok=True, rate_limited=False)
    types = event_types if event_types is not None else ["buy"]
    type_key = ",".join(sorted(types))
    cache_key = f"{chain}:{wallet_l}:{type_key}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return ActivityFetchResult(rows=list(cached), ok=True, rate_limited=False)

    if gmgn_circuit_open():
        logger.info(
            "gmgn_empty_due_to_429 circuit_open wallet=%s pool=%d",
            wallet_l[:10],
            gmgn_key_pool_size(),
        )
        return ActivityFetchResult(rows=[], ok=False, rate_limited=True)

    # Docs: 1 page. Paid hot-path: caller caps pages (follow-up uses 1).
    pages = max(1, min(int(max_pages), 3 if gmgn_api_configured() else 1))
    tried: set[str] = set()
    attempts = 1 + (_MAX_KEY_RETRIES if gmgn_api_configured() else 0)
    saw_rate_limit = False
    hard_error = False

    for _attempt in range(attempts):
        if gmgn_circuit_open():
            saw_rate_limit = True
            break
        slot = await _pick_slot()
        if slot is None or not slot.available() or slot.key in tried:
            # Exhausted unique available keys.
            alt = next(
                (s for s in _ensure_slots() if s.available() and s.key not in tried),
                None,
            )
            slot = alt
        if slot is None or not slot.available():
            saw_rate_limit = saw_rate_limit or gmgn_circuit_open()
            break
        tried.add(slot.key)

        out: list[dict[str, Any]] = []
        cursor: str | None = None
        rate_limited = False

        async with slot.sem:
            if not slot.available():
                continue
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(20.0, connect=8.0),
                headers={
                    "X-APIKEY": slot.key,
                    "Content-Type": "application/json",
                    "User-Agent": "gnomode-gmgn-portfolio/1.0",
                },
            ) as client:
                for _ in range(pages):
                    if not slot.available():
                        break
                    await _pace_slot(slot)
                    if not slot.available():
                        break
                    params: list[tuple[str, str]] = [
                        ("chain", chain),
                        ("wallet_address", wallet_l),
                        ("limit", str(max(1, min(int(limit), 50)))),
                        ("timestamp", str(int(time.time()))),
                        ("client_id", str(uuid.uuid4())),
                    ]
                    for t in types:
                        params.append(("type", t))
                    if cursor:
                        params.append(("cursor", cursor))
                    try:
                        resp = await client.get(
                            f"{_HOST}/v1/user/wallet_activity", params=params
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("GMGN wallet_activity error: %s", exc)
                        hard_error = True
                        break
                    if resp.status_code in {429, 402, 502, 503}:
                        ra = resp.headers.get("Retry-After")
                        try:
                            cool = float(ra) if ra else None
                        except (TypeError, ValueError):
                            cool = None
                        ban = False
                        if resp.status_code == 429:
                            try:
                                body = resp.json()
                            except Exception:  # noqa: BLE001
                                body = {}
                            err = str(
                                (body or {}).get("error")
                                or (body or {}).get("message")
                                or ""
                            ).upper()
                            ban = "RATE_LIMIT_BANNED" in err or "BANNED" in err
                        _trip_slot(
                            slot,
                            resp.status_code,
                            retry_after=cool,
                            ban=ban,
                        )
                        rate_limited = True
                        saw_rate_limit = True
                        break
                    if resp.status_code != 200:
                        logger.warning(
                            "GMGN wallet_activity %s key=%s: %s",
                            resp.status_code,
                            slot.label,
                            resp.text[:160],
                        )
                        hard_error = True
                        break
                    _note_success()
                    try:
                        payload = resp.json()
                    except Exception:  # noqa: BLE001
                        hard_error = True
                        break
                    if (
                        not isinstance(payload, dict)
                        or int(payload.get("code") or 0) != 0
                    ):
                        logger.warning(
                            "GMGN wallet_activity bad payload: %s",
                            str(payload)[:160],
                        )
                        hard_error = True
                        break
                    data = payload.get("data") or {}
                    items = (
                        data.get("activities") if isinstance(data, dict) else None
                    )
                    if not isinstance(items, list):
                        items = []
                    out.extend(item for item in items if isinstance(item, dict))
                    nxt = (
                        str(data.get("next") or "")
                        if isinstance(data, dict)
                        else ""
                    )
                    if not nxt or not items:
                        break
                    cursor = nxt
                    await asyncio.sleep(0.25 if gmgn_api_configured() else 0.5)

        if out:
            _cache_put(cache_key, out)
            return ActivityFetchResult(rows=out, ok=True, rate_limited=False)
        if rate_limited:
            # Do not stampede sibling keys for the same wallet.
            break
        if hard_error:
            break
        # Empty 200 payload — authoritative empty, don't burn other keys.
        return ActivityFetchResult(rows=[], ok=True, rate_limited=False)

    if saw_rate_limit:
        logger.info(
            "gmgn_empty_due_to_429 wallet=%s tried=%d",
            wallet_l[:10],
            len(tried),
        )
        return ActivityFetchResult(rows=[], ok=False, rate_limited=True)
    return ActivityFetchResult(rows=[], ok=False, rate_limited=False)


async def fetch_wallet_activity(
    wallet: str,
    *,
    chain: str = _CHAIN,
    event_types: list[str] | None = None,
    limit: int = 50,
    max_pages: int = 20,
) -> list[dict[str, Any]]:
    """Paginated wallet activity rows (newest first from API)."""
    result = await fetch_wallet_activity_result(
        wallet,
        chain=chain,
        event_types=event_types,
        limit=limit,
        max_pages=max_pages,
    )
    return list(result.rows)


def _parse_buy(row: dict[str, Any]) -> GmgnBuy | None:
    et = str(row.get("event_type") or row.get("type") or "").lower()
    if et and et not in {"buy", "add"}:
        return None
    tok = row.get("token") or {}
    if isinstance(tok, dict):
        addr = str(tok.get("address") or tok.get("token_address") or "").lower()
        sym = str(tok.get("symbol") or "")
    else:
        addr = str(tok or "").lower()
        sym = ""
    if not addr:
        return None
    tx = str(row.get("tx_hash") or row.get("transaction_hash") or "")
    try:
        ts = int(row.get("timestamp") or 0)
    except (TypeError, ValueError):
        ts = 0
    cost: float | None
    try:
        raw = row.get("cost_usd")
        cost = float(raw) if raw is not None and raw != "" else None
    except (TypeError, ValueError):
        cost = None
    return GmgnBuy(
        token=addr,
        symbol=sym,
        tx_hash=tx,
        timestamp=ts,
        cost_usd=cost,
        event_type=et or "buy",
    )


def _unique_buys_from_rows(rows: list[dict[str, Any]]) -> list[GmgnBuy]:
    buys = [b for b in (_parse_buy(r) for r in rows) if b is not None]
    # API is newest-first; keep earliest buy per token
    by_tok: dict[str, GmgnBuy] = {}
    for b in buys:
        prev = by_tok.get(b.token)
        if prev is None or (b.timestamp and b.timestamp < prev.timestamp):
            by_tok[b.token] = b
    return sorted(by_tok.values(), key=lambda x: (x.timestamp or 0, x.token))


async def fetch_unique_buys(
    wallet: str,
    *,
    chain: str = _CHAIN,
    max_pages: int = 20,
) -> UniqueBuysResult:
    """Distinct buy tokens plus whether the OpenAPI fetch actually succeeded."""
    fetched = await fetch_wallet_activity_result(
        wallet, chain=chain, event_types=["buy"], max_pages=max_pages
    )
    return UniqueBuysResult(
        buys=_unique_buys_from_rows(fetched.rows),
        ok=fetched.ok,
        rate_limited=fetched.rate_limited,
    )


async def unique_buy_tokens(
    wallet: str,
    *,
    chain: str = _CHAIN,
    max_pages: int = 20,
) -> list[GmgnBuy]:
    """Distinct tokens with a buy, chronological (oldest → newest).

    First buy per token wins (matches follow-up «1 deal = 1 unique token»).
    """
    return (await fetch_unique_buys(wallet, chain=chain, max_pages=max_pages)).buys


async def compare_followup_to_gmgn(
    wallet: str,
    deals: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare follow-up DB deals to GMGN's post-seed unique-buy order.

    ``deals`` items need ``token``, ``deal_index``, optional ``token_symbol``.
    """
    gmgn = await unique_buy_tokens(wallet)
    seed = next(
        (d for d in deals if int(d.get("deal_index") or 0) == 1),
        None,
    )
    seed_token = str((seed or {}).get("token") or "").lower()
    seed_buy = next(
        (b for b in gmgn if b.token == seed_token and b.timestamp > 0),
        None,
    )
    post_seed = (
        [
            b
            for b in gmgn
            if b.token != seed_token and b.timestamp > seed_buy.timestamp
        ]
        if seed_buy is not None
        else []
    )
    gmgn_rank = {seed_token: 1} if seed_buy is not None else {}
    gmgn_rank.update({b.token: i for i, b in enumerate(post_seed, 2)})
    rows = []
    for d in deals:
        tok = str(d.get("token") or "").lower()
        db_idx = int(d.get("deal_index") or 0)
        g_idx = gmgn_rank.get(tok)
        rows.append(
            {
                "token": tok,
                "symbol": d.get("token_symbol") or "",
                "db_index": db_idx,
                "gmgn_index": g_idx,
                "match": g_idx == db_idx if g_idx is not None else False,
            }
        )
    missing = [
        {
            "gmgn_index": i,
            "token": b.token,
            "symbol": b.symbol,
            "timestamp": b.timestamp,
        }
        for i, b in enumerate(post_seed, 2)
        if b.token not in {str(d.get("token") or "").lower() for d in deals}
    ]
    return {
        "wallet": wallet.lower(),
        "gmgn_unique_buys": len(gmgn),
        "gmgn_post_seed_buys": len(post_seed),
        "seed_found_in_gmgn": seed_buy is not None,
        "db_deals": len(deals),
        "using_docs_api_key": not gmgn_api_configured(),
        "key_pool_size": gmgn_key_pool_size(),
        "circuit_open": gmgn_circuit_open(),
        "deals": rows,
        "gmgn_order": [
            {
                "index": i,
                "token": b.token,
                "symbol": b.symbol,
                "timestamp": b.timestamp,
                "tx_hash": b.tx_hash,
                "cost_usd": b.cost_usd,
            }
            for i, b in enumerate(gmgn, 1)
        ],
        "gmgn_post_seed_order": [
            {
                "index": i,
                "token": b.token,
                "symbol": b.symbol,
                "timestamp": b.timestamp,
                "tx_hash": b.tx_hash,
                "cost_usd": b.cost_usd,
            }
            for i, b in enumerate(post_seed, 2)
        ],
        "missing_from_db": missing,
    }
