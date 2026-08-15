"""Pool discovery via DexScreener (fast path) + optional on-chain factories."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from .blockscout import fetch_address_info
from .chain import RpcClient, checksum, http_client
from .constants import (
    DEXSCREENER_API,
    QUOTE_TOKENS,
    UNI_V4_POOL_MANAGER,
    USDG,
    V4_INITIALIZE_TOPIC,
    V3_FEE_TIERS,
    WETH,
    ZERO,
)
from .models import PoolInfo

logger = logging.getLogger(__name__)

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_BYTES32_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")


def _is_address(value: str | None) -> bool:
    return bool(value and _ADDR_RE.match(value))


def _is_bytes32(value: str | None) -> bool:
    return bool(value and _BYTES32_RE.match(value))


def _norm_hex(value: str) -> str:
    return value if value.startswith("0x") else f"0x{value}"


def _topic_address(value: str) -> str:
    raw = (value or ZERO).lower().removeprefix("0x")
    return "0x" + raw.zfill(64)[-64:]


def _address_from_topic(value: Any) -> str:
    raw = value.hex() if isinstance(value, (bytes, bytearray)) else str(value)
    raw = raw.removeprefix("0x")
    addr = "0x" + raw[-40:]
    return ZERO if int(addr, 16) == 0 else checksum(addr)


def _rpc_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 0)


async def discover_v4_pools_onchain(
    rpc: RpcClient,
    token: str,
    *,
    lookback_blocks: int = 50_000,
) -> list[PoolInfo]:
    """Find recent V4 pools by indexed Initialize currencies, no indexer.

    ``Initialize`` indexes both currencies, so two address-filtered getLogs
    calls cover token-as-currency0 and token-as-currency1. This is primarily a
    cold-index fallback for brand-new pools; the receipt path in replay is even
    faster when the buy initializes the pool in the same transaction.
    """
    token = checksum(token)
    tip = int(await rpc.block_number())
    start = max(1, tip - max(1, int(lookback_blocks)))
    topic = _topic_address(token)
    by_currency0, by_currency1 = await asyncio.gather(
        rpc.get_logs_chunked(
            address=UNI_V4_POOL_MANAGER,
            topics=[V4_INITIALIZE_TOPIC, None, topic],
            from_block=start,
            to_block=tip,
            chunk_size=50_000,
            parallel=2,
        ),
        rpc.get_logs_chunked(
            address=UNI_V4_POOL_MANAGER,
            topics=[V4_INITIALIZE_TOPIC, None, None, topic],
            from_block=start,
            to_block=tip,
            chunk_size=50_000,
            parallel=2,
        ),
    )
    out: list[PoolInfo] = []
    seen: set[str] = set()
    for log in [*by_currency0, *by_currency1]:
        topics = list(log.get("topics") or [])
        if len(topics) < 4:
            continue
        pool_id = _norm_hex(
            topics[1].hex()
            if isinstance(topics[1], (bytes, bytearray))
            else str(topics[1])
        ).lower()
        if pool_id in seen:
            continue
        currency0 = _address_from_topic(topics[2])
        currency1 = _address_from_topic(topics[3])
        if token.lower() not in (currency0.lower(), currency1.lower()):
            continue
        quote = currency1 if currency0.lower() == token.lower() else currency0
        q_meta = QUOTE_TOKENS.get(quote.lower(), {})
        if quote.lower() not in QUOTE_TOKENS:
            continue
        out.append(
            PoolInfo(
                address=checksum(UNI_V4_POOL_MANAGER),
                dex="uniswap_v4",
                quote=quote,
                quote_symbol=str(q_meta.get("symbol") or "?"),
                token0=currency0,
                token1=currency1,
                pool_id=pool_id,
                created_block=_rpc_int(log.get("blockNumber") or 0),
            )
        )
        seen.add(pool_id)
    return out


async def fetch_dexscreener_pairs(token: str) -> list[dict[str, Any]]:
    url = f"{DEXSCREENER_API}/latest/dex/tokens/{token}"
    resp = await http_client().get(url)
    if resp.status_code != 200:
        logger.warning("DexScreener %s: %s", resp.status_code, resp.text[:200])
        return []
    pairs = resp.json().get("pairs") or []
    rh = [p for p in pairs if str(p.get("chainId", "")).lower() in ("robinhood", "4663")]
    return rh or pairs


def _quote_rank(addr: str) -> int:
    a = addr.lower()
    if a == USDG.lower():
        return 0
    if a in (WETH.lower(), ZERO.lower()):
        return 1
    if a in QUOTE_TOKENS:
        return 2
    return 99


def _currency_order(a: str, b: str) -> tuple[str, str]:
    aa, bb = a.lower(), b.lower()
    if int(aa, 16) < int(bb, 16):
        return checksum(a) if _is_address(a) else a, checksum(b) if _is_address(b) else b
    return checksum(b) if _is_address(b) else b, checksum(a) if _is_address(a) else a


def _pools_from_dexscreener(token: str, pairs: list[dict[str, Any]]) -> list[PoolInfo]:
    pools: list[PoolInfo] = []
    seen: set[str] = set()
    for p in pairs:
        pair_raw = _norm_hex(str(p.get("pairAddress") or ""))
        base = (p.get("baseToken") or {}).get("address") or ""
        quote = (p.get("quoteToken") or {}).get("address") or ""
        if not base:
            continue
        addrs = {base.lower(), (quote or ZERO).lower()}
        if token.lower() not in addrs:
            continue

        labels = [str(x).lower() for x in (p.get("labels") or [])]
        dex_id = str(p.get("dexId", "")).lower()
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
        created = p.get("pairCreatedAt")
        created_ms = int(created) if created else None

        is_v4 = "v4" in labels or "v4" in dex_id or _is_bytes32(pair_raw)
        is_v3 = (not is_v4) and ("v3" in labels or "clmm" in labels or "v3" in dex_id)
        is_v2 = (not is_v4 and not is_v3)

        quote_addr = quote if base.lower() == token.lower() else base
        if not quote_addr or quote_addr.lower() == token.lower():
            other = quote if quote else ZERO
            quote_addr = other if other.lower() != token.lower() else ZERO
        if not _is_address(quote_addr) and quote_addr.lower() != ZERO.lower():
            quote_addr = ZERO

        q_meta = QUOTE_TOKENS.get(quote_addr.lower(), {})
        q_symbol = (
            q_meta.get("symbol")
            or (p.get("quoteToken") or {}).get("symbol")
            or (p.get("baseToken") or {}).get("symbol")
            or "?"
        )

        if is_v4 and _is_bytes32(pair_raw):
            key = pair_raw.lower()
            if key in seen:
                continue
            currency_b = checksum(quote_addr) if _is_address(quote_addr) else ZERO
            if currency_b.lower() == ZERO.lower():
                currency_b = ZERO
            t0, t1 = _currency_order(token, currency_b)
            pools.append(
                PoolInfo(
                    address=checksum(UNI_V4_POOL_MANAGER),
                    dex="uniswap_v4",
                    quote=checksum(quote_addr) if _is_address(quote_addr) else ZERO,
                    quote_symbol="ETH" if quote_addr.lower() == ZERO.lower() else q_symbol,
                    token0=t0 if _is_address(t0) else ZERO,
                    token1=t1 if _is_address(t1) else ZERO,
                    liquidity_usd=liq,
                    pool_id=pair_raw.lower(),
                    pair_created_at_ms=created_ms,
                )
            )
            seen.add(key)
            continue

        if not _is_address(pair_raw):
            continue
        pair_addr = checksum(pair_raw)
        if pair_addr.lower() in seen:
            continue
        dex = "uniswap_v3" if is_v3 else ("uniswap_v2" if is_v2 else "uniswap_v2")
        pools.append(
            PoolInfo(
                address=pair_addr,
                dex=dex,
                quote=checksum(quote_addr) if _is_address(quote_addr) else ZERO,
                quote_symbol=q_symbol,
                token0=checksum(base) if _is_address(base) else token,
                token1=checksum(quote_addr) if _is_address(quote_addr) else ZERO,
                liquidity_usd=liq,
                pair_created_at_ms=created_ms,
            )
        )
        seen.add(pair_addr.lower())
    return pools


async def _enrich_v2v3(rpc: RpcClient, pool: PoolInfo, token: str) -> PoolInfo | None:
    try:
        if pool.dex == "uniswap_v2":
            c = rpc.v2_pair(pool.address)
            t0, t1 = await asyncio.gather(
                rpc._call(lambda: c.functions.token0().call()),
                rpc._call(lambda: c.functions.token1().call()),
            )
        else:
            c = rpc.v3_pool(pool.address)
            t0, t1, fee = await asyncio.gather(
                rpc._call(lambda: c.functions.token0().call()),
                rpc._call(lambda: c.functions.token1().call()),
                rpc._call(lambda: c.functions.fee().call()),
            )
            pool.fee = int(fee)
        t0, t1 = checksum(t0), checksum(t1)
        if token.lower() not in (t0.lower(), t1.lower()):
            return None
        quote_addr = t1 if t0.lower() == token.lower() else t0
        q_meta = QUOTE_TOKENS.get(quote_addr.lower(), {})
        pool.token0 = t0
        pool.token1 = t1
        pool.quote = quote_addr
        pool.quote_symbol = q_meta.get("symbol") or pool.quote_symbol
        return pool
    except Exception as exc:  # noqa: BLE001
        logger.debug("Enrich %s failed: %s", pool.address, exc)
        return None


async def discover_pools(rpc: RpcClient, token: str, *, deep: bool = False) -> list[PoolInfo]:
    token = checksum(token)
    pairs = await fetch_dexscreener_pairs(token)
    pools = _pools_from_dexscreener(token, pairs)

    # Fast path: DexScreener already has a liquid trading pool — only enrich the winners
    liquid = [p for p in pools if p.liquidity_usd > 0 or p.dex == "uniswap_v4"]
    if liquid and not deep:
        # Enrich top V2/V3 candidates in parallel (max 3)
        to_enrich = [p for p in liquid if p.dex in ("uniswap_v2", "uniswap_v3")][:3]
        if to_enrich:
            enriched = await asyncio.gather(*[_enrich_v2v3(rpc, p, token) for p in to_enrich])
            keep_v4 = [p for p in liquid if p.dex == "uniswap_v4"]
            pools = [p for p in enriched if p] + keep_v4
        else:
            pools = liquid
    elif deep or not pools:
        # Slow fallback: factory lookups
        seen = { (p.pool_id or p.address).lower() for p in pools }
        tasks = []
        for quote in (USDG, WETH):
            tasks.append(("v2", quote, None))
            for fee in V3_FEE_TIERS:
                tasks.append(("v3", quote, fee))

        async def lookup(kind: str, quote: str, fee: int | None):
            try:
                if kind == "v2":
                    pair = await rpc.get_v2_pair(token, quote)
                    if pair and pair.lower() not in seen:
                        return PoolInfo(
                            address=pair,
                            dex="uniswap_v2",
                            quote=checksum(quote),
                            quote_symbol=QUOTE_TOKENS[quote.lower()]["symbol"],
                            token0=token,
                            token1=checksum(quote),
                        )
                else:
                    pool = await rpc.get_v3_pool(token, quote, fee or 3000)
                    if pool and pool.lower() not in seen:
                        return PoolInfo(
                            address=pool,
                            dex="uniswap_v3",
                            quote=checksum(quote),
                            quote_symbol=QUOTE_TOKENS[quote.lower()]["symbol"],
                            token0=token,
                            token1=checksum(quote),
                            fee=fee,
                        )
            except Exception:  # noqa: BLE001
                return None
            return None

        async def lookup_v4() -> list[PoolInfo]:
            try:
                return await discover_v4_pools_onchain(rpc, token)
            except Exception as exc:  # noqa: BLE001
                logger.debug("V4 Initialize lookup failed for %s: %s", token, exc)
                return []

        found_raw, v4_found = await asyncio.gather(
            asyncio.gather(*[lookup(k, q, f) for k, q, f in tasks]),
            lookup_v4(),
        )
        found = list(found_raw)
        for p in found:
            if p:
                pools.append(p)
                seen.add(p.address.lower())
        for p in v4_found:
            key = (p.pool_id or p.address).lower()
            if key not in seen:
                pools.append(p)
                seen.add(key)
        to_enrich = [p for p in pools if p.dex in ("uniswap_v2", "uniswap_v3")]
        enriched = await asyncio.gather(*[_enrich_v2v3(rpc, p, token) for p in to_enrich])
        v4 = [p for p in pools if p.dex == "uniswap_v4"]
        pools = [p for p in enriched if p] + v4

    # Liquidity first. Preferring USDG/WETH *before* liq picked dust V4 USDG
    # pools over the real V3 WETH market (NASDANQ / axiomTrade early buys → 0).
    pools.sort(
        key=lambda p: (
            -p.liquidity_usd,
            _quote_rank(p.quote),
            {"uniswap_v3": 0, "uniswap_v2": 1, "uniswap_v4": 2}.get(p.dex, 9),
        )
    )
    return pools


async def pick_best_pool(rpc: RpcClient, token: str) -> PoolInfo | None:
    pools = await discover_pools(rpc, token, deep=False)
    if not pools:
        pools = await discover_pools(rpc, token, deep=True)
    if not pools:
        return None
    with_liq = [p for p in pools if p.liquidity_usd > 0]
    candidates = with_liq or pools
    # Quote preference only among pools near the deepest book — never let a
    # $400 USDG V4 beat a $45k WETH V3 just because USDG ranks higher.
    best_liq = max(p.liquidity_usd for p in candidates)
    floor = best_liq * 0.5 if best_liq > 0 else 0.0
    competitive = [p for p in candidates if p.liquidity_usd >= floor] or candidates
    preferred = [p for p in competitive if p.quote.lower() in QUOTE_TOKENS]
    return (preferred or competitive)[0]


async def estimate_start_block(rpc: RpcClient, pool: PoolInfo) -> int:
    latest = await rpc.block_number()
    if pool.pair_created_at_ms:
        try:
            block = await rpc._call(lambda: rpc.w3.eth.get_block(latest))
            latest_ts = int(block["timestamp"])
            created_ts = pool.pair_created_at_ms // 1000
            elapsed = max(0, latest_ts - created_ts)
            # ~10 blocks/sec on Robinhood Chain; small cushion
            blocks_ago = int(elapsed * 10) + 2_000
            return max(1, latest - blocks_ago)
        except Exception as exc:  # noqa: BLE001
            logger.debug("timestamp estimate failed: %s", exc)

    lookup = pool.address if pool.dex != "uniswap_v4" else (pool.pool_id or pool.address)
    if pool.dex != "uniswap_v4" and _is_address(pool.address):
        info = await fetch_address_info(pool.address)
        if info:
            for key in ("creation_block_number",):
                val = info.get(key)
                if val is not None:
                    try:
                        return max(1, int(val) - 2)
                    except (TypeError, ValueError):
                        pass
            creation = info.get("creation_tx_hash") or info.get("creation_transaction_hash")
            if creation:
                try:
                    tx = await rpc._call(lambda: rpc.w3.eth.get_transaction_receipt(creation))
                    return max(1, int(tx["blockNumber"]) - 2)
                except Exception:  # noqa: BLE001
                    pass

    del lookup
    # Blind lookback covers watch max_pair_age (~24h @ 10 blk/s) when DS/BS
    # creation metadata is missing — do not shrink below ~24h of blocks.
    return max(1, latest - 300_000)


async def find_pair_created_block(rpc: RpcClient, pair: str, token: str) -> int | None:
    del token
    return await estimate_start_block(
        rpc,
        PoolInfo(
            address=pair if _is_address(pair) else UNI_V4_POOL_MANAGER,
            dex="uniswap_v4" if _is_bytes32(pair) else "uniswap_v3",
            quote=WETH,
            quote_symbol="WETH",
            token0=ZERO,
            token1=ZERO,
            pool_id=pair if _is_bytes32(pair) else None,
        ),
    )
