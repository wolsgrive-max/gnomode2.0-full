"""Replay Uniswap pool events and collect early buyers (mcap < threshold)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import httpx

from .blockscout import fetch_token_info, iter_token_transfers
from .chain import RpcClient, decode_int256, decode_uint256, topic_address
from .config import settings
from .constants import (
    DEXSCREENER_API,
    KNOWN_ROUTERS,
    QUOTE_TOKENS,
    SYNC_TOPIC,
    TRANSFER_TOPIC,
    UNI_V4_POOL_MANAGER,
    UNIVERSAL_ROUTER,
    UNI_V2_ROUTER,
    UNI_V3_ROUTER,
    USDG,
    V2_SWAP_TOPIC,
    V3_SWAP_TOPIC,
    V4_INITIALIZE_TOPIC,
    V4_SWAP_TOPIC,
    WETH,
    ZERO,
)
from .models import BuyerRow, ParseRequest, PoolInfo, TokenParseResult
from .pools import estimate_start_block, pick_best_pool
from .security import honeypot_reason_for_token
from .wallet_metrics import enrich_and_filter_buyers

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]


@dataclass
class BuyEvent:
    wallet: str
    amount_tokens: float
    amount_usd: float
    mcap: float
    tx: str
    block: int


@dataclass
class WalletAgg:
    bought_tokens: float = 0.0
    bought_usd: float = 0.0
    mcap_at_first_buy: float = 0.0
    buys_count: int = 0
    first_tx: str = ""
    first_block: int = 0
    buys: list[BuyEvent] = field(default_factory=list)


_ETH_CACHE: dict[str, float] = {"price": 0.0, "ts": 0.0}


async def _eth_usd_price() -> float:
    """Spot ETH/USD via DexScreener, cached ~60s."""
    now = time.time()
    if _ETH_CACHE["price"] > 0 and now - _ETH_CACHE["ts"] < 60:
        return _ETH_CACHE["price"]
    try:
        from .chain import http_client
        url = f"{DEXSCREENER_API}/latest/dex/tokens/{WETH}"
        resp = await http_client().get(url)
        if resp.status_code != 200:
            return _ETH_CACHE["price"]
        pairs = resp.json().get("pairs") or []

        for p in pairs:
            if str(p.get("chainId", "")).lower() not in ("robinhood", "4663"):
                continue
            base = ((p.get("baseToken") or {}).get("address") or "").lower()
            quote = ((p.get("quoteToken") or {}).get("address") or "").lower()
            price = p.get("priceUsd")
            if not price:
                continue
            if base == WETH.lower() and quote == USDG.lower():
                _ETH_CACHE.update(price=float(price), ts=now)
                return float(price)
            if quote == WETH.lower() and base == USDG.lower():
                native = p.get("priceNative")
                val = (1.0 / float(native)) if native and float(native) > 0 else float(price)
                _ETH_CACHE.update(price=val, ts=now)
                return val

        for p in pairs:
            if str(p.get("chainId", "")).lower() not in ("robinhood", "4663"):
                continue
            quote = ((p.get("quoteToken") or {}).get("address") or "").lower()
            base = ((p.get("baseToken") or {}).get("address") or "").lower()
            if quote != WETH.lower() or base == WETH.lower():
                continue
            price_usd = p.get("priceUsd")
            price_native = p.get("priceNative")
            if price_usd and price_native and float(price_native) > 0:
                eth = float(price_usd) / float(price_native)
                if 100 < eth < 100_000:
                    _ETH_CACHE.update(price=eth, ts=now)
                    return eth

        r = await http_client().get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "ethereum", "vs_currencies": "usd"},
        )
        if r.status_code == 200:
            eth = float(r.json().get("ethereum", {}).get("usd") or 0)
            if eth > 0:
                _ETH_CACHE.update(price=eth, ts=now)
                return eth
    except Exception as exc:  # noqa: BLE001
        logger.warning("ETH price lookup failed: %s", exc)
    return _ETH_CACHE["price"]



def quote_to_usd(quote_addr: str, eth_usd: float) -> float:
    meta = QUOTE_TOKENS.get(quote_addr.lower())
    if not meta:
        return 0.0
    if meta["is_stable"]:
        return 1.0
    return eth_usd


def v2_price_token_in_quote(
    reserve0: int,
    reserve1: int,
    token_is_token0: bool,
    token_decimals: int,
    quote_decimals: int,
) -> float:
    if reserve0 == 0 or reserve1 == 0:
        return 0.0
    r0 = reserve0 / (10**token_decimals if token_is_token0 else 10**quote_decimals)
    r1 = reserve1 / (10**quote_decimals if token_is_token0 else 10**token_decimals)
    if token_is_token0:
        # token0 = token, token1 = quote → price = r1/r0
        return r1 / r0 if r0 else 0.0
    # token1 = token, token0 = quote → price = r0/r1
    return r0 / r1 if r1 else 0.0


def v2_reserves_before_swap(
    reserve0_after: int,
    reserve1_after: int,
    amount0_in: int,
    amount1_in: int,
    amount0_out: int,
    amount1_out: int,
) -> tuple[int, int] | None:
    """Recover pre-swap V2 reserves from post-Sync reserves + Swap amounts.

    Sync emits *post*-trade reserves. When we have no prior Sync state (first
    swap in a replay window / same-tx Sync), reverse the balance change.
    """
    r0 = int(reserve0_after) - int(amount0_in) + int(amount0_out)
    r1 = int(reserve1_after) - int(amount1_in) + int(amount1_out)
    if r0 <= 0 or r1 <= 0:
        return None
    return r0, r1


def entry_mcap_usd(
    *,
    quote_in_raw: int,
    token_out_raw: int,
    quote_decimals: int,
    token_decimals: int,
    quote_usd: float,
    supply_tokens: float,
    spot_mcap: float = 0.0,
) -> float:
    """Mcap at buy: prefer execution (fill) price × supply, else pre-swap spot.

    Fill matches GMGN-style entry on thin pools where a buy moves spot a lot.
    Post-swap spot alone overstates entry; pre-swap alone understates paid price.
    """
    if (
        quote_in_raw > 0
        and token_out_raw > 0
        and quote_usd > 0
        and supply_tokens > 0
        and token_decimals >= 0
        and quote_decimals >= 0
    ):
        tokens = token_out_raw / (10**token_decimals)
        usd = (quote_in_raw / (10**quote_decimals)) * quote_usd
        if tokens > 0:
            return usd / tokens * supply_tokens
    return float(spot_mcap) if spot_mcap and spot_mcap > 0 else 0.0


def _rpc_int(value: Any, default: int = 0) -> int:
    """Parse RPC int that may be hex string, int, or bytes."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int.from_bytes(value, "big")
    s = str(value).strip()
    if not s:
        return default
    if s.startswith(("0x", "0X")):
        return int(s, 16)
    return int(s)


def v3_price_from_sqrt(
    sqrt_price_x96: int,
    token_is_token0: bool,
    token_decimals: int,
    quote_decimals: int,
) -> float:
    """Return quote tokens per 1 base token, adjusted for decimals."""
    if sqrt_price_x96 == 0:
        return 0.0
    # Uniswap V3: token1/token0 = (sqrtPriceX96 / 2^96)^2 * 10^(dec0 - dec1)
    if token_is_token0:
        # quote is token1
        return ((sqrt_price_x96 / (2**96)) ** 2) * (10 ** (token_decimals - quote_decimals))
    # token is token1, quote is token0 → invert token1/token0
    t1_per_t0 = ((sqrt_price_x96 / (2**96)) ** 2) * (10 ** (quote_decimals - token_decimals))
    return (1.0 / t1_per_t0) if t1_per_t0 else 0.0


_Q96 = 2**96


def v3_sqrt_before_swap(
    sqrt_after: int,
    liquidity: int,
    amount0: int,
    amount1: int,
) -> int | None:
    """Recover pre-swap sqrtPriceX96 from a Uniswap V3 Swap event.

    Swap emits the *post*-trade sqrtPriceX96. For early-buyer mcap we need the
    price *before* the buy (GMGN-style entry mcap). Uses liquidity + amounts.
    """
    if sqrt_after <= 0 or liquidity <= 0:
        return None
    L = int(liquidity)
    a = int(sqrt_after)
    # zeroForOne: sell token0 for token1 (amount0>0, amount1<0) → sqrt decreases
    if amount0 > 0 and amount1 < 0:
        A = int(amount0)
        denom = L * _Q96 - A * a
        if denom <= 0:
            return None
        before = (L * _Q96 * a) // denom
        return int(before) if before > 0 else None
    # oneForZero: sell token1 for token0 (amount1>0, amount0<0) → sqrt increases
    if amount1 > 0 and amount0 < 0:
        A = int(amount1)
        before = a - (A * _Q96) // L
        return int(before) if before > 0 else None
    return None


def v3_mcap_from_swap(
    *,
    amount0: int,
    amount1: int,
    sqrt_after: int,
    liquidity: int,
    token_is_token0: bool,
    decimals: int,
    quote_decimals: int,
    quote_usd: float,
    supply_tokens: float,
    prev_sqrt: int | None = None,
) -> tuple[float, int]:
    """Return (mcap_usd at entry, sqrt used). Prefer pre-swap sqrt."""
    sqrt_before = v3_sqrt_before_swap(sqrt_after, liquidity, amount0, amount1)
    sqrt_used = sqrt_before or prev_sqrt or sqrt_after
    price = v3_price_from_sqrt(sqrt_used, token_is_token0, decimals, quote_decimals)
    mcap = price * quote_usd * supply_tokens if price > 0 else 0.0
    return mcap, int(sqrt_after)


def _log_data_hex(log: dict[str, Any]) -> str:
    data = log.get("data") or "0x"
    if isinstance(data, bytes):
        return "0x" + data.hex()
    return str(data)


def _log_addr(log: dict[str, Any]) -> str:
    addr = log.get("address") or ""
    if isinstance(addr, bytes):
        return "0x" + addr.hex()
    return str(addr).lower()


def _log_topics(log: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for t in log.get("topics") or []:
        if isinstance(t, bytes):
            out.append("0x" + t.hex())
        else:
            out.append(str(t).lower())
    return out


def _log_index(log: dict[str, Any]) -> int:
    return _rpc_int(log.get("logIndex"), 0)


def _log_block(log: dict[str, Any]) -> int:
    return _rpc_int(log.get("blockNumber"), 0)


@dataclass(frozen=True)
class EntryAtTx:
    mcap: float | None = None
    bought_usd: float | None = None


_ENTRY_AT_TX_CACHE: dict[str, tuple[float, EntryAtTx]] = {}
_ENTRY_AT_TX_TTL_SEC = 600.0
_ENTRY_AT_TX_EMPTY_TTL_SEC = 90.0


def _entry_cache_key(token: str, tx_hash: str) -> str:
    return f"{token.lower()}:{tx_hash.lower()}"


async def estimate_entry_at_tx(
    token: str,
    tx_hash: str,
    *,
    rpc: RpcClient | None = None,
) -> EntryAtTx:
    """Best-effort entry mcap + USD spent for a buy tx (fill / pre-swap)."""
    txh = (tx_hash or "").strip().lower()
    if not txh.startswith("0x") or len(txh) < 66:
        return EntryAtTx()
    cache_key = _entry_cache_key(token, txh)
    now = time.time()
    hit = _ENTRY_AT_TX_CACHE.get(cache_key)
    if hit:
        cached_at, cached_val = hit
        ttl = _ENTRY_AT_TX_TTL_SEC if cached_val.mcap is not None else _ENTRY_AT_TX_EMPTY_TTL_SEC
        if now - cached_at < ttl:
            return cached_val

    rpc = rpc or RpcClient()
    empty = EntryAtTx()

    def _cache(val: EntryAtTx) -> EntryAtTx:
        _ENTRY_AT_TX_CACHE[cache_key] = (now, val)
        if len(_ENTRY_AT_TX_CACHE) > 2048:
            ordered = sorted(_ENTRY_AT_TX_CACHE.items(), key=lambda kv: kv[1][0])
            for k, _ in ordered[: len(ordered) // 2]:
                _ENTRY_AT_TX_CACHE.pop(k, None)
        return val

    try:
        receipts = await rpc.batch_get_receipts([txh])
        receipt = receipts.get(txh)
        if not receipt:
            return _cache(empty)
        logs = receipt.get("logs") or []
        if not logs:
            return _cache(empty)

        pool = await pick_best_pool(rpc, token)
        if not pool:
            return _cache(empty)
        meta = await rpc.token_meta(token)
        decimals = int(meta.get("decimals") or 18)
        supply_raw = int(meta.get("total_supply_raw") or 0)
        supply_tokens = supply_raw / (10**decimals) if decimals >= 0 else 0.0
        if supply_tokens <= 0:
            return _cache(empty)
        eth_usd = await _eth_usd_price()
        quote_usd = quote_to_usd(pool.quote, eth_usd)
        if quote_usd <= 0:
            quote_usd = float(eth_usd or 0)
        if quote_usd <= 0:
            quote_usd = 2000.0
            logger.warning("estimate_entry_at_tx: ETH/USD missing, fallback $%s", quote_usd)
        quote_decimals = QUOTE_TOKENS.get(pool.quote.lower(), {}).get("decimals", 18)
        token_is_token0 = pool.token0.lower() == token.lower()
        pool_l = pool.address.lower()
        manager_l = UNI_V4_POOL_MANAGER.lower()

        reserve0 = reserve1 = 0
        pre_reserves: tuple[int, int] | None = None
        prev_sqrt: int | None = None
        best_mcap: float | None = None
        best_bought: float | None = None

        def _consider(mcap: float, quote_in_raw: int) -> None:
            nonlocal best_mcap, best_bought
            if mcap <= 0:
                return
            best_mcap = mcap
            if quote_in_raw > 0 and quote_usd > 0:
                best_bought = (quote_in_raw / (10**quote_decimals)) * quote_usd

        for log in sorted(logs, key=_log_index):
            addr = _log_addr(log)
            topics = _log_topics(log)
            if not topics:
                continue
            topic0 = topics[0].lower()
            data_hex = _log_data_hex(log)

            if pool.dex == "uniswap_v2" and addr == pool_l:
                if topic0 == SYNC_TOPIC.lower():
                    new0 = decode_uint256(data_hex, 0)
                    new1 = decode_uint256(data_hex, 1)
                    if reserve0 or reserve1:
                        pre_reserves = (reserve0, reserve1)
                    reserve0, reserve1 = new0, new1
                elif topic0 == V2_SWAP_TOPIC.lower():
                    a0_in = decode_uint256(data_hex, 0)
                    a1_in = decode_uint256(data_hex, 1)
                    a0_out = decode_uint256(data_hex, 2)
                    a1_out = decode_uint256(data_hex, 3)
                    r0, r1 = pre_reserves if pre_reserves else (reserve0, reserve1)
                    if not pre_reserves and (reserve0 or reserve1):
                        reversed_r = v2_reserves_before_swap(
                            reserve0, reserve1, a0_in, a1_in, a0_out, a1_out
                        )
                        if reversed_r:
                            r0, r1 = reversed_r
                    spot = 0.0
                    if r0 and r1:
                        price = v2_price_token_in_quote(
                            r0, r1, token_is_token0, decimals, quote_decimals
                        )
                        spot = price * quote_usd * supply_tokens
                    quote_in = a1_in if token_is_token0 else a0_in
                    token_out = a0_out if token_is_token0 else a1_out
                    mcap = entry_mcap_usd(
                        quote_in_raw=quote_in,
                        token_out_raw=token_out,
                        quote_decimals=quote_decimals,
                        token_decimals=decimals,
                        quote_usd=quote_usd,
                        supply_tokens=supply_tokens,
                        spot_mcap=spot,
                    )
                    _consider(mcap, quote_in)
                continue

            if pool.dex == "uniswap_v3" and addr == pool_l and topic0 == V3_SWAP_TOPIC.lower():
                amount0 = decode_int256(data_hex, 0)
                amount1 = decode_int256(data_hex, 1)
                sqrt_after = decode_uint256(data_hex, 2)
                liquidity = decode_uint256(data_hex, 3)
                spot, _ = v3_mcap_from_swap(
                    amount0=amount0,
                    amount1=amount1,
                    sqrt_after=sqrt_after,
                    liquidity=liquidity,
                    token_is_token0=token_is_token0,
                    decimals=decimals,
                    quote_decimals=quote_decimals,
                    quote_usd=quote_usd,
                    supply_tokens=supply_tokens,
                    prev_sqrt=prev_sqrt,
                )
                prev_sqrt = sqrt_after
                if token_is_token0:
                    quote_in = amount1 if amount1 > 0 else 0
                    token_out = -amount0 if amount0 < 0 else 0
                else:
                    quote_in = amount0 if amount0 > 0 else 0
                    token_out = -amount1 if amount1 < 0 else 0
                mcap = entry_mcap_usd(
                    quote_in_raw=quote_in,
                    token_out_raw=token_out,
                    quote_decimals=quote_decimals,
                    token_decimals=decimals,
                    quote_usd=quote_usd,
                    supply_tokens=supply_tokens,
                    spot_mcap=spot,
                )
                _consider(mcap, quote_in)
                continue

            if (
                pool.dex == "uniswap_v4"
                and addr == manager_l
                and topic0 == V4_SWAP_TOPIC.lower()
            ):
                amount0 = decode_int256(data_hex, 0)
                amount1 = decode_int256(data_hex, 1)
                sqrt_after = decode_uint256(data_hex, 2)
                liquidity = decode_uint256(data_hex, 3)
                spot, _ = v3_mcap_from_swap(
                    amount0=-amount0,
                    amount1=-amount1,
                    sqrt_after=sqrt_after,
                    liquidity=liquidity,
                    token_is_token0=token_is_token0,
                    decimals=decimals,
                    quote_decimals=quote_decimals,
                    quote_usd=quote_usd,
                    supply_tokens=supply_tokens,
                    prev_sqrt=prev_sqrt,
                )
                prev_sqrt = sqrt_after
                if token_is_token0:
                    quote_in = -amount1 if amount1 < 0 else 0
                    token_out = amount0 if amount0 > 0 else 0
                else:
                    quote_in = -amount0 if amount0 < 0 else 0
                    token_out = amount1 if amount1 > 0 else 0
                mcap = entry_mcap_usd(
                    quote_in_raw=quote_in,
                    token_out_raw=token_out,
                    quote_decimals=quote_decimals,
                    token_decimals=decimals,
                    quote_usd=quote_usd,
                    supply_tokens=supply_tokens,
                    spot_mcap=spot,
                )
                _consider(mcap, quote_in)
                continue

        result = EntryAtTx(
            mcap=best_mcap if best_mcap and best_mcap > 0 else None,
            bought_usd=best_bought if best_bought and best_bought > 0 else None,
        )
        return _cache(result)
    except Exception as exc:  # noqa: BLE001
        logger.warning("estimate_entry_at_tx failed %s: %s", txh[:12], exc)
        return _cache(empty)


async def estimate_mcap_at_tx(
    token: str,
    tx_hash: str,
    *,
    rpc: RpcClient | None = None,
) -> float | None:
    """Best-effort entry mcap for a buy tx (fill / pre-swap), else None."""
    return (await estimate_entry_at_tx(token, tx_hash, rpc=rpc)).mcap


def is_excluded(addr: str, pool: str, extra: set[str] | None = None) -> bool:
    a = addr.lower()
    if a in KNOWN_ROUTERS or a == pool.lower():
        return True
    if extra and a in extra:
        return True
    return False


async def parse_token(
    rpc: RpcClient,
    token: str,
    mcap_threshold: float,
    on_progress: ProgressCb | None = None,
    *,
    exclude_honeypots: bool = True,
    wallet_filters: ParseRequest | None = None,
) -> TokenParseResult:
    async def prog(stage: str, message: str, percent: float) -> None:
        if on_progress:
            await on_progress(stage, message, percent)

    token = rpc.w3.to_checksum_address(token)
    await prog("meta", f"Loading token {token[:10]}…", 0.02)

    if exclude_honeypots:
        await prog("security", "Checking honeypot (GMGN)…", 0.03)
        reason = await honeypot_reason_for_token(token)
        if reason:
            meta = await rpc.token_meta(token)
            return TokenParseResult(
                token=token,
                symbol=meta.get("symbol") or "",
                name=meta.get("name") or "",
                decimals=int(meta.get("decimals") or 18),
                total_supply=0.0,
                error=f"Honeypot skipped ({reason})",
                stats={"honeypot": True, "honeypot_reason": reason},
            )

    # Parallel: token meta + pool discovery + ETH price + tip block
    meta_task = asyncio.create_task(rpc.token_meta(token))
    pool_task = asyncio.create_task(pick_best_pool(rpc, token))
    eth_task = asyncio.create_task(_eth_usd_price())
    tip_task = asyncio.create_task(rpc.block_number())
    meta, pool, eth_usd, latest = await asyncio.gather(
        meta_task, pool_task, eth_task, tip_task
    )

    if not meta["symbol"]:
        bs = await fetch_token_info(token)
        if bs:
            meta["symbol"] = bs.get("symbol") or meta["symbol"]
            meta["name"] = bs.get("name") or meta["name"]
            try:
                meta["decimals"] = int(bs.get("decimals") or meta["decimals"])
            except (TypeError, ValueError):
                pass

    decimals = int(meta["decimals"])
    supply_raw = int(meta["total_supply_raw"])
    supply_tokens = supply_raw / (10**decimals) if decimals >= 0 else 0.0

    result = TokenParseResult(
        token=token,
        symbol=meta["symbol"],
        name=meta["name"],
        decimals=decimals,
        total_supply=supply_tokens,
    )

    await prog("pools", "Discovering pools…", 0.08)
    if not pool:
        result.error = "No Uniswap V2/V3 pool found for this token"
        return result
    result.pool = pool

    quote_usd = quote_to_usd(pool.quote, eth_usd)
    if quote_usd <= 0:
        if pool.quote.lower() in (WETH.lower(), ZERO.lower()):
            quote_usd = eth_usd if eth_usd > 0 else 2000.0
            logger.warning("Using ETH price $%s", quote_usd)
        else:
            quote_usd = 1.0

    quote_decimals = QUOTE_TOKENS.get(pool.quote.lower(), {}).get("decimals", 18)
    if pool.quote.lower() == ZERO.lower():
        quote_decimals = 18
    token_is_token0 = pool.token0.lower() == token.lower()

    await prog("logs", "Fetching pool events…", 0.12)
    start_block = await estimate_start_block(rpc, pool)
    await prog("logs", f"Scanning blocks {start_block}→{latest}…", 0.14)

    # Optional V4 Initialize refine (narrow window around estimate)
    if pool.dex == "uniswap_v4" and pool.pool_id:
        try:
            window_from = max(1, start_block - 50_000)
            init_logs = await rpc.get_logs_chunked(
                address=UNI_V4_POOL_MANAGER,
                topics=[V4_INITIALIZE_TOPIC, pool.pool_id],
                from_block=window_from,
                to_block=min(latest, start_block + 200_000),
                chunk_size=max(settings.log_chunk_size * 5, 20_000),
            )
            if init_logs:
                start_block = int(init_logs[0]["blockNumber"])
                await prog("logs", f"Pool initialized at {start_block}", 0.16)
        except Exception as exc:  # noqa: BLE001
            logger.info("V4 Initialize lookup: %s", exc)

    try:
        if pool.dex == "uniswap_v2":
            buyers = await _replay_v2(
                rpc,
                token=token,
                pool=pool,
                start_block=start_block,
                end_block=latest,
                decimals=decimals,
                quote_decimals=quote_decimals,
                token_is_token0=token_is_token0,
                supply_tokens=supply_tokens,
                quote_usd=quote_usd,
                mcap_threshold=mcap_threshold,
                on_progress=prog,
            )
        elif pool.dex == "uniswap_v4":
            buyers = await _replay_v4(
                rpc,
                token=token,
                pool=pool,
                start_block=start_block,
                end_block=latest,
                decimals=decimals,
                quote_decimals=quote_decimals,
                token_is_token0=token_is_token0,
                supply_tokens=supply_tokens,
                quote_usd=quote_usd,
                mcap_threshold=mcap_threshold,
                on_progress=prog,
            )
        else:
            buyers = await _replay_v3(
                rpc,
                token=token,
                pool=pool,
                start_block=start_block,
                end_block=latest,
                decimals=decimals,
                quote_decimals=quote_decimals,
                token_is_token0=token_is_token0,
                supply_tokens=supply_tokens,
                quote_usd=quote_usd,
                mcap_threshold=mcap_threshold,
                on_progress=prog,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("RPC replay failed, trying Blockscout fallback")
        await prog("fallback", f"RPC limited ({exc}); Blockscout fallback…", 0.4)
        buyers = await _fallback_blockscout(
            rpc,
            token=token,
            pool=pool,
            decimals=decimals,
            quote_decimals=quote_decimals,
            token_is_token0=token_is_token0,
            supply_tokens=supply_tokens,
            quote_usd=quote_usd,
            mcap_threshold=mcap_threshold,
            on_progress=prog,
        )

    for b in buyers:
        b.token_symbol = result.symbol

    buyers_found = len(buyers)
    await prog("replay", f"Early buyers under mcap: {buyers_found}", 0.85)
    if wallet_filters is not None and buyers:
        buyers = await enrich_and_filter_buyers(
            rpc,
            token=token,
            buyers=buyers,
            req=wallet_filters,
            start_block=start_block,
            end_block=latest,
            on_progress=prog,
        )
    elif not buyers:
        await prog("replay", "No early buyers found under mcap threshold", 0.9)

    result.buyers = buyers
    result.stats = {
        "pool": pool.address,
        "dex": pool.dex,
        "quote": pool.quote_symbol,
        "quote_usd": quote_usd,
        "start_block": start_block,
        "end_block": latest,
        "buyers": len(buyers),
        "buyers_before_wallet_filters": buyers_found,
        "eth_usd": eth_usd,
    }
    if buyers_found != len(buyers):
        await prog(
            "done",
            f"Done for token: {buyers_found} early → {len(buyers)} after filters",
            1.0,
        )
    else:
        await prog("done", f"Done for token: {len(buyers)} early buyers", 1.0)
    return result


async def _replay_v2(
    rpc: RpcClient,
    *,
    token: str,
    pool: PoolInfo,
    start_block: int,
    end_block: int,
    decimals: int,
    quote_decimals: int,
    token_is_token0: bool,
    supply_tokens: float,
    quote_usd: float,
    mcap_threshold: float,
    on_progress: ProgressCb,
) -> list[BuyerRow]:
    async def log_prog(frac: float, _a: int, _b: int) -> None:
        await on_progress("logs", "Fetching Sync/Swap/Transfer logs…", 0.12 + 0.55 * frac)

    # Fetch Sync, Swap, Transfer in parallel topic batches to reduce payload
    sync_logs, swap_logs, xfer_logs = await _gather_three(
        rpc.get_logs_chunked(
            address=pool.address,
            topics=[SYNC_TOPIC],
            from_block=start_block,
            to_block=end_block,
            on_progress=log_prog,
        ),
        rpc.get_logs_chunked(
            address=pool.address,
            topics=[V2_SWAP_TOPIC],
            from_block=start_block,
            to_block=end_block,
        ),
        rpc.get_logs_chunked(
            address=token,
            topics=[TRANSFER_TOPIC, "0x" + "0" * 24 + pool.address.lower().replace("0x", "")],
            from_block=start_block,
            to_block=end_block,
        ),
    )

    await on_progress("replay", "Replaying mcap timeline…", 0.72)

    # Map block+tx → reserves after Sync (Sync usually before Swap in same tx)
    # Build timeline of all events sorted by (block, logIndex)
    events: list[tuple[int, int, str, Any]] = []
    for log in sync_logs:
        events.append((_log_block(log), _log_index(log), "sync", log))
    for log in swap_logs:
        events.append((_log_block(log), _log_index(log), "swap", log))
    for log in xfer_logs:
        events.append((_log_block(log), _log_index(log), "xfer", log))
    events.sort(key=lambda x: (x[0], x[1]))

    # Index transfers from pool by tx hash
    xfers_by_tx: dict[str, list[Any]] = defaultdict(list)
    for log in xfer_logs:
        tx = log["transactionHash"]
        txh = tx.hex() if isinstance(tx, bytes) else str(tx)
        xfers_by_tx[txh.lower()].append(log)

    reserve0 = 0
    reserve1 = 0
    mcap = 0.0
    crossed = False
    aggs: dict[str, WalletAgg] = {}
    early_stop_block: int | None = None
    # Uniswap V2 emits Sync *before* Swap in the same tx, with *post*-trade reserves.
    # Stash pre-Sync reserves per tx so Swap mcap is entry (pre-buy), not post.
    pre_reserves_by_tx: dict[str, tuple[int, int]] = {}

    for block, _idx, kind, log in events:
        if kind == "sync":
            data = log["data"]
            new0 = decode_uint256(data, 0)
            new1 = decode_uint256(data, 1)
            tx = log["transactionHash"]
            txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
            if reserve0 or reserve1:
                pre_reserves_by_tx[txh] = (reserve0, reserve1)
            reserve0, reserve1 = new0, new1
            price = v2_price_token_in_quote(
                reserve0, reserve1, token_is_token0, decimals, quote_decimals
            )
            mcap = price * quote_usd * supply_tokens
            if mcap >= mcap_threshold and not crossed:
                crossed = True
                early_stop_block = block
            continue

        if kind != "swap":
            continue

        tx = log["transactionHash"]
        txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
        data = log["data"]
        amount0_in = decode_uint256(data, 0)
        amount1_in = decode_uint256(data, 1)
        amount0_out = decode_uint256(data, 2)
        amount1_out = decode_uint256(data, 3)

        r0, r1 = pre_reserves_by_tx.get(txh, (0, 0))
        if not (r0 and r1) and (reserve0 or reserve1):
            reversed_r = v2_reserves_before_swap(
                reserve0, reserve1, amount0_in, amount1_in, amount0_out, amount1_out
            )
            if reversed_r:
                r0, r1 = reversed_r
            else:
                r0, r1 = reserve0, reserve1
        elif not (r0 and r1):
            r0, r1 = reserve0, reserve1

        price = v2_price_token_in_quote(
            r0, r1, token_is_token0, decimals, quote_decimals
        )
        # Market level for threshold: pre-swap spot (not post-Sync).
        mcap_now = price * quote_usd * supply_tokens if r0 and r1 else mcap

        if r0 == 0 and r1 == 0:
            continue
        if mcap_now >= mcap_threshold:
            crossed = True
            continue

        topics = log["topics"]
        # topics[2] = `to` indexed
        to_addr = topic_address(topics[2]) if len(topics) > 2 else ""

        # Prefer Transfer from pool → real wallet
        buyer = None
        amount_raw = 0
        for xfer in xfers_by_tx.get(txh, []):
            xtopics = xfer["topics"]
            if len(xtopics) < 3:
                continue
            frm = topic_address(xtopics[1])
            to = topic_address(xtopics[2])
            if frm.lower() != pool.address.lower():
                continue
            if is_excluded(to, pool.address):
                # might be router — look for further hop? for v1 take swap `to` if EOA-like
                continue
            buyer = to
            amount_raw = decode_uint256(xfer["data"], 0)
            break

        if buyer is None:
            if is_excluded(to_addr, pool.address):
                continue
            amount_raw = amount0_out if token_is_token0 else amount1_out
            if amount_raw == 0:
                continue
            buyer = to_addr

        if amount_raw == 0:
            continue

        amount_tokens = amount_raw / (10**decimals)
        quote_in = amount1_in if token_is_token0 else amount0_in
        token_out = amount0_out if token_is_token0 else amount1_out
        amount_usd = (quote_in / (10**quote_decimals)) * quote_usd
        buy_mcap = entry_mcap_usd(
            quote_in_raw=quote_in,
            token_out_raw=token_out or amount_raw,
            quote_decimals=quote_decimals,
            token_decimals=decimals,
            quote_usd=quote_usd,
            supply_tokens=supply_tokens,
            spot_mcap=mcap_now,
        )

        _record_buy(
            aggs,
            wallet=buyer,
            amount_tokens=amount_tokens,
            amount_usd=amount_usd,
            mcap=buy_mcap,
            tx=txh,
            block=block,
        )

    # Optional early stop note
    _ = early_stop_block
    return _aggs_to_rows(aggs, token, "")


async def _gather_three(a, b, c):
    import asyncio

    return await asyncio.gather(a, b, c)


async def _resolve_buyers_batch(
    rpc: RpcClient,
    *,
    token: str,
    pool_or_manager: str,
    early_swaps: list[Any],
    xfers_by_tx: dict[str, list[Any]],
) -> dict[str, tuple[str, int]]:
    """Resolve EOA buyers for many swaps with batched getCode / receipts."""
    results: dict[str, tuple[str, int]] = {}
    need_receipt: list[str] = []
    candidates_by_tx: dict[str, list[tuple[int, str, int]]] = {}
    addrs_to_check: list[str] = []

    for log in early_swaps:
        tx = log["transactionHash"]
        txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
        cands: list[tuple[int, str, int]] = []
        for xfer in xfers_by_tx.get(txh, []):
            xt = xfer["topics"]
            if len(xt) < 3:
                continue
            frm = topic_address(xt[1])
            to = topic_address(xt[2])
            if frm.lower() != pool_or_manager.lower():
                continue
            if is_excluded(to, pool_or_manager):
                continue
            cands.append((int(xfer["logIndex"]), to, decode_uint256(xfer["data"], 0)))
            addrs_to_check.append(to)
        # swap recipient (V3 topics[2])
        if len(log.get("topics") or []) > 2:
            recip = topic_address(log["topics"][2])
            if not is_excluded(recip, pool_or_manager):
                addrs_to_check.append(recip)
                if not cands:
                    cands.append((10**9, recip, 0))
        candidates_by_tx[txh] = cands

    code_cache = await rpc.batch_is_eoa(addrs_to_check)

    for txh, cands in candidates_by_tx.items():
        eoa_hits = [(i, to, amt) for i, to, amt in cands if code_cache.get(to.lower(), False)]
        if eoa_hits:
            eoa_hits.sort(key=lambda x: x[0])
            _, buyer, amt = eoa_hits[-1]
            results[txh] = (buyer, amt)
        else:
            need_receipt.append(txh)

    if need_receipt:
        receipts = await rpc.batch_get_receipts(need_receipt)
        more_addrs: list[str] = []
        chains: dict[str, list[tuple[int, str, int]]] = {}
        token_l = token.lower()
        for txh in need_receipt:
            receipt = receipts.get(txh.lower())
            if not receipt:
                continue
            chain: list[tuple[int, str, int]] = []
            for lg in receipt.get("logs") or []:
                addr = lg.get("address") or ""
                addr_s = (addr if isinstance(addr, str) else "0x" + bytes(addr).hex()).lower()
                if not addr_s.startswith("0x"):
                    addr_s = "0x" + addr_s
                if addr_s != token_l:
                    continue
                topics = lg.get("topics") or []
                if len(topics) < 3:
                    continue
                t0 = topics[0] if isinstance(topics[0], str) else (
                    topics[0].hex() if hasattr(topics[0], "hex") else str(topics[0])
                )
                if "ddf252ad" not in t0.lower():
                    continue
                to = topic_address(topics[2])
                if is_excluded(to, pool_or_manager):
                    continue
                amt = decode_uint256(lg.get("data") or "0x0", 0)
                idx = int(lg.get("logIndex") or lg.get("log_index") or 0)
                # logIndex may be hex string from raw JSON-RPC
                if isinstance(lg.get("logIndex"), str):
                    idx = int(lg["logIndex"], 16)
                chain.append((idx, to, amt))
                more_addrs.append(to)
            chains[txh] = chain

        if more_addrs:
            code_cache.update(await rpc.batch_is_eoa(more_addrs, code_cache))

        still_need_tx_from: list[str] = []
        for txh in need_receipt:
            chain = chains.get(txh) or []
            eoa_hits = [(i, to, amt) for i, to, amt in chain if code_cache.get(to.lower(), False)]
            if eoa_hits:
                eoa_hits.sort(key=lambda x: x[0])
                _, buyer, amt = eoa_hits[-1]
                results[txh] = (buyer, amt)
            else:
                still_need_tx_from.append(txh)

        # Last resort: batch eth_getTransaction for tx.from
        if still_need_tx_from:
            calls = [("eth_getTransactionByHash", [h]) for h in still_need_tx_from]
            try:
                raws = await rpc._jsonrpc_batch(calls)
            except Exception:  # noqa: BLE001
                raws = [None] * len(still_need_tx_from)
            from_addrs = []
            parsed = []
            for h, raw in zip(still_need_tx_from, raws, strict=False):
                if not raw or not raw.get("from"):
                    continue
                frm = checksum(raw["from"])
                from_addrs.append(frm)
                parsed.append((h, frm))
            if from_addrs:
                code_cache.update(await rpc.batch_is_eoa(from_addrs, code_cache))
            for h, frm in parsed:
                if code_cache.get(frm.lower(), False) and not is_excluded(frm, pool_or_manager):
                    results[h] = (frm, 0)

    return results


async def _replay_v3(
    rpc: RpcClient,
    *,
    token: str,
    pool: PoolInfo,
    start_block: int,
    end_block: int,
    decimals: int,
    quote_decimals: int,
    token_is_token0: bool,
    supply_tokens: float,
    quote_usd: float,
    mcap_threshold: float,
    on_progress: ProgressCb,
) -> list[BuyerRow]:
    """Stream V3 swaps until mcap crosses threshold; record EOA buyers only."""
    # Start with a modest window: the RPC caps ~10k logs/query and times out on
    # very large ranges. Smaller windows avoid the slow Blockscout fallback.
    chunk = min(max(settings.log_chunk_size, 40_000), 50_000)
    early_swaps: list[Any] = []
    stop_block = start_block
    crossed = False
    cursor = start_block
    total = max(end_block - start_block + 1, 1)
    prev_sqrt: int | None = None

    while cursor <= end_block and not crossed:
        end = min(cursor + chunk - 1, end_block)
        await on_progress(
            "logs",
            f"V3 swaps {cursor}–{end}…",
            0.12 + 0.5 * ((cursor - start_block) / total),
        )
        try:
            part = await rpc.get_logs(
                address=pool.address,
                topics=[V3_SWAP_TOPIC],
                from_block=cursor,
                to_block=end,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if chunk > 2_000 and any(
                x in msg
                for x in ("limit", "range", "too large", "response", "timed out", "timeout", "query")
            ):
                chunk = max(chunk // 2, 2_000)
                continue
            raise

        part.sort(key=lambda lg: (_log_block(lg), _log_index(lg)))
        for log in part:
            data = log["data"]
            amount0 = decode_int256(data, 0)
            amount1 = decode_int256(data, 1)
            sqrt_price = decode_uint256(data, 2)
            liquidity = decode_uint256(data, 3)
            mcap_now, _ = v3_mcap_from_swap(
                amount0=amount0,
                amount1=amount1,
                sqrt_after=sqrt_price,
                liquidity=liquidity,
                token_is_token0=token_is_token0,
                decimals=decimals,
                quote_decimals=quote_decimals,
                quote_usd=quote_usd,
                supply_tokens=supply_tokens,
                prev_sqrt=prev_sqrt,
            )
            prev_sqrt = sqrt_price
            token_delta = amount0 if token_is_token0 else amount1
            # V3 pool delta: negative token ⇒ tokens left the pool ⇒ buy
            is_buy = token_delta < 0
            if not is_buy:
                if mcap_now >= mcap_threshold:
                    crossed = True
                    stop_block = _log_block(log)
                    break
                continue
            if mcap_now >= mcap_threshold:
                crossed = True
                stop_block = _log_block(log)
                break
            early_swaps.append(log)
            stop_block = _log_block(log)

        if crossed:
            break
        cursor = end + 1

    if not early_swaps:
        await on_progress("replay", "No buys under mcap threshold", 0.9)
        return []

    xfer_to = min(end_block, stop_block + 5)
    await on_progress("logs", f"Fetching transfers ≤ block {xfer_to}…", 0.7)
    xfer_logs = await rpc.get_logs_chunked(
        address=token,
        topics=[TRANSFER_TOPIC, "0x" + "0" * 24 + pool.address.lower().replace("0x", "")],
        from_block=start_block,
        to_block=xfer_to,
        chunk_size=chunk,
    )
    xfers_by_tx: dict[str, list[Any]] = defaultdict(list)
    for log in xfer_logs:
        tx = log["transactionHash"]
        txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
        xfers_by_tx[txh].append(log)

    await on_progress("replay", f"Resolving {len(early_swaps)} buyers…", 0.85)
    buyers_map = await _resolve_buyers_batch(
        rpc,
        token=token,
        pool_or_manager=pool.address,
        early_swaps=early_swaps,
        xfers_by_tx=xfers_by_tx,
    )
    aggs: dict[str, WalletAgg] = {}
    prev_sqrt: int | None = None

    for log in early_swaps:
        data = log["data"]
        amount0 = decode_int256(data, 0)
        amount1 = decode_int256(data, 1)
        sqrt_price = decode_uint256(data, 2)
        liquidity = decode_uint256(data, 3)
        spot_mcap, _ = v3_mcap_from_swap(
            amount0=amount0,
            amount1=amount1,
            sqrt_after=sqrt_price,
            liquidity=liquidity,
            token_is_token0=token_is_token0,
            decimals=decimals,
            quote_decimals=quote_decimals,
            quote_usd=quote_usd,
            supply_tokens=supply_tokens,
            prev_sqrt=prev_sqrt,
        )
        prev_sqrt = sqrt_price
        # Post-swap spot only for USD fill estimate when quote_in missing.
        price_post = v3_price_from_sqrt(sqrt_price, token_is_token0, decimals, quote_decimals)
        if token_is_token0:
            token_delta, quote_delta = amount0, amount1
        else:
            token_delta, quote_delta = amount1, amount0
        amount_raw = abs(token_delta)
        quote_in = quote_delta if quote_delta > 0 else 0
        token_out = -token_delta if token_delta < 0 else 0
        block = _log_block(log)
        tx = log["transactionHash"]
        txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
        resolved = buyers_map.get(txh)
        if not resolved:
            continue
        buyer, amt = resolved
        if amt:
            amount_raw = amt
            token_out = amt

        amount_tokens = amount_raw / (10**decimals)
        amount_usd = (
            (quote_in / (10**quote_decimals)) * quote_usd
            if quote_in
            else amount_tokens * price_post * quote_usd
        )
        buy_mcap = entry_mcap_usd(
            quote_in_raw=quote_in,
            token_out_raw=token_out or amount_raw,
            quote_decimals=quote_decimals,
            token_decimals=decimals,
            quote_usd=quote_usd,
            supply_tokens=supply_tokens,
            spot_mcap=spot_mcap,
        )
        _record_buy(
            aggs,
            wallet=buyer,
            amount_tokens=amount_tokens,
            amount_usd=amount_usd,
            mcap=buy_mcap,
            tx=txh,
            block=block,
        )

    return _aggs_to_rows(aggs, token, "")


async def _replay_v4(
    rpc: RpcClient,
    *,
    token: str,
    pool: PoolInfo,
    start_block: int,
    end_block: int,
    decimals: int,
    quote_decimals: int,
    token_is_token0: bool,
    supply_tokens: float,
    quote_usd: float,
    mcap_threshold: float,
    on_progress: ProgressCb,
) -> list[BuyerRow]:
    if not pool.pool_id:
        return []

    pool_id = pool.pool_id if pool.pool_id.startswith("0x") else f"0x{pool.pool_id}"
    manager = checksum(UNI_V4_POOL_MANAGER)
    # Start with a modest window: the RPC caps ~10k logs/query and times out on
    # very large ranges. Smaller windows avoid the slow Blockscout fallback.
    chunk = min(max(settings.log_chunk_size, 40_000), 50_000)

    early_swaps: list[Any] = []
    stop_block = start_block
    crossed = False
    cursor = start_block
    total = max(end_block - start_block + 1, 1)
    prev_sqrt: int | None = None

    while cursor <= end_block and not crossed:
        end = min(cursor + chunk - 1, end_block)
        await on_progress(
            "logs",
            f"V4 swaps {cursor}–{end}…",
            0.12 + 0.5 * ((cursor - start_block) / total),
        )
        try:
            part = await rpc.get_logs(
                address=manager,
                topics=[V4_SWAP_TOPIC, pool_id],
                from_block=cursor,
                to_block=end,
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if chunk > 2_000 and any(
                x in msg
                for x in ("limit", "range", "too large", "response", "timed out", "timeout", "query")
            ):
                chunk = max(chunk // 2, 2_000)
                continue
            raise

        part.sort(key=lambda lg: (_log_block(lg), _log_index(lg)))
        for log in part:
            data = log["data"]
            # V4 amounts are swapper deltas (sign opposite of V3 pool deltas).
            amount0 = decode_int256(data, 0)
            amount1 = decode_int256(data, 1)
            sqrt_price = decode_uint256(data, 2)
            liquidity = decode_uint256(data, 3)
            pool_amount0, pool_amount1 = -amount0, -amount1
            mcap_now, _ = v3_mcap_from_swap(
                amount0=pool_amount0,
                amount1=pool_amount1,
                sqrt_after=sqrt_price,
                liquidity=liquidity,
                token_is_token0=token_is_token0,
                decimals=decimals,
                quote_decimals=quote_decimals,
                quote_usd=quote_usd,
                supply_tokens=supply_tokens,
                prev_sqrt=prev_sqrt,
            )
            prev_sqrt = sqrt_price
            # V4 Swap amounts on Robinhood match the *swapper* delta (verified via Transfer):
            # positive token amount ⇒ wallet received tokens ⇒ buy.
            token_delta = amount0 if token_is_token0 else amount1
            is_buy = token_delta > 0
            if not is_buy:
                if mcap_now >= mcap_threshold:
                    crossed = True
                    stop_block = _log_block(log)
                    break
                continue
            if mcap_now >= mcap_threshold:
                crossed = True
                stop_block = _log_block(log)
                break
            early_swaps.append(log)
            stop_block = _log_block(log)

        if crossed:
            break
        cursor = end + 1

    if not early_swaps:
        await on_progress("replay", "No buys under mcap threshold", 0.9)
        return []

    xfer_to = min(end_block, stop_block + 5)
    await on_progress("logs", f"Fetching transfers ≤ block {xfer_to}…", 0.7)
    router_froms = [
        manager,
        checksum(UNIVERSAL_ROUTER),
        checksum(UNI_V2_ROUTER),
        checksum(UNI_V3_ROUTER),
    ]

    import asyncio

    async def transfers_from(frm: str):
        return await rpc.get_logs_chunked(
            address=token,
            topics=[TRANSFER_TOPIC, "0x" + "0" * 24 + frm.lower().replace("0x", "")],
            from_block=start_block,
            to_block=xfer_to,
            chunk_size=chunk,
        )

    xfer_batches = await asyncio.gather(*[transfers_from(a) for a in router_froms])
    xfers_by_tx: dict[str, list[Any]] = defaultdict(list)
    for batch in xfer_batches:
        for log in batch:
            tx = log["transactionHash"]
            txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
            xfers_by_tx[txh].append(log)

    await on_progress("replay", f"Resolving {len(early_swaps)} buyers…", 0.85)
    buyers_map = await _resolve_buyers_batch(
        rpc,
        token=token,
        pool_or_manager=manager,
        early_swaps=early_swaps,
        xfers_by_tx=xfers_by_tx,
    )
    aggs: dict[str, WalletAgg] = {}
    prev_sqrt: int | None = None

    for log in early_swaps:
        data = log["data"]
        amount0 = decode_int256(data, 0)
        amount1 = decode_int256(data, 1)
        sqrt_price = decode_uint256(data, 2)
        liquidity = decode_uint256(data, 3)
        pool_amount0, pool_amount1 = -amount0, -amount1
        spot_mcap, _ = v3_mcap_from_swap(
            amount0=pool_amount0,
            amount1=pool_amount1,
            sqrt_after=sqrt_price,
            liquidity=liquidity,
            token_is_token0=token_is_token0,
            decimals=decimals,
            quote_decimals=quote_decimals,
            quote_usd=quote_usd,
            supply_tokens=supply_tokens,
            prev_sqrt=prev_sqrt,
        )
        prev_sqrt = sqrt_price
        price_post = v3_price_from_sqrt(sqrt_price, token_is_token0, decimals, quote_decimals)

        if token_is_token0:
            token_delta = amount0
            quote_delta = amount1
        else:
            token_delta = amount1
            quote_delta = amount0

        amount_raw = abs(token_delta)
        # Swapper pays quote (negative quote delta) and receives token (positive).
        quote_in = -quote_delta if quote_delta < 0 else 0
        token_out = token_delta if token_delta > 0 else 0
        block = _log_block(log)
        tx = log["transactionHash"]
        txh = (tx.hex() if isinstance(tx, bytes) else str(tx)).lower()
        resolved = buyers_map.get(txh)
        if not resolved:
            continue
        buyer, amt = resolved
        if amt:
            amount_raw = amt
            token_out = amt

        amount_tokens = amount_raw / (10**decimals)
        amount_usd = (
            (quote_in / (10**quote_decimals)) * quote_usd
            if quote_in
            else amount_tokens * price_post * quote_usd
        )
        buy_mcap = entry_mcap_usd(
            quote_in_raw=quote_in,
            token_out_raw=token_out or amount_raw,
            quote_decimals=quote_decimals,
            token_decimals=decimals,
            quote_usd=quote_usd,
            supply_tokens=supply_tokens,
            spot_mcap=spot_mcap,
        )
        _record_buy(
            aggs,
            wallet=buyer,
            amount_tokens=amount_tokens,
            amount_usd=amount_usd,
            mcap=buy_mcap,
            tx=txh,
            block=block,
        )

    return _aggs_to_rows(aggs, token, "")


def checksum(addr: str) -> str:
    from web3 import Web3

    if addr.lower() == ZERO.lower():
        return ZERO
    return Web3.to_checksum_address(addr)


async def _gather_two(a, b):
    import asyncio

    return await asyncio.gather(a, b)


async def _fallback_blockscout(
    rpc: RpcClient,
    *,
    token: str,
    pool: PoolInfo,
    decimals: int,
    quote_decimals: int,
    token_is_token0: bool,
    supply_tokens: float,
    quote_usd: float,
    mcap_threshold: float,
    on_progress: ProgressCb,
) -> list[BuyerRow]:
    """Approximate early buyers from Blockscout transfers + periodic reserve snapshots."""
    await on_progress("fallback", "Loading transfers from Blockscout…", 0.45)

    # Get current reserves as rough price floor/ceiling — sample getReserves once
    price = 0.0
    try:
        if pool.dex == "uniswap_v2":
            c = rpc.v2_pair(pool.address)
            r = await rpc._call(lambda: c.functions.getReserves().call())
            price = v2_price_token_in_quote(
                int(r[0]), int(r[1]), token_is_token0, decimals, quote_decimals
            )
        else:
            c = rpc.v3_pool(pool.address)
            slot0 = await rpc._call(lambda: c.functions.slot0().call())
            price = v3_price_from_sqrt(int(slot0[0]), token_is_token0, decimals, quote_decimals)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Reserve snapshot failed: %s", exc)

    current_mcap = price * quote_usd * supply_tokens
    # Without historical reserves we cannot be precise. Heuristic:
    # take earliest N transfers from pool while we walk oldest-first.
    transfers = []
    async for item in iter_token_transfers(token):
        transfers.append(item)
        if len(transfers) >= 5000:
            break

    # Blockscout returns newest first — reverse to chronological
    transfers.reverse()
    await on_progress("fallback", f"Analyzing {len(transfers)} transfers…", 0.7)

    aggs: dict[str, WalletAgg] = {}
    # Prefer on-chain entry mcap from the buy receipt when possible; else concave
    # ramp from ~0 to current (heuristic only).
    n = max(len(transfers), 1)
    sem = asyncio.Semaphore(4)
    mcap_cache: dict[str, float | None] = {}

    async def _tx_mcap(txh: str) -> float | None:
        key = txh.lower()
        if key in mcap_cache:
            return mcap_cache[key]
        async with sem:
            if key in mcap_cache:
                return mcap_cache[key]
            try:
                val = await estimate_mcap_at_tx(token, key, rpc=rpc)
            except Exception:  # noqa: BLE001
                val = None
            mcap_cache[key] = val
            return val

    # Pre-resolve mcap for the earliest pool→wallet transfers (cap RPC load).
    early_candidates: list[tuple[int, str, str, int, int, float]] = []
    for i, item in enumerate(transfers):
        frm = ((item.get("from") or {}) if isinstance(item.get("from"), dict) else {})
        to = ((item.get("to") or {}) if isinstance(item.get("to"), dict) else {})
        from_addr = (frm.get("hash") if isinstance(frm, dict) else item.get("from")) or ""
        to_addr = (to.get("hash") if isinstance(to, dict) else item.get("to")) or ""
        if isinstance(from_addr, dict):
            from_addr = from_addr.get("hash", "")
        if isinstance(to_addr, dict):
            to_addr = to_addr.get("hash", "")

        if str(from_addr).lower() != pool.address.lower():
            continue
        if is_excluded(str(to_addr), pool.address):
            continue

        frac = i / n
        est_mcap = current_mcap * (frac**1.6) if current_mcap > 0 else 0.0
        if current_mcap > 0 and current_mcap < mcap_threshold:
            est_mcap = min(current_mcap, mcap_threshold * 0.99) * (0.05 + 0.95 * (frac**1.4))
        if est_mcap >= mcap_threshold:
            continue

        total = item.get("total") or {}
        value = total.get("value") if isinstance(total, dict) else item.get("value")
        try:
            amount_raw = int(value or 0)
        except (TypeError, ValueError):
            continue
        tx = str(item.get("transaction_hash") or item.get("tx_hash") or "")
        block = _rpc_int(item.get("block_number"), 0)
        early_candidates.append((i, str(to_addr), tx, amount_raw, block, est_mcap))
        if len(early_candidates) >= 120:
            break

    unique_txs = list({c[2].lower() for c in early_candidates if c[2].startswith("0x")})
    if unique_txs:
        await on_progress(
            "fallback",
            f"Resolving entry mcap for {len(unique_txs)} txs…",
            0.78,
        )
        await asyncio.gather(*[_tx_mcap(t) for t in unique_txs[:80]])

    for _i, to_addr, tx, amount_raw, block, est_mcap in early_candidates:
        amount_tokens = amount_raw / (10**decimals)
        amount_usd = amount_tokens * price * quote_usd
        buy_mcap = est_mcap
        if tx:
            resolved = mcap_cache.get(tx.lower())
            if resolved and resolved > 0:
                buy_mcap = resolved
        if buy_mcap >= mcap_threshold:
            continue
        _record_buy(
            aggs,
            wallet=str(to_addr),
            amount_tokens=amount_tokens,
            amount_usd=amount_usd,
            mcap=buy_mcap,
            tx=str(tx),
            block=block,
        )

    rows = _aggs_to_rows(aggs, token, "")
    for r in rows:
        r.token_symbol = ""  # filled by caller if needed
    return rows


def _record_buy(
    aggs: dict[str, WalletAgg],
    *,
    wallet: str,
    amount_tokens: float,
    amount_usd: float,
    mcap: float,
    tx: str,
    block: int,
) -> None:
    key = wallet.lower()
    agg = aggs.get(key)
    if agg is None:
        agg = WalletAgg(
            bought_tokens=amount_tokens,
            bought_usd=amount_usd,
            mcap_at_first_buy=mcap,
            buys_count=1,
            first_tx=tx,
            first_block=block,
        )
        aggs[key] = agg
    else:
        agg.bought_tokens += amount_tokens
        agg.bought_usd += amount_usd
        agg.buys_count += 1
        if block < agg.first_block or agg.first_block == 0:
            agg.first_block = block
            agg.first_tx = tx
            agg.mcap_at_first_buy = mcap


def _aggs_to_rows(aggs: dict[str, WalletAgg], token: str, symbol: str) -> list[BuyerRow]:
    rows = [
        BuyerRow(
            wallet=rpc_checksum(w),
            token=token,
            token_symbol=symbol,
            bought_tokens=round(a.bought_tokens, 6),
            bought_usd=round(a.bought_usd, 4),
            mcap_at_first_buy=round(a.mcap_at_first_buy, 2),
            buys_count=a.buys_count,
            first_tx=a.first_tx,
            first_block=a.first_block,
        )
        for w, a in aggs.items()
    ]
    rows.sort(key=lambda r: (r.mcap_at_first_buy, -r.bought_usd))
    return rows


def rpc_checksum(addr: str) -> str:
    try:
        from web3 import Web3

        return Web3.to_checksum_address(addr)
    except Exception:  # noqa: BLE001
        return addr
