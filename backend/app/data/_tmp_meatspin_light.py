"""Light MEATSPIN checks: gecko ATH, entry mcap, early-window membership. Do not commit."""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

TOKEN = "0x285ec8958774074cd924103d040fb92be9d2d42a"
TX = "0x34b7547c88b9275828b6e87e340a10175c3b865c7004ab746536e0e5def556a6"
WALLET = "0xfd8bd978f198503a0ba9c5d7f7586e23fc4a4b40"
POOL = "0xeBC09fAF28AD758021272Bf42188c34718d238FD"
TH = 30_000.0


async def main() -> None:
    from app.ath_gecko import fetch_token_ath_mcap
    from app.chain import RpcClient, topic_address
    from app.constants import QUOTE_TOKENS, V3_SWAP_TOPIC
    from app.followup import estimate_token_peak_mcap
    from app.pools import pick_best_pool, estimate_start_block
    from app.replay import (
        _eth_usd_price,
        _log_block,
        _log_index,
        _mcap_above_streak,
        decode_int256,
        decode_uint256,
        estimate_mcap_at_tx,
        v3_mcap_from_swap,
    )

    print("=== gecko / peak ===", flush=True)
    g = await fetch_token_ath_mcap(TOKEN, pool=POOL)
    print("gecko", g, flush=True)
    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    print("peak_est", peak, flush=True)

    rpc = RpcClient()
    print("tx_mcap", await estimate_mcap_at_tx(TOKEN, TX, rpc=rpc), flush=True)

    # receipt: block, transfer from pool?
    rec = (await rpc.batch_get_receipts([TX])).get(TX.lower())
    buy_block = int(rec["blockNumber"], 0) if rec else None
    print("buy_block", buy_block, flush=True)
    pool_l = POOL.lower()
    if rec:
        for lg in rec.get("logs") or []:
            topics = lg.get("topics") or []
            if not topics:
                continue
            t0 = topics[0].hex() if hasattr(topics[0], "hex") else str(topics[0])
            if "ddf252ad" not in t0.lower():
                continue
            frm = topic_address(topics[1]) if len(topics) > 1 else ""
            to = topic_address(topics[2]) if len(topics) > 2 else ""
            if to.lower() == WALLET.lower() or frm.lower() == pool_l:
                print(
                    "xfer",
                    frm[:12],
                    "->",
                    to[:12],
                    "from_pool",
                    frm.lower() == pool_l,
                    "to_wallet",
                    to.lower() == WALLET.lower(),
                    flush=True,
                )

    pool = await pick_best_pool(rpc, TOKEN)
    print("best_pool", pool.dex if pool else None, pool.address if pool else None, flush=True)
    assert pool is not None
    start = await estimate_start_block(rpc, pool)
    meta = await rpc.token_meta(TOKEN)
    decimals = int(meta["decimals"])
    supply = int(meta["total_supply_raw"]) / (10**decimals)
    token_is_token0 = pool.token0.lower() == TOKEN.lower()
    qdec = int(QUOTE_TOKENS.get(pool.quote.lower(), {}).get("decimals") or 18)
    qusd = await _eth_usd_price()
    print("start", start, "supply", supply, flush=True)

    # Scan swaps until buy_block+50: was window open? was this buy under th?
    cursor = start
    end_at = (buy_block or start) + 200
    prev = None
    above_since = None
    under_buys = 0
    saw_buy_block = False
    mcap_at_buy_block = None
    while cursor <= end_at:
        end = min(cursor + 20_000 - 1, end_at)
        part = await rpc.get_logs(
            address=pool.address,
            topics=[V3_SWAP_TOPIC],
            from_block=cursor,
            to_block=end,
        )
        part.sort(key=lambda lg: (_log_block(lg), _log_index(lg)))
        for log in part:
            data = log["data"]
            a0 = decode_int256(data, 0)
            a1 = decode_int256(data, 1)
            sq = decode_uint256(data, 2)
            liq = decode_uint256(data, 3)
            mcap, _ = v3_mcap_from_swap(
                amount0=a0,
                amount1=a1,
                sqrt_after=sq,
                liquidity=liq,
                token_is_token0=token_is_token0,
                decimals=decimals,
                quote_decimals=qdec,
                quote_usd=qusd,
                supply_tokens=supply,
                prev_sqrt=prev,
            )
            prev = sq
            b = _log_block(log)
            stop, above_since = _mcap_above_streak(
                mcap_now=mcap, threshold=TH, block=b, above_since=above_since
            )
            token_delta = a0 if token_is_token0 else a1
            is_buy = token_delta < 0
            if is_buy and (not stop) and mcap < TH:
                under_buys += 1
            if buy_block and abs(b - buy_block) <= 2:
                saw_buy_block = True
                mcap_at_buy_block = mcap
                print(
                    "near_buy_block",
                    b,
                    "mcap",
                    round(mcap, 1),
                    "stop",
                    stop,
                    "collect",
                    (not stop) and is_buy and mcap < TH,
                    "is_buy",
                    is_buy,
                    flush=True,
                )
        cursor = end + 1
        await asyncio.sleep(0.15)

    print(
        "under_buys_until_buy",
        under_buys,
        "saw_buy_block",
        saw_buy_block,
        "mcap_at_buy_block",
        mcap_at_buy_block,
        flush=True,
    )

    # Would screen fail with DS-only ath?
    print(
        "DS_spot_ath_would_fail_50k",
        True,
        "gecko_peak",
        getattr(g, "ath_mcap", g),
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
