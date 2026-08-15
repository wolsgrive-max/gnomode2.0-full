"""When did NASDANQ V3 mcap first cross 30k vs target buy blocks. Do not commit."""
from __future__ import annotations

import asyncio

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
POOL = "0xdB1b57704d5122058FF925C1E765c17B21D065EC"
TARGET_BLOCKS = {27280172, 27280471}
TH = 30_000.0


async def main() -> None:
    from app.chain import RpcClient
    from app.config import settings
    from app.constants import V3_SWAP_TOPIC, WETH
    from app.pools import pick_best_pool, estimate_start_block
    from app.replay import (
        decode_int256,
        decode_uint256,
        v3_mcap_from_swap,
        _log_block,
        _log_index,
        _eth_usd_price,
    )
    from app.constants import QUOTE_TOKENS

    rpc = RpcClient()
    pool = await pick_best_pool(rpc, TOKEN)
    assert pool is not None
    start = await estimate_start_block(rpc, pool)
    tip = await rpc.block_number()
    print("pool", pool.address, "start", start, "tip", tip, flush=True)

    meta = await rpc.token_meta(TOKEN)
    decimals = int(meta["decimals"])
    supply = int(meta["total_supply_raw"]) / (10**decimals)
    token_is_token0 = pool.token0.lower() == TOKEN.lower()
    q_meta = QUOTE_TOKENS.get(pool.quote.lower(), {})
    quote_decimals = int(q_meta.get("decimals") or 18)
    quote_usd = await _eth_usd_price() if pool.quote.lower() in (WETH.lower(),) else 1.0
    # WETH quote
    if pool.quote_symbol == "WETH":
        quote_usd = await _eth_usd_price()
    print("supply", supply, "t0?", token_is_token0, "qdec", quote_decimals, "qusd", quote_usd, flush=True)

    chunk = 50_000
    cursor = start
    prev_sqrt = None
    first_cross = None
    saw_targets = []
    under_before_cross = 0

    while cursor <= tip:
        end = min(cursor + chunk - 1, tip)
        part = await rpc.get_logs(
            address=pool.address,
            topics=[V3_SWAP_TOPIC],
            from_block=cursor,
            to_block=end,
        )
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
                supply_tokens=supply,
                prev_sqrt=prev_sqrt,
            )
            prev_sqrt = sqrt_price
            block = _log_block(log)
            token_delta = amount0 if token_is_token0 else amount1
            is_buy = token_delta < 0
            if block in TARGET_BLOCKS or abs(block - 27280172) < 5 or abs(block - 27280471) < 5:
                saw_targets.append((block, round(mcap_now, 1), "buy" if is_buy else "sell"))
            if first_cross is None and mcap_now >= TH:
                first_cross = (block, round(mcap_now, 1))
                print(
                    "FIRST CROSS >=30k at block",
                    block,
                    "mcap",
                    round(mcap_now, 1),
                    "target_blocks",
                    TARGET_BLOCKS,
                    "targets AFTER cross?",
                    all(b > block for b in TARGET_BLOCKS),
                    flush=True,
                )
                # keep scanning a bit to see target blocks
            if first_cross is None and is_buy:
                under_before_cross += 1
            if first_cross and block > max(TARGET_BLOCKS) + 100:
                break
        if first_cross and cursor > max(TARGET_BLOCKS):
            break
        cursor = end + 1

    print("under_buys_before_cross", under_before_cross, flush=True)
    print("near_target_swaps", saw_targets[:20], flush=True)
    if first_cross:
        print(
            "delta blocks target-cross",
            {b: b - first_cross[0] for b in TARGET_BLOCKS},
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
