"""How long NASDANQ stayed above 30k before dump. Do not commit."""
from __future__ import annotations

import asyncio

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
TH = 30_000.0
END_AT = 27_280_471 + 1_000


async def main() -> None:
    from app.chain import RpcClient
    from app.constants import QUOTE_TOKENS, V3_SWAP_TOPIC
    from app.pools import estimate_start_block, pick_best_pool
    from app.replay import (
        _eth_usd_price,
        _log_block,
        _log_index,
        decode_int256,
        decode_uint256,
        v3_mcap_from_swap,
    )

    rpc = RpcClient()
    pool = await pick_best_pool(rpc, TOKEN)
    assert pool is not None
    start = await estimate_start_block(rpc, pool)
    meta = await rpc.token_meta(TOKEN)
    decimals = int(meta["decimals"])
    supply = int(meta["total_supply_raw"]) / (10**decimals)
    token_is_token0 = pool.token0.lower() == TOKEN.lower()
    qdec = int(QUOTE_TOKENS.get(pool.quote.lower(), {}).get("decimals") or 18)
    qusd = await _eth_usd_price()
    cursor = start
    prev = None
    first_cross = None
    back_under = None
    max_above = 0
    cur_above = 0
    while cursor <= END_AT:
        end = min(cursor + 50_000 - 1, END_AT)
        part = await rpc.get_logs(
            address=pool.address,
            topics=[V3_SWAP_TOPIC],
            from_block=cursor,
            to_block=end,
        )
        part.sort(key=lambda lg: (_log_block(lg), _log_index(lg)))
        for log in part:
            data = log["data"]
            mcap, _ = v3_mcap_from_swap(
                amount0=decode_int256(data, 0),
                amount1=decode_int256(data, 1),
                sqrt_after=decode_uint256(data, 2),
                liquidity=decode_uint256(data, 3),
                token_is_token0=token_is_token0,
                decimals=decimals,
                quote_decimals=qdec,
                quote_usd=qusd,
                supply_tokens=supply,
                prev_sqrt=prev,
            )
            prev = decode_uint256(data, 2)
            b = _log_block(log)
            if mcap >= TH:
                if first_cross is None:
                    first_cross = b
                    print("first_cross", b, "mcap", round(mcap, 1), flush=True)
                cur_above += 1
                max_above = max(max_above, cur_above)
            else:
                if first_cross and back_under is None and b > first_cross:
                    back_under = b
                    print(
                        "back_under",
                        b,
                        "mcap",
                        round(mcap, 1),
                        "swap_streak_above",
                        cur_above,
                        "blocks_from_cross",
                        b - first_cross,
                        flush=True,
                    )
                cur_above = 0
        cursor = end + 1
    print(
        "summary first_cross",
        first_cross,
        "back_under",
        back_under,
        "max_swap_streak",
        max_above,
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
