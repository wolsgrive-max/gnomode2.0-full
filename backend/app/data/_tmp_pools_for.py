"""Pools + DexScreener coverage for a token: python _tmp_pools_for.py <token>"""
from __future__ import annotations

import asyncio
import json
import sys

sys.path.insert(0, "/app/backend")

from app.chain import RpcClient  # noqa: E402
from app.pools import discover_pools, fetch_dexscreener_pairs, pick_best_pool  # noqa: E402


async def main() -> None:
    t = sys.argv[1].strip()
    pairs = await fetch_dexscreener_pairs(t)
    print(f"dexscreener pairs = {len(pairs or [])}", flush=True)
    for p in (pairs or [])[:8]:
        print(
            json.dumps(
                {
                    "dexId": p.get("dexId"),
                    "pairAddress": p.get("pairAddress"),
                    "base": ((p.get("baseToken") or {}).get("symbol")),
                    "quote": ((p.get("quoteToken") or {}).get("symbol")),
                    "liq": (p.get("liquidity") or {}).get("usd"),
                    "fdv": p.get("fdv"),
                    "mcap": p.get("marketCap"),
                    "created": p.get("pairCreatedAt"),
                }
            ),
            flush=True,
        )

    rpc = RpcClient()
    pools = await discover_pools(rpc, t, deep=True)
    print(f"discover_pools(deep) = {len(pools)}", flush=True)
    for p in pools[:10]:
        print(
            f"  {p.dex} {p.address} quote={p.quote_symbol} liq={p.liquidity_usd} fee={p.fee}",
            flush=True,
        )
    best = await pick_best_pool(rpc, t)
    print("best_pool:", best.address if best else None,
          best.dex if best else "", best.quote_symbol if best else "",
          best.liquidity_usd if best else "", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
