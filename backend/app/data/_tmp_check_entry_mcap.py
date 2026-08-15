"""Check entry mcap + presence for two NASDANQ buyers. Do not commit."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
# open buys from prior audit
BUYS = [
    (
        "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
        "0xa3397fbe25ccdb715f6cee3059e8b9108c1e34232be0a30b00ff21875ddfa39c",
        1785812024,
        2.785053396276456e-05,
    ),
    (
        "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
        "0x78c68d029ac10aea",  # truncated — reload from activity
        1785812024,
        1.8525077963924883e-05,
    ),
]


async def main() -> None:
    from app.chain import RpcClient
    from app.gmgn_portfolio import fetch_wallet_activity_result
    from app.pools import pick_best_pool
    from app.replay import estimate_mcap_at_tx, estimate_entry_at_tx

    rpc = RpcClient()
    pool = await pick_best_pool(rpc, TOKEN)
    print(
        "pool",
        pool.dex if pool else None,
        pool.address if pool else None,
        "liq",
        getattr(pool, "liquidity_usd", None),
        flush=True,
    )

    # refresh full open txs
    rows = []
    for w, _tx, _ts, _px in BUYS:
        act = await fetch_wallet_activity_result(
            w, event_types=["buy"], limit=10, max_pages=1
        )
        opens = [r for r in act.rows if r.get("is_open_or_close") == 1]
        r = opens[-1] if opens else None
        if not r:
            print(w[:12], "no open", flush=True)
            continue
        tx = str(r.get("tx_hash") or "")
        ts = int(r.get("timestamp") or 0)
        px = float(r.get("price_usd") or 0)
        rows.append((w, tx, ts, px))
        print(
            w[:12],
            "open",
            datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "tx",
            tx[:18],
            "gmgn_px",
            px,
            flush=True,
        )

    for w, tx, ts, px in rows:
        try:
            mcap = await estimate_mcap_at_tx(rpc, TOKEN, tx)
            print(w[:12], "estimate_mcap_at_tx", mcap, "th30k", flush=True)
        except Exception as exc:
            print(w[:12], "mcap_err", type(exc).__name__, exc, flush=True)
        try:
            entry = await estimate_entry_at_tx(rpc, TOKEN, tx)
            print(w[:12], "entry", entry, flush=True)
        except Exception as exc:
            print(w[:12], "entry_err", type(exc).__name__, exc, flush=True)

        # tx receipt: were there V3 swaps / transfers to wallet?
        try:
            receipt = await rpc._call(lambda: rpc.w3.eth.get_transaction_receipt(tx))
            logs = list(receipt.get("logs") or [])
            print(
                w[:12],
                "receipt logs",
                len(logs),
                "status",
                receipt.get("status"),
                "to",
                receipt.get("to"),
                flush=True,
            )
            # any log address == pool?
            pool_a = (pool.address or "").lower() if pool else ""
            hit_pool = sum(1 for lg in logs if str(lg.get("address") or "").lower() == pool_a)
            print(w[:12], "logs_on_best_pool", hit_pool, flush=True)
        except Exception as exc:
            print(w[:12], "receipt_err", exc, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
