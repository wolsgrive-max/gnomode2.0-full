"""Estimate mcap at known FROGLET buy txs. Do not commit."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
TXS = {
    "0x0e507839ecdf7a6eacfdce67427c4b6975328659": (
        "0xe0f830d6bbeb09729e92295e2aa62eeb9698906ad629392f69a68fbec306c86b",
        1786030109,  # gmgn ts approx from earlier audit
    ),
    "0x91a54dfd4c346cb6a81cbc1357da673161568dbb": (
        "0x9bfe9e223773d00431ef89a2096bdc7389150ae621b5fb81c008b9accc2445fb",
        1786029523,
    ),
    "0x14de114921829c059ca4934d5ff2c226452b93c4": (
        "0x46e035701982cafe0c7af1d46a364750d600db3f7646bb70c521c37ec56de3c5",
        1786027486,
    ),
    "0x952b61bd0185533e926154f0e4e98452ee1f1186": (
        "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278",
        1786027462,
    ),
}


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def main() -> None:
    from app.chain import RpcClient
    from app.pools import fetch_dexscreener_pairs
    from app.replay import estimate_mcap_at_tx, parse_token

    rpc = RpcClient()
    pairs = await fetch_dexscreener_pairs(F)
    for p in sorted(
        pairs or [],
        key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0),
        reverse=True,
    )[:3]:
        print(
            "pair",
            p.get("pairAddress"),
            "dex",
            p.get("dexId"),
            "liq",
            (p.get("liquidity") or {}).get("usd"),
            "mcap",
            p.get("marketCap") or p.get("fdv"),
            flush=True,
        )

    print("\n=== estimate_mcap_at_tx ===", flush=True)
    for w, (tx, ts) in TXS.items():
        print(f"\n{w}", flush=True)
        print(f"  gmgn_ts≈{_iso(ts)} tx={tx}", flush=True)
        try:
            receipt = await rpc.w3.eth.get_transaction_receipt(tx)
            print(
                f"  block={receipt['blockNumber']} status={receipt['status']} "
                f"to={receipt.get('to')} logs={len(receipt['logs'])}",
                flush=True,
            )
            addrs = sorted({lg["address"].lower() for lg in receipt["logs"]})
            print(f"  log_addrs={addrs}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  receipt ERR {type(exc).__name__}: {exc}", flush=True)
            receipt = None

        try:
            mcap = await estimate_mcap_at_tx(F, tx, rpc=rpc)
            print(f"  estimate_mcap_at_tx={mcap}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  estimate ERR {type(exc).__name__}: {exc}", flush=True)

    # Also: among 30k early buyers, what's max first_buy_block / time — compare
    print("\n=== parse 30k sample ===", flush=True)
    res = await parse_token(
        rpc, F, mcap_threshold=30_000.0, exclude_honeypots=False, wallet_filters=None
    )
    print(f"early={len(res.buyers)} err={res.error}", flush=True)
    known_l = {k.lower() for k in TXS}
    for b in res.buyers:
        if b.wallet.lower() in known_l:
            print("FOUND IN EARLY", b.wallet, b.mcap_at_first_buy, b.buys_count, flush=True)
    # show last few by block if field exists
    rows = sorted(
        res.buyers,
        key=lambda b: int(getattr(b, "first_buy_block", 0) or 0),
    )
    if rows:
        print(
            f"first_block={getattr(rows[0], 'first_buy_block', None)} "
            f"mcap={rows[0].mcap_at_first_buy}",
            flush=True,
        )
        print(
            f"last_block={getattr(rows[-1], 'first_buy_block', None)} "
            f"mcap={rows[-1].mcap_at_first_buy}",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
