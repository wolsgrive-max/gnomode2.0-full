"""Audit FROGLET buy mcap for 4 known wallets. Do not commit."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN = [
    "0x0e507839ecdf7a6eacfdce67427c4b6975328659",
    "0x91a54dfd4c346cb6a81cbc1357da673161568dbb",
    "0x14de114921829c059ca4934d5ff2c226452b93c4",
    "0x952b61bd0185533e926154f0e4e98452ee1f1186",
]


def _iso(ts: int | float | None) -> str:
    if ts is None:
        return "?"
    n = float(ts)
    if n > 1e12:
        n /= 1000.0
    return datetime.fromtimestamp(n, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


async def main() -> None:
    from app.blockscout import _get_json, scan_address_token_transfers
    from app.chain import RpcClient
    from app.gmgn_portfolio import fetch_unique_buys
    from app.replay import parse_token
    from app.watch_store import watch_store

    cfg = watch_store.load_config()
    thr = float(cfg.wallet.mcap_threshold or 30_000)
    print(f"watch mcap_threshold={thr}", flush=True)

    rpc = RpcClient()

    print("\n=== GMGN + Blockscout buys ===", flush=True)
    buy_meta: dict[str, dict] = {}
    for w in KNOWN:
        print(f"\n----- {w} -----", flush=True)
        ub = await fetch_unique_buys(w, max_pages=3)
        frog = [b for b in ub.buys if b.token.lower() == F.lower()]
        uniq = len({b.token.lower() for b in ub.buys})
        print(f"gmgn ok={ub.ok} unique={uniq} frog={len(frog)}", flush=True)
        tx_hash = frog[0].tx_hash if frog else None
        ts = frog[0].timestamp if frog else None
        cost = frog[0].cost_usd if frog else None
        if frog:
            print(f"gmgn ts={_iso(ts)} cost_usd={cost} tx={tx_hash}", flush=True)
        block = None
        if tx_hash:
            got = await _get_json(f"/transactions/{tx_hash}")
            if got and got[0] == 200 and isinstance(got[1], dict):
                tx = got[1]
                block = tx.get("block_number")
                if isinstance(block, dict):
                    block = block.get("number")
                print(f"tx block={block} result={tx.get('result') or tx.get('status')}", flush=True)
            else:
                print(f"tx fetch fail status={None if not got else got[0]}", flush=True)

        items, _, _ = await scan_address_token_transfers(w, max_pages=4, direction="to")
        frog_in = [
            it
            for it in items
            if str(((it.get("token") or {}).get("address") or "")).lower() == F.lower()
        ]
        frog_in.sort(key=lambda it: int(it.get("block_number") or 0))
        print(f"bs frog inbound scanned={len(frog_in)}", flush=True)
        first_bs = frog_in[0] if frog_in else None
        if first_bs:
            print(
                f"bs FIRST block={first_bs.get('block_number')} ts={_iso(first_bs.get('timestamp'))} "
                f"tx={first_bs.get('transaction_hash')}",
                flush=True,
            )
        buy_meta[w.lower()] = {
            "gmgn_ts": ts,
            "gmgn_tx": tx_hash,
            "gmgn_block": block,
            "gmgn_cost": cost,
            "bs_first_block": (first_bs or {}).get("block_number") if first_bs else None,
            "bs_first_tx": (first_bs or {}).get("transaction_hash") if first_bs else None,
        }

    # One wide parse: capture mcap_at_first_buy for known wallets if they appear at all
    print("\n=== parse_token thr=1_000_000 (wide) ===", flush=True)
    wide = await parse_token(
        rpc, F, mcap_threshold=1_000_000.0, exclude_honeypots=False, wallet_filters=None
    )
    by = {b.wallet.lower(): b for b in wide.buyers}
    print(f"wide early={len(wide.buyers)} err={wide.error}", flush=True)
    if wide.buyers:
        mcaps = sorted(float(b.mcap_at_first_buy or 0) for b in wide.buyers)
        print(
            f"wide mcap min={mcaps[0]:.0f} p50={mcaps[len(mcaps)//2]:.0f} max={mcaps[-1]:.0f}",
            flush=True,
        )

    print("\n=== verdict ===", flush=True)
    for w in KNOWN:
        b = by.get(w.lower())
        meta = buy_meta[w.lower()]
        if b is None:
            print(
                f"{w}\n  NOT in parse even at 1M\n"
                f"  gmgn_block={meta['gmgn_block']} bs_first_block={meta['bs_first_block']}",
                flush=True,
            )
            continue
        m = float(b.mcap_at_first_buy or 0)
        print(
            f"{w}\n"
            f"  parse mcap_at_first_buy={m:.2f} buys_count={b.buys_count} "
            f"block={getattr(b, 'first_buy_block', None)}\n"
            f"  under_30k={'YES' if m < thr else 'NO'} "
            f"gmgn_block={meta['gmgn_block']} bs_first_block={meta['bs_first_block']}",
            flush=True,
        )

    print("\n=== parse_token thr=30000 ===", flush=True)
    narrow = await parse_token(
        rpc, F, mcap_threshold=thr, exclude_honeypots=False, wallet_filters=None
    )
    by30 = {b.wallet.lower(): b for b in narrow.buyers}
    print(f"30k early={len(narrow.buyers)}", flush=True)
    for w in KNOWN:
        print(f"  {w[:12]} in_30k={w.lower() in by30}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
