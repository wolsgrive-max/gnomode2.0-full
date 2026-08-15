"""Why V4 FROGLET buys miss parse. Do not commit."""

from __future__ import annotations

import asyncio

from eth_utils import to_checksum_address

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN = {
    "0x0e507839ecdf7a6eacfdce67427c4b6975328659": "0xe0f830d6bbeb09729e92295e2aa62eeb9698906ad629392f69a68fbec306c86b",
    "0x91a54dfd4c346cb6a81cbc1357da673161568dbb": "0x9bfe9e223773d00431ef89a2096bdc7389150ae621b5fb81c008b9accc2445fb",
    "0x14de114921829c059ca4934d5ff2c226452b93c4": "0x46e035701982cafe0c7af1d46a364750d600db3f7646bb70c521c37ec56de3c5",
    "0x952b61bd0185533e926154f0e4e98452ee1f1186": "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278",
}


def _hex(x: object) -> str:
    if hasattr(x, "hex"):
        h = x.hex()
    else:
        h = str(x)
    if not h.startswith("0x"):
        h = "0x" + h
    return h.lower()


async def main() -> None:
    from app.chain import RpcClient
    from app.constants import (
        TRANSFER_TOPIC,
        UNI_V4_POOL_MANAGER,
        UNIVERSAL_ROUTER,
        V2_SWAP_TOPIC,
        V3_SWAP_TOPIC,
        V4_SWAP_TOPIC,
    )
    from app.pools import pick_best_pool
    from app.replay import estimate_mcap_at_tx, parse_token

    rpc = RpcClient()
    w3 = rpc.w3
    pool = await pick_best_pool(rpc, F)
    print(
        "best_pool",
        None
        if not pool
        else f"dex={pool.dex} addr={pool.address} pool_id={pool.pool_id} quote={pool.quote_symbol}",
        flush=True,
    )

    known_blocks: list[int] = []
    for w, txh in KNOWN.items():
        print(f"\n===== {w} =====", flush=True)
        receipt = await w3.eth.get_transaction_receipt(txh)
        tx = await w3.eth.get_transaction(txh)
        known_blocks.append(int(receipt["blockNumber"]))
        print(
            f"block={receipt['blockNumber']} from={tx['from']} to={tx['to']} logs={len(receipt['logs'])}",
            flush=True,
        )
        mcap = await estimate_mcap_at_tx(F, txh, rpc=rpc)
        print(f"mcap_est={mcap}", flush=True)

        has_v4_swap = False
        for i, lg in enumerate(receipt["logs"]):
            tops = [_hex(t) for t in lg["topics"]]
            t0 = tops[0] if tops else ""
            addr = lg["address"]
            if t0 == TRANSFER_TOPIC.lower():
                frm = "0x" + tops[1][-40:]
                to = "0x" + tops[2][-40:]
                amt = int(_hex(lg["data"]), 16)
                print(
                    f" log{i} Transfer {addr[:10]}… from={frm[:10]}… to={to[:10]}… "
                    f"amt={amt} to_wallet={to.lower()==w.lower()} "
                    f"from_pm={frm.lower()==UNI_V4_POOL_MANAGER.lower()} "
                    f"from_ur={frm.lower()==UNIVERSAL_ROUTER.lower()}",
                    flush=True,
                )
            elif t0 == V4_SWAP_TOPIC.lower():
                has_v4_swap = True
                print(f" log{i} V4_Swap addr={addr} topics_n={len(tops)}", flush=True)
                if len(tops) > 1:
                    print(f"   poolId_topic={tops[1]}", flush=True)
                if len(tops) > 2:
                    print(f"   sender_topic={tops[2]}", flush=True)
            elif t0 == V3_SWAP_TOPIC.lower():
                print(f" log{i} V3_Swap", flush=True)
            elif t0 == V2_SWAP_TOPIC.lower():
                print(f" log{i} V2_Swap", flush=True)
            else:
                print(f" log{i} OTHER addr={addr} t0={t0}", flush=True)
        print(f"has_v4_swap={has_v4_swap}", flush=True)

    if pool and pool.pool_id:
        pid = pool.pool_id if pool.pool_id.startswith("0x") else f"0x{pool.pool_id}"
        lo, hi = min(known_blocks) - 2, max(known_blocks) + 2
        print(f"\n=== V4 Swap logs pool_id={pid} blocks {lo}-{hi} ===", flush=True)
        try:
            logs = await rpc.get_logs(
                address=to_checksum_address(UNI_V4_POOL_MANAGER),
                topics=[V4_SWAP_TOPIC, pid],
                from_block=lo,
                to_block=hi,
            )
        except Exception as exc:  # noqa: BLE001
            print("get_logs err", type(exc).__name__, exc, flush=True)
            logs = []
        want = {h.lower() for h in KNOWN.values()}
        hit = set()
        for lg in logs:
            th = _hex(lg.get("transactionHash") or lg.get("transaction_hash"))
            if th in want:
                hit.add(th)
        print(f"logs={len(logs)} known_hit={len(hit)}/{len(want)}", flush=True)
        for w, txh in KNOWN.items():
            print(f"  {w[:12]} in_swap_logs={txh.lower() in hit}", flush=True)

        print("\n=== any V4 swaps on exact known blocks (no pool filter) ===", flush=True)
        for w, txh in KNOWN.items():
            r = await w3.eth.get_transaction_receipt(txh)
            b = int(r["blockNumber"])
            try:
                logs2 = await rpc.get_logs(
                    address=to_checksum_address(UNI_V4_POOL_MANAGER),
                    topics=[V4_SWAP_TOPIC],
                    from_block=b,
                    to_block=b,
                )
            except Exception as exc:  # noqa: BLE001
                print(w[:12], "err", exc, flush=True)
                continue
            txs = {_hex(lg.get("transactionHash")) for lg in logs2}
            print(
                f"  {w[:12]} block={b} v4_swaps_in_block={len(logs2)} our_tx={txh.lower() in txs}",
                flush=True,
            )

    print("\n=== parse 30k membership ===", flush=True)
    res = await parse_token(
        rpc, F, mcap_threshold=30_000, exclude_honeypots=False, wallet_filters=None
    )
    by = {b.wallet.lower() for b in res.buyers}
    print(
        f"early={len(res.buyers)} dex={(res.stats or {}).get('dex')} pool={(res.stats or {}).get('pool')}",
        flush=True,
    )
    for w in KNOWN:
        print(f"  {w[:12]} in_early={w.lower() in by}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
