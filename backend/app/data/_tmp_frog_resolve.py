"""Pinpoint FROGLET V4 miss: early_swaps vs resolve. Do not commit."""

from __future__ import annotations

import asyncio
from collections import defaultdict

from eth_utils import to_checksum_address

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN_TX = {
    "0xe0f830d6bbeb09729e92295e2aa62eeb9698906ad629392f69a68fbec306c86b",
    "0x9bfe9e223773d00431ef89a2096bdc7389150ae621b5fb81c008b9accc2445fb",
    "0x46e035701982cafe0c7af1d46a364750d600db3f7646bb70c521c37ec56de3c5",
    "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278",
}


def _txh(log: dict) -> str:
    tx = log["transactionHash"]
    h = tx.hex() if isinstance(tx, bytes) else str(tx)
    return (h if h.startswith("0x") else "0x" + h).lower()


async def main() -> None:
    from app.chain import RpcClient
    from app.constants import (
        TRANSFER_TOPIC,
        UNI_V2_ROUTER,
        UNI_V3_ROUTER,
        UNI_V4_POOL_MANAGER,
        UNIVERSAL_ROUTER,
        V4_SWAP_TOPIC,
    )
    from app.pools import estimate_start_block, pick_best_pool
    from app.chain import topic_address
    from app.replay import (
        _log_block,
        _resolve_buyers_batch,
        checksum,
        decode_int256,
        decode_uint256,
        is_excluded,
        v3_mcap_from_swap,
        _mcap_above_streak,
        _eth_usd_price,
        _quote_usd_price,
    )
    
    rpc = RpcClient()
    pool = await pick_best_pool(rpc, F)
    assert pool and pool.pool_id
    start = await estimate_start_block(rpc, pool)
    tip = await rpc.block_number()
    print(f"start={start} tip={tip} pool_id={pool.pool_id}", flush=True)

    # Fetch known receipts for blocks
    w3 = rpc.w3
    for tx in KNOWN_TX:
        r = await w3.eth.get_transaction_receipt(tx)
        print(f" known tx block={r['blockNumber']} vs start={start} before_start={int(r['blockNumber'])<int(start or 0)}", flush=True)

    # Get decimals/supply from a lightweight call
    from app.constants import ERC20_ABI, QUOTE_TOKENS

    c = rpc.w3.eth.contract(address=to_checksum_address(F), abi=ERC20_ABI)
    decimals = int(await rpc._call(lambda: c.functions.decimals().call()))
    supply_raw = int(await rpc._call(lambda: c.functions.totalSupply().call()))
    supply_tokens = supply_raw / (10**decimals)
    token_is_token0 = pool.token0.lower() == F.lower()
    quote = pool.quote
    qinfo = QUOTE_TOKENS.get(quote.lower()) or {"decimals": 18}
    quote_decimals = int(qinfo.get("decimals") or 18)
    eth_usd = await _eth_usd_price(rpc=rpc)
    quote_usd = await _quote_usd_price(quote, eth_usd)
    print(f"dec={decimals} supply={supply_tokens:.0f} t0={token_is_token0} quote_usd={quote_usd}", flush=True)

    pid = pool.pool_id if pool.pool_id.startswith("0x") else f"0x{pool.pool_id}"
    manager = checksum(UNI_V4_POOL_MANAGER)
    thr = 30_000.0
    last_known = 0
    for tx in KNOWN_TX:
        r = await w3.eth.get_transaction_receipt(tx)
        last_known = max(last_known, int(r["blockNumber"]))
    print(f"last_known={last_known}", flush=True)

    early_swaps = []
    crossed = False
    above_since = None
    prev_sqrt = None
    cursor = int(start)
    chunk = 50_000
    known_seen_in_logs = set()
    known_dropped_mcap = []
    known_dropped_stop = []

    while cursor <= last_known + 100 and not crossed:
        end = min(cursor + chunk - 1, last_known + 100)
        part = await rpc.get_logs(
            address=manager,
            topics=[V4_SWAP_TOPIC, pid],
            from_block=cursor,
            to_block=end,
        )
        part.sort(key=lambda lg: (_log_block(lg), int(lg.get("logIndex") or 0)))
        for log in part:
            txh = _txh(log)
            data = log["data"]
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
            token_delta = amount0 if token_is_token0 else amount1
            is_buy = token_delta > 0
            block = _log_block(log)
            stop, above_since = _mcap_above_streak(
                mcap_now=mcap_now, threshold=thr, block=block, above_since=above_since
            )
            if txh in KNOWN_TX:
                known_seen_in_logs.add(txh)
                print(
                    f"KNOWN hit block={block} mcap_now={mcap_now:.0f} is_buy={is_buy} "
                    f"stop={stop} above_since={above_since} ge_thr={mcap_now>=thr}",
                    flush=True,
                )
            if stop:
                crossed = True
                if txh in KNOWN_TX:
                    known_dropped_stop.append(txh)
                break
            if not is_buy:
                continue
            if mcap_now >= thr:
                if txh in KNOWN_TX:
                    known_dropped_mcap.append((txh, mcap_now))
                continue
            early_swaps.append(log)
        if crossed:
            break
        cursor = end + 1

    early_tx = {_txh(l) for l in early_swaps}
    print(f"\nearly_swaps={len(early_swaps)} known_in_early={len(KNOWN_TX & early_tx)}/{len(KNOWN_TX)}", flush=True)
    print(f"known_seen_in_logs={len(known_seen_in_logs)} crossed={crossed}", flush=True)
    print(f"dropped_mcap={known_dropped_mcap}", flush=True)

    # Resolve buyers for known txs that are in early_swaps
    if early_swaps:
        xfer_to = last_known + 5
        router_froms = [
            manager,
            checksum(UNIVERSAL_ROUTER),
            checksum(UNI_V2_ROUTER),
            checksum(UNI_V3_ROUTER),
        ]

        async def transfers_from(frm: str):
            return await rpc.get_logs_chunked(
                address=F,
                topics=[TRANSFER_TOPIC, "0x" + "0" * 24 + frm.lower().replace("0x", "")],
                from_block=int(start),
                to_block=xfer_to,
                chunk_size=chunk,
            )

        batches = await asyncio.gather(*[transfers_from(a) for a in router_froms])
        xfers_by_tx: dict[str, list] = defaultdict(list)
        for batch in batches:
            for log in batch:
                xfers_by_tx[_txh(log)].append(log)

        # Show xfer candidates for known
        for txh in KNOWN_TX:
            xs = xfers_by_tx.get(txh, [])
            print(f"\nxfers for {txh[:14]} n={len(xs)}", flush=True)
            for x in xs:
                tops = x["topics"]
                frm = topic_address(tops[1])
                to = topic_address(tops[2])
                print(
                    f"  {frm[:10]}→{to[:10]} excluded_to={is_excluded(to, manager)} "
                    f"from_pm={frm.lower()==manager.lower()}",
                    flush=True,
                )

        buyers_map = await _resolve_buyers_batch(
            rpc,
            token=F,
            pool_or_manager=manager,
            early_swaps=[l for l in early_swaps if _txh(l) in KNOWN_TX] or early_swaps[:5],
            xfers_by_tx=xfers_by_tx,
        )
        print("\nresolve known:", flush=True)
        for txh in KNOWN_TX:
            print(f"  {txh[:14]} → {buyers_map.get(txh)}", flush=True)

        # Force resolve only known swaps regardless of early filter
        known_logs = []
        # re-fetch exact
        for txh in KNOWN_TX:
            r = await w3.eth.get_transaction_receipt(txh)
            for lg in r["logs"]:
                tops = [((t.hex() if hasattr(t, "hex") else str(t))) for t in lg["topics"]]
                t0 = tops[0].lower() if tops else ""
                if "40e9cecb" in t0:
                    # normalize to get_logs-like dict
                    known_logs.append(
                        {
                            "transactionHash": bytes.fromhex(txh[2:]),
                            "data": lg["data"],
                            "topics": lg["topics"],
                            "blockNumber": r["blockNumber"],
                            "logIndex": lg["logIndex"],
                            "address": lg["address"],
                        }
                    )
        buyers2 = await _resolve_buyers_batch(
            rpc,
            token=F,
            pool_or_manager=manager,
            early_swaps=known_logs,
            xfers_by_tx=xfers_by_tx,
        )
        print("\nforce resolve known swaps:", flush=True)
        for txh in KNOWN_TX:
            print(f"  {txh[:14]} → {buyers2.get(txh.lower())}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
