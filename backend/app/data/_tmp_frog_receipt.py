"""Debug why receipt fallback returns None. Do not commit."""

from __future__ import annotations

import asyncio

from eth_utils import to_checksum_address

TX = "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278"
F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
WALLET = "0x952b61bd0185533e926154f0e4e98452ee1f1186"


async def main() -> None:
    from app.chain import RpcClient, topic_address
    from app.constants import UNI_V4_POOL_MANAGER
    from app.replay import checksum, is_excluded

    rpc = RpcClient()
    manager = checksum(UNI_V4_POOL_MANAGER)

    # hash key variants
    r = await rpc.w3.eth.get_transaction_receipt(TX)
    th = r["transactionHash"]
    print("type", type(th), "hex", th.hex() if hasattr(th, "hex") else th, flush=True)
    print("isinstance bytes", isinstance(th, bytes), flush=True)

    receipts = await rpc.batch_get_receipts([TX, TX.lower(), "0x" + TX[2:].upper()])
    print("batch keys", list(receipts.keys())[:5], "n=", len(receipts), flush=True)
    for k, v in receipts.items():
        print(" key", k, "has", v is not None, flush=True)

    receipt = receipts.get(TX.lower()) or receipts.get(TX) or next(iter(receipts.values()), None)
    if not receipt:
        print("NO RECEIPT", flush=True)
        return
    token_l = F.lower()
    chain = []
    for lg in receipt.get("logs") or []:
        addr = lg.get("address") or ""
        addr_s = (addr if isinstance(addr, str) else "0x" + bytes(addr).hex()).lower()
        if not addr_s.startswith("0x"):
            addr_s = "0x" + addr_s
        if addr_s != token_l:
            print("skip addr", addr_s[:12], flush=True)
            continue
        topics = lg.get("topics") or []
        t0 = topics[0] if isinstance(topics[0], str) else (
            topics[0].hex() if hasattr(topics[0], "hex") else str(topics[0])
        )
        print("t0", t0[:20], "ntopics", len(topics), flush=True)
        if "ddf252ad" not in t0.lower():
            continue
        to = topic_address(topics[2])
        excl = is_excluded(to, manager)
        amt = int(topics and 0 or 0)
        from app.chain import decode_uint256
        amt = decode_uint256(lg.get("data") or "0x0", 0)
        print(f" transfer to={to} excl={excl} amt={amt}", flush=True)
        if not excl:
            chain.append(to)

    print("chain eoas candidates", chain, flush=True)
    codes = await rpc.batch_is_eoa(chain + [WALLET])
    print("is_eoa", codes, flush=True)

    # tx.from fallback
    raws = await rpc._jsonrpc_batch([("eth_getTransactionByHash", [TX])])
    print("tx raw from", (raws[0] or {}).get("from") if raws else None, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
