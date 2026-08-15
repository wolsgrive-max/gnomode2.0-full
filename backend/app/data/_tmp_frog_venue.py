"""Decode FROGLET buy venue. Do not commit."""

from __future__ import annotations

import asyncio
import json

ROUTER = "0x8876789976dEcBfCbBbe364623C63652db8C0904"
VENUE = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
TXS = [
    "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278",
    "0x46e035701982cafe0c7af1d46a364750d600db3f7646bb70c521c37ec56de3c5",
    "0x9bfe9e223773d00431ef89a2096bdc7389150ae621b5fb81c008b9accc2445fb",
    "0xe0f830d6bbeb09729e92295e2aa62eeb9698906ad629392f69a68fbec306c86b",
]


async def main() -> None:
    import httpx
    from app.blockscout import _get_json
    from app.chain import RpcClient
    from app.constants import UNIVERSAL_ROUTER

    print("UNIVERSAL_ROUTER match", ROUTER.lower() == UNIVERSAL_ROUTER.lower(), flush=True)
    rpc = RpcClient()
    w3 = rpc.w3

    for addr, label in [(ROUTER, "router"), (VENUE, "venue"), (F, "token")]:
        code = await w3.eth.get_code(addr)
        print(label, "code_bytes", len(code), flush=True)

    for addr, label in [(ROUTER, "router"), (VENUE, "venue")]:
        got = await _get_json(f"/addresses/{addr}")
        if got and got[0] == 200 and isinstance(got[1], dict):
            d = got[1]
            print(
                label,
                "name=",
                d.get("name"),
                "verified=",
                d.get("is_verified"),
                "proxy=",
                d.get("proxy_type") or d.get("implementations"),
                flush=True,
            )
            # try contract source meta
            for k in ("token", "metadata", "public_tags", "private_tags", "watchlist_names"):
                if d.get(k):
                    print(" ", k, str(d.get(k))[:200], flush=True)
        else:
            print(label, "bs", None if not got else got[0], flush=True)

    # ABI from blockscout
    for addr, label in [(VENUE, "venue"), (ROUTER, "router")]:
        got = await _get_json(f"/smart-contracts/{addr}")
        if got and got[0] == 200 and isinstance(got[1], dict):
            d = got[1]
            print(
                f"\n{label} contract name={d.get('name')} verified={d.get('is_verified')}",
                flush=True,
            )
            abi = d.get("abi")
            if abi:
                evs = [x for x in abi if x.get("type") == "event"]
                fns = [x for x in abi if x.get("type") == "function"]
                print(" events:", [e.get("name") for e in evs][:30], flush=True)
                print(" fns:", [f.get("name") for f in fns][:40], flush=True)
        else:
            print(label, "smart-contract", None if not got else got[0], flush=True)

    tx = TXS[0]
    receipt = await w3.eth.get_transaction_receipt(tx)
    t = await w3.eth.get_transaction(tx)
    raw = bytes(t["input"])
    sel = "0x" + raw[:4].hex()
    print(f"\ntx {tx[:14]} from={t['from']} to={t['to']} sel={sel} input_len={len(raw)}", flush=True)

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://www.4byte.directory/api/v1/signatures/",
            params={"hex_signature": sel},
        )
        print("4byte", r.status_code, r.text[:500], flush=True)
        r2 = await client.get(
            "https://api.openchain.xyz/signature-database/v1/lookup",
            params={"function": sel, "filter": "true"},
        )
        print("openchain", r2.status_code, r2.text[:500], flush=True)

    print("\nlogs detail:", flush=True)
    for i, lg in enumerate(receipt["logs"]):
        topics = [t.hex() if hasattr(t, "hex") else str(t) for t in lg["topics"]]
        data = lg["data"].hex() if hasattr(lg["data"], "hex") else str(lg["data"])
        print(f" log{i} {lg['address']}", flush=True)
        for j, tp in enumerate(topics):
            print(f"  t{j}={tp}", flush=True)
        print(f"  data={data}", flush=True)

    # topic0 signature lookup
    t0 = receipt["logs"][0]["topics"][0].hex()
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://www.4byte.directory/api/v1/event-signatures/",
            params={"hex_signature": t0 if t0.startswith("0x") else "0x" + t0},
        )
        print("\nevent0 4byte", r.status_code, r.text[:400], flush=True)
        if len(receipt["logs"]) > 1:
            t1 = receipt["logs"][1]["topics"][0].hex()
            r = await client.get(
                "https://www.4byte.directory/api/v1/event-signatures/",
                params={"hex_signature": t1 if t1.startswith("0x") else "0x" + t1},
            )
            print("event1 4byte", r.status_code, r.text[:400], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
