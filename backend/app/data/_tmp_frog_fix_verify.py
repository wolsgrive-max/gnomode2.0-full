"""Live verify FROGLET V4 resolve fix. Do not commit."""

from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path("/home/shneining/gnomode2.0")
sys.path.insert(0, str(ROOT / "backend"))
for line in (ROOT / ".env").read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

F = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN = {
    "0x0e507839ecdf7a6eacfdce67427c4b6975328659": "0xe0f830d6bbeb09729e92295e2aa62eeb9698906ad629392f69a68fbec306c86b",
    "0x91a54dfd4c346cb6a81cbc1357da673161568dbb": "0x9bfe9e223773d00431ef89a2096bdc7389150ae621b5fb81c008b9accc2445fb",
    "0x14de114921829c059ca4934d5ff2c226452b93c4": "0x46e035701982cafe0c7af1d46a364750d600db3f7646bb70c521c37ec56de3c5",
    "0x952b61bd0185533e926154f0e4e98452ee1f1186": "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278",
}


async def main() -> None:
    from app.chain import RpcClient
    from app.constants import UNI_V4_POOL_MANAGER, V4_SWAP_TOPIC
    from app.replay import _norm_tx_hash, _resolve_buyers_batch, checksum, parse_token

    rpc = RpcClient()
    manager = checksum(UNI_V4_POOL_MANAGER)
    swaps = []
    xfers_by_tx: dict[str, list] = defaultdict(list)
    for _w, tx in KNOWN.items():
        r = await rpc.w3.eth.get_transaction_receipt(tx)
        for lg in r["logs"]:
            tops = lg["topics"]
            t0 = tops[0].hex() if hasattr(tops[0], "hex") else str(tops[0])
            if not t0.startswith("0x"):
                t0 = "0x" + t0
            if t0.lower() == V4_SWAP_TOPIC.lower():
                swaps.append(lg)
            if lg["address"].lower() == F.lower() and "ddf252ad" in t0.lower():
                xfers_by_tx[_norm_tx_hash(lg["transactionHash"])].append(lg)

    got = await _resolve_buyers_batch(
        rpc,
        token=F,
        pool_or_manager=manager,
        early_swaps=swaps,
        xfers_by_tx=dict(xfers_by_tx),
    )
    print("resolved", len(got), flush=True)
    for w, tx in KNOWN.items():
        r = got.get(_norm_tx_hash(tx))
        ok = bool(r and r[0].lower() == w.lower())
        print(
            f"{w[:12]} -> {None if not r else r[0][:12]} {'OK' if ok else 'MISS'}",
            flush=True,
        )

    res = await parse_token(
        rpc, F, mcap_threshold=30_000, exclude_honeypots=False, wallet_filters=None
    )
    by = {b.wallet.lower() for b in res.buyers}
    print("parse early", len(res.buyers), flush=True)
    for w in KNOWN:
        print(f" in_early {w[:12]} {w.lower() in by}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
