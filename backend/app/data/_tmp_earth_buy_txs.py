"""Inspect EARTHCOIN buy txs: what did the wallet actually spend?"""
from __future__ import annotations

import asyncio
import json
import sys

sys.path.insert(0, "/app/backend")

from app.buy_gate import QUOTE_TOKENS, wallet_sent_quote_in_tx  # noqa: E402
from app.wallet_metrics import _bs_get, blockscout_api_base  # noqa: E402

W = "0x3da325c18f5b1b4805c09bc93e8df12e69ad1add"
EARTH = "0x320d9b0f1b438567d28452b715f8766a7617043e"
SPCX = "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea"

# From earlier unique audit (EARTHCOIN buys)
TXS = [
    "0x8435ade3947d",  # first buy — need full hash from transfers
]


async def earth_txs() -> list[str]:
    base = blockscout_api_base()
    r = await _bs_get(
        f"{base}/addresses/{W}/token-transfers",
        {"filter": "to", "token": EARTH},
    )
    if r is None or r.status_code != 200:
        print("earth transfers status", getattr(r, "status_code", None), flush=True)
        return []
    out = []
    for it in (r.json() or {}).get("items") or []:
        tx = it.get("transaction_hash") or it.get("tx_hash")
        print(
            f"EARTH xfer {it.get('timestamp')} method={it.get('method')} "
            f"tx={tx} val={(it.get('total') or {}).get('value')}",
            flush=True,
        )
        if tx:
            out.append(tx)
    return out


async def inspect_tx(tx: str) -> None:
    base = blockscout_api_base()
    r = await _bs_get(f"{base}/transactions/{tx}", {})
    if r is None or r.status_code != 200:
        print(f"tx {tx[:14]} status={getattr(r,'status_code',None)}", flush=True)
        return
    d = r.json()
    print("\n===", tx, "===", flush=True)
    print(
        json.dumps(
            {
                "method": d.get("method"),
                "value_wei": d.get("value"),
                "value_eth": (int(d.get("value") or 0) / 1e18) if d.get("value") else 0,
                "from": (d.get("from") or {}).get("hash"),
                "to": (d.get("to") or {}).get("hash"),
                "to_name": (d.get("to") or {}).get("name"),
                "decoded": (d.get("decoded_input") or {}).get("method_call"),
            },
            indent=2,
        ),
        flush=True,
    )
    paid_quote = await wallet_sent_quote_in_tx(W, tx)
    print("wallet_sent_WETH/USDG =", paid_quote, flush=True)

    r2 = await _bs_get(f"{base}/transactions/{tx}/token-transfers", {})
    if r2 is None or r2.status_code != 200:
        print("transfers status", getattr(r2, "status_code", None), flush=True)
        return
    for it in (r2.json() or {}).get("items") or []:
        tok = it.get("token") or {}
        addr = str(tok.get("address") or "").lower()
        frm = ((it.get("from") or {}).get("hash") or "").lower()
        to = ((it.get("to") or {}).get("hash") or "").lower()
        role = []
        if frm == W:
            role.append("WALLET_SENT")
        if to == W:
            role.append("WALLET_GOT")
        if addr in QUOTE_TOKENS:
            role.append("QUOTE")
        if addr == SPCX:
            role.append("SPCX")
        if addr == EARTH:
            role.append("EARTH")
        print(
            f"  {tok.get('symbol'):10} {addr[:12]}… "
            f"from={frm[:12]} to={to[:12]} "
            f"val={(it.get('total') or {}).get('value')} "
            f"{'/'.join(role) or '-'}",
            flush=True,
        )


async def main() -> None:
    txs = await earth_txs()
    # Also include the first SPCX swap for context
    for tx in txs[:8]:
        await inspect_tx(tx)
        await asyncio.sleep(0.3)


if __name__ == "__main__":
    asyncio.run(main())
