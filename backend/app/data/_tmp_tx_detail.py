"""Inspect a tx: did the wallet actually pay quote for the token?"""
from __future__ import annotations

import asyncio
import json
import sys

sys.path.insert(0, "/app/backend")

from app.buy_gate import QUOTE_TOKENS, wallet_sent_quote_in_tx  # noqa: E402
from app.wallet_metrics import _bs_get, blockscout_api_base  # noqa: E402


async def main() -> None:
    tx = sys.argv[1]
    w = sys.argv[2].strip().lower()
    base = blockscout_api_base()
    r = await _bs_get(f"{base}/transactions/{tx}", {})
    if r is None or r.status_code != 200:
        print("tx status", getattr(r, "status_code", None), flush=True)
        return
    d = r.json()
    print(
        json.dumps(
            {
                "method": d.get("method"),
                "value": d.get("value"),
                "from": (d.get("from") or {}).get("hash"),
                "to": (d.get("to") or {}).get("hash"),
                "to_name": (d.get("to") or {}).get("name"),
                "decoded": (d.get("decoded_input") or {}).get("method_call"),
            },
            indent=2,
        ),
        flush=True,
    )
    paid = await wallet_sent_quote_in_tx(w, tx)
    print("wallet_sent_quote_in_tx =", paid, flush=True)

    r2 = await _bs_get(f"{base}/transactions/{tx}/token-transfers", {})
    if r2 is not None and r2.status_code == 200:
        for it in (r2.json() or {}).get("items") or []:
            tok = it.get("token") or {}
            addr = str(tok.get("address") or "").lower()
            print(
                f"  {tok.get('symbol')} {addr[:16]}… quote={addr in QUOTE_TOKENS} "
                f"from={(it.get('from') or {}).get('hash','')[:14]} "
                f"to={(it.get('to') or {}).get('hash','')[:14]} "
                f"val={(it.get('total') or {}).get('value')}",
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
