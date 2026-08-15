"""Which tokens does the unique-buys counter see? python _tmp_unique_detail.py <addr>"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/app/backend")

from app.buy_gate import (  # noqa: E402
    QUOTE_TOKENS,
    is_dex_buy_transfer,
    is_wallet_initiated_buy,
    method_is_creator_launch,
    method_is_non_buy,
    transaction_sender,
)
from app.wallet_metrics import _bs_get, _parse_ts, blockscout_api_base  # noqa: E402


async def main() -> None:
    w = sys.argv[1].strip().lower()
    hours = float(sys.argv[2]) if len(sys.argv) > 2 else 720.0
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    url = f"{blockscout_api_base()}/addresses/{w}/token-transfers"
    params: dict[str, object] = {"filter": "to"}
    counted: set[str] = set()
    page = 0
    while page < 6:
        resp = await _bs_get(url, params)
        if resp is None or resp.status_code != 200:
            print("bs status", getattr(resp, "status_code", None), flush=True)
            break
        data = resp.json()
        items = data.get("items") or []
        stop = False
        for it in items:
            ts = _parse_ts(it.get("timestamp"))
            if ts is not None and ts < cutoff:
                stop = True
                break
            tok = it.get("token") or {}
            addr = str(tok.get("address") or tok.get("address_hash") or "").lower()
            sym = tok.get("symbol")
            method = it.get("method")
            txh = it.get("transaction_hash") or it.get("tx_hash")
            if not addr:
                continue
            quote = addr in QUOTE_TOKENS
            dex = is_dex_buy_transfer(it, w)
            nonbuy = method_is_non_buy(method)
            creator = method_is_creator_launch(method)
            ok = await is_wallet_initiated_buy(it, w)
            sender = await transaction_sender(txh) if txh else None
            mark = ""
            if ok and not quote and addr not in counted:
                counted.add(addr)
                mark = " <== COUNTED"
            print(
                f"  {it.get('timestamp')} {str(sym):12} {addr[:16]}… "
                f"method={method} quote={quote} dex_buy={dex} non_buy={nonbuy} "
                f"creator={creator} initiated={ok} "
                f"tx_from={(sender or '')[:12]} tx={str(txh)[:14]}{mark}",
                flush=True,
            )
        nxt = data.get("next_page_params")
        if stop or not nxt or not items:
            break
        params = dict(nxt)
        params.setdefault("filter", "to")
        page += 1
    print(f"COUNTED unique = {len(counted)}: {sorted(counted)}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
