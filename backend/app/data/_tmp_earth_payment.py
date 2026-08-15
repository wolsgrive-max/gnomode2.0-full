"""EARTH buy payment via RPC receipt + GMGN activity. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys

sys.path.insert(0, "/app/backend")

from app.chain import RpcClient, topic_address  # noqa: E402
from app.constants import TRANSFER_TOPIC, USDG, WETH  # noqa: E402
from app.gmgn_portfolio import fetch_wallet_activity_result  # noqa: E402

W = "0x3da325c18f5b1b4805c09bc93e8df12e69ad1add"
EARTH = "0x320d9b0f1b438567d28452b715f8766a7617043e"
SPCX = "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea"

TXS = [
    "0x8435ade3947d768a4756db3aa611357e02a4ec98f99a0c4506e90fc2d47f1e1d",  # first buy
    "0x03419f593e4945fc80514bdeffebaef782445e5c60e5f286b94b48823336121a",  # execute
    "0xd8d393f4c206fb26adf9b2b29866ec1afd5b15d572e1ee863f70a854de209d22",  # later swap
    "0x52e65831e03eaad638cab1d09dcad42d9b169f7a0b953cf2faf44c24dd81741b",  # SPCX buy
]


def label(addr: str) -> str:
    a = addr.lower()
    if a == W.lower():
        return "WALLET"
    if a == EARTH:
        return "EARTH"
    if a == SPCX:
        return "SPCX"
    if a == WETH.lower():
        return "WETH"
    if a == USDG.lower():
        return "USDG"
    if a == "0x0000000000000000000000000000000000000000":
        return "ZERO"
    return addr[:12]


async def main() -> None:
    rpc = RpcClient()
    for tx in TXS:
        print(f"\n=== {tx} ===", flush=True)
        try:
            raw = await rpc._call(
                lambda t=tx: rpc.w3.eth.get_transaction(t)
            )
            value = int(raw.get("value") or 0)
            print(
                f"tx.from={raw.get('from')} to={raw.get('to')} "
                f"value_eth={value/1e18}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            print("get_tx err", str(exc)[:120], flush=True)
            raw = None

        try:
            rcpt = await rpc._call(
                lambda t=tx: rpc.w3.eth.get_transaction_receipt(t)
            )
        except Exception as exc:  # noqa: BLE001
            print("receipt err", str(exc)[:120], flush=True)
            continue

        sent_from_wallet = []
        got_by_wallet = []
        for lg in rcpt.get("logs") or []:
            topics = lg.get("topics") or []
            if not topics:
                continue
            t0 = topics[0].hex() if hasattr(topics[0], "hex") else str(topics[0])
            if t0.lower() != TRANSFER_TOPIC.lower():
                continue
            if len(topics) < 3:
                continue
            frm = topic_address(topics[1]).lower()
            to = topic_address(topics[2]).lower()
            token = str(lg.get("address") or "").lower()
            data = lg.get("data") or "0x"
            try:
                amount = int(data, 16) if isinstance(data, str) else int(data.hex(), 16)
            except Exception:  # noqa: BLE001
                amount = 0
            row = f"{label(token):6} {amount}  {label(frm)} -> {label(to)}"
            if frm == W.lower():
                sent_from_wallet.append(row)
            if to == W.lower():
                got_by_wallet.append(row)

        print("WALLET SENT:", flush=True)
        for r in sent_from_wallet or ["  (none)"]:
            print(" ", r if r.startswith(" ") else r, flush=True)
        if not sent_from_wallet and (raw and int(raw.get("value") or 0) > 0):
            print(f"  native ETH {int(raw['value'])/1e18}", flush=True)
        print("WALLET GOT:", flush=True)
        for r in got_by_wallet or ["  (none)"]:
            print(" ", r, flush=True)

    print("\n=== GMGN activity buys ===", flush=True)
    act = await fetch_wallet_activity_result(W, event_types=["buy"], limit=50, max_pages=2)
    for r in act.rows:
        t = r.get("token")
        addr = (t.get("address") if isinstance(t, dict) else t) or ""
        sym = (t.get("symbol") if isinstance(t, dict) else None)
        print(
            f"  {r.get('timestamp')} {sym} {str(addr)[:14]}… "
            f"method={r.get('method')} cost_usd={r.get('cost_usd')} "
            f"price={r.get('price_usd')} tx={str(r.get('tx_hash') or '')[:16]}",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
