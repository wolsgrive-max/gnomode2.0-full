"""Dump raw Transfer logs from EARTH buy receipts."""
from __future__ import annotations

import asyncio
import sys

sys.path.insert(0, "/app/backend")

from app.chain import RpcClient, topic_address  # noqa: E402
from app.constants import TRANSFER_TOPIC  # noqa: E402

W = "0x3da325c18f5b1b4805c09bc93e8df12e69ad1add"
KNOWN = {
    "0x320d9b0f1b438567d28452b715f8766a7617043e": "EARTH",
    "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea": "SPCX",
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73": "WETH",
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168": "USDG",
}

TXS = [
    ("first_buy", "0x8435ade3947d768a4756db3aa611357e02a4ec98f99a0c4506e90fc2d47f1e1d"),
    ("execute_eth", "0x03419f593e4945fc80514bdeffebaef782445e5c60e5f286b94b48823336121a"),
    ("swap_later", "0xd8d393f4c206fb26adf9b2b29866ec1afd5b15d572e1ee863f70a854de209d22"),
    ("spcx_buy", "0x52e65831e03eaad638cab1d09dcad42d9b169f7a0b953cf2faf44c24dd81741b"),
]


def _hex(x) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return "0x" + x.hex()
    if hasattr(x, "hex") and not isinstance(x, str):
        return x.hex() if str(x.hex()).startswith("0x") else "0x" + x.hex()
    return str(x)


async def main() -> None:
    rpc = RpcClient()
    receipts = await rpc.batch_get_receipts([t for _, t in TXS])
    for name, tx in TXS:
        print(f"\n=== {name} {tx[:18]}… ===", flush=True)
        rcpt = receipts.get(tx.lower())
        if not rcpt:
            print("  no receipt", flush=True)
            continue
        logs = rcpt.get("logs") or []
        print(f"  logs={len(logs)}", flush=True)
        n_xfer = 0
        for lg in logs:
            topics = lg.get("topics") or []
            if not topics:
                continue
            t0 = _hex(topics[0]).lower()
            if t0 != TRANSFER_TOPIC.lower():
                continue
            n_xfer += 1
            if len(topics) < 3:
                continue
            frm = topic_address(topics[1]).lower()
            to = topic_address(topics[2]).lower()
            token = _hex(lg.get("address")).lower()
            data = _hex(lg.get("data"))
            try:
                amount = int(data, 16) if data and data != "0x" else 0
            except Exception:  # noqa: BLE001
                amount = -1
            mark = ""
            if frm == W.lower() or to == W.lower():
                mark = " <== WALLET"
            sym = KNOWN.get(token, token[:12])
            print(
                f"  Transfer {sym:6} {amount}  {frm[:12]}->{to[:12]}{mark}",
                flush=True,
            )
        print(f"  transfer_events={n_xfer}", flush=True)

        # Identify contract at first_buy target
        if name == "first_buy":
            txo = await rpc._call(lambda: rpc.w3.eth.get_transaction(tx))
            to = txo.get("to")
            print(f"  to={to}", flush=True)
            try:
                code = await rpc._call(lambda: rpc.w3.eth.get_code(to))
                print(f"  to_code_len={len(code or b'')}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print("  code err", exc, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
