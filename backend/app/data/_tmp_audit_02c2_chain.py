"""On-chain identity check for 0x02c2fa…. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")

from app.chain import RpcClient  # noqa: E402
from app.config import settings  # noqa: E402

W = "0x02c2faedb05cc1ddd40738a975f57d217ad33ecc"


async def main() -> None:
    rpc = RpcClient()
    is_eoa = await rpc.is_eoa(W)
    print(f"is_eoa={is_eoa} (contract={not is_eoa})", flush=True)

    if not is_eoa:
        sigs = {
            "token0()": "0x0dfe1681",
            "token1()": "0xd21220a7",
            "fee()": "0xddca3f43",
            "factory()": "0xc45a0155",
            "symbol()": "0x95d89b41",
        }
        for name, sel in sigs.items():
            try:
                res = await rpc.eth_call_raw({"to": W, "data": sel})
                print(f"  {name} -> {res}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  {name} -> err {str(exc)[:80]}", flush=True)

    # Blockscout: address info + token transfers count
    import httpx

    base = settings.blockscout_base_url.rstrip("/")
    async with httpx.AsyncClient(timeout=30) as cl:
        for path in (f"/api/v2/addresses/{W}", f"/api/v2/addresses/{W}/counters"):
            try:
                r = await cl.get(base + path)
                data = r.json() if r.status_code == 200 else {"status": r.status_code}
                print(path, json.dumps(data)[:600], flush=True)
            except Exception as exc:  # noqa: BLE001
                print(path, "err", str(exc)[:120], flush=True)
        try:
            r = await cl.get(
                base + f"/api/v2/addresses/{W}/token-transfers", params={"type": "ERC-20"}
            )
            items = (r.json() or {}).get("items") or []
            print(f"token_transfers={len(items)}", flush=True)
            for it in items[:10]:
                tok = (it.get("token") or {}).get("symbol")
                addr = (it.get("token") or {}).get("address")
                print(
                    f"  {it.get('timestamp')} {tok} {addr} "
                    f"from={(it.get('from') or {}).get('hash','')[:12]} "
                    f"to={(it.get('to') or {}).get('hash','')[:12]}",
                    flush=True,
                )
        except Exception as exc:  # noqa: BLE001
            print("transfers err", str(exc)[:150], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
