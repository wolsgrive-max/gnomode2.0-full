"""One-off audit: why wallets missed Хвать ingest. Do not commit."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Load .env without printing secrets
_ROOT = Path(__file__).resolve().parents[3]
_env = _ROOT / ".env"
if _env.exists():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

sys.path.insert(0, str(_ROOT / "backend"))

from app.gmgn_portfolio import (  # noqa: E402
    fetch_unique_buys,
    fetch_wallet_activity_result,
    gmgn_api_configured,
)

WALLETS = [
    "0xbcad3e14df548a11b7aad8d633fe9468d22b552f",
    "0x193363b4c0fff9f10500eb3f49ae985f4e2038b6",
    "0xe6a8a653e3f96cc5095eafc669a942a2aa4b8948",
    "0x321301b0d0eb9ab5d1f5a479f31bf0b9bf704f7d",
    "0xb9c9d36b9562199c236080dc9b36304ba9ef4375",
    "0xaff478e532cdb96df78d326ab6a702e95ab9ba51",
    "0x7a61aa6b0a412b56ff6ad5e1918c6024fc8cfafd",
    "0x9580ce3c36d8297e49c65999933da3ac94226e85",
    "0x82e70369817baf7f8d9427158d07a38881df17be",
    "0xf526a0d140162a0cafce077b1d93598e0ab881f0",
]

DATA = Path(__file__).resolve().parent
OUT = DATA / "_tmp_hvat_miss_audit.json"


def fmt_ts(ts: int | float | None) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


async def main() -> None:
    print("gmgn configured:", gmgn_api_configured(), flush=True)
    hold = json.loads((DATA / "watch_hold.json").read_text())
    seen_keys = {
        str(k).lower()
        for k in (json.loads((DATA / "watch_seen.json").read_text()).get("keys") or [])
    }
    parsed = {str(a).lower() for a in (hold.get("parsed") or [])}
    parsed_at = {str(k).lower(): v for k, v in (hold.get("parsed_at") or {}).items()}
    hold_map = {str(k).lower(): v for k, v in (hold.get("hold") or {}).items()}

    results: list[dict] = []
    for w in WALLETS:
        print(f"fetching {w[:12]}...", flush=True)
        act = await fetch_wallet_activity_result(
            w, event_types=["buy"], limit=50, max_pages=3
        )
        ub = await fetch_unique_buys(w, max_pages=3)
        buys = ub.buys
        print(
            f"  ok={ub.ok} rl={ub.rate_limited} unique={len(buys)} "
            f"raw={len(act.rows)} act_ok={act.ok}",
            flush=True,
        )
        for b in buys[:15]:
            flags = []
            if b.token in parsed:
                flags.append("PARSED")
            if b.token in seen_keys:
                flags.append("SEEN")
            if b.token in hold_map:
                flags.append("HOLD")
            flag_s = "/".join(flags) if flags else "NOT_IN_WATCH"
            sym = (b.symbol or "?")[:12]
            print(
                f"    {fmt_ts(b.timestamp)} {sym:12} {b.token[:14]}… "
                f"cost={b.cost_usd} {flag_s}",
                flush=True,
            )
        methods: dict[str, int] = {}
        for r in act.rows:
            m = str(r.get("method") or r.get("event_type") or r.get("type") or "")
            methods[m] = methods.get(m, 0) + 1
        print("  methods:", methods, flush=True)
        results.append(
            {
                "wallet": w,
                "ok": ub.ok,
                "rate_limited": ub.rate_limited,
                "n_unique": len(buys),
                "buys": [
                    {
                        "token": b.token,
                        "symbol": b.symbol,
                        "ts": b.timestamp,
                        "cost": b.cost_usd,
                        "tx": b.tx_hash,
                        "parsed": b.token in parsed,
                        "seen": b.token in seen_keys,
                        "hold": b.token in hold_map,
                        "parsed_at": parsed_at.get(b.token),
                    }
                    for b in buys
                ],
                "raw_sample": [
                    {
                        "et": r.get("event_type") or r.get("type"),
                        "method": r.get("method"),
                        "ts": r.get("timestamp"),
                        "token": (
                            (r.get("token") or {}).get("address")
                            if isinstance(r.get("token"), dict)
                            else r.get("token")
                        ),
                        "symbol": (
                            (r.get("token") or {}).get("symbol")
                            if isinstance(r.get("token"), dict)
                            else None
                        ),
                        "cost_usd": r.get("cost_usd"),
                        "price_usd": r.get("price_usd") or r.get("token_price_usd"),
                        "amount_usd": r.get("amount_usd"),
                        "keys": sorted(r.keys()),
                    }
                    for r in act.rows[:10]
                ],
            }
        )
        await asyncio.sleep(1.0)

    OUT.write_text(json.dumps(results, indent=2))
    print("wrote", OUT, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
