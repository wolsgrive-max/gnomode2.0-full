"""Deep dive NASDANQ for HVAT miss. Do not commit."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
WALLETS = [
    "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
    "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
]
DATA = Path(__file__).resolve().parent


def fmt_ts(ts: float | int | None) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


async def main() -> None:
    hold = json.loads((DATA / "watch_hold.json").read_text())
    seen = set(
        str(k).lower()
        for k in (json.loads((DATA / "watch_seen.json").read_text()).get("keys") or [])
    )
    parsed = {str(a).lower() for a in (hold.get("parsed") or [])}
    parsed_at = {str(k).lower(): v for k, v in (hold.get("parsed_at") or {}).items()}
    hold_map = {str(k).lower(): v for k, v in (hold.get("hold") or {}).items()}
    cfg = json.loads((DATA / "watch.json").read_text())

    print("TOKEN", TOKEN)
    print("in parsed?", TOKEN.lower() in parsed, "at", parsed_at.get(TOKEN.lower()))
    print("in hold?", TOKEN.lower() in hold_map, hold_map.get(TOKEN.lower()))
    print("seen pairs:", [k for k in seen if TOKEN.lower() in k])

    from app.token_index import token_index

    print("index peaks", token_index.mcap_peaks([TOKEN]))
    # entry lookup
    for meth in ("get", "get_entry", "lookup", "find"):
        if hasattr(token_index, meth):
            try:
                print(meth, getattr(token_index, meth)(TOKEN))
            except Exception as exc:
                print(meth, "err", exc)
    # scan known attrs for this token
    for attr in ("_by_addr", "_entries", "entries", "_data"):
        if hasattr(token_index, attr):
            obj = getattr(token_index, attr)
            print("attr", attr, type(obj), len(obj) if hasattr(obj, "__len__") else "")
            if isinstance(obj, dict) and TOKEN.lower() in {str(k).lower() for k in obj}:
                print("  FOUND in", attr, obj.get(TOKEN) or obj.get(TOKEN.lower()))

    from app.pools import fetch_dexscreener_pairs

    pairs = await fetch_dexscreener_pairs(TOKEN)
    print("dex pairs", len(pairs))
    for p in pairs[:5]:
        liq = p.get("liquidity")
        liq_usd = liq.get("usd") if isinstance(liq, dict) else liq
        created = p.get("pairCreatedAt")
        age_h = None
        if created:
            age_h = (time.time() * 1000 - float(created)) / 3_600_000
        print(
            {
                "mcap": p.get("marketCap") or p.get("fdv"),
                "liq": liq_usd,
                "created": created,
                "created_fmt": fmt_ts(float(created) / 1000) if created else None,
                "age_h": round(age_h, 2) if age_h is not None else None,
                "max_age": cfg["screen"].get("max_pair_age_hours"),
                "buys24": (p.get("txns") or {}).get("h24", {}).get("buys"),
                "sells24": (p.get("txns") or {}).get("h24", {}).get("sells"),
                "base": (p.get("baseToken") or {}).get("symbol"),
            }
        )

    from app.followup import estimate_token_peak_mcap

    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    print("peak_est", peak)

    # security / honeypot
    try:
        from app.security import resolve_honeypot_reason

        print("hp", await resolve_honeypot_reason(TOKEN))
    except Exception as exc:
        print("resolve_honeypot_reason:", type(exc).__name__, exc)
    try:
        from app.gmgn import check_token_security

        sec = await check_token_security(TOKEN)
        print("gmgn_sec", sec)
    except Exception as exc:
        print("gmgn_sec err", type(exc).__name__, exc)

    from app.gmgn_portfolio import fetch_wallet_activity_result

    for w in WALLETS:
        act = await fetch_wallet_activity_result(
            w, event_types=["buy"], limit=20, max_pages=1
        )
        print(f"\nwallet {w[:12]} raw_buys={len(act.rows)} ok={act.ok}")
        for r in act.rows:
            t = r.get("token")
            addr = (t.get("address") if isinstance(t, dict) else t) or ""
            if str(addr).lower() != TOKEN.lower():
                continue
            price = r.get("price_usd")
            cost = r.get("cost_usd")
            print(
                {
                    "ts": fmt_ts(r.get("timestamp")),
                    "cost": cost,
                    "price": price,
                    "method": r.get("method"),
                    "launchpad": r.get("launchpad") or r.get("launchpad_platform"),
                    "tx": (r.get("tx_hash") or "")[:18],
                    "from": r.get("from_address"),
                    "to": r.get("to_address"),
                    "is_open_or_close": r.get("is_open_or_close"),
                }
            )

    # Would screen filters pass? Use ScreenRequest on this token via index/screener helpers
    from app.models import ScreenRequest, ScreenedToken
    from app.screener import _passes_primary

    if pairs:
        p = pairs[0]
        liq = p.get("liquidity")
        liq_usd = float((liq.get("usd") if isinstance(liq, dict) else liq) or 0)
        mcap = float(p.get("marketCap") or p.get("fdv") or 0)
        created = p.get("pairCreatedAt")
        age_h = (
            (time.time() * 1000 - float(created)) / 3_600_000 if created else None
        )
        row = ScreenedToken(
            address=TOKEN,
            symbol=(p.get("baseToken") or {}).get("symbol") or "NASDANQ",
            market_cap=mcap,
            ath_mcap=float(peak or mcap or 0),
            liquidity=liq_usd,
            pair_age_hours=age_h,
            traders=None,
            is_honeypot=False,
        )
        req = ScreenRequest(
            **{
                k: v
                for k, v in cfg["screen"].items()
                if k in ScreenRequest.model_fields
            }
        )
        print("\nscreen row", row.model_dump())
        print("passes_primary?", _passes_primary(row, req))
        print("ATH gate: peak", peak, ">=", req.min_ath_mcap, "?", (peak or 0) >= float(req.min_ath_mcap or 0))

    # Check tx method via blockscout for creator
    from app.blockscout import _get_json, blockscout_api_base

    audit = json.loads((DATA / "_tmp_audit_two_wallets.json").read_text())
    for wr in audit["wallets"]:
        for b in wr["buys"]:
            tx = b.get("tx")
            if not tx:
                continue
            url = f"{blockscout_api_base()}/transactions/{tx}"
            try:
                data = await _get_json(url)
                method = data.get("method") or data.get("decoded_input", {}).get("method_call")
                frm = (data.get("from") or {})
                if isinstance(frm, dict):
                    frm = frm.get("hash")
                print(
                    "tx",
                    tx[:16],
                    "method=",
                    method,
                    "from=",
                    frm,
                    "wallet=",
                    wr["wallet"][:12],
                )
            except Exception as exc:
                print("tx fetch", tx[:16], exc)


if __name__ == "__main__":
    asyncio.run(main())
