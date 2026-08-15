"""Generic miss audit: python _tmp_audit_any.py <address>. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")

from app.buy_gate import method_is_creator_launch  # noqa: E402
from app.chain import RpcClient  # noqa: E402
from app.gmgn_portfolio import (  # noqa: E402
    fetch_unique_buys,
    fetch_wallet_activity_result,
    gmgn_api_configured,
)
from app.models import tokens_unique_period_hours  # noqa: E402
from app.wallet_metrics import batch_tokens_traded_7d, batch_wallet_balances  # noqa: E402


def fmt(ts) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


async def main() -> None:
    addr = sys.argv[1].strip().lower()
    cfg = load("watch.json")
    hold_raw = load("watch_hold.json")
    hold = {str(k).lower(): v for k, v in (hold_raw.get("hold") or {}).items()}
    parsed_at = {str(k).lower(): v for k, v in (hold_raw.get("parsed_at") or {}).items()}
    parsed = {str(a).lower() for a in (hold_raw.get("parsed") or [])} | set(parsed_at)
    seen = {str(k).lower() for k in (load("watch_seen.json").get("keys") or [])}
    wf = cfg.get("wallet") or {}
    sf = cfg.get("screen") or {}

    rpc = RpcClient()
    is_eoa = await rpc.is_eoa(addr)
    print(f"=== {addr} ===", flush=True)
    print(f"is_eoa={is_eoa} (contract={not is_eoa})", flush=True)

    if not is_eoa:
        for name, sel in {
            "symbol()": "0x95d89b41",
            "token0()": "0x0dfe1681",
            "token1()": "0xd21220a7",
            "factory()": "0xc45a0155",
        }.items():
            try:
                res = await rpc.eth_call_raw({"to": addr, "data": sel})
                txt = ""
                if name == "symbol()" and res and len(res) > 130:
                    ln = int(res[66:130], 16)
                    txt = bytes.fromhex(res[130 : 130 + ln * 2]).decode("utf8", "ignore")
                print(f"  {name} -> {res[:80]} {txt}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  {name} -> err {str(exc)[:70]}", flush=True)
        print("  hold_entry:", json.dumps(hold.get(addr)), flush=True)
        print("  in_parsed:", addr in parsed, "parsed_at:", fmt(parsed_at.get(addr)), flush=True)

    print("gmgn:", gmgn_api_configured(), flush=True)
    ub = await fetch_unique_buys(addr, max_pages=5)
    act = await fetch_wallet_activity_result(addr, event_types=["buy"], limit=50, max_pages=3)
    print(f"unique_buys={len(ub.buys)} ok={ub.ok} rl={ub.rate_limited} raw={len(act.rows)}", flush=True)

    period = wf.get("tokens_unique_period") or "30d"
    uniq = await batch_tokens_traded_7d(
        [addr], lookback_hours=tokens_unique_period_hours(period), enough=None, too_many=None
    )
    n_uniq = uniq.get(addr)
    bals = await batch_wallet_balances(rpc, [addr])
    bal = bals.get(addr)
    min_u, max_u = wf.get("min_tokens_traded_7d"), wf.get("max_tokens_traded_7d")
    min_bal = wf.get("min_wallet_balance_eth")
    print(
        f"unique_{period}={n_uniq} need [{min_u},{max_u}] -> "
        f"{'PASS' if (n_uniq is not None and (min_u is None or n_uniq>=min_u) and (max_u is None or n_uniq<=max_u)) else 'FAIL'}",
        flush=True,
    )
    print(
        f"bal={bal} min={min_bal} -> "
        f"{'PASS' if (bal is not None and (min_bal is None or bal>=float(min_bal))) else 'FAIL'}",
        flush=True,
    )

    from app.followup import estimate_token_peak_mcap

    min_ath = sf.get("min_ath_mcap")
    for b in ub.buys:
        tok = b.token.lower()
        raw = None
        for r in act.rows:
            t = r.get("token")
            a2 = (t.get("address") if isinstance(t, dict) else t) or ""
            if str(a2).lower() == tok:
                raw = r
                break
        method = (raw or {}).get("method")
        try:
            est = await estimate_token_peak_mcap(tok, min_needed=1.0)
            peak = float(getattr(est, "peak", 0) or 0) or None
        except Exception:  # noqa: BLE001
            peak = None
        flags = []
        if tok in parsed:
            flags.append("PARSED")
        if tok in hold:
            flags.append("HOLD_PENDING" if (hold[tok] or {}).get("queued_at") else "HOLD")
        if f"{addr}:{tok}" in seen:
            flags.append("SEEN")
        if method and method_is_creator_launch(method):
            flags.append("CREATOR_LAUNCH")
        ath_pass = (peak >= float(min_ath)) if (peak and min_ath) else None
        print(
            f"  buy {fmt(b.timestamp)} {(b.symbol or '?'):12} {tok[:16]}… "
            f"cost={b.cost_usd} method={method} peak={peak} ath_pass={ath_pass} "
            f"hold_ath={(hold.get(tok) or {}).get('ath_mcap')} "
            f"flags={flags or ['NOT_IN_WATCH']}",
            flush=True,
        )
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    asyncio.run(main())
