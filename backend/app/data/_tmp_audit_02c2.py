"""One-off audit: why 0x02c2fa… was missed. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
if not (DATA.parents[2] / "backend").exists():
    sys.path.insert(0, "/app/backend")
else:
    sys.path.insert(0, str(DATA.parents[2] / "backend"))

from app.buy_gate import method_is_creator_launch  # noqa: E402
from app.chain import RpcClient  # noqa: E402
from app.gmgn_portfolio import (  # noqa: E402
    fetch_unique_buys,
    fetch_wallet_activity_result,
    gmgn_api_configured,
)
from app.models import tokens_unique_period_hours  # noqa: E402
from app.wallet_metrics import batch_tokens_traded_7d, batch_wallet_balances  # noqa: E402

WALLET = "0x02c2faedb05cc1ddd40738a975f57d217ad33ecc"
OUT = DATA / "_tmp_audit_02c2.json"


def fmt_ts(ts) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load_json(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


async def peak_for(token: str) -> tuple[float | None, str | None]:
    try:
        from app.followup import estimate_token_peak_mcap

        peak = await estimate_token_peak_mcap(token, min_needed=1.0)
        return (float(peak) if peak else None), "gecko/ds"
    except Exception as exc:  # noqa: BLE001
        return None, f"err:{str(exc)[:80]}"


async def main() -> None:
    cfg = load_json("watch.json")
    hold_raw = load_json("watch_hold.json")
    seen_keys = {str(k).lower() for k in (load_json("watch_seen.json").get("keys") or [])}
    parsed_at = {str(k).lower(): v for k, v in (hold_raw.get("parsed_at") or {}).items()}
    parsed = {str(a).lower() for a in (hold_raw.get("parsed") or [])} | set(parsed_at)
    hold_map = {str(k).lower(): v for k, v in (hold_raw.get("hold") or {}).items()}

    wf = cfg.get("wallet") or {}
    sf = cfg.get("screen") or {}
    print("gmgn:", gmgn_api_configured(), flush=True)
    print("wallet filters:", json.dumps(wf), flush=True)
    print(
        "screen: min_ath=%s max_age=%s min_liq=%s"
        % (sf.get("min_ath_mcap"), sf.get("max_pair_age_hours"), sf.get("min_liq")),
        flush=True,
    )
    print(f"parsed={len(parsed)} hold={len(hold_map)} seen={len(seen_keys)}", flush=True)

    ub = await fetch_unique_buys(WALLET, max_pages=5)
    act = await fetch_wallet_activity_result(
        WALLET, event_types=["buy"], limit=50, max_pages=3
    )
    print(
        f"gmgn unique={len(ub.buys)} ok={ub.ok} rl={ub.rate_limited} raw={len(act.rows)}",
        flush=True,
    )

    period = wf.get("tokens_unique_period") or "30d"
    lookback_h = tokens_unique_period_hours(period)
    uniq = await batch_tokens_traded_7d(
        [WALLET], lookback_hours=lookback_h, enough=None, too_many=None
    )
    n_uniq = uniq.get(WALLET.lower())

    rpc = RpcClient()
    bals = await batch_wallet_balances(rpc, [WALLET])
    bal = bals.get(WALLET.lower())
    print(f"unique_{period}={n_uniq} (need [{wf.get('min_tokens_traded_7d')},"
          f"{wf.get('max_tokens_traded_7d')}]) bal={bal} "
          f"(min={wf.get('min_wallet_balance_eth')})", flush=True)

    min_ath = sf.get("min_ath_mcap")
    details = []
    for b in ub.buys:
        tok = b.token.lower()
        raw = None
        for r in act.rows:
            t = r.get("token")
            addr = (t.get("address") if isinstance(t, dict) else t) or ""
            if str(addr).lower() == tok:
                raw = r
                break
        method = (raw or {}).get("method")
        peak, src = await peak_for(tok)
        flags = []
        if tok in parsed:
            flags.append("PARSED")
        if tok in hold_map:
            flags.append("HOLD")
        if f"{WALLET.lower()}:{tok}" in seen_keys:
            flags.append("SEEN")
        if method and method_is_creator_launch(method):
            flags.append("CREATOR_LAUNCH")
        ath_pass = (
            (float(peak) >= float(min_ath)) if (peak and min_ath) else None
        )
        print(
            f"  {fmt_ts(b.timestamp)} {(b.symbol or '?'):12} {tok[:16]}… "
            f"cost={b.cost_usd} method={method} peak={peak} ath_pass={ath_pass} "
            f"flags={flags or ['NOT_IN_WATCH']} hold_ath="
            f"{(hold_map.get(tok) or {}).get('ath_mcap')} "
            f"parsed_at={parsed_at.get(tok)}",
            flush=True,
        )
        details.append(
            {
                "token": tok,
                "symbol": b.symbol,
                "ts": b.timestamp,
                "ts_fmt": fmt_ts(b.timestamp),
                "cost": b.cost_usd,
                "method": method,
                "peak": peak,
                "peak_src": src,
                "ath_pass": ath_pass,
                "flags": flags,
                "hold_entry": hold_map.get(tok),
                "parsed_at": parsed_at.get(tok),
            }
        )
        await asyncio.sleep(0.3)

    OUT.write_text(
        json.dumps(
            {
                "wallet": WALLET,
                "filters": {"wallet": wf, "screen": sf},
                "n_unique_gmgn": len(ub.buys),
                "n_unique_period": n_uniq,
                "period": period,
                "balance_eth": bal,
                "buys": details,
            },
            indent=2,
            default=str,
        )
    )
    print("wrote", OUT, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
