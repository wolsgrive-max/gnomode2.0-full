"""Is 0x02c2fa… (LEVCAT token) in watch hold/parsed? Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")

T = "0x02c2faedb05cc1ddd40738a975f57d217ad33ecc"


def fmt(ts) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


async def main() -> None:
    hold_raw = load("watch_hold.json")
    hold = {str(k).lower(): v for k, v in (hold_raw.get("hold") or {}).items()}
    parsed_at = {str(k).lower(): v for k, v in (hold_raw.get("parsed_at") or {}).items()}
    parsed = {str(a).lower() for a in (hold_raw.get("parsed") or [])} | set(parsed_at)
    seen = {str(k).lower() for k in (load("watch_seen.json").get("keys") or [])}
    cfg = load("watch.json")

    print("in_hold:", json.dumps(hold.get(T)), flush=True)
    print("in_parsed:", T in parsed, "parsed_at:", fmt(parsed_at.get(T)), flush=True)
    hits = [k for k in seen if T in k]
    print("seen_pairs:", len(hits), hits[:10], flush=True)

    from app.followup import estimate_token_peak_mcap
    from app.token_index import token_index

    try:
        peaks = token_index.mcap_peaks([T])
        print("index_peak:", peaks.get(T), flush=True)
    except Exception as exc:  # noqa: BLE001
        print("index err", str(exc)[:120], flush=True)

    peak = await estimate_token_peak_mcap(T, min_needed=1.0)
    print("gecko/ds peak:", peak, "min_ath:", (cfg.get("screen") or {}).get("min_ath_mcap"), flush=True)

    # Screener row?
    from app.models import ScreenRequest
    from app.screener_feed import fetch_screened_tokens

    sr = ScreenRequest(**(cfg.get("screen") or {}))
    rows = await fetch_screened_tokens(sr.model_copy(update={"min_ath_mcap": None}), on_progress=None)
    match = [r for r in rows if r.address.lower() == T]
    print(f"screener rows={len(rows)} match={len(match)}", flush=True)
    for r in match:
        print(
            f"  {r.symbol} mcap={r.market_cap} ath={r.ath_mcap} "
            f"liq={r.liquidity_usd} age={r.pair_age_hours}",
            flush=True,
        )

    # Early buyers via parse path
    from app.models import ParseRequest
    from app.replay import parse_token
    from app.chain import RpcClient

    wf = cfg.get("wallet") or {}
    req = ParseRequest(
        tokens=[T],
        mcap_threshold=wf.get("mcap_threshold"),
        exclude_honeypots=wf.get("exclude_honeypots") or False,
        min_wallet_balance_eth=wf.get("min_wallet_balance_eth"),
        max_wallet_balance_eth=wf.get("max_wallet_balance_eth"),
        min_hold_time_minutes=wf.get("min_hold_time_minutes"),
        max_hold_time_minutes=wf.get("max_hold_time_minutes"),
        min_tokens_traded_7d=wf.get("min_tokens_traded_7d"),
        max_tokens_traded_7d=wf.get("max_tokens_traded_7d"),
        tokens_unique_period=wf.get("tokens_unique_period") or "30d",
    )
    rpc = RpcClient()
    try:
        res = await parse_token(rpc, T, req, threshold=float(wf.get("mcap_threshold") or 30000))
        buyers = getattr(res, "buyers", None) or []
        print(f"parse buyers={len(buyers)} err={getattr(res, 'error', None)}", flush=True)
        for b in buyers[:20]:
            print(f"  {b.wallet} mcap={getattr(b,'mcap_at_buy',None)}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print("parse err", str(exc)[:300], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
