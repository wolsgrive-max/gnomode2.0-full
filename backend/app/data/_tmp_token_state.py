"""Token state in watch: python _tmp_token_state.py <token>"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")


def load(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


def fmt(ts) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


async def main() -> None:
    t = sys.argv[1].strip().lower()
    cfg = load("watch.json")
    raw = load("watch_hold.json")
    hold = {str(k).lower(): v for k, v in (raw.get("hold") or {}).items()}
    parsed_at = {str(k).lower(): v for k, v in (raw.get("parsed_at") or {}).items()}
    parsed = {str(a).lower() for a in (raw.get("parsed") or [])} | set(parsed_at)
    seen = {str(k).lower() for k in (load("watch_seen.json").get("keys") or [])}

    print("hold:", json.dumps(hold.get(t)), flush=True)
    print("parsed:", t in parsed, "at", fmt(parsed_at.get(t)), flush=True)
    print("seen_pairs:", len([k for k in seen if t in k]), flush=True)

    from app.followup import estimate_token_peak_mcap
    from app.models import ScreenRequest
    from app.screener_feed import fetch_screened_tokens

    sf = cfg.get("screen") or {}
    est = await estimate_token_peak_mcap(t, min_needed=1.0)
    print("gecko/ds peak:", est, "min_ath:", sf.get("min_ath_mcap"), flush=True)

    sr = ScreenRequest(**sf)
    rows = await fetch_screened_tokens(
        sr.model_copy(update={"min_ath_mcap": None, "max_pair_age_hours": None}),
        on_progress=None,
    )
    m = [r for r in rows if r.address.lower() == t]
    print(f"screener rows={len(rows)} match={len(m)} (max_age filter off)", flush=True)
    for r in m:
        print(
            f"  {r.symbol} mcap={r.market_cap} ath={r.ath_mcap} liq={r.liquidity_usd} "
            f"age={r.pair_age_hours} (max_age cfg={sf.get('max_pair_age_hours')})",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
