"""Is the token in the donor screen with all filters relaxed?"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")

T = sys.argv[1].strip().lower() if len(sys.argv) > 1 else ""


def load(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


async def main() -> None:
    cfg = load("watch.json")
    sf = dict(cfg.get("screen") or {})
    from app.models import ScreenRequest
    from app.screener_feed import fetch_screened_tokens, using_remote_screener

    print("remote_screener:", using_remote_screener(), flush=True)
    print("cfg screen:", json.dumps(sf), flush=True)

    relaxed = ScreenRequest(
        **{
            **sf,
            "min_ath_mcap": None,
            "max_pair_age_hours": None,
            "min_liq": 0,
            "min_market_cap": None,
            "max_results": 5000,
        }
    )
    rows = await fetch_screened_tokens(relaxed, on_progress=None)
    print(f"relaxed rows={len(rows)}", flush=True)
    m = [r for r in rows if r.address.lower() == T]
    print(f"match={len(m)}", flush=True)
    for r in m:
        print(
            f"  {r.symbol} mcap={r.market_cap} ath={r.ath_mcap} "
            f"liq={r.liquidity_usd} age={r.pair_age_hours}",
            flush=True,
        )
    if not m:
        from app.token_index import token_index

        known = token_index.known_addresses()
        print("in_local_index:", T in {a.lower() for a in known}, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
