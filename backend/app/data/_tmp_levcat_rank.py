"""Rank of LEVCAT in the current drain-all parse queue. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

DATA = Path(__file__).resolve().parent
sys.path.insert(0, "/app/backend")

T = "0x02c2faedb05cc1ddd40738a975f57d217ad33ecc"


def load(name: str) -> dict:
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else {}


async def main() -> None:
    cfg = load("watch.json")
    hold_raw = load("watch_hold.json")
    hold = {str(k).lower(): v for k, v in (hold_raw.get("hold") or {}).items()}
    parsed_at = {str(k).lower(): v for k, v in (hold_raw.get("parsed_at") or {}).items()}

    from app.models import ScreenRequest
    from app.screener_feed import fetch_screened_tokens
    from app.watch_qualify import classify_for_parse, parse_queue_sort_key

    sf = cfg.get("screen") or {}
    min_ath = sf.get("min_ath_mcap")
    max_age = sf.get("max_pair_age_hours")
    sr = ScreenRequest(**sf)
    rows = await fetch_screened_tokens(sr.model_copy(update={"min_ath_mcap": None}), on_progress=None)
    now = time.time()
    d = classify_for_parse(
        rows,
        min_ath_mcap=min_ath,
        hold=hold,
        parsed=parsed_at,
        index_addresses=None,
        now=now,
        max_pair_age_hours=max_age,
    )
    age_by = {r.address.lower(): r.pair_age_hours for r in rows}
    ath_by = {
        r.address.lower(): max(float(r.ath_mcap or 0), float(r.market_cap or 0))
        for r in rows
    }
    for a, e in hold.items():
        ath_by.setdefault(a, float(e.get("ath_mcap") or 0))
    sym_by = {r.address.lower(): (r.symbol or "?") for r in rows}

    cands = sorted(
        d.candidates,
        key=lambda a: parse_queue_sort_key(
            a,
            hold=hold,
            pair_age_hours=age_by,
            ath_mcap=ath_by,
            max_pair_age_hours=max_age,
            now=now,
        ),
    )
    print(f"qualify={len(cands)}", flush=True)
    pos = next((i for i, a in enumerate(cands) if a.lower() == T), None)
    print(f"LEVCAT position: {pos} of {len(cands)}", flush=True)
    for i, a in enumerate(cands[:15]):
        mark = " <== LEVCAT" if a.lower() == T else ""
        print(
            f"  {i:3} {sym_by.get(a,'?'):12} ath={ath_by.get(a,0):>12,.0f} "
            f"age={age_by.get(a)}{mark}",
            flush=True,
        )
    if pos is not None and pos > 15:
        a = cands[pos]
        print(
            f"  {pos:3} {sym_by.get(a,'?'):12} ath={ath_by.get(a,0):>12,.0f} "
            f"age={age_by.get(a)} <== LEVCAT",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
