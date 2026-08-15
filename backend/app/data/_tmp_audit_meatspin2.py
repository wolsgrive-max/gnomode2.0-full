"""MEATSPIN screen/pool/peak. Do not commit."""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

TOKEN = "0x285ec8958774074cd924103d040fb92be9d2d42a"
TX = "0x34b7547c88b9275828b6e87e340a10175c3b865c7004ab746536e0e5def556a6"
DATA = Path(__file__).resolve().parent


async def main() -> None:
    cfg = json.loads((DATA / "watch.json").read_text())

    from app.token_index import token_index
    from app.followup import estimate_token_peak_mcap
    from app.pools import fetch_dexscreener_pairs, pick_best_pool
    from app.screener import _passes_primary, _in_range
    from app.models import ScreenRequest, ScreenedToken
    from app.chain import RpcClient
    from app.replay import estimate_mcap_at_tx
    from app.blockscout import _get_json
    from app.security import honeypot_reason_for_token

    pool = {t.address.lower(): t for t in token_index.get_tokens()}
    print("pool", len(pool), "in?", TOKEN in pool, flush=True)
    hits = [
        t
        for t in pool.values()
        if "meat" in (t.symbol or "").lower() or t.address.lower() == TOKEN
    ]
    print(
        "hits",
        [
            (h.symbol, h.address, h.ath_mcap, h.liquidity_usd, h.pair_age_hours)
            for h in hits[:8]
        ],
        flush=True,
    )

    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    pairs = await fetch_dexscreener_pairs(TOKEN)
    print("n_pairs", len(pairs or []), flush=True)
    mcap = liq = 0.0
    age = None
    if pairs:
        for i, p in enumerate(pairs[:6]):
            pl = p.get("liquidity")
            plu = float((pl.get("usd") if isinstance(pl, dict) else pl) or 0)
            mc = float(p.get("marketCap") or p.get("fdv") or 0)
            cr = p.get("pairCreatedAt")
            ag = (time.time() * 1000 - float(cr)) / 3600000 if cr else None
            print(
                f" pair[{i}]",
                p.get("dexId"),
                p.get("pairAddress"),
                "liq",
                plu,
                "mcap",
                mc,
                "age_h",
                ag,
                "base",
                (p.get("baseToken") or {}).get("symbol"),
                flush=True,
            )
            if i == 0:
                mcap, liq, age = mc, plu, ag
    print("peak", peak, flush=True)

    req = ScreenRequest(
        **{k: v for k, v in cfg["screen"].items() if k in ScreenRequest.model_fields}
    )
    row = ScreenedToken(
        address=TOKEN,
        symbol="MEATSPIN",
        name="MEATSPIN",
        market_cap=mcap,
        ath_mcap=float(getattr(peak, "peak", None) or mcap or 0),
        liquidity_usd=liq,
        pair_age_hours=age,
        traders_24h=0,
        buys_24h=0,
        sells_24h=0,
    )
    print("passes_primary", _passes_primary(row, req), flush=True)
    print(
        "liq",
        _in_range(row.liquidity_usd, req.min_liq, req.max_liq),
        liq,
        "min",
        req.min_liq,
        flush=True,
    )
    print(
        "ath",
        row.ath_mcap,
        ">=",
        req.min_ath_mcap,
        row.ath_mcap >= float(req.min_ath_mcap or 0),
        flush=True,
    )
    print("age", age, "max", req.max_pair_age_hours, flush=True)

    try:
        hp = await honeypot_reason_for_token(TOKEN)
        print("honeypot_reason", hp, flush=True)
    except Exception as e:
        print("honeypot err", type(e).__name__, e, flush=True)

    rpc = RpcClient()
    print("best_pool", await pick_best_pool(rpc, TOKEN), flush=True)
    print("tx_mcap", await estimate_mcap_at_tx(TOKEN, TX, rpc=rpc), flush=True)
    code, info = await _get_json(f"/tokens/{TOKEN}")
    if isinstance(info, dict):
        print(
            "token",
            {
                k: info.get(k)
                for k in ("symbol", "name", "decimals", "total_supply", "holder_count")
            },
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
