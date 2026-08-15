"""NASDANQ index membership + buy mcap. Do not commit."""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
DATA = Path(__file__).resolve().parent


async def main() -> None:
    cfg = json.loads((DATA / "watch.json").read_text())
    audit = json.loads((DATA / "_tmp_audit_two_wallets.json").read_text())

    from app.token_index import token_index
    from app.followup import estimate_token_peak_mcap
    from app.pools import fetch_dexscreener_pairs
    from app.screener import _passes_primary, _in_range
    from app.models import ScreenRequest, ScreenedToken
    from app.blockscout import _get_json
    from app.gmgn_portfolio import fetch_wallet_activity_result
    from app.buy_gate import method_is_launch_buy, method_is_creator_launch

    pool = token_index.get_tokens()
    by = {t.address.lower(): t for t in pool}
    print("pool", len(pool), "NASDANQ in pool?", TOKEN.lower() in by)
    if TOKEN.lower() in by:
        print("index row", by[TOKEN.lower()].model_dump())
    else:
        hits = [t for t in pool if "nasdanq" in (t.symbol or "").lower()]
        print(
            "symbol hits",
            [
                (h.symbol, h.address[:14], h.ath_mcap, h.liquidity_usd, h.pair_age_hours)
                for h in hits[:5]
            ],
        )

    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    pairs = await fetch_dexscreener_pairs(TOKEN)
    p = pairs[0]
    liq = p.get("liquidity")
    liq_usd = float((liq.get("usd") if isinstance(liq, dict) else liq) or 0)
    mcap = float(p.get("marketCap") or p.get("fdv") or 0)
    cur_price = float(p.get("priceUsd") or 0)
    created = p.get("pairCreatedAt")
    age_h = (time.time() * 1000 - float(created)) / 3_600_000 if created else None
    row = ScreenedToken(
        address=TOKEN,
        symbol="NASDANQ",
        name="NASDANQ",
        market_cap=mcap,
        ath_mcap=float(peak.peak),
        liquidity_usd=liq_usd,
        pair_age_hours=age_h,
        traders_24h=0,
        buys_24h=2803,
        sells_24h=2096,
    )
    req = ScreenRequest(
        **{k: v for k, v in cfg["screen"].items() if k in ScreenRequest.model_fields}
    )
    print("synthetic passes_primary", _passes_primary(row, req))
    print(
        "liq",
        _in_range(row.liquidity_usd, req.min_liq, req.max_liq),
        row.liquidity_usd,
    )
    print("ath ok", float(peak.peak) >= float(req.min_ath_mcap or 0), peak.peak)
    print("age", age_h, "max", req.max_pair_age_hours)

    code, info = await _get_json(f"/tokens/{TOKEN}")
    print("token http", code, type(info).__name__)
    human = 0.0
    if isinstance(info, dict):
        supply = float(info.get("total_supply") or 0)
        decimals = int(info.get("decimals") or 18)
        human = supply / (10**decimals) if supply else 0.0
        print("supply", human, info.get("symbol"))

    th = float(cfg["wallet"]["mcap_threshold"])
    for wr in audit["wallets"]:
        w = wr["wallet"]
        act = await fetch_wallet_activity_result(
            w, event_types=["buy"], limit=10, max_pages=1
        )
        opens = [r for r in act.rows if r.get("is_open_or_close") == 1]
        r = opens[-1] if opens else None
        if not r:
            print(w[:12], "no open")
            continue
        price = float(r.get("price_usd") or 0)
        mcap_buy = price * human if human else None
        mcap_scaled = mcap * (price / cur_price) if cur_price and price and mcap else None
        cand = mcap_buy or mcap_scaled
        print(
            w[:12],
            "price",
            price,
            "cur",
            cur_price,
            "mcap_supply",
            round(mcap_buy, 2) if mcap_buy else None,
            "mcap_scaled",
            round(mcap_scaled, 2) if mcap_scaled else None,
            "UNDER?" if cand and cand < th else "OVER?",
            "th",
            th,
            "bal",
            wr.get("balance_eth"),
        )
        tx = r.get("tx_hash")
        code, data = await _get_json(f"/transactions/{tx}")
        if isinstance(data, dict):
            method = data.get("method")
            mid = (data.get("decoded_input") or {}).get("method_id")
            frm = data.get("from")
            frm = frm.get("hash") if isinstance(frm, dict) else frm
            print(
                "  method",
                method,
                mid,
                "launch_buy",
                method_is_launch_buy(method) or method_is_launch_buy(mid),
                "creator",
                method_is_creator_launch(method) or method_is_creator_launch(mid),
                "from",
                frm,
            )

    # Why missing from index? Check Transfer scan coverage / enrichment
    print("\nindex status", token_index.status().model_dump() if hasattr(token_index, "status") else "n/a")


if __name__ == "__main__":
    asyncio.run(main())
