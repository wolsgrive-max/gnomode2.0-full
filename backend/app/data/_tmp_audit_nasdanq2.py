"""NASDANQ screen + buy mcap + txs. Do not commit."""
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
    from app.screener import _passes_primary
    from app.models import ScreenRequest, ScreenedToken
    from app.blockscout import _get_json, blockscout_api_base
    from app.gmgn_portfolio import fetch_wallet_activity_result
    from app.buy_gate import method_is_launch_buy, method_is_creator_launch

    print("index peaks", token_index.mcap_peaks([TOKEN]))
    size = None
    for attr in ("_entries", "entries", "_by_addr"):
        if hasattr(token_index, attr):
            obj = getattr(token_index, attr)
            size = len(obj)
            print("index", attr, size)
            key = TOKEN.lower()
            hit = obj.get(key) if isinstance(obj, dict) else None
            print("  contains?", hit is not None)
            if hit is not None:
                print("  entry", hit)

    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    print("peak", peak)

    pairs = await fetch_dexscreener_pairs(TOKEN)
    p = pairs[0]
    liq = p.get("liquidity")
    liq_usd = float((liq.get("usd") if isinstance(liq, dict) else liq) or 0)
    mcap = float(p.get("marketCap") or p.get("fdv") or 0)
    created = p.get("pairCreatedAt")
    age_h = (time.time() * 1000 - float(created)) / 3_600_000 if created else None
    row = ScreenedToken(
        address=TOKEN,
        symbol="NASDANQ",
        market_cap=mcap,
        ath_mcap=float(peak.peak),
        liquidity=liq_usd,
        pair_age_hours=age_h,
        traders=None,
        is_honeypot=False,
    )
    req = ScreenRequest(
        **{k: v for k, v in cfg["screen"].items() if k in ScreenRequest.model_fields}
    )
    print("passes_primary", _passes_primary(row, req))
    print(
        "ATH",
        peak.peak,
        ">=",
        req.min_ath_mcap,
        "?",
        float(peak.peak) >= float(req.min_ath_mcap or 0),
    )
    print("age_h", round(age_h or -1, 2), "max", req.max_pair_age_hours)
    print("liq", liq_usd, "min", req.min_liq)

    # token supply → buy mcap
    info = await _get_json(f"{blockscout_api_base()}/tokens/{TOKEN}")
    supply = float(info.get("total_supply") or 0)
    decimals = int(info.get("decimals") or 18)
    human = supply / (10**decimals) if supply else 0
    print("supply_human", human, "symbol", info.get("symbol"))

    for wr in audit["wallets"]:
        w = wr["wallet"]
        act = await fetch_wallet_activity_result(
            w, event_types=["buy"], limit=10, max_pages=1
        )
        opens = [r for r in act.rows if r.get("is_open_or_close") == 1]
        r = opens[-1] if opens else None
        if not r:
            print(w[:12], "no open buy")
            continue
        price = float(r.get("price_usd") or 0)
        mcap_buy = price * human if price and human else None
        th = float(cfg["wallet"]["mcap_threshold"])
        print(
            w[:12],
            "open_ts",
            r.get("timestamp"),
            "price",
            price,
            "mcap_buy",
            round(mcap_buy, 2) if mcap_buy else None,
            "th",
            th,
            "UNDER" if mcap_buy and mcap_buy < th else "OVER/UNK",
            "cost",
            r.get("cost_usd"),
            "tx",
            (r.get("tx_hash") or "")[:18],
        )
        tx = r.get("tx_hash")
        if tx:
            data = await _get_json(f"{blockscout_api_base()}/transactions/{tx}")
            method = data.get("method")
            di = data.get("decoded_input") or {}
            mid = di.get("method_id")
            frm = data.get("from")
            if isinstance(frm, dict):
                frm = frm.get("hash")
            to = data.get("to")
            if isinstance(to, dict):
                to = to.get("hash")
            print(
                "  method=",
                method,
                "id=",
                mid,
                "launch_buy?",
                method_is_launch_buy(method) or method_is_launch_buy(mid),
                "creator?",
                method_is_creator_launch(method) or method_is_creator_launch(mid),
                "from=",
                frm,
                "to=",
                to,
            )

    # Why not in index? Check screener output for this token among live screen
    from app.screener import screen_tokens

    print("\nRunning live screen_tokens with watch screen filters…")
    screened = await screen_tokens(req)
    addrs = {s.address.lower() for s in screened}
    print("screened count", len(screened), "NASDANQ in?", TOKEN.lower() in addrs)
    if TOKEN.lower() not in addrs:
        # try without ATH / age to see which gate kills it
        loose = req.model_copy(
            update={"min_ath_mcap": None, "max_pair_age_hours": None, "exclude_honeypots": False}
        )
        screened2 = await screen_tokens(loose)
        addrs2 = {s.address.lower() for s in screened2}
        print("loose screen count", len(screened2), "in?", TOKEN.lower() in addrs2)
        # find if anywhere in index candidates
    # check index enrichment state
    for meth in ("is_enriched", "enriched", "needs_enrich", "get_row", "row"):
        if hasattr(token_index, meth):
            print("ti", meth, getattr(token_index, meth))


if __name__ == "__main__":
    asyncio.run(main())
