"""Why MEATSPIN never parsed for HVAT. Do not commit."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
W = "0xfd8bd978f198503a0ba9c5d7f7586e23fc4a4b40"


async def main() -> None:
    audit = json.loads((DATA / "_tmp_audit_fd8b.json").read_text())
    buy = audit["buys"][0]
    token = buy["token"]
    tx = buy["tx"]
    cfg = json.loads((DATA / "watch.json").read_text())
    hold = json.loads((DATA / "watch_hold.json").read_text())
    parsed = {a.lower() for a in (hold.get("parsed") or [])}
    hold_map = {k.lower(): v for k, v in (hold.get("hold") or {}).items()}
    print("TOKEN", token, "sym", buy["symbol"], "buy", buy["ts_fmt"], "tx", tx, flush=True)
    print("parsed?", token in parsed, "hold?", token in hold_map, flush=True)

    from app.token_index import token_index
    from app.followup import estimate_token_peak_mcap
    from app.pools import fetch_dexscreener_pairs, pick_best_pool
    from app.screener import _passes_primary, _in_range
    from app.models import ScreenRequest, ScreenedToken
    from app.blockscout import _get_json
    from app.chain import RpcClient
    from app.replay import estimate_mcap_at_tx
    from app.honeypot import check_honeypot
    from app.buy_gate import method_is_launch_buy, method_is_creator_launch
    from app.gmgn_portfolio import fetch_wallet_activity_result

    pool = {t.address.lower(): t for t in token_index.get_tokens()}
    print("in index?", token in pool, "pool_size", len(pool), flush=True)
    if token in pool:
        print("index", pool[token].model_dump(), flush=True)
    hits = [t for t in pool.values() if "meat" in (t.symbol or "").lower()]
    print(
        "meat symbol hits",
        [(h.symbol, h.address[:14], h.ath_mcap) for h in hits[:5]],
        flush=True,
    )

    peak = await estimate_token_peak_mcap(token, min_needed=50_000)
    pairs = await fetch_dexscreener_pairs(token)
    print("n_pairs", len(pairs or []), flush=True)
    mcap = liq_usd = 0.0
    age_h = None
    if pairs:
        p = pairs[0]
        liq = p.get("liquidity")
        liq_usd = float((liq.get("usd") if isinstance(liq, dict) else liq) or 0)
        mcap = float(p.get("marketCap") or p.get("fdv") or 0)
        created = p.get("pairCreatedAt")
        age_h = (time.time() * 1000 - float(created)) / 3_600_000 if created else None
        print(
            "dex pair",
            p.get("pairAddress"),
            "dex",
            p.get("dexId"),
            "mcap",
            mcap,
            "liq",
            liq_usd,
            "age_h",
            age_h,
            "created",
            created,
            flush=True,
        )
        for i, pp in enumerate(pairs[:5]):
            pl = pp.get("liquidity")
            print(
                f"  pair[{i}]",
                pp.get("dexId"),
                pp.get("pairAddress"),
                "liq",
                (pl.get("usd") if isinstance(pl, dict) else pl),
                "mcap",
                pp.get("marketCap") or pp.get("fdv"),
                "created",
                pp.get("pairCreatedAt"),
                flush=True,
            )

    print("peak", peak, flush=True)

    req = ScreenRequest(
        **{k: v for k, v in cfg["screen"].items() if k in ScreenRequest.model_fields}
    )
    row = ScreenedToken(
        address=token,
        symbol=buy["symbol"] or "MEATSPIN",
        name=buy["symbol"] or "MEATSPIN",
        market_cap=float(mcap or 0),
        ath_mcap=float(getattr(peak, "peak", None) or mcap or 0),
        liquidity_usd=float(liq_usd or 0),
        pair_age_hours=age_h,
        traders_24h=0,
        buys_24h=0,
        sells_24h=0,
    )
    print("passes_primary", _passes_primary(row, req), flush=True)
    print(
        "liq ok",
        _in_range(row.liquidity_usd, req.min_liq, req.max_liq),
        row.liquidity_usd,
        "min",
        req.min_liq,
        flush=True,
    )
    print(
        "ath ok",
        float(row.ath_mcap) >= float(req.min_ath_mcap or 0),
        row.ath_mcap,
        "min",
        req.min_ath_mcap,
        flush=True,
    )
    print(
        "age",
        age_h,
        "max",
        req.max_pair_age_hours,
        "pass",
        (age_h is None) or (age_h <= float(req.max_pair_age_hours or 1e9)),
        flush=True,
    )

    try:
        hp = await check_honeypot(token)
        print("honeypot", hp, flush=True)
    except Exception as e:
        print("honeypot err", e, flush=True)

    rpc = RpcClient()
    pool_best = await pick_best_pool(rpc, token)
    print("best_pool", pool_best, flush=True)
    tx_mcap = await estimate_mcap_at_tx(token, tx, rpc=rpc)
    print("tx_mcap", tx_mcap, "th", cfg["wallet"]["mcap_threshold"], flush=True)

    act = await fetch_wallet_activity_result(W, event_types=["buy"], limit=10, max_pages=1)
    for r in act.rows:
        t = r.get("token")
        addr = (t.get("address") if isinstance(t, dict) else t) or ""
        if str(addr).lower() == token:
            print(
                "raw method",
                r.get("method"),
                "launchpad",
                r.get("launchpad") or r.get("launchpad_platform"),
                "is_open",
                r.get("is_open_or_close"),
                flush=True,
            )
            print(
                "creator?",
                method_is_creator_launch(r.get("method")),
                "launch_buy?",
                method_is_launch_buy(r.get("method")),
                flush=True,
            )

    code, info = await _get_json(f"/tokens/{token}")
    if isinstance(info, dict):
        print(
            "token info",
            {
                k: info.get(k)
                for k in (
                    "symbol",
                    "name",
                    "decimals",
                    "total_supply",
                    "holder_count",
                )
            },
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
