"""Inspect MEATSPIN in live index via HTTP... can't. Dump via attaching? 
Instead: dry-parse + compare index entry fields by re-enriching like screener does.
Do not commit.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

TOKEN = "0x285ec8958774074cd924103d040fb92be9d2d42a"
TARGET = "0xfd8bd978f198503a0ba9c5d7f7586e23fc4a4b40"
DATA = Path(__file__).resolve().parent


async def main() -> None:
    cfg = json.loads((DATA / "watch.json").read_text())
    from app.chain import RpcClient
    from app.models import ParseRequest, ScreenRequest, ScreenedToken
    from app.replay import parse_token
    from app.screener import _passes_primary
    from app.pools import fetch_dexscreener_pairs
    from app.followup import estimate_token_peak_mcap
    from app.token_index import token_index
    import time

    print("local process index size", len(token_index.get_tokens()), flush=True)
    e = token_index._tokens.get(TOKEN.lower())
    print("local entry", e, flush=True)

    pairs = await fetch_dexscreener_pairs(TOKEN)
    peak = await estimate_token_peak_mcap(TOKEN, min_needed=50_000)
    print("peak", peak, flush=True)

    p = pairs[0]
    liq = p.get("liquidity")
    liq_usd = float((liq.get("usd") if isinstance(liq, dict) else liq) or 0)
    mcap = float(p.get("marketCap") or p.get("fdv") or 0)
    created = p.get("pairCreatedAt")
    age_h = (time.time() * 1000 - float(created)) / 3_600_000 if created else None
    row = ScreenedToken(
        address=TOKEN,
        symbol="MEATSPIN",
        name="MEATSPIN",
        market_cap=mcap,
        ath_mcap=float(peak.peak),
        liquidity_usd=liq_usd,
        pair_age_hours=age_h,
        traders_24h=0,
        buys_24h=0,
        sells_24h=0,
    )
    print("synthetic row", row.model_dump(), flush=True)

    req = ScreenRequest(
        **{k: v for k, v in cfg["screen"].items() if k in ScreenRequest.model_fields}
    )
    print("live screen req", req.model_dump(), flush=True)
    print("passes_primary", _passes_primary(row, req), flush=True)

    # What if ath was only current mcap (no gecko peak yet)?
    row_low = row.model_copy(update={"ath_mcap": row.market_cap})
    print(
        "passes if ath=cur_mcap",
        _passes_primary(row_low, req),
        "ath",
        row_low.ath_mcap,
        flush=True,
    )

    w = cfg["wallet"]
    filters = ParseRequest(
        tokens=[TOKEN],
        mcap_threshold=float(w["mcap_threshold"]),
        exclude_honeypots=bool(w.get("exclude_honeypots")),
        min_wallet_balance_eth=w.get("min_wallet_balance_eth"),
        max_wallet_balance_eth=w.get("max_wallet_balance_eth"),
        min_tokens_traded_7d=w.get("min_tokens_traded_7d"),
        max_tokens_traded_7d=w.get("max_tokens_traded_7d"),
        tokens_unique_period=w.get("tokens_unique_period") or "30d",
    )
    rpc = RpcClient()

    async def prog(stage: str, message: str, percent: float) -> None:
        if percent >= 0.75 or stage in ("done", "replay", "filter", "wallets", "launch"):
            print(f"  [{percent:.0%}] {stage}: {message}", flush=True)

    print("\n=== dry parse WITH filters ===", flush=True)
    result = await parse_token(
        rpc,
        TOKEN,
        float(w["mcap_threshold"]),
        on_progress=prog,
        exclude_honeypots=False,
        wallet_filters=filters,
    )
    buyers = result.buyers or []
    print("error", result.error, flush=True)
    print("stats", result.stats, flush=True)
    print("buyers", len(buyers), flush=True)
    hits = [b for b in buyers if b.wallet.lower() == TARGET]
    print("TARGET in filtered", bool(hits), flush=True)
    for b in hits:
        print(
            "kept",
            {
                "mcap": getattr(b, "mcap_at_buy", None),
                "bal": getattr(b, "wallet_balance_eth", None),
                "t7": getattr(b, "tokens_traded_7d", None),
                "tx": getattr(b, "tx_hash", None) or getattr(b, "first_tx", None),
            },
            flush=True,
        )

    print("\n=== dry parse NO wallet filters ===", flush=True)
    result2 = await parse_token(
        rpc,
        TOKEN,
        float(w["mcap_threshold"]),
        on_progress=prog,
        exclude_honeypots=False,
        wallet_filters=None,
    )
    buyers2 = result2.buyers or []
    print("error", result2.error, "early", len(buyers2), flush=True)
    hits2 = [b for b in buyers2 if b.wallet.lower() == TARGET]
    print("TARGET in early", bool(hits2), flush=True)
    for b in hits2:
        print(
            {
                "wallet": b.wallet,
                "mcap": getattr(b, "mcap_at_buy", None),
                "bought_usd": getattr(b, "bought_usd", None),
                "tx": getattr(b, "tx_hash", None) or getattr(b, "first_tx", None),
                "bal": getattr(b, "wallet_balance_eth", None),
            },
            flush=True,
        )
    if not hits2:
        # show nearest by time / sample
        print(
            "sample",
            [
                (
                    b.wallet[:14],
                    getattr(b, "mcap_at_buy", None),
                    getattr(b, "tx_hash", None) or getattr(b, "first_tx", None),
                )
                for b in buyers2[:20]
            ],
            flush=True,
        )
        # check if TARGET tx appears anywhere
        target_tx = "0x34b7547c88b9275828b6e87e340a10175c3b865c7004ab746536e0e5def556a6"
        for b in buyers2:
            tx = (getattr(b, "tx_hash", None) or getattr(b, "first_tx", None) or "").lower()
            if tx == target_tx or b.wallet.lower() == TARGET:
                print("FOUND", b.wallet, tx, getattr(b, "mcap_at_buy", None), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
