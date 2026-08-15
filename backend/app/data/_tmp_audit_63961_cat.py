"""Audit CATSTRAT buy for 0x63961. Do not commit."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

ROOT = Path("/home/shneining/gnomode2.0")
sys.path.insert(0, str(ROOT / "backend"))
for line in (ROOT / ".env").read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

W = "0x63961ac45591e09ccfd3386a2ab6cd1e6befda43"
T = "0x655c8b48ea31deeadda63998b534c965e6d019cc"
TX = "0x841b3750c4bf1a0f6824d92658c4f0cc8770e87cf5d836f5664a02bddeaa2f29"


async def main() -> None:
    from app.chain import RpcClient
    from app.followup import estimate_token_peak_mcap
    from app.followup_store import followup_store
    from app.models import ParseRequest
    from app.pools import fetch_dexscreener_pairs, pick_best_pool
    from app.replay import estimate_mcap_at_tx, parse_token
    from app.wallet_metrics import enrich_and_filter_buyers
    from app.watch_store import watch_store

    cfg = watch_store.load_config()
    wcfg = cfg.wallet
    rpc = RpcClient()
    w3 = rpc.w3
    hold = {k.lower(): v for k, v in watch_store.load_hold().items()}
    parsed = {k.lower(): v for k, v in watch_store.load_parsed_at().items()}
    print("hold", T in hold, hold.get(T), flush=True)
    print("parsed", T in parsed, parsed.get(T), flush=True)

    r = await w3.eth.get_transaction_receipt(TX)
    tx = await w3.eth.get_transaction(TX)
    print(
        "block",
        r["blockNumber"],
        "from",
        tx["from"],
        "to",
        tx["to"],
        "logs",
        len(r["logs"]),
        flush=True,
    )
    for i, lg in enumerate(r["logs"]):
        tops = [(t.hex() if hasattr(t, "hex") else str(t)) for t in lg["topics"]]
        t0 = tops[0] if tops else ""
        if not t0.startswith("0x"):
            t0 = "0x" + t0
        kind = "?"
        if "ddf252ad" in t0.lower():
            kind = "Transfer"
        elif "40e9cecb" in t0.lower():
            kind = "V4_Swap"
        elif "c42079f9" in t0.lower():
            kind = "V3_Swap"
        print(f" log{i} {kind} addr={lg['address']}", flush=True)
        if kind == "Transfer" and len(tops) >= 3:
            frm = "0x" + tops[1][-40:]
            to = "0x" + tops[2][-40:]
            print(
                f"  {frm} -> {to} wallet={to.lower() == W.lower()}",
                flush=True,
            )

    mcap = await estimate_mcap_at_tx(T, TX, rpc=rpc)
    print(
        "entry_mcap",
        mcap,
        "under30k",
        None if mcap is None else mcap < 30000,
        flush=True,
    )

    peak = await estimate_token_peak_mcap(T, min_needed=1.0)
    pairs = await fetch_dexscreener_pairs(T)
    spot = 0.0
    age = None
    liq = 0.0
    if pairs:
        best = max(
            pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0)
        )
        spot = float(best.get("marketCap") or best.get("fdv") or 0)
        liq = float((best.get("liquidity") or {}).get("usd") or 0)
        c = best.get("pairCreatedAt")
        if c:
            age = (time.time() * 1000 - float(c)) / 3.6e6
        print("dex", best.get("dexId"), "pair", best.get("pairAddress"), flush=True)
    print(
        "peak",
        getattr(peak, "peak", peak),
        "spot",
        spot,
        "liq",
        liq,
        "age_h",
        None if age is None else round(age, 2),
        flush=True,
    )
    ath = max(float(getattr(peak, "peak", 0) or 0), spot)
    print(
        "ath",
        ath,
        "gate",
        cfg.screen.min_ath_mcap,
        "qualify",
        ath >= float(cfg.screen.min_ath_mcap or 0),
        flush=True,
    )
    print(
        "age_ok",
        age is None or age <= float(cfg.screen.max_pair_age_hours or 999),
        flush=True,
    )

    pool = await pick_best_pool(rpc, T)
    print(
        "pool",
        None
        if not pool
        else (pool.dex, pool.address, pool.pool_id, pool.quote_symbol),
        flush=True,
    )

    res = await parse_token(
        rpc, T, 30000, exclude_honeypots=False, wallet_filters=None
    )
    row = next((x for x in res.buyers if x.wallet.lower() == W.lower()), None)
    print("early_n", len(res.buyers), "in_early", row is not None, flush=True)
    if row:
        print(
            " row buys",
            row.buys_count,
            "mcap",
            row.mcap_at_first_buy,
            flush=True,
        )
        print("buys_count==1", row.buys_count == 1, flush=True)
        req = ParseRequest(
            tokens=[T],
            mcap_threshold=wcfg.mcap_threshold,
            min_wallet_balance_eth=wcfg.min_wallet_balance_eth,
            max_wallet_balance_eth=wcfg.max_wallet_balance_eth,
            min_tokens_traded_7d=wcfg.min_tokens_traded_7d,
            max_tokens_traded_7d=wcfg.max_tokens_traded_7d,
            tokens_unique_period=wcfg.tokens_unique_period,
            exclude_honeypots=False,
        )
        filtered = await enrich_and_filter_buyers(
            rpc, token=T, buyers=[row], req=req, start_block=0, end_block=0
        )
        print(
            "after_filters",
            len(filtered),
            "unique",
            (filtered[0].tokens_traded_7d if filtered else row.tokens_traded_7d),
            "bal",
            (filtered[0].wallet_balance_eth if filtered else None),
            flush=True,
        )
    else:
        print("NOT in early set", flush=True)

    hit = [
        r
        for r in followup_store.list_wallets(limit=5000)
        if r.address.lower() == W.lower()
    ]
    print(
        "followup",
        [(h.status, h.deal_count) for h in hit] or "MISSING",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
