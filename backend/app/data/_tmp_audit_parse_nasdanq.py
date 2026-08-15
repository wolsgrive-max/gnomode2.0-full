"""Dry parse NASDANQ for early buyers. Does not touch watch_seen. Do not commit."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
TARGETS = {
    "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
    "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
}
DATA = Path(__file__).resolve().parent


async def main() -> None:
    cfg = json.loads((DATA / "watch.json").read_text())
    from app.chain import RpcClient
    from app.models import ParseRequest
    from app.replay import parse_token

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
    th = float(w["mcap_threshold"])

    async def prog(stage: str, message: str, percent: float) -> None:
        if percent >= 0.8 or stage in ("done", "replay", "filter", "wallets", "launch"):
            print(f"  [{percent:.0%}] {stage}: {message}", flush=True)

    print("parsing WITH filters", filters.model_dump(), flush=True)
    result = await parse_token(
        rpc,
        TOKEN,
        th,
        on_progress=prog,
        exclude_honeypots=False,
        wallet_filters=filters,
    )
    buyers = result.buyers or []
    stats = result.stats or {}
    print("error", result.error, flush=True)
    print("stats", stats, flush=True)
    print("buyers_after_filters", len(buyers), flush=True)
    print("buyers_before_filters", stats.get("buyers_before_wallet_filters"), flush=True)
    for b in buyers:
        if b.wallet.lower() in TARGETS:
            print("TARGET kept", b.wallet, b.mcap_at_buy, b.wallet_balance_eth, flush=True)

    print("\n--- reparse WITHOUT wallet filters ---", flush=True)
    result2 = await parse_token(
        rpc, TOKEN, th, on_progress=prog, exclude_honeypots=False, wallet_filters=None
    )
    buyers2 = result2.buyers or []
    print("error", result2.error, "early", len(buyers2), flush=True)
    hits = [b for b in buyers2 if b.wallet.lower() in TARGETS]
    print("TARGET hits among early:", len(hits), flush=True)
    for b in hits:
        print(
            {
                "wallet": b.wallet,
                "mcap": getattr(b, "mcap_at_buy", None) or getattr(b, "entry_mcap", None),
                "bought_usd": getattr(b, "bought_usd", None),
                "tx": (getattr(b, "tx_hash", None) or getattr(b, "first_tx", "") or "")[:18],
                "bal": getattr(b, "wallet_balance_eth", None),
                "t7": getattr(b, "tokens_traded_7d", None),
            },
            flush=True,
        )
    if not hits:
        print("sample early:", [(b.wallet[:12], getattr(b, "mcap_at_buy", None)) for b in buyers2[:15]], flush=True)



if __name__ == "__main__":
    asyncio.run(main())
