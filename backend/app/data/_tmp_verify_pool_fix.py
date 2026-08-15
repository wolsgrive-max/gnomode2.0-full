"""Verify NASDANQ early buyers after pool fix. Do not commit."""
from __future__ import annotations

import asyncio

TOKEN = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
TARGETS = {
    "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
    "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
}


async def prog(stage: str, message: str, percent: float) -> None:
    if stage in ("replay", "launch", "done", "filter", "logs") or percent >= 0.8:
        print(f"[{percent:.0%}] {stage}: {message}", flush=True)


async def main() -> None:
    from app.chain import RpcClient
    from app.models import ParseRequest
    from app.replay import parse_token

    rpc = RpcClient()
    r = await parse_token(
        rpc,
        TOKEN,
        30_000.0,
        on_progress=prog,
        exclude_honeypots=False,
        wallet_filters=None,
    )
    st = r.stats or {}
    print(
        "pool",
        st.get("pool"),
        st.get("dex"),
        "early",
        st.get("buyers_before_wallet_filters"),
        "err",
        r.error,
        flush=True,
    )
    buyers = r.buyers or []
    hits = [b for b in buyers if b.wallet.lower() in TARGETS]
    print("TARGET hits", len(hits), "of", len(buyers), flush=True)
    for b in hits:
        mcap = getattr(b, "mcap_at_buy", None)
        if mcap is None:
            mcap = getattr(b, "entry_mcap", None)
        tx = getattr(b, "first_tx", None) or getattr(b, "tx_hash", "") or ""
        print(b.wallet, "mcap", mcap, "tx", tx[:18], flush=True)

    # with live filters
    filt = ParseRequest(
        tokens=[TOKEN],
        mcap_threshold=30_000.0,
        exclude_honeypots=False,
        min_wallet_balance_eth=0.001,
        min_tokens_traded_7d=1.0,
        max_tokens_traded_7d=1.0,
        tokens_unique_period="30d",
    )
    print("\n--- with wallet filters ---", flush=True)
    r2 = await parse_token(
        rpc,
        TOKEN,
        30_000.0,
        on_progress=prog,
        exclude_honeypots=False,
        wallet_filters=filt,
    )
    st2 = r2.stats or {}
    print(
        "before",
        st2.get("buyers_before_wallet_filters"),
        "after",
        len(r2.buyers or []),
        flush=True,
    )
    hits2 = [b for b in (r2.buyers or []) if b.wallet.lower() in TARGETS]
    print("TARGET after filters", len(hits2), flush=True)
    for b in hits2:
        print(
            b.wallet[:12],
            "bal",
            b.wallet_balance_eth,
            "t7",
            b.tokens_traded_7d,
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main())
