"""Audit wallet 0x63961… miss. Do not commit."""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
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


def iso(ts: object) -> str:
    try:
        n = float(ts)  # type: ignore[arg-type]
        if n > 1e12:
            n /= 1000
        return datetime.fromtimestamp(n, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


async def main() -> None:
    from app.chain import RpcClient
    from app.followup import estimate_token_peak_mcap
    from app.followup_store import followup_store
    from app.gmgn_portfolio import fetch_unique_buys
    from app.models import tokens_unique_period_hours
    from app.pools import fetch_dexscreener_pairs
    from app.replay import estimate_mcap_at_tx, parse_token
    from app.wallet_metrics import batch_tokens_traded_7d, batch_wallet_balances
    from app.watch_store import watch_store

    cfg = watch_store.load_config()
    wcfg = cfg.wallet
    print(
        "filters unique",
        wcfg.min_tokens_traded_7d,
        wcfg.max_tokens_traded_7d,
        wcfg.tokens_unique_period,
        flush=True,
    )
    print("bal", wcfg.min_wallet_balance_eth, wcfg.max_wallet_balance_eth, flush=True)
    print("mcap_th", wcfg.mcap_threshold, flush=True)
    print("min_ath", cfg.screen.min_ath_mcap, "max_age", cfg.screen.max_pair_age_hours, flush=True)

    rows = followup_store.list_wallets(limit=5000)
    hit = [r for r in rows if r.address.lower() == W.lower()]
    print(
        "followup",
        [(h.status, h.deal_count, str(h.first_token)[:14]) for h in hit] or "MISSING",
        flush=True,
    )
    seen = watch_store.load_seen()
    print("in_watch_seen", any(W.lower() in k.lower() for k in seen), flush=True)

    rpc = RpcClient()
    bals = await batch_wallet_balances(rpc, [W])
    print("balance_eth", bals.get(W.lower()), flush=True)

    ub = await fetch_unique_buys(W, max_pages=4)
    print(
        "gmgn_ok",
        ub.ok,
        "buys",
        len(ub.buys),
        "rate_limited",
        ub.rate_limited,
        flush=True,
    )
    buys = sorted(ub.buys, key=lambda b: b.timestamp or 0)
    for b in buys[:20]:
        print(
            f"  {iso(b.timestamp)} {b.symbol or '?':12} {b.token[:14]}… "
            f"cost={b.cost_usd} tx={(b.tx_hash or '')[:16]}",
            flush=True,
        )

    period_h = tokens_unique_period_hours(wcfg.tokens_unique_period)
    uniq = await batch_tokens_traded_7d(rpc, [W], period_hours=period_h)
    print("unique_metric", uniq.get(W.lower()), "period_h", period_h, flush=True)

    hold = watch_store.load_hold()
    parsed = watch_store.load_parsed_at()
    try:
        pending = set(
            watch_store.load_pending_parse(min_ath_mcap=cfg.screen.min_ath_mcap) or []
        )
    except Exception:
        pending = set()

    # Focus on most recent buys
    for b in list(reversed(buys))[:6]:
        t = b.token.lower()
        print(f"\n=== {b.symbol} {t} ===", flush=True)
        print(" hold", t in {k.lower() for k in hold}, hold.get(t) or hold.get(b.token), flush=True)
        print(" parsed", t in {k.lower() for k in parsed}, flush=True)
        print(" pending", t in pending, flush=True)
        mcap = None
        try:
            if b.tx_hash:
                mcap = await estimate_mcap_at_tx(t, b.tx_hash, rpc=rpc)
            print(
                " entry_mcap",
                mcap,
                "under_th",
                None if mcap is None else mcap < float(wcfg.mcap_threshold or 0),
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            print(" entry_mcap ERR", type(exc).__name__, exc, flush=True)

        try:
            peak = await estimate_token_peak_mcap(t, min_needed=1.0)
            pairs = await fetch_dexscreener_pairs(t)
            spot = 0.0
            age = None
            if pairs:
                best = max(
                    pairs,
                    key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
                )
                spot = float(best.get("marketCap") or best.get("fdv") or 0)
                created = best.get("pairCreatedAt")
                if created:
                    age = (time.time() * 1000 - float(created)) / 3_600_000
            print(
                " peak",
                getattr(peak, "peak", peak),
                "spot",
                spot,
                "age_h",
                None if age is None else round(age, 2),
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            print(" peak ERR", type(exc).__name__, exc, flush=True)

        # Is wallet in parse early set?
        if mcap is not None and mcap < float(wcfg.mcap_threshold or 30_000):
            try:
                res = await parse_token(
                    rpc,
                    t,
                    mcap_threshold=float(wcfg.mcap_threshold or 30_000),
                    exclude_honeypots=False,
                    wallet_filters=None,
                )
                in_early = any(x.wallet.lower() == W.lower() for x in res.buyers)
                buys1 = [x for x in res.buyers if x.buys_count == 1]
                in_buys1 = any(x.wallet.lower() == W.lower() for x in buys1)
                row = next((x for x in res.buyers if x.wallet.lower() == W.lower()), None)
                print(
                    f" parse early={len(res.buyers)} in_early={in_early} "
                    f"buys1={in_buys1} row_buys={None if not row else row.buys_count} "
                    f"row_mcap={None if not row else row.mcap_at_first_buy}",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(" parse ERR", type(exc).__name__, exc, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
