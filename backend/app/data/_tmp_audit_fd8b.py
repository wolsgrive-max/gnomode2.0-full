"""Audit 0xfd8b HVAT miss. Do not commit."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path

W = "0xfd8bd978f198503a0ba9c5d7f7586e23fc4a4b40"
PREV = [
    "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
    "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
]
NAS = "0x51fb76be80ab6daaa345d818f4e06441816b4fea"
DATA = Path(__file__).resolve().parent
OUT = DATA / "_tmp_audit_fd8b.json"


def fmt_ts(ts: float | int | None) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


async def main() -> None:
    from app.gmgn_portfolio import (
        fetch_unique_buys,
        fetch_wallet_activity_result,
        gmgn_api_configured,
    )
    from app.models import tokens_unique_period_hours
    from app.wallet_metrics import batch_tokens_traded_7d, batch_wallet_balances
    from app.buy_gate import method_is_creator_launch, method_is_launch_buy
    from app.chain import RpcClient
    from app.blockscout import _get_json
    from app.pools import fetch_dexscreener_pairs, pick_best_pool
    from app.followup import estimate_token_peak_mcap
    from app.replay import estimate_mcap_at_tx
    from app.token_index import token_index

    cfg = json.loads((DATA / "watch.json").read_text())
    hold = json.loads((DATA / "watch_hold.json").read_text())
    seen = {
        str(k).lower()
        for k in (json.loads((DATA / "watch_seen.json").read_text()).get("keys") or [])
    }
    parsed = {str(a).lower() for a in (hold.get("parsed") or [])}
    wf = cfg.get("wallet") or {}
    sf = cfg.get("screen") or {}
    print("gmgn", gmgn_api_configured(), flush=True)
    print("filters", wf, flush=True)
    print("screen ath", sf.get("min_ath_mcap"), "age", sf.get("max_pair_age_hours"), flush=True)

    lookback_h = tokens_unique_period_hours(wf.get("tokens_unique_period") or "30d")
    unique = await batch_tokens_traded_7d(
        [W], lookback_hours=lookback_h, enough=None, too_many=None
    )
    n_u = unique.get(W.lower())
    rpc = RpcClient()
    bals = await batch_wallet_balances(rpc, [W])
    bal = bals.get(W.lower())
    print(f"unique_30d={n_u} bal={bal}", flush=True)

    ub = await fetch_unique_buys(W, max_pages=5)
    act = await fetch_wallet_activity_result(W, event_types=["buy"], limit=50, max_pages=3)
    print(
        f"gmgn unique={len(ub.buys)} ok={ub.ok} rl={ub.rate_limited} raw={len(act.rows)}",
        flush=True,
    )

    code, info = await _get_json(f"/tokens/{NAS}")
    human = 0.0
    if isinstance(info, dict):
        supply = float(info.get("total_supply") or 0)
        decimals = int(info.get("decimals") or 18)
        human = supply / (10**decimals) if supply else 0.0
        print("NAS supply", human, info.get("symbol"), flush=True)

    pairs = await fetch_dexscreener_pairs(NAS)
    cur_mcap = float((pairs[0].get("marketCap") or pairs[0].get("fdv") or 0)) if pairs else 0
    peak = await estimate_token_peak_mcap(NAS, min_needed=50_000)
    print(
        "NAS cur_mcap",
        cur_mcap,
        "peak",
        peak.peak if peak else None,
        "parsed?",
        NAS in parsed,
        flush=True,
    )

    pool_toks = {t.address.lower(): t for t in token_index.get_tokens()}
    print("NAS in index?", NAS in pool_toks, flush=True)
    if NAS in pool_toks:
        print("index row", pool_toks[NAS].model_dump(), flush=True)

    buys_out = []
    for b in ub.buys:
        tok = b.token.lower()
        raw = None
        for r in act.rows:
            t = r.get("token")
            addr = (t.get("address") if isinstance(t, dict) else t) or ""
            if str(addr).lower() == tok:
                raw = r
                break
        method = (raw or {}).get("method")
        price = None
        try:
            price = float((raw or {}).get("price_usd") or 0) or None
        except Exception:
            pass
        mcap_buy = price * human if price and human and tok == NAS else None
        tx_mcap: float | str | None = None
        if b.tx_hash:
            try:
                tx_mcap = await estimate_mcap_at_tx(tok, b.tx_hash, rpc=rpc)
            except Exception as e:
                tx_mcap = f"err:{e}"
        creator = method_is_creator_launch(method) if method else False
        launch = method_is_launch_buy(method) if method else False
        print(
            f"  buy {fmt_ts(b.timestamp)} {(b.symbol or '?'):12} {tok[:16]}… "
            f"cost={b.cost_usd} method={method} price={price} mcap_px={mcap_buy} "
            f"tx_mcap={tx_mcap} creator={creator} launch={launch}",
            flush=True,
        )
        buys_out.append(
            {
                "token": tok,
                "symbol": b.symbol,
                "ts": b.timestamp,
                "ts_fmt": fmt_ts(b.timestamp),
                "cost": b.cost_usd,
                "tx": b.tx_hash,
                "method": method,
                "price_usd": price,
                "mcap_from_price": mcap_buy,
                "tx_mcap": tx_mcap,
                "creator": creator,
                "launch": launch,
                "parsed": tok in parsed,
                "seen": any(W.lower() in k and tok in k for k in seen),
            }
        )

    print("\n--- prev wallets NAS buys ---", flush=True)
    for pw in PREV:
        ua = await fetch_unique_buys(pw, max_pages=2)
        for b in ua.buys:
            if b.token.lower() == NAS:
                print(
                    f"  {pw[:12]} {fmt_ts(b.timestamp)} cost={b.cost_usd} tx={b.tx_hash}",
                    flush=True,
                )
        await asyncio.sleep(0.5)

    try:
        pool = await pick_best_pool(rpc, NAS)
        print("best pool", pool, flush=True)
    except Exception as e:
        print("pool err", e, flush=True)

    min_bal = wf.get("min_wallet_balance_eth")
    min_u, max_u = wf.get("min_tokens_traded_7d"), wf.get("max_tokens_traded_7d")
    mcap_th = wf.get("mcap_threshold")
    reject: list[str] = []
    if n_u is not None:
        ok = True
        if min_u is not None and n_u < float(min_u):
            ok = False
        if max_u is not None and n_u > float(max_u):
            ok = False
        reject.append(f"unique_30d={n_u} {'PASS' if ok else 'FAIL'} need [{min_u},{max_u}]")
    if bal is not None and min_bal is not None:
        reject.append(
            f"bal={bal:.8f} {'PASS' if bal >= float(min_bal) else 'FAIL'} (min={min_bal})"
        )
    for d in buys_out:
        if d["mcap_from_price"] is not None and mcap_th:
            reject.append(
                f"mcap_at_buy~{d['mcap_from_price']:.0f} vs th={mcap_th} "
                f"{'PASS' if d['mcap_from_price'] <= float(mcap_th) else 'FAIL late'}"
            )
        if (
            d["tx_mcap"] is not None
            and not isinstance(d["tx_mcap"], str)
            and mcap_th
        ):
            reject.append(
                f"tx_mcap={d['tx_mcap']:.0f} vs th={mcap_th} "
                f"{'PASS' if float(d['tx_mcap']) <= float(mcap_th) else 'FAIL late'}"
            )
        if d["creator"]:
            reject.append("CREATOR_LAUNCH")
        if not d["parsed"]:
            reject.append(f"token {d['symbol']} NEVER_PARSED")
    print("REJECT:", reject, flush=True)

    out = {
        "ts": time.time(),
        "wallet": W,
        "followup": False,
        "seen_hits": [k for k in seen if W.lower() in k],
        "unique_30d": n_u,
        "bal": bal,
        "n_gmgn": len(ub.buys),
        "peak_nas": getattr(peak, "peak", None),
        "cur_mcap": cur_mcap,
        "nas_parsed": NAS in parsed,
        "nas_in_index": NAS in pool_toks,
        "buys": buys_out,
        "reject": reject,
        "raw": [
            {
                "method": r.get("method"),
                "ts": r.get("timestamp"),
                "token": (
                    (r.get("token") or {}).get("address")
                    if isinstance(r.get("token"), dict)
                    else r.get("token")
                ),
                "symbol": (
                    (r.get("token") or {}).get("symbol")
                    if isinstance(r.get("token"), dict)
                    else None
                ),
                "cost_usd": r.get("cost_usd"),
                "price_usd": r.get("price_usd"),
                "tx": r.get("tx_hash"),
                "is_open": r.get("is_open_or_close"),
            }
            for r in act.rows[:20]
        ],
    }
    OUT.write_text(json.dumps(out, indent=2, default=str))
    print("wrote", OUT, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
