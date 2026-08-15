"""Audit two missed HVAT wallets. Do not commit."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent
ROOT = DATA.parents[2]
sys.path.insert(0, str(ROOT / "backend") if (ROOT / "backend").exists() else str(DATA.parents[1]))

# Inside docker: /app/backend/app/data → parents[2]=/app
if not (ROOT / "backend").exists():
    # running in container: /app/backend/app/data
    sys.path.insert(0, "/app/backend")

from app.buy_gate import method_is_creator_launch  # noqa: E402
from app.gmgn_portfolio import (  # noqa: E402
    fetch_unique_buys,
    fetch_wallet_activity_result,
    gmgn_api_configured,
)
from app.models import tokens_unique_period_hours  # noqa: E402
from app.wallet_metrics import (  # noqa: E402
    batch_tokens_traded_7d,
    batch_wallet_balances,
)

WALLETS = [
    "0x7e84c2e64f77cafc7fd283c88d1bfb55b09be552",
    "0x6a7c99fab3b8008a5238e1280fee1ad75631e9ae",
]

OUT = DATA / "_tmp_audit_two_wallets.json"


def fmt_ts(ts: int | float | None) -> str:
    if not ts:
        return "?"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def load_json(name: str) -> dict:
    p = DATA / name
    if not p.exists():
        return {}
    return json.loads(p.read_text())


async def token_meta(token: str) -> dict:
    """Best-effort mcap/ATH/honeypot via existing helpers if available."""
    out: dict = {"token": token}
    try:
        from app.screener import fetch_token_overview  # type: ignore
    except Exception:
        fetch_token_overview = None
    try:
        from app.token_meta import get_token_meta  # type: ignore
    except Exception:
        get_token_meta = None
    try:
        from app.honeypot import check_honeypot  # type: ignore
    except Exception:
        check_honeypot = None

    # Try dex/index peaks
    try:
        from app import token_index

        peaks = token_index.mcap_peaks([token])
        if token.lower() in peaks:
            peak, sym = peaks[token.lower()]
            out["index_peak"] = peak
            out["index_sym"] = sym
    except Exception as exc:
        out["index_err"] = str(exc)[:120]

    if get_token_meta:
        try:
            meta = await get_token_meta(token)
            if meta:
                out["meta"] = {
                    k: getattr(meta, k, None) if not isinstance(meta, dict) else meta.get(k)
                    for k in (
                        "symbol",
                        "market_cap",
                        "ath_mcap",
                        "liquidity",
                        "pair_age_hours",
                        "is_honeypot",
                        "honeypot",
                    )
                }
        except Exception as exc:
            out["meta_err"] = str(exc)[:160]

    if check_honeypot:
        try:
            hp = await check_honeypot(token)
            out["honeypot"] = hp if isinstance(hp, (bool, str, dict)) else str(hp)[:200]
        except Exception as exc:
            out["honeypot_err"] = str(exc)[:120]

    return out


async def estimate_buy_mcap(token: str, ts: int | None, price_usd: float | None) -> dict:
    """Rough early mcap estimate from GMGN price if supply known."""
    info: dict = {}
    try:
        from app.followup import estimate_token_peak_mcap

        peak = await estimate_token_peak_mcap(token, min_needed=50_000)
        info["peak_est"] = peak
    except Exception as exc:
        info["peak_err"] = str(exc)[:160]
    info["price_usd"] = price_usd
    info["buy_ts"] = ts
    return info


async def audit_one(w: str, cfg: dict, hold: dict, seen_keys: set[str]) -> dict:
    print(f"\n=== {w} ===", flush=True)
    parsed = {str(a).lower() for a in (hold.get("parsed") or [])}
    parsed_at = {str(k).lower(): v for k, v in (hold.get("parsed_at") or {}).items()}
    hold_map = {str(k).lower(): v for k, v in (hold.get("hold") or {}).items()}

    # followup
    db = DATA / "followup.db"
    fu = None
    deals = []
    if db.exists():
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM wallets WHERE lower(address)=?", (w.lower(),)
        ).fetchone()
        fu = dict(row) if row else None
        deals = [
            dict(r)
            for r in con.execute(
                "SELECT * FROM deals WHERE lower(wallet)=? ORDER BY deal_index",
                (w.lower(),),
            )
        ]
        con.close()

    seen_hits = [k for k in seen_keys if w.lower() in k]
    # also token|wallet or wallet|token patterns
    for k in list(seen_keys):
        if w.lower() in k.lower():
            if k not in seen_hits:
                seen_hits.append(k)

    ub = await fetch_unique_buys(w, max_pages=5)
    act = await fetch_wallet_activity_result(w, event_types=["buy"], limit=50, max_pages=3)
    buys = ub.buys
    print(
        f"  gmgn unique={len(buys)} ok={ub.ok} rl={ub.rate_limited} raw_buys={len(act.rows)}",
        flush=True,
    )

    # unique window vs live filters
    lookback_h = tokens_unique_period_hours(
        (cfg.get("wallet") or {}).get("tokens_unique_period") or "30d"
    )
    period = (cfg.get("wallet") or {}).get("tokens_unique_period") or "30d"
    unique_count = await batch_tokens_traded_7d(
        [w], lookback_hours=lookback_h, enough=None, too_many=None
    )
    n_unique_period = unique_count.get(w.lower())

    # balance
    try:
        from app.chain import RpcClient

        rpc = RpcClient()
        bals = await batch_wallet_balances(rpc, [w])
        bal = bals.get(w.lower())
    except Exception as exc:
        bal = None
        print(f"  bal err: {exc}", flush=True)

    wallet_filters = cfg.get("wallet") or {}
    min_bal = wallet_filters.get("min_wallet_balance_eth")
    min_u = wallet_filters.get("min_tokens_traded_7d")
    max_u = wallet_filters.get("max_tokens_traded_7d")
    mcap_th = wallet_filters.get("mcap_threshold")
    min_ath = (cfg.get("screen") or {}).get("min_ath_mcap")

    reject_steps: list[str] = []
    if n_unique_period is not None:
        ok_u = True
        if min_u is not None and n_unique_period < float(min_u):
            ok_u = False
        if max_u is not None and n_unique_period > float(max_u):
            ok_u = False
        if not ok_u:
            reject_steps.append(
                f"unique_{period}={n_unique_period} need [{min_u},{max_u}]"
            )
        else:
            reject_steps.append(f"unique_{period}={n_unique_period} PASS")
    else:
        reject_steps.append(f"unique_{period}=UNKNOWN (fail-open)")

    if bal is not None and min_bal is not None:
        if bal < float(min_bal):
            reject_steps.append(f"bal={bal:.6f}<{min_bal} FAIL")
        else:
            reject_steps.append(f"bal={bal:.6f}>={min_bal} PASS")
    else:
        reject_steps.append(f"bal={bal} (min={min_bal})")

    buy_details = []
    for b in buys:
        tok = b.token.lower()
        flags = []
        if tok in parsed:
            flags.append("PARSED")
        if tok in hold_map:
            flags.append("HOLD")
        # seen keys often "wallet|token" or similar
        seen_tok = f"{w.lower()}:{tok}" in seen_keys
        if seen_tok:
            flags.append("SEEN")

        # find raw row for method / price
        raw = None
        for r in act.rows:
            t = r.get("token")
            addr = (t.get("address") if isinstance(t, dict) else t) or ""
            if str(addr).lower() == tok and (
                not b.tx_hash or str(r.get("tx_hash") or "").lower() == b.tx_hash.lower()
            ):
                raw = r
                break
        method = (raw or {}).get("method")
        creator = method_is_creator_launch(method) if method else False
        if creator:
            flags.append("CREATOR_LAUNCH")

        meta = await token_meta(tok)
        peak = meta.get("index_peak")
        if peak is None and isinstance(meta.get("meta"), dict):
            peak = meta["meta"].get("ath_mcap") or meta["meta"].get("market_cap")
        ath_pass = None
        if peak is not None and min_ath:
            ath_pass = float(peak) >= float(min_ath)

        # price at buy → rough
        price = None
        if raw:
            try:
                price = float(raw.get("price_usd") or 0) or None
            except (TypeError, ValueError):
                price = None

        detail = {
            "token": tok,
            "symbol": b.symbol,
            "ts": b.timestamp,
            "ts_fmt": fmt_ts(b.timestamp),
            "cost": b.cost_usd,
            "tx": b.tx_hash,
            "method": method,
            "creator_launch": creator,
            "flags": flags,
            "parsed": tok in parsed,
            "parsed_at": parsed_at.get(tok),
            "hold": tok in hold_map,
            "seen": seen_tok,
            "meta": meta,
            "ath_vs_min": {"peak": peak, "min_ath": min_ath, "pass": ath_pass},
            "price_usd": price,
            "mcap_threshold": mcap_th,
        }
        print(
            f"  buy {fmt_ts(b.timestamp)} {(b.symbol or '?'):12} {tok[:14]}… "
            f"cost={b.cost_usd} flags={flags} ath_pass={ath_pass} peak={peak}",
            flush=True,
        )
        buy_details.append(detail)
        await asyncio.sleep(0.3)

    # Overall reject hypothesis
    hypothesis = []
    if fu:
        hypothesis.append("IN_FOLLOWUP already")
    else:
        hypothesis.append("NOT_in_followup")
    if seen_hits:
        hypothesis.append(f"watch_seen hits={len(seen_hits)}")
    else:
        hypothesis.append("NOT_in_watch_seen")

    n_gmgn = len(buys)
    if max_u is not None and n_gmgn > float(max_u):
        hypothesis.append(f"GMGN_unique_buys={n_gmgn}>{max_u} would fail unique filter")
    elif min_u is not None and n_gmgn < float(min_u):
        hypothesis.append(f"GMGN_unique_buys={n_gmgn}<{min_u}")
    else:
        hypothesis.append(f"GMGN_unique_buys={n_gmgn} ok for [{min_u},{max_u}]")

    if n_unique_period is not None and max_u is not None and n_unique_period > float(max_u):
        hypothesis.append(f"Blockscout_unique_{period}={n_unique_period} FAIL (filter)")
    if bal is not None and min_bal is not None and bal < float(min_bal):
        hypothesis.append("balance FAIL")

    for d in buy_details:
        if d["creator_launch"]:
            hypothesis.append(f"creator_launch on {d['symbol']}")
        if d["parsed"] and not d.get("ath_vs_min", {}).get("pass"):
            hypothesis.append(f"token {d['symbol']} PARSED but ATH maybe low")
        if not d["parsed"] and d.get("ath_vs_min", {}).get("pass") is False:
            hypothesis.append(f"token {d['symbol']} never qualified ATH<{min_ath}")
        if not d["parsed"] and d.get("ath_vs_min", {}).get("peak") is None:
            hypothesis.append(f"token {d['symbol']} NOT_PARSED / no index peak")

    print("  reject_steps:", reject_steps, flush=True)
    print("  hypothesis:", hypothesis, flush=True)

    return {
        "wallet": w,
        "followup": fu,
        "deals": deals,
        "seen_hits": seen_hits[:20],
        "gmgn_ok": ub.ok,
        "gmgn_rl": ub.rate_limited,
        "n_unique_gmgn": n_gmgn,
        "n_unique_blockscout_period": n_unique_period,
        "period": period,
        "lookback_h": lookback_h,
        "balance_eth": bal,
        "reject_steps": reject_steps,
        "hypothesis": hypothesis,
        "buys": buy_details,
        "raw_methods": {
            str(r.get("method") or r.get("event_type") or "?"): None
            for r in act.rows[:30]
        },
        "raw_sample": [
            {
                "et": r.get("event_type"),
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
                "from": r.get("from_address"),
                "to": r.get("to_address"),
                "launchpad": r.get("launchpad") or r.get("launchpad_platform"),
            }
            for r in act.rows[:15]
        ],
    }


async def main() -> None:
    print("gmgn configured:", gmgn_api_configured(), flush=True)
    cfg = load_json("watch.json")
    hold = load_json("watch_hold.json")
    seen_raw = load_json("watch_seen.json")
    seen_keys = {str(k).lower() for k in (seen_raw.get("keys") or [])}
    print(
        f"filters: unique={cfg.get('wallet')} screen_ath={cfg.get('screen',{}).get('min_ath_mcap')}",
        flush=True,
    )
    print(f"parsed={len(hold.get('parsed') or [])} seen={len(seen_keys)}", flush=True)

    results = []
    for w in WALLETS:
        results.append(await audit_one(w, cfg, hold, seen_keys))
        await asyncio.sleep(1.0)

    # shared tokens / pattern
    toks = []
    for r in results:
        for b in r["buys"]:
            toks.append((r["wallet"][:12], b["token"], b["symbol"], b["ts"]))
    shared = {}
    for _, t, sym, _ in toks:
        shared.setdefault(t, []).append(sym)
    shared = {t: v for t, v in shared.items() if len(v) > 1}

    out = {
        "ts": time.time(),
        "filters": {
            "wallet": cfg.get("wallet"),
            "screen": {
                k: (cfg.get("screen") or {}).get(k)
                for k in (
                    "min_ath_mcap",
                    "max_pair_age_hours",
                    "exclude_honeypots",
                    "min_liq",
                )
            },
        },
        "shared_tokens": shared,
        "wallets": results,
    }
    OUT.write_text(json.dumps(out, indent=2, default=str))
    print("wrote", OUT, flush=True)
    if shared:
        print("SHARED TOKENS:", shared)
    else:
        print("No shared tokens between wallets")


if __name__ == "__main__":
    asyncio.run(main())
