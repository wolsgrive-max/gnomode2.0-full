#!/usr/bin/env python3
"""One-shot: parse missed ATH-gate tokens and ingest early buyers into follow-up.

Finds tokens with ATH≥watch min_ath and age≤max_pair_age that are absent from
hold / pending / parsed (plus a hard-coded FROGLET seed). Parses with current
Хвать wallet filters and calls ``ingest_from_watch``.

Usage (in gnomode container)::

    python /app/scripts/backfill_missed_parse.py
    python /app/scripts/backfill_missed_parse.py --dry-run --limit 10
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("backfill_missed")

# Confirmed miss from watch outage / donor silent omission.
FROGLET = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
KNOWN_SEED = [FROGLET]


async def _peak_for(addr: str) -> tuple[float, float | None, str]:
    """Return (peak_mcap, pair_age_hours, symbol) via DS + peak estimator."""
    from app.followup import estimate_token_peak_mcap
    from app.pools import fetch_dexscreener_pairs

    pairs = await fetch_dexscreener_pairs(addr)
    symbol = ""
    age_h: float | None = None
    spot = 0.0
    if pairs:
        best = max(
            pairs,
            key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0.0),
        )
        bt = best.get("baseToken") or {}
        symbol = str(bt.get("symbol") or "")
        spot = float(best.get("marketCap") or best.get("fdv") or 0.0)
        created = best.get("pairCreatedAt")
        if created:
            try:
                age_h = (time.time() * 1000.0 - float(created)) / 3_600_000.0
            except (TypeError, ValueError):
                age_h = None
    peak = await estimate_token_peak_mcap(addr, min_needed=1.0)
    ath = max(float(peak.peak or 0.0), spot)
    return ath, age_h, symbol


async def _collect_candidates(*, limit: int) -> list[tuple[str, float, float | None, str]]:
    from app.token_index import token_index
    from app.watch_qualify import ath_gate_enabled
    from app.watch_store import watch_store

    cfg = watch_store.load_config()
    min_ath = float(cfg.screen.min_ath_mcap or 0.0)
    max_age = cfg.screen.max_pair_age_hours
    max_age_f = (
        float(max_age) if max_age is not None and float(max_age) > 0 else None
    )
    hold = watch_store.load_hold()
    parsed = watch_store.load_parsed_at()
    pending = set(
        watch_store.load_pending_parse(min_ath_mcap=cfg.screen.min_ath_mcap)
        if ath_gate_enabled(cfg.screen.min_ath_mcap)
        else []
    )
    known: set[str] = set(hold) | set(parsed) | pending

    out: list[tuple[str, float, float | None, str]] = []
    seen: set[str] = set()

    async def consider(addr: str, *, force: bool = False) -> None:
        key = addr.lower()
        if key in seen:
            return
        seen.add(key)
        if not force and key in known:
            return
        try:
            ath, age_h, symbol = await _peak_for(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("peak failed %s: %s", key[:12], type(exc).__name__)
            return
        if min_ath > 0 and ath < min_ath:
            logger.info("skip %s ath=%.0f < %.0f", symbol or key[:12], ath, min_ath)
            return
        if max_age_f is not None and age_h is not None and age_h > max_age_f:
            logger.info(
                "skip %s age=%.1fh > %.0fh", symbol or key[:12], age_h, max_age_f
            )
            return
        out.append((key, ath, age_h, symbol))

    for seed in KNOWN_SEED:
        await consider(seed, force=True)

    # Local index: high-ATH young tokens missing from pipeline state.
    indexed = token_index.get_tokens()
    indexed.sort(
        key=lambda t: max(float(t.ath_mcap or 0.0), float(t.market_cap or 0.0)),
        reverse=True,
    )
    for row in indexed:
        if len(out) >= limit:
            break
        peak = max(float(row.ath_mcap or 0.0), float(row.market_cap or 0.0))
        if min_ath > 0 and peak < min_ath:
            continue
        if max_age_f is not None and row.pair_age_hours is not None:
            if float(row.pair_age_hours) > max_age_f:
                continue
        key = row.address.lower()
        if key in known or key in seen:
            continue
        seen.add(key)
        out.append((key, peak, row.pair_age_hours, row.symbol or ""))

    out.sort(key=lambda x: x[1], reverse=True)
    return out[:limit]


async def _run(*, dry_run: bool, limit: int) -> int:
    from app.chain import RpcClient
    from app.followup_store import followup_store
    from app.models import ParseRequest
    from app.replay import parse_token
    from app.watch_store import watch_store

    cfg = watch_store.load_config()
    candidates = await _collect_candidates(limit=limit)
    logger.info(
        "candidates=%s dry_run=%s min_ath=%s max_age=%s",
        len(candidates),
        dry_run,
        cfg.screen.min_ath_mcap,
        cfg.screen.max_pair_age_hours,
    )
    for addr, ath, age, sym in candidates:
        logger.info(
            "  %s %s ath=%.0f age=%s",
            sym or "?",
            addr,
            ath,
            f"{age:.2f}h" if age is not None else "?",
        )

    if dry_run:
        return 0

    threshold = (
        cfg.wallet.mcap_threshold
        if cfg.wallet.mcap_threshold is not None
        else 15_000.0
    )
    rpc = RpcClient()
    total_ingested = 0
    now = time.time()

    for addr, ath, age, sym in candidates:
        logger.info("parse %s (%s)…", sym or addr[:12], addr)
        wallet_req = ParseRequest(
            tokens=[addr],
            mcap_threshold=cfg.wallet.mcap_threshold,
            exclude_honeypots=cfg.wallet.exclude_honeypots,
            min_wallet_balance_eth=cfg.wallet.min_wallet_balance_eth,
            max_wallet_balance_eth=cfg.wallet.max_wallet_balance_eth,
            min_hold_time_minutes=cfg.wallet.min_hold_time_minutes,
            max_hold_time_minutes=cfg.wallet.max_hold_time_minutes,
            min_tokens_traded_7d=cfg.wallet.min_tokens_traded_7d,
            max_tokens_traded_7d=cfg.wallet.max_tokens_traded_7d,
            tokens_unique_period=cfg.wallet.tokens_unique_period,
        )
        try:
            result = await parse_token(
                rpc,
                addr,
                mcap_threshold=float(threshold),
                exclude_honeypots=bool(cfg.wallet.exclude_honeypots),
                wallet_filters=wallet_req,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("parse failed %s: %s", addr[:12], exc)
            continue
        if result.error:
            logger.warning("parse error %s: %s", addr[:12], result.error)
            # Still stamp hold so watch can retry / track ATH.
            watch_store.apply_qualify_updates(
                ath_updates={addr: (ath, sym or result.symbol or "")},
                held=[],
                expired=[],
                candidates=[addr] if ath >= float(cfg.screen.min_ath_mcap or 0) else [],
                now=now,
            )
            continue
        buyers = [b for b in result.buyers if b.buys_count == 1]
        logger.info(
            "%s → %s buyers (buys=1), symbol=%s",
            addr[:12],
            len(buyers),
            result.symbol,
        )
        watch_store.apply_qualify_updates(
            ath_updates={addr: (ath, result.symbol or sym or "")},
            held=[],
            expired=[],
            candidates=[addr],
            now=now,
        )
        if buyers:
            # Backfill must not be blocked by live max_mcap_alert (spot ATH often
            # already > gate while seed buys were early).
            inserted = followup_store.ingest_buyers(
                buyers,
                max_deals=int(followup_store.load_config().max_deals or 5),
                max_mcap_alert=None,
            )
            total_ingested += len(inserted)
            logger.info("ingested %s deals for %s", len(inserted), result.symbol or addr[:12])
            for b in buyers:
                logger.info(
                    "  wallet=%s bal=%s unique=%s mcap=%.0f",
                    b.wallet,
                    b.wallet_balance_eth,
                    b.tokens_traded_7d,
                    b.mcap_at_first_buy,
                )
        # Only mark parsed when we actually processed buyers (avoid sticky miss).
        if buyers or not result.error:
            if buyers:
                watch_store.mark_token_parsed(addr)

    logger.info("done: tokens=%s ingested_deals=%s", len(candidates), total_ingested)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(dry_run=args.dry_run, limit=args.limit)))


if __name__ == "__main__":
    main()
