"""FastAPI entrypoint for Robinhood early-buyer parser."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .jobs import jobs
from .models import (
    FollowupConfig,
    FollowupStatus,
    FollowupWalletFiltersUpdate,
    FollowupWalletRow,
    IndexStatus,
    JobResponse,
    ParseRequest,
    ScreenJobResponse,
    ScreenRequest,
    WatchConfig,
    WatchStatus,
    MigrationResponse,
)
from .migrations import migrated_tokens
from .screen_jobs import screen_jobs
from .token_index import token_index
from .gnome_banter import gnome_banter
from .followup import followup_runner
from .followup_store import followup_store
from .watch import watch_runner
from .watch_store import watch_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI(title="Gnomode — Robinhood Early Buyers", version="2.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _start_background() -> None:
    from .gnome_lifecycle import install_death_hooks

    install_death_hooks()
    # Background: cold-build the 24h token index, keep it fresh + ATH peaks (Gecko).
    asyncio.create_task(token_index.run_refresh_loop())
    asyncio.create_task(token_index.run_hot_enrich_loop())
    asyncio.create_task(watch_runner.run_loop())
    asyncio.create_task(followup_runner.run_loop())
    from .followup_bot import followup_bot

    asyncio.create_task(followup_bot.run_loop())
    asyncio.create_task(gnome_banter.run_loop())


@app.on_event("shutdown")
async def _shutdown_announce() -> None:
    from .gnome_lifecycle import announce_death

    announce_death("остановка сервера (shutdown)")


@app.get("/api/index/status", response_model=IndexStatus)
async def index_status():
    return IndexStatus(**token_index.status())


@app.post("/api/index/refresh", response_model=IndexStatus)
async def index_refresh():
    # Fire-and-forget incremental refresh (no-op if one is already running).
    asyncio.create_task(token_index.refresh(full=False))
    return IndexStatus(**token_index.status())


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "chain_id": 4663,
        "rpc_url": settings.rpc_url.split("/v2/")[0] if "/v2/" in settings.rpc_url else settings.rpc_url,
        "mcap_threshold": settings.mcap_threshold,
    }


@app.get("/api/migrations", response_model=MigrationResponse)
async def migrations(
    launchpads: str = "pons,flap",
    use_dexscreener: bool = True,
    max_age_hours: float | None = None,
    min_liquidity_usd: float | None = None,
    max_liquidity_usd: float | None = None,
    min_traders_24h: int | None = None,
    max_traders_24h: int | None = None,
):
    started = time.monotonic()
    selected = {item.strip().lower() for item in launchpads.split(",") if item.strip()}
    selected &= {"pons", "flap"}
    if not selected:
        raise HTTPException(400, "Select Pons or Flap")
    tokens, errors = await migrated_tokens(
        selected,
        use_dexscreener,
        max_age_hours,
        min_liquidity_usd,
        max_liquidity_usd,
        min_traders_24h,
        max_traders_24h,
    )
    return MigrationResponse(
        tokens=tokens,
        errors=errors,
        count=len(tokens),
        duration_ms=round((time.monotonic() - started) * 1000),
    )


@app.post("/api/parse", response_model=JobResponse)
async def start_parse(req: ParseRequest):
    tokens = []
    for raw in req.tokens:
        for part in raw.replace(";", "\n").replace(",", "\n").split():
            part = part.strip()
            if part:
                tokens.append(part)
    if not tokens:
        raise HTTPException(400, "Provide at least one token address")
    # Dedupe preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for t in tokens:
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(t)
    req.tokens = uniq
    return await jobs.create(req)


@app.get("/api/parse/{job_id}", response_model=JobResponse)
async def get_parse(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.post("/api/screen", response_model=ScreenJobResponse)
async def start_screen(req: ScreenRequest):
    return await screen_jobs.create(req)


@app.get("/api/screen/{job_id}", response_model=ScreenJobResponse)
async def get_screen(job_id: str):
    job = screen_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@app.get("/api/watch", response_model=WatchConfig)
async def get_watch():
    return watch_store.load_config()


@app.put("/api/watch", response_model=WatchConfig)
async def put_watch(cfg: WatchConfig):
    saved = watch_store.save_config(cfg)
    watch_runner.notify_config_changed()
    return saved


@app.get("/api/watch/status", response_model=WatchStatus)
async def get_watch_status():
    st = watch_runner.status()
    bits = gnome_banter.status_bits()
    return st.model_copy(update=bits)


@app.post("/api/watch/run", response_model=WatchStatus)
async def watch_run_now():
    return await watch_runner.run_now()


@app.post("/api/watch/stop", response_model=WatchStatus)
async def watch_stop():
    return await watch_runner.stop()


@app.post("/api/watch/reset-counters", response_model=WatchStatus)
async def watch_reset_counters():
    return watch_runner.reset_counters()


@app.post("/api/watch/test-telegram")
async def watch_test_telegram():
    from .telegram import test_telegram_connection

    cfg = watch_store.load_config()
    try:
        result = await test_telegram_connection(
            chat_id=cfg.telegram_chat_id,
            topic_id=cfg.telegram_topic_id,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
    watch_runner._append_log("telegram", result.get("message") or "Telegram OK")
    return result


@app.post("/api/watch/clear-seen")
async def watch_clear_seen():
    watch_store.clear_seen()
    return {"ok": True, "seen_count": 0}


@app.get("/api/hvat/status")
async def get_hvat_status():
    from .hvat import hvat_status

    return hvat_status()


@app.post("/api/hvat/enable")
async def hvat_enable():
    """Enable Хвать profile: 1-trade wallets, first buy ≤20k, follow-up #2/#3."""
    from .hvat import apply_hvat_profile

    return apply_hvat_profile(enable=True)


@app.put("/api/hvat/filters")
async def hvat_save_filters(payload: dict):
    """Save token + wallet filters for Хвать (stored in watch config)."""
    from .hvat import save_hvat_filters

    try:
        return save_hvat_filters(
            screen=payload.get("screen") or {},
            wallet=payload.get("wallet") or {},
            max_tokens_per_cycle=payload.get("max_tokens_per_cycle"),
            interval_sec=payload.get("interval_sec"),
            sync_followup_mcap=bool(payload.get("sync_followup_mcap", True)),
            followup=payload.get("followup"),
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/hvat/disable")
async def hvat_disable():
    from .hvat import apply_hvat_profile

    return apply_hvat_profile(enable=False)


@app.post("/api/hvat/run")
async def hvat_run_now():
    """Kick both watch and follow-up cycles."""
    from .hvat import apply_hvat_profile

    apply_hvat_profile(enable=True)
    watch_st = await watch_runner.run_now()
    follow_st = await followup_runner.run_now()
    return {"ok": True, "watch": watch_st, "followup": follow_st}


@app.get("/api/followup", response_model=FollowupConfig)
async def get_followup():
    return followup_store.load_config()


@app.put("/api/followup", response_model=FollowupConfig)
async def put_followup(cfg: FollowupConfig):
    saved = followup_store.save_config(cfg)
    followup_runner.notify_config_changed()
    return saved


@app.get("/api/followup/status", response_model=FollowupStatus)
async def get_followup_status():
    return followup_runner.status()


@app.get("/api/followup/wallets", response_model=list[FollowupWalletRow])
async def get_followup_wallets(
    status: str | None = None,
    limit: int = 200,
    offset: int = 0,
):
    return followup_store.list_wallets(
        status=status, limit=limit, offset=offset, include_deals=True
    )


@app.put("/api/followup/wallets/filters")
async def put_followup_wallet_filters(payload: FollowupWalletFiltersUpdate):
    """Apply (or clear) per-wallet #2/#3 alert filters to one or many wallets."""
    updated = followup_store.set_wallet_alert_filters(payload.addresses, payload.filters)
    if not updated:
        raise HTTPException(404, "no matching wallets")
    return {
        "ok": True,
        "updated": updated,
        "count": len(updated),
        "filters": payload.filters,
    }


@app.delete("/api/followup/wallets/{address}")
async def delete_followup_wallet(address: str):
    """Remove a tracked wallet (and its deals/alerts) from follow-up."""
    ok = followup_store.delete_wallet(address)
    if not ok:
        raise HTTPException(404, "wallet not found")
    return {"ok": True, "address": address.strip().lower()}


@app.post("/api/followup/run", response_model=FollowupStatus)
async def followup_run_now():
    return await followup_runner.run_now()


@app.post("/api/followup/stop", response_model=FollowupStatus)
async def followup_stop():
    return await followup_runner.stop()


@app.post("/api/followup/reset-counters", response_model=FollowupStatus)
async def followup_reset_counters():
    return followup_runner.reset_counters()


@app.post("/api/followup/test-telegram")
async def followup_test_telegram():
    from .telegram import test_telegram_connection

    cfg = followup_store.load_config()
    try:
        result = await test_telegram_connection(
            chat_id=cfg.telegram_chat_id,
            topic_id=cfg.telegram_topic_id,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
    followup_runner._append_log("telegram", result.get("message") or "Telegram OK")
    return result


@app.post("/api/followup/test-raybot")
async def followup_test_raybot():
    from .raybot import RayBotClient

    client = RayBotClient()
    try:
        return await client.test_connection()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/followup/webhook/raybot")
async def followup_raybot_webhook(request: Request):
    """Optional RayBot webhook: acknowledge quickly; process deals in background."""
    expected = (settings.raybot_webhook_auth or "").strip()
    if expected:
        auth = request.headers.get("Authorization") or ""
        if auth != expected:
            raise HTTPException(401, "Unauthorized")
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001
        payload = {}
    event_type = str(
        payload.get("event_type")
        or request.headers.get("X-RayBot-Event")
        or ""
    ).lower()
    if event_type == "test":
        return {"ok": True}
    asyncio.create_task(_handle_raybot_event(payload, event_type))
    return JSONResponse({"ok": True})


async def _handle_raybot_event(payload: dict, event_type: str) -> None:
    from .followup import alert_kwargs_from_config, estimate_token_mcap, should_alert_deal
    from .telegram import (
        resolve_chat_id,
        resolve_topic_id,
        send_followup_deal,
        telegram_configured,
    )

    if event_type not in ("buy", "evm_buy", "swap"):
        return
    cfg = followup_store.load_config()
    followed = payload.get("followed_wallets") or []
    tokens = payload.get("tokens") or {}
    event = payload.get("event") or {}
    mint = str(event.get("mintOut") or event.get("mint") or "")
    for change in payload.get("token_changes") or []:
        if str(change.get("direction") or "").lower() == "in":
            mint = str(change.get("mint") or mint)
            break
    symbol = ""
    mcap = None
    if mint and isinstance(tokens, dict):
        meta = tokens.get(mint) or tokens.get(mint.lower()) or {}
        symbol = str(meta.get("symbol") or "")
        try:
            price = float(meta.get("price_usd") or 0)
            supply = float(meta.get("supply") or 0)
            if price > 0 and supply > 0:
                mcap = price * supply
        except (TypeError, ValueError):
            mcap = None
    tx = str(payload.get("id") or "")
    # Prefer on-chain entry (fill / pre-swap) over live RayBot quote × supply.
    if mint and tx.startswith("0x"):
        try:
            from .replay import estimate_entry_at_tx

            entry = await estimate_entry_at_tx(mint, tx)
            if entry.mcap and entry.mcap > 0:
                mcap = entry.mcap
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).debug("raybot entry mcap failed: %s", exc)
    if mcap is None and mint:
        mcap = await estimate_token_mcap(mint)
    for w in followed:
        addr = str((w or {}).get("address") or "").lower()
        if not addr or not mint:
            continue
        deal = followup_store.record_deal(
            wallet=addr,
            token=mint,
            token_symbol=symbol,
            mcap_at_buy=mcap,
            tx_hash=tx,
            max_deals=cfg.max_deals,
        )
        if not deal:
            continue
        if not should_alert_deal(
            deal.deal_index,
            deal.mcap_at_buy,
            bought_usd=deal.bought_usd,
            **alert_kwargs_from_config(cfg),
        ):
            continue
        chat = resolve_chat_id(cfg.telegram_chat_id)
        if not telegram_configured(chat):
            continue
        if not followup_store.mark_notified(deal.wallet, deal.token):
            continue
        try:
            await send_followup_deal(
                chat,
                wallet=deal.wallet,
                token=deal.token,
                token_symbol=deal.token_symbol,
                deal_index=deal.deal_index,
                mcap_at_buy=deal.mcap_at_buy,
                topic_id=resolve_topic_id(cfg.telegram_topic_id),
            )
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning("webhook alert failed: %s", exc)


# Serve built frontend if present
_frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
if _frontend_dist.is_dir():
    app.mount("/assets", StaticFiles(directory=_frontend_dist / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def spa(full_path: str):
        index = _frontend_dist / "index.html"
        file_path = _frontend_dist / full_path
        if full_path and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(index)
