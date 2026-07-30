"""FastAPI entrypoint for Robinhood early-buyer parser."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .database import get_db
from .gnome_banter import gnome_banter
from .jobs import jobs
from .models import (
    BlacklistRequest,
    BlacklistRow,
    IndexStatus,
    JobResponse,
    McapSnapshotRow,
    McapTrackerAddRequest,
    McapTrackerRow,
    MigratedTokenRow,
    MigrationBusStatus,
    MigrationParseRequest,
    MigrationParseResult,
    MigrationScanJobResponse,
    MigrationScanRequest,
    ParseRequest,
    ScreenJobResponse,
    ScreenRequest,
    SniperFollowStatus,
    SniperRow,
    UserFilters,
    UserFiltersUpdate,
    WalletTradeRow,
    WatchConfig,
    WatchStatus,
)
from .migration_pipeline import handle_migration, parse_token_migration
from .migration_scan import migration_scans

from .screen_jobs import screen_jobs
from .sniper_follow import sniper_follow
from .telegram_bot import telegram_bot
from .token_index import token_index
from .watch import watch_runner
from .watch_store import watch_store
from .ws_migration import migration_bus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from .gnome_lifecycle import install_death_hooks, announce_death

    install_death_hooks()
    get_db()
    try:
        n = get_db().deactivate_wallets_above_first_mcap(settings.sniper_max_first_mcap)
        if n:
            logging.getLogger(__name__).info(
                "Deactivated %d tracked wallets with first_mcap > %.0f",
                n,
                settings.sniper_max_first_mcap,
            )
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).exception("Failed pruning high-mcap tracked wallets")
    migration_bus.set_handler(handle_migration)
    asyncio.create_task(token_index.run_refresh_loop())
    asyncio.create_task(_supervise_watch_loop())
    asyncio.create_task(gnome_banter.run_loop())
    asyncio.create_task(migration_bus.run())
    asyncio.create_task(_mcap_tracker_loop())
    asyncio.create_task(sniper_follow.run_loop())
    asyncio.create_task(telegram_bot.run_loop())

    async def _rhj_sync() -> None:
        try:
            from .rhj_assets import sync_rhj_blacklist

            n = await sync_rhj_blacklist()
            logging.getLogger(__name__).info("RHJ blacklist sync: %d", n)
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).exception("RHJ blacklist sync failed")

    asyncio.create_task(_rhj_sync())

    yield

    migration_bus.stop()
    sniper_follow.stop()
    telegram_bot.stop()
    announce_death("остановка сервера (shutdown)")


async def _supervise_watch_loop() -> None:
    """Restart watch runner if the task dies (cancel/crash left enabled=true, next=null)."""
    log = logging.getLogger(__name__)
    while True:
        task = asyncio.create_task(watch_runner.run_loop(), name="watch_runner")
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            log.info("Watch supervisor cancelled (shutdown)")
            raise
        if task.cancelled():
            log.warning("Watch runner task cancelled — restart in 5s")
        else:
            exc = task.exception()
            if exc is not None:
                log.error("Watch runner died: %s — restart in 5s", exc, exc_info=exc)
            else:
                log.warning("Watch runner exited cleanly — restart in 5s")
        await asyncio.sleep(5.0)


async def _mcap_tracker_loop() -> None:
    log = logging.getLogger(__name__)
    while True:
        try:
            await asyncio.sleep(settings.mcap_tracker_interval_sec)
            if settings.mcap_tracker_enabled:
                from .mcap_checker import check_mcap_tracker

                await check_mcap_tracker()
        except asyncio.CancelledError:
            break
        except Exception:  # noqa: BLE001
            log.exception("MCAP tracker loop error")


app = FastAPI(title="Gnomode — Robinhood Early Buyers", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/index/status", response_model=IndexStatus)
async def index_status():
    return IndexStatus(**token_index.status())


@app.post("/api/index/refresh", response_model=IndexStatus)
async def index_refresh():
    asyncio.create_task(token_index.refresh(full=False))
    return IndexStatus(**token_index.status())


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "chain_id": 4663,
        "rpc_url": settings.rpc_url.split("/v2/")[0] if "/v2/" in settings.rpc_url else settings.rpc_url,
        "mcap_threshold": settings.mcap_threshold,
        "migration_bus": settings.migration_bus_enabled,
        "last_migration_block": migration_bus.last_seen_block,
    }


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


@app.delete("/api/parse/{job_id}")
async def cancel_parse(job_id: str):
    ok = await jobs.cancel(job_id)
    if not ok:
        raise HTTPException(404, "Job not found or already finished")
    return {"status": "cancelled"}


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


@app.get("/api/snipers", response_model=list[SniperRow])
async def list_snipers(limit: int = Query(default=100, ge=1, le=1000)):
    rows = await get_db().aget_snipers_by_trade_count(limit=limit)
    return [
        SniperRow(
            address=r["address"],
            first_seen=r.get("first_seen"),
            trade_count=int(r.get("trade_count") or 0),
            winrate=r.get("winrate"),
            first_token=r.get("first_token"),
            first_mcap=r.get("first_mcap"),
            is_active=bool(r.get("is_active", 1)),
        )
        for r in rows
    ]


@app.get("/api/snipers/follow/status", response_model=SniperFollowStatus)
async def sniper_follow_status():
    return SniperFollowStatus(**sniper_follow.status)


@app.get("/api/snipers/filters", response_model=UserFilters)
async def get_sniper_filters(chat_id: str = Query(default="")):
    from .telegram import resolve_chat_id

    cid = (chat_id or "").strip() or resolve_chat_id()
    if not cid:
        raise HTTPException(400, "chat_id required (or set TELEGRAM_CHAT_ID)")
    row = await get_db().aget_user_filters(cid) or await get_db().aupsert_user_filters(cid)
    return UserFilters(
        chat_id=str(row.get("chat_id") or cid),
        min_buy_usd=float(row.get("min_buy_usd") or 0),
        max_mcap_usd=float(row.get("max_mcap_usd") or 0),
        exclude_honeypots=bool(row.get("exclude_honeypots", 1)),
        min_liq_usd=float(row.get("min_liq_usd") or 0),
        max_liq_usd=float(row.get("max_liq_usd") or 0),
        updated_at=row.get("updated_at"),
    )


@app.put("/api/snipers/filters", response_model=UserFilters)
async def put_sniper_filters(body: UserFiltersUpdate, chat_id: str = Query(default="")):
    from .telegram import resolve_chat_id

    cid = (chat_id or "").strip() or resolve_chat_id()
    if not cid:
        raise HTTPException(400, "chat_id required (or set TELEGRAM_CHAT_ID)")
    row = await get_db().aupsert_user_filters(
        cid,
        min_buy_usd=body.min_buy_usd,
        max_mcap_usd=body.max_mcap_usd,
        exclude_honeypots=body.exclude_honeypots,
        min_liq_usd=body.min_liq_usd,
        max_liq_usd=body.max_liq_usd,
    )
    return UserFilters(
        chat_id=str(row.get("chat_id") or cid),
        min_buy_usd=float(row.get("min_buy_usd") or 0),
        max_mcap_usd=float(row.get("max_mcap_usd") or 0),
        exclude_honeypots=bool(row.get("exclude_honeypots", 1)),
        min_liq_usd=float(row.get("min_liq_usd") or 0),
        max_liq_usd=float(row.get("max_liq_usd") or 0),
        updated_at=row.get("updated_at"),
    )


@app.get("/api/migrations", response_model=list[MigratedTokenRow])
async def list_migrations(limit: int = Query(default=100, ge=1, le=1000)):
    rows = await get_db().aget_top_tokens(limit=limit * 2)
    # Hide empty junk that may remain from older scans
    cleaned = [
        r
        for r in rows
        if (r.get("symbol") or "").strip() or (r.get("name") or "").strip()
    ][:limit]
    return [
        MigratedTokenRow(
            address=r["address"],
            symbol=r.get("symbol"),
            name=r.get("name"),
            launchpad_id=r.get("launchpad_id"),
            dex=r.get("dex"),
            pool_id=r.get("pool_id"),
            curve_address=r.get("curve_address"),
            migration_block=r.get("migration_block"),
            migration_tx=r.get("migration_tx"),
            honeypot=bool(r.get("honeypot")),
            start_mcap=r.get("start_mcap"),
            mcap_usd=r.get("mcap_usd"),
            liquidity_usd=r.get("liquidity_usd"),
            created_at=r.get("created_at"),
        )
        for r in cleaned
    ]


@app.get("/api/migrations/status", response_model=MigrationBusStatus)
async def migrations_status():
    return MigrationBusStatus(**migration_bus.status())


@app.post("/api/migrations/parse", response_model=MigrationParseResult)
async def migrations_parse(req: MigrationParseRequest):
    raw = req.token.strip()
    if not raw.startswith("0x") or len(raw) < 42:
        raise HTTPException(400, "Invalid token address")
    try:
        summary = await parse_token_migration(
            raw, launchpad_id=req.launchpad_id or None
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
    return MigrationParseResult(**summary)


@app.post("/api/migrations/scan", response_model=MigrationScanJobResponse)
async def migrations_scan_start(req: MigrationScanRequest | None = None):
    """Find migrated tokens on-chain and parse snipers (background job)."""
    body = req or MigrationScanRequest()
    job = await migration_scans.create(hours=body.hours, max_tokens=body.max_tokens)
    return MigrationScanJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        result=job.result,
        error=job.error,
    )


@app.get("/api/migrations/scan/{job_id}", response_model=MigrationScanJobResponse)
async def migrations_scan_status(job_id: str):
    job = migration_scans.get(job_id)
    if not job:
        raise HTTPException(404, "Scan job not found")
    return MigrationScanJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        result=job.result,
        error=job.error,
    )


@app.post("/api/migrations/gap-fill")
async def migrations_gap_fill(blocks: int = Query(default=20_000, ge=100, le=200_000)):
    tip_block = migration_bus.last_seen_block
    from_block = max(1, tip_block - blocks + 1) if tip_block else None
    n = await migration_bus.gap_fill(from_block)
    return {
        "ok": True,
        "emitted": n,
        "last_seen_block": migration_bus.last_seen_block,
    }


@app.get("/api/mcap-tracker", response_model=list[McapTrackerRow])
async def get_mcap_tracker_list():
    rows = await get_db().aget_mcap_tracker_all()
    return [McapTrackerRow(**r) for r in rows]


@app.get("/api/mcap-tracker/{address}")
async def get_mcap_tracker_detail(address: str):
    from datetime import datetime, timedelta, timezone

    db = get_db()
    token = await db.aget_mcap_tracker_one(address)
    if not token:
        raise HTTPException(404, "Token not found in tracker")
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    snapshots = await db.aget_mcap_snapshots(address, since_iso=since)
    return {
        "token": McapTrackerRow(**token).model_dump(),
        "snapshots": [McapSnapshotRow(**s).model_dump() for s in snapshots],
    }


@app.post("/api/mcap-tracker")
async def add_mcap_tracker(req: McapTrackerAddRequest):
    from .mcap_checker import add_token_to_tracker

    added = await add_token_to_tracker(
        token_address=req.address,
        symbol=req.symbol,
        name=req.name,
        launchpad_id=req.launchpad_id,
        dex=req.dex,
        first_seen_mcap=req.first_seen_mcap,
    )
    if not added:
        raise HTTPException(400, "Token already tracked or migrated")
    return {"ok": True}


@app.delete("/api/mcap-tracker/{address}")
async def delete_mcap_tracker(address: str):
    await get_db().adelete_mcap_tracker(address)
    return {"ok": True}


@app.get("/api/trades", response_model=list[WalletTradeRow])
async def list_trades(
    wallet: str | None = None,
    token: str | None = None,
    limit: int = Query(default=200, ge=1, le=2000),
):
    rows = await get_db().aget_trades(wallet=wallet, token=token, limit=limit)
    return [
        WalletTradeRow(
            id=int(r["id"]),
            wallet=r["wallet"],
            token=r["token"],
            mcap_at_trade=r.get("mcap_at_trade"),
            amount_usd=r.get("amount_usd"),
            tx_hash=r.get("tx_hash"),
            block=r.get("block"),
            trade_number=r.get("trade_number"),
            created_at=r.get("created_at"),
        )
        for r in rows
    ]


@app.get("/api/blacklist", response_model=list[BlacklistRow])
async def list_blacklist():
    rows = await get_db().alist_blacklist()
    return [
        BlacklistRow(
            address=r["address"],
            reason=r.get("reason"),
            source=r.get("source"),
            created_at=r.get("created_at"),
        )
        for r in rows
    ]


@app.post("/api/blacklist", response_model=BlacklistRow)
async def add_blacklist(req: BlacklistRequest):
    addr = req.address.strip()
    if not addr.startswith("0x") or len(addr) < 42:
        raise HTTPException(400, "Invalid address")
    await get_db().aadd_blacklist(addr, reason=req.reason, source=req.source or "manual")
    return BlacklistRow(
        address=addr.lower(),
        reason=req.reason,
        source=req.source or "manual",
        created_at=None,
    )


@app.delete("/api/blacklist/{address}")
async def delete_blacklist(address: str):
    await get_db().aremove_blacklist(address)
    return {"ok": True}


@app.post("/api/blacklist/sync-rhj")
async def sync_rhj():
    from .rhj_assets import sync_rhj_blacklist

    n = await sync_rhj_blacklist()
    return {"ok": True, "count": n}


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
