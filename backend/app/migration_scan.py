"""On-demand scan for *graduated* launchpad tokens on Robinhood Chain.

Only Bags ``Migrated`` / V4 Initialize with BagsV4Hook and hood.fun ``Graduated``.
Does NOT treat raw V3 PoolCreated / generic V4 Initialize as migrations.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Awaitable, Callable

from .chain import RpcClient, checksum
from .config import settings
from .constants import (
    BAGS_DEPLOY_BLOCK,
    BAGS_FACTORY,
    BAGS_LENS,
    BAGS_LENS_ABI,
    BAGS_MIGRATED_TOPIC,
    BAGS_V4_HOOK,
    BLOCKS_PER_SECOND,
    FLAP_LAUNCHED_TO_DEX_TOPIC,
    FLAP_LAUNCHPAD,
    FLAP_VAULT_PORTAL,
    HOODFUN_GRADUATED_TOPIC,
    HOODFUN_LAUNCHPAD,
    HOODFUN_LAUNCHPAD_LEGACY,
)
from .database import get_db
from .goplus import check_token_security
from .launchpads.bags import parse_bags_migrated_log
from .launchpads.flap import parse_flap_launched_log
from .launchpads.hoodfun import parse_hoodfun_graduated_log
from .launchpads.types import MigrationEvent
from .migration_validate import verify_migration_event
from .models import JobProgress, JobStatus
from .rhj_assets import is_rhj_token

logger = logging.getLogger(__name__)

ProgressCb = Callable[[str, str, float], Awaitable[None]]

# Bonding-curve pads that graduation scan persists.
_GRADUATED_LAUNCHPADS = frozenset({"bags", "hoodfun", "flap"})


def _norm_log(lg: Any) -> dict[str, Any]:
    data = lg.get("data")
    if isinstance(data, (bytes, bytearray)):
        data = "0x" + data.hex()
    return {
        "address": lg.get("address"),
        "topics": list(lg.get("topics") or []),
        "data": data,
        "blockNumber": int(lg["blockNumber"]),
        "transactionHash": lg.get("transactionHash"),
        "logIndex": int(lg.get("logIndex") or 0),
    }


async def _get_logs_topic_only(
    rpc: RpcClient,
    *,
    topic: str,
    from_block: int,
    to_block: int,
    chunk: int | None = None,
) -> list[Any]:
    """eth_getLogs without address filter (Bags Migrated is per-curve)."""
    size = chunk or max(10_000, min(settings.log_chunk_size, 100_000))
    out: list[Any] = []
    start = from_block
    while start <= to_block:
        end = min(start + size - 1, to_block)
        try:
            batch = await rpc._call(
                lambda s=start, e=end: rpc.w3.eth.get_logs(
                    {
                        "fromBlock": s,
                        "toBlock": e,
                        "topics": [topic],
                    }
                )
            )
            if batch:
                out.extend(batch)
        except Exception as exc:  # noqa: BLE001
            # Shrink chunk on RPC limit errors
            if size > 5_000:
                size = max(5_000, size // 2)
                logger.warning(
                    "topic-only getLogs %s→%s failed (%s); retry chunk=%s",
                    start,
                    end,
                    exc,
                    size,
                )
                continue
            logger.warning("topic-only getLogs %s→%s failed: %s", start, end, exc)
        start = end + 1
    return out


_BAGS_FACTORY_ABI = [
    {
        "inputs": [],
        "name": "allTokensLength",
        "outputs": [{"type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"type": "uint256"}, {"type": "uint256"}],
        "name": "getTokens",
        "outputs": [{"type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]


def _pool_id_hex(pool_id: Any) -> str | None:
    if pool_id is None:
        return None
    if isinstance(pool_id, (bytes, bytearray)):
        h = pool_id.hex()
    else:
        h = str(pool_id)
    if not h.startswith("0x"):
        h = "0x" + h
    if set(h[2:].lower()) <= {"0"}:
        return None
    return h.lower()


async def _discover_bags_via_lens(
    rpc: RpcClient,
    *,
    from_block: int,
    to_block: int,
    lookback: int,
    on_progress: ProgressCb | None = None,
) -> list[MigrationEvent]:
    """Bags graduations via Lens.migrated on recent factory tokens.

    Topic-only eth_getLogs is sparse/rate-limited; Lens is the source of truth
    for whether a Bags token has already graduated. Migration block/tx are
    filled from the curve ``Migrated`` log when present in the scan window.
    """
    events: list[MigrationEvent] = []
    factory = rpc.w3.eth.contract(address=checksum(BAGS_FACTORY), abi=_BAGS_FACTORY_ABI)
    lens = rpc.w3.eth.contract(address=checksum(BAGS_LENS), abi=BAGS_LENS_ABI)
    n = int(await rpc._call(lambda: factory.functions.allTokensLength().call()))
    start = max(0, n - lookback)
    toks = await rpc._call(lambda: factory.functions.getTokens(start, lookback).call())
    if on_progress:
        await on_progress(
            "scan",
            f"Bags Lens: {len(toks)} токенов (registry {start}→{n})…",
            0.15,
        )

    sem = asyncio.Semaphore(6)

    async def one(token_raw: Any) -> MigrationEvent | None:
        token = checksum(token_raw)
        async with sem:
            try:
                state = await rpc._call(
                    lambda t=token: lens.functions.getTokenState(t).call()
                )
            except Exception:  # noqa: BLE001
                return None
        exists = state[0] if isinstance(state, (tuple, list)) else state.exists
        migrated = state[1] if isinstance(state, (tuple, list)) else state.migrated
        curve = state[2] if isinstance(state, (tuple, list)) else state.curve
        pool_id = state[4] if isinstance(state, (tuple, list)) else state.poolId
        if not exists or not migrated:
            return None

        curve_addr = checksum(curve) if curve else None
        pool_hex = _pool_id_hex(pool_id)
        return MigrationEvent(
            token=token,
            launchpad_id="bags",
            dex="uniswap_v4",
            block=to_block,
            tx="0x" + "0" * 64,
            pool_id=pool_hex,
            curve_address=curve_addr,
            hooks=BAGS_V4_HOOK,
            source="scan",
        )

    chunk = 40
    for i in range(0, len(toks), chunk):
        batch = toks[i : i + chunk]
        got = await asyncio.gather(*[one(t) for t in batch])
        events.extend(ev for ev in got if ev)
        if on_progress:
            await on_progress(
                "scan",
                f"Bags Lens: {min(i + chunk, len(toks))}/{len(toks)}, "
                f"миграций {len(events)}",
                0.15 + 0.4 * (min(i + chunk, len(toks)) / max(len(toks), 1)),
            )
    return events


async def discover_migration_events(
    rpc: RpcClient,
    *,
    from_block: int,
    to_block: int,
    hours: float = 168.0,
    on_progress: ProgressCb | None = None,
) -> list[MigrationEvent]:
    """All bonding-curve graduations: Bags + hood.fun + Flap."""
    events: list[MigrationEvent] = []

    async def prog(msg: str, pct: float) -> None:
        if on_progress:
            await on_progress("scan", msg, pct)

    # Bags: full factory registry via Lens when window ≥7d; else a deep recent slice.
    factory = rpc.w3.eth.contract(address=checksum(BAGS_FACTORY), abi=_BAGS_FACTORY_ABI)
    try:
        bags_total = int(
            await rpc._call(lambda: factory.functions.allTokensLength().call())
        )
    except Exception:  # noqa: BLE001
        bags_total = 500
    if hours >= 168:
        lookback = bags_total
    else:
        lookback = max(400, min(1_000, int(hours * 8) + 200))

    await prog(f"Bags Lens.migrated (lookback {lookback}/{bags_total})…", 0.04)
    try:
        lens_events = await _discover_bags_via_lens(
            rpc,
            from_block=from_block,
            to_block=to_block,
            lookback=lookback,
            on_progress=on_progress,
        )
        events.extend(lens_events)
        await prog(f"Bags через Lens: {len(lens_events)}", 0.5)
    except Exception:  # noqa: BLE001
        logger.exception("Bags Lens scan failed")

    # Topic-only Migrated is expensive on public RPC — short windows only.
    if hours <= 48:
        mig_from = max(from_block, BAGS_DEPLOY_BLOCK)
        if mig_from <= to_block:
            await prog(f"Bags Migrated topic {mig_from}→{to_block}…", 0.52)
            try:
                bags_logs = await _get_logs_topic_only(
                    rpc,
                    topic=BAGS_MIGRATED_TOPIC,
                    from_block=mig_from,
                    to_block=to_block,
                    chunk=50_000,
                )
                for lg in bags_logs:
                    ev = parse_bags_migrated_log(_norm_log(lg))
                    if ev:
                        try:
                            ev.curve_address = checksum(str(lg.get("address")))
                        except Exception:  # noqa: BLE001
                            pass
                        ev.hooks = BAGS_V4_HOOK
                        ev.source = "scan"
                        events.append(ev)
                await prog(f"Bags Migrated logs: {len(bags_logs)}", 0.58)
            except Exception:  # noqa: BLE001
                logger.exception("Bags Migrated topic scan failed")

    await prog("hood.fun Graduated…", 0.58)
    hood_n = 0
    for pad in (HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY):
        try:
            hood_logs = await rpc.get_logs_chunked(
                address=pad,
                topics=[HOODFUN_GRADUATED_TOPIC],
                from_block=from_block,
                to_block=to_block,
                parallel=2,
            )
            for lg in hood_logs:
                ev = parse_hoodfun_graduated_log(_norm_log(lg))
                if ev:
                    ev.source = "scan"
                    events.append(ev)
                    hood_n += 1
        except Exception:  # noqa: BLE001
            logger.debug("hood.fun Graduated scan %s failed", pad[:10], exc_info=True)
    await prog(f"hood.fun Graduated: {hood_n}", 0.68)

    await prog("Flap LaunchedToDEX…", 0.7)
    flap_n = 0
    for pad in (FLAP_LAUNCHPAD, FLAP_VAULT_PORTAL):
        try:
            flap_logs = await rpc.get_logs_chunked(
                address=pad,
                topics=[FLAP_LAUNCHED_TO_DEX_TOPIC],
                from_block=from_block,
                to_block=to_block,
                parallel=2,
            )
            for lg in flap_logs:
                ev = parse_flap_launched_log(_norm_log(lg))
                if ev:
                    ev.source = "scan"
                    events.append(ev)
                    flap_n += 1
        except Exception:  # noqa: BLE001
            logger.debug("Flap LaunchedToDEX scan %s failed", pad[:10], exc_info=True)
    await prog(f"Flap LaunchedToDEX: {flap_n}", 0.8)

    by_token: dict[str, MigrationEvent] = {}
    for ev in sorted(events, key=lambda e: (e.block, e.tx)):
        if ev.launchpad_id not in _GRADUATED_LAUNCHPADS:
            continue
        key = ev.token.lower()
        prev = by_token.get(key)
        if prev is None:
            by_token[key] = ev
            continue
        prev_real = not prev.tx.endswith("0" * 64)
        new_real = not ev.tx.endswith("0" * 64)
        if new_real and not prev_real:
            by_token[key] = ev
        elif new_real == prev_real and ev.block >= prev.block:
            by_token[key] = ev

    found = list(by_token.values())
    found.sort(key=lambda e: -e.block)
    await prog(f"Уникальных миграций: {len(found)}", 0.85)
    return found


async def store_migration(event: MigrationEvent, rpc: RpcClient | None = None) -> dict[str, Any]:
    """Persist a verified graduation — reject bonding / empty / RWA / honeypot."""
    db = get_db()
    token = event.token
    summary: dict[str, Any] = {
        "ok": False,
        "token": token,
        "launchpad_id": event.launchpad_id,
        "dex": event.dex,
        "honeypot": False,
        "snipers": 0,
        "new_pairs": 0,
        "message": "",
        "skipped": False,
    }

    if event.launchpad_id.endswith("_bonding"):
        summary["skipped"] = True
        summary["message"] = "not_migrated_yet"
        return summary

    if await db.ais_blacklisted(token):
        summary["skipped"] = True
        summary["message"] = "blacklisted"
        return summary

    client = rpc or RpcClient(concurrency=2)

    ok, reason, enrich = await verify_migration_event(client, event)
    if not ok:
        summary["skipped"] = True
        summary["message"] = reason
        return summary

    if enrich.get("launchpad_id"):
        event.launchpad_id = str(enrich["launchpad_id"])
    if enrich.get("curve"):
        event.curve_address = event.curve_address or enrich["curve"]
    if enrich.get("pool_id"):
        event.pool_id = event.pool_id or enrich["pool_id"]

    symbol = str(enrich.get("symbol") or "").strip()
    name = str(enrich.get("name") or "").strip()
    if not symbol and not name:
        summary["skipped"] = True
        summary["message"] = "empty_token"
        return summary

    if await is_rhj_token(token):
        summary["skipped"] = True
        summary["message"] = "rhj_stock_token"
        return summary

    # GoPlus hard gate: honeypot / cannot_buy / cannot_sell
    try:
        sec = await check_token_security(token)
        if sec.blocked:
            summary["skipped"] = True
            summary["honeypot"] = True
            summary["message"] = f"goplus:{sec.reason or 'blocked'}"
            await db.ainsert_token(
                address=token,
                symbol=symbol,
                name=name,
                launchpad_id=event.launchpad_id,
                dex=event.dex,
                pool_id=event.pool_id,
                curve_address=event.curve_address,
                migration_block=event.block,
                migration_tx=event.tx,
                honeypot=True,
            )
            return summary
    except Exception as exc:  # noqa: BLE001
        logger.debug("GoPlus check failed for %s: %s", token[:12], exc)

    await db.ainsert_token(
        address=token,
        symbol=symbol,
        name=name,
        launchpad_id=event.launchpad_id,
        dex=event.dex,
        pool_id=event.pool_id,
        curve_address=event.curve_address,
        migration_block=event.block,
        migration_tx=event.tx,
        honeypot=False,
    )
    summary["ok"] = True
    summary["message"] = f"{symbol} · {event.launchpad_id} · {event.dex}"

    # RayBot: discover first-N curve buyers in background (non-blocking).
    try:
        from .sniper_discover import discover_snipers_for_migration, spawn_sniper_discovery

        # Manual/API parse with shared rpc → await so caller sees sniper count.
        # Live WS bus → fire-and-forget to keep the handler snappy.
        if event.source in ("manual", "scan") and rpc is not None:
            disc = await discover_snipers_for_migration(event, rpc=client)
            summary["snipers"] = int(disc.get("snipers") or 0)
            summary["new_pairs"] = summary["snipers"]
            if disc.get("message"):
                summary["message"] += f" · snipers {summary['snipers']}"
        else:
            spawn_sniper_discovery(event)
    except Exception:  # noqa: BLE001
        logger.exception("sniper discovery hook failed for %s", token[:12])

    return summary


async def scan_and_process_migrations(
    *,
    hours: float = 168.0,
    max_tokens: int = 200,
    on_progress: ProgressCb | None = None,
) -> dict[str, Any]:
    rpc = RpcClient(concurrency=4)
    tip = await rpc.block_number()
    window = max(1_000, int(hours * 3600 * BLOCKS_PER_SECOND))
    from_block = max(1, tip - window)

    db = get_db()
    purged = 0
    try:
        purged = await db.apurge_non_graduated_tokens()
        if purged and on_progress:
            await on_progress(
                "scan",
                f"Удалено прежних не-миграций: {purged}",
                0.02,
            )
    except Exception:  # noqa: BLE001
        logger.exception("purge_non_graduated_tokens failed")

    events = await discover_migration_events(
        rpc,
        from_block=from_block,
        to_block=tip,
        hours=hours,
        on_progress=on_progress,
    )
    events = events[: max(1, max_tokens)] if events else []

    processed = 0
    skipped = 0
    honeypots = 0
    summaries: list[dict[str, Any]] = []

    for i, ev in enumerate(events):
        if on_progress:
            await on_progress(
                "save",
                f"{i + 1}/{len(events)}: {ev.token[:10]}… [{ev.launchpad_id}]",
                0.75 + 0.2 * (i / max(len(events), 1)),
            )
        try:
            summary = await store_migration(ev, rpc=rpc)
            summaries.append(summary)
            if summary.get("honeypot"):
                honeypots += 1
                skipped += 1
            elif summary.get("skipped"):
                skipped += 1
            elif summary.get("ok"):
                processed += 1
            else:
                skipped += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("store migration failed %s", ev.token[:12])
            summaries.append(
                {
                    "ok": False,
                    "token": ev.token,
                    "launchpad_id": ev.launchpad_id,
                    "message": str(exc),
                    "skipped": True,
                }
            )
            skipped += 1

    snipers_total = sum(int(s.get("snipers") or 0) for s in summaries)

    if on_progress:
        await on_progress(
            "done",
            f"Сохранено {processed} миграций · snipers {snipers_total} (пропущено {skipped})",
            1.0,
        )

    return {
        "from_block": from_block,
        "to_block": tip,
        "found": processed,
        "candidates": len(events),
        "skipped": skipped,
        "processed": processed,
        "purged": purged,
        "snipers": snipers_total,
        "honeypots": honeypots,
        "results": summaries,
    }


class MigrationScanJob:
    def __init__(self) -> None:
        self.job_id: str = ""
        self.status: JobStatus = JobStatus.queued
        self.progress: JobProgress = JobProgress()
        self.result: dict[str, Any] | None = None
        self.error: str | None = None


class MigrationScanStore:
    def __init__(self) -> None:
        self._jobs: dict[str, MigrationScanJob] = {}
        self._active: str | None = None

    def get(self, job_id: str) -> MigrationScanJob | None:
        return self._jobs.get(job_id)

    def active(self) -> MigrationScanJob | None:
        if self._active:
            return self._jobs.get(self._active)
        return None

    async def create(self, *, hours: float = 168.0, max_tokens: int = 500) -> MigrationScanJob:
        if self._active and self._jobs.get(self._active):
            job = self._jobs[self._active]
            if job.status in (JobStatus.queued, JobStatus.running):
                return job

        job_id = uuid.uuid4().hex[:12]
        job = MigrationScanJob()
        job.job_id = job_id
        job.status = JobStatus.queued
        job.progress = JobProgress(stage="queued", message="В очереди", percent=0)
        self._jobs[job_id] = job
        self._active = job_id
        asyncio.create_task(self._run(job_id, hours, max_tokens))
        return job

    async def _run(self, job_id: str, hours: float, max_tokens: int) -> None:
        job = self._jobs[job_id]
        job.status = JobStatus.running
        job.progress = JobProgress(stage="scan", message="Сканирование сети…", percent=1)

        async def on_progress(stage: str, message: str, percent: float) -> None:
            job.progress = JobProgress(
                stage=stage,
                message=message,
                percent=round(percent * 100, 1),
            )

        try:
            result = await scan_and_process_migrations(
                hours=hours,
                max_tokens=max_tokens,
                on_progress=on_progress,
            )
            job.result = result
            job.status = JobStatus.done
            job.progress = JobProgress(
                stage="done",
                message=(
                    f"Сохранено миграций: {result['processed']} "
                    f"(найдено {result.get('candidates', 0)}, "
                    f"пропущено {result.get('skipped', 0)}"
                    + (
                        f", очищено {result.get('purged', 0)}"
                        if result.get("purged")
                        else ""
                    )
                    + ")"
                ),
                percent=100,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Migration scan job %s failed", job_id)
            job.status = JobStatus.error
            job.error = str(exc)
            job.progress = JobProgress(stage="error", message=str(exc), percent=100)
        finally:
            if self._active == job_id:
                self._active = None


migration_scans = MigrationScanStore()
