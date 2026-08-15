"""Scheduled watch pipeline: catch-up → screen → parse → dedup → Telegram."""

from __future__ import annotations

import asyncio
import logging
import time

from .chain import RpcClient, RpcSemBusy
from .config import settings
from .jobs import jobs
from .models import (
    BuyerRow,
    JobLogEntry,
    ParseRequest,
    ScreenRequest,
    ScreenedToken,
    WatchConfig,
    WatchScreenFilters,
    WatchStatus,
)
from .replay import attach_launch_buyers, parse_token
from .screener_feed import fetch_screened_tokens, using_remote_screener
from .telegram import resolve_chat_id, resolve_topic_id, send_buyers, telegram_configured
from .token_index import token_index
from .wallet_metrics import (
    balance_filter_active,
    enrich_and_filter_buyers,
    hold_time_filter_active,
    tokens_7d_filter_active,
)
from .watch_qualify import (
    ATH_PROBE_CAP,
    ATH_PROBE_PARALLEL,
    ATH_PROBE_PARALLEL_BG,
    ATH_PROBE_SYNC_NEAR_GATE,
    HOLD_ENRICH_CAP,
    PARSE_CONCURRENCY,
    PARSE_EXPRESS_WORKERS,
    PARSE_TOPUP_EVERY,
    REPARSE_YOUNG_COOLDOWN_SEC,
    ath_gate_enabled,
    classify_for_parse,
    estimate_drain_eta_hours,
    parse_queue_sort_key,
    select_ath_probe_batch,
    select_hold_enrich_batch,
    should_defer_ath_probe,
    should_mark_parsed,
    split_express_bulk,
)
from .watch_store import WatchStore, catchup_lookback_hours, watch_store

logger = logging.getLogger(__name__)

_LOG_MAX = 400
# Reloads / brief downtime must not open a useless 0.2–0.5h catch-up window.
_MIN_CATCHUP_GAP_SEC = 3600.0
# Brief yield between back-to-back drain cycles (avoid tight spin on wake).
_DRAIN_CONTINUE_SLEEP_SEC = 0.5


def should_drain_without_sleep(
    *,
    pending_count: int,
    enabled: bool,
    user_stopped: bool,
    force_run: bool = False,
) -> bool:
    """True → skip ``interval_sec`` and start the next cycle immediately.

    Keeps parsing while ATH-qualify pending remains (interrupted drain / mid-cycle
    inject left work). User Stop and disabled autoparse still take the normal
    schedule path. Manual force-run already bypasses sleep via its own branch.
    """
    if force_run or user_stopped or not enabled:
        return False
    return int(pending_count) > 0


async def drain_work_queue(
    items: list,
    *,
    concurrency: int,
    handle,
    should_stop=None,
    idle_poll_sec: float = 0.05,
    items_lock: asyncio.Lock | None = None,
) -> None:
    """Continuous worker pool over a mutable front-pop ``items`` list.

    Unlike wave ``gather`` batches, a free worker immediately claims the next
    item — a slow V4 parse does not idle the other slots. Handlers may append
    to ``items`` (mid-cycle top-up) under the same ``items_lock``; peers waiting
    on an empty queue retry until ``inflight == 0``.
    """
    await drain_express_bulk_queues(
        express=items,
        bulk=[],
        concurrency=concurrency,
        express_workers=concurrency,
        handle=handle,
        should_stop=should_stop,
        idle_poll_sec=idle_poll_sec,
        items_lock=items_lock,
    )


async def drain_express_bulk_queues(
    *,
    express: list,
    bulk: list,
    concurrency: int,
    express_workers: int,
    handle,
    should_stop=None,
    idle_poll_sec: float = 0.05,
    items_lock: asyncio.Lock | None = None,
) -> None:
    """Dual-lane worker pool: express workers prefer fresh/strict-deadline.

    Bulk workers prefer the ETA-urgent/high-ATH backlog. Either lane falls back
    to the other when empty so slots never idle while work remains.
    """
    lock = items_lock if items_lock is not None else asyncio.Lock()
    inflight = 0
    n = max(1, int(concurrency))
    n_express = max(0, min(int(express_workers), n))

    async def _worker(*, prefer_express: bool) -> None:
        nonlocal inflight
        while True:
            if should_stop is not None and should_stop():
                return
            job = None
            async with lock:
                if prefer_express:
                    if express:
                        job = express.pop(0)
                    elif bulk:
                        job = bulk.pop(0)
                else:
                    if bulk:
                        job = bulk.pop(0)
                    elif express:
                        job = express.pop(0)
                if job is not None:
                    inflight += 1
                elif inflight == 0:
                    return
            if job is None:
                await asyncio.sleep(idle_poll_sec)
                continue
            try:
                await handle(job)
            finally:
                async with lock:
                    inflight -= 1

    workers = [
        _worker(prefer_express=(i < n_express)) for i in range(n)
    ]
    await asyncio.gather(*workers)


class _WatchStopped(Exception):
    """Cooperative stop requested during a watch cycle."""


def apply_catchup_to_screen(
    screen: WatchScreenFilters,
    lookback_hours: float,
) -> WatchScreenFilters:
    """Constrain screen to tokens no older than the catch-up window."""
    data = screen.model_dump()
    user_max = data.get("max_pair_age_hours")
    if user_max is None:
        data["max_pair_age_hours"] = lookback_hours
    else:
        data["max_pair_age_hours"] = min(float(user_max), lookback_hours)
    return WatchScreenFilters(**data)


def _followup_hist_lag_blocks() -> int | None:
    """Approx hist cursor lag (live watermark − hist). None if unknown."""
    try:
        from .followup_store import followup_store

        hist = followup_store.get_logwatch_cursor()
        live = followup_store.get_logwatch_live_cursor()
        if hist is None or live is None:
            return None
        return max(0, int(live) - int(hist))
    except Exception:  # noqa: BLE001
        return None


class WatchRunner:
    def __init__(self, store: WatchStore | None = None) -> None:
        self._store = store or watch_store
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._force_run = False
        self._stop_requested = False
        self._running = False
        self._needs_catchup = True
        self._was_enabled = False
        self._is_catchup_run = False
        self._catchup_lookback_hours: float | None = None
        self._next_run_ts: float | None = None
        self._sleep_interval_sec: int | None = None
        self._last_run_ts: float | None = self._store.load_last_success_ts()
        self._last_run_duration_sec: float | None = None
        self._last_error: str | None = None
        self._last_message: str = ""
        self._last_tokens_screened = 0
        self._last_tokens_parsed = 0
        self._last_tokens_held = 0
        self._last_tokens_qualified = 0
        self._last_buyers_found = 0
        self._last_buyers_new = 0
        self._last_buyers_sent = 0
        self._last_buyers_skipped = 0
        self._log: list[JobLogEntry] = []

    def _append_log(
        self,
        stage: str,
        message: str,
        *,
        percent: float = 0.0,
        token: str | None = None,
    ) -> None:
        entry = JobLogEntry(
            ts=time.time(),
            stage=stage,
            message=message,
            percent=percent,
            token=token,
        )
        if self._log:
            last = self._log[-1]
            if last.stage == entry.stage and last.message == entry.message:
                self._log[-1] = entry
                return
        self._log.append(entry)
        if len(self._log) > _LOG_MAX:
            self._log = self._log[-_LOG_MAX:]

    def status(self) -> WatchStatus:
        cfg = self._store.load_config()
        chat = resolve_chat_id(cfg.telegram_chat_id)
        last_success = self._store.load_last_success_ts()
        lookback = None
        if self._needs_catchup or self._is_catchup_run:
            lookback = (
                self._catchup_lookback_hours
                if self._catchup_lookback_hours is not None
                else catchup_lookback_hours(last_success)
            )
        return WatchStatus(
            enabled=cfg.enabled,
            running=self._running,
            telegram_configured=telegram_configured(chat),
            next_run_ts=self._next_run_ts,
            last_run_ts=self._last_run_ts or last_success,
            last_run_duration_sec=self._last_run_duration_sec,
            last_error=self._last_error,
            last_message=self._last_message,
            last_tokens_screened=self._last_tokens_screened,
            last_tokens_parsed=self._last_tokens_parsed,
            last_tokens_held=self._last_tokens_held,
            last_tokens_qualified=self._last_tokens_qualified,
            last_buyers_found=self._last_buyers_found,
            last_buyers_new=self._last_buyers_new,
            last_buyers_sent=self._last_buyers_sent,
            last_buyers_skipped=self._last_buyers_skipped,
            seen_count=self._store.seen_count(),
            hold_count=self._store.hold_count(),
            parsed_token_count=self._store.parsed_token_count(),
            needs_catchup=self._needs_catchup,
            catchup_lookback_hours=lookback,
            is_catchup_run=self._is_catchup_run,
            gnome_banter_enabled=bool(getattr(cfg, "gnome_banter_enabled", True)),
            gnome_banter_next_ts=None,
            stop_requested=self._stop_requested,
            log=list(self._log),
        )

    def notify_config_changed(self) -> None:
        """Wake the loop so enabled/interval changes apply. Do not touch _was_enabled."""
        cfg = self._store.load_config()
        if cfg.enabled and not self._was_enabled:
            self._needs_catchup = True
        self._append_log("config", "Конфиг обновлён")
        self._wake.set()
        try:
            from .gnome_banter import gnome_banter

            gnome_banter.notify_config_changed()
        except Exception:  # noqa: BLE001
            pass

    async def run_now(self) -> WatchStatus:
        if self._running:
            self._append_log("run", "Запрос Run now — цикл уже идёт")
            return self.status()
        self._stop_requested = False
        self._force_run = True
        self._append_log("run", "Запланирован немедленный цикл")
        self._wake.set()
        return self.status()

    async def stop(self) -> WatchStatus:
        """Request cooperative stop of the current cycle."""
        self._stop_requested = True
        self._force_run = False
        self._last_message = "Остановка…"
        self._append_log("stop", "Запрошена принудительная остановка")
        self._wake.set()
        return self.status()

    def reset_counters(self) -> WatchStatus:
        """Clear last-cycle stats shown in the UI (does not touch dedup / schedule)."""
        self._last_run_duration_sec = None
        self._last_error = None
        self._last_tokens_screened = 0
        self._last_tokens_parsed = 0
        self._last_tokens_held = 0
        self._last_tokens_qualified = 0
        self._last_buyers_found = 0
        self._last_buyers_new = 0
        self._last_buyers_sent = 0
        self._last_buyers_skipped = 0
        self._last_message = "Счётчики сброшены"
        self._append_log("reset", "Счётчики последнего цикла сброшены")
        return self.status()

    async def run_loop(self) -> None:
        from .gnome_lifecycle import announce_death_async, announce_work_start

        logger.info("Watch runner started")
        self._append_log("boot", "Watch runner запущен")
        self._was_enabled = False
        try:
            while True:
                cfg = self._store.load_config()
                force = self._force_run
                self._force_run = False

                if not cfg.enabled and not force:
                    if self._was_enabled:
                        self._needs_catchup = True
                        self._append_log("schedule", "Автопарс выключен")
                    self._was_enabled = False
                    self._next_run_ts = None
                    self._sleep_interval_sec = None
                    self._last_message = self._last_message or "Автопарс выключен"
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=5.0)
                    except TimeoutError:
                        pass
                    continue

                if cfg.enabled and not self._was_enabled:
                    self._needs_catchup = True
                    self._append_log("schedule", "Автопарс включён")
                    try:
                        await asyncio.wait_for(announce_work_start(), timeout=20.0)
                    except TimeoutError:
                        self._append_log("error", "Таймаут Telegram «За работу!» — продолжаем")
                    except Exception as exc:  # noqa: BLE001
                        self._append_log("error", f"«За работу!» не отправилось: {exc}")
                self._was_enabled = cfg.enabled

                # Skip catch-up after reload / short gaps — e.g. "0.3 ч" only finds
                # brand-new pairs and ignores the user's normal max_pair_age (24h).
                do_catchup = self._needs_catchup
                if do_catchup:
                    last_ok = self._store.load_last_success_ts()
                    if last_ok is not None:
                        gap_sec = max(0.0, time.time() - last_ok)
                        if gap_sec < _MIN_CATCHUP_GAP_SEC:
                            self._append_log(
                                "schedule",
                                f"Догон пропущен — пауза {gap_sec / 60:.0f} мин "
                                f"(< {_MIN_CATCHUP_GAP_SEC / 60:.0f} мин), обычный цикл",
                            )
                            do_catchup = False
                            self._needs_catchup = False
                        else:
                            self._append_log(
                                "schedule",
                                f"Догон за {catchup_lookback_hours(last_ok):.1f} ч",
                            )

                trigger = "catchup" if do_catchup else ("manual" if force else "schedule")
                user_stopped = False
                try:
                    await self.run_cycle(trigger=trigger, catchup=do_catchup)
                    if do_catchup and not self._stop_requested:
                        self._needs_catchup = False
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Watch cycle crashed")
                    self._last_error = str(exc)
                    self._last_message = f"Цикл упал: {exc}"
                    self._append_log("error", f"Цикл упал: {exc}")
                    try:
                        from .followup import followup_runner
                        from .followup_store import followup_store

                        fcfg = followup_store.load_config()
                        await followup_runner._ops_alert(
                            fcfg,
                            kind="watch_cycle_error",
                            text=f"⚠️ Watch (Хвать) cycle error: {exc}",
                        )
                    except Exception:  # noqa: BLE001
                        pass

                if self._stop_requested:
                    user_stopped = True
                    self._append_log("stop", "Цикл остановлен пользователем")
                    self._last_message = "Остановлено"
                    self._stop_requested = False

                if self._force_run:
                    continue

                cfg = self._store.load_config()
                if not cfg.enabled:
                    self._next_run_ts = None
                    self._sleep_interval_sec = None
                    continue

                pending_left = 0
                min_ath = cfg.screen.min_ath_mcap
                if ath_gate_enabled(min_ath):
                    pending_left = len(
                        self._store.load_pending_parse(min_ath_mcap=min_ath)
                    )
                if should_drain_without_sleep(
                    pending_count=pending_left,
                    enabled=True,
                    user_stopped=user_stopped,
                ):
                    self._next_run_ts = time.time()
                    self._sleep_interval_sec = 0
                    self._append_log(
                        "schedule",
                        f"Очередь {pending_left} qualify — без паузы, продолжаем парс",
                    )
                    await asyncio.sleep(_DRAIN_CONTINUE_SLEEP_SEC)
                    continue

                interval = max(60, int(cfg.interval_sec))
                # Next run = end of last cycle + interval (stable schedule).
                base = self._last_run_ts or time.time()
                deadline = base + interval
                if deadline <= time.time():
                    deadline = time.time() + 1.0
                self._next_run_ts = deadline
                self._sleep_interval_sec = interval
                self._append_log(
                    "schedule",
                    f"Следующий цикл через {max(0, int(deadline - time.time()))}с "
                    f"(интервал {interval}с)",
                )

                while True:
                    if self._force_run:
                        break
                    # Stop while idle: acknowledge, keep the same deadline.
                    if self._stop_requested:
                        self._stop_requested = False
                        self._append_log("stop", "Стоп вне цикла — расписание не сброшено")
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=remaining)
                    except TimeoutError:
                        break

                    if self._force_run:
                        break

                    cfg = self._store.load_config()
                    if not cfg.enabled:
                        self._next_run_ts = None
                        self._sleep_interval_sec = None
                        break

                    new_interval = max(60, int(cfg.interval_sec))
                    if new_interval != self._sleep_interval_sec:
                        # Interval changed: recompute from last run, keep schedule honest.
                        base = self._last_run_ts or time.time()
                        deadline = base + new_interval
                        if deadline <= time.time():
                            deadline = time.time() + 1.0
                        self._next_run_ts = deadline
                        self._sleep_interval_sec = new_interval
                        self._append_log(
                            "schedule",
                            f"Интервал изменён → {new_interval}с, "
                            f"след. через {max(0, int(deadline - time.time()))}с",
                        )
                    # Unrelated config edits: keep the same deadline (do not reset timer).
        except asyncio.CancelledError:
            await announce_death_async("задача автопарса отменена (shutdown)")
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Watch runner died")
            await announce_death_async(f"падение автопарса: {exc}")
            raise

    async def run_cycle(
        self,
        *,
        trigger: str = "schedule",
        catchup: bool = False,
    ) -> WatchStatus:
        async with self._lock:
            if self._running:
                return self.status()
            if self._stop_requested:
                self._last_message = "Остановлено до старта"
                self._append_log("stop", self._last_message)
                self._stop_requested = False
                return self.status()
            self._running = True
            self._is_catchup_run = catchup
            started = time.time()
            if catchup:
                lookback = catchup_lookback_hours(self._store.load_last_success_ts())
                self._catchup_lookback_hours = lookback
                self._last_message = f"Догоняющий цикл ({lookback:.1f} ч)…"
                self._append_log("catchup", self._last_message)
            else:
                self._catchup_lookback_hours = None
                self._last_message = f"Запуск ({trigger})…"
                self._append_log("cycle", self._last_message)
            self._last_error = None
            completed = False
            try:
                completed = await self._cycle_body(catchup=catchup)
            finally:
                self._running = False
                self._is_catchup_run = False
                self._last_run_ts = time.time()
                self._last_run_duration_sec = self._last_run_ts - started
                if completed:
                    self._store.save_last_success_ts(self._last_run_ts)
                self._append_log(
                    "cycle",
                    f"Цикл завершён за {self._last_run_duration_sec:.1f}с "
                    f"({self._last_message})",
                    percent=100,
                )
            return self.status()

    async def _cycle_body(self, *, catchup: bool = False) -> bool:
        """Run one cycle. Returns True if the run should advance last_success_ts."""
        cfg = self._store.load_config()
        chat = resolve_chat_id(cfg.telegram_chat_id)
        if not telegram_configured(chat):
            self._last_error = "Telegram не настроен (TELEGRAM_BOT_TOKEN / chat id)"
            self._last_message = self._last_error
            self._append_log("error", self._last_error)
            self._last_tokens_screened = 0
            self._last_tokens_parsed = 0
            self._last_buyers_found = 0
            self._last_buyers_new = 0
            self._last_buyers_sent = 0
            self._last_buyers_skipped = 0
            return False

        try:
            topic_id = resolve_topic_id(cfg.telegram_topic_id)
        except RuntimeError as exc:
            self._last_error = str(exc)
            self._last_message = self._last_error
            self._append_log("error", self._last_error)
            return False

        if jobs.has_active():
            self._last_message = "Пропуск — идёт ручной парсинг"
            self._append_log("skip", self._last_message)
            logger.info("Watch cycle skipped: active parse job")
            return False

        if self._stop_requested:
            self._last_message = "Остановлено до старта"
            self._append_log("stop", self._last_message)
            return False

        # TG-only retry for prior undelivered buyers (no RPC / re-parse).
        try:
            flushed = await self._flush_alert_outbox(chat, topic_id=topic_id)
            if flushed:
                self._append_log(
                    "telegram",
                    f"Alert outbox: дослано {flushed} кош. с прошлых сбоев",
                    percent=1,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Alert outbox flush failed: %s", exc)

        async def on_progress(stage: str, message: str, percent: float) -> None:
            if self._stop_requested:
                raise _WatchStopped()
            self._last_message = message
            self._append_log(stage, message, percent=round(percent * 100, 1))

        screen = cfg.screen
        if catchup:
            lookback = self._catchup_lookback_hours or catchup_lookback_hours(
                self._store.load_last_success_ts()
            )
            screen = apply_catchup_to_screen(screen, lookback)
            self._last_message = (
                f"Скрининг токенов за последние {lookback:.1f} ч (догон)…"
            )
            # Warm index + refresh ATH for hold tokens before classify.
            try:
                await self._catchup_refresh_hold(cfg, on_progress=on_progress)
            except _WatchStopped:
                self._last_message = "Остановлено во время догона hold"
                self._append_log("stop", self._last_message)
                return False
        else:
            self._last_message = "Скрининг токенов…"
        self._append_log("screen", self._last_message, percent=5)

        async def _stop_waiter() -> None:
            while not self._stop_requested:
                await asyncio.sleep(0.25)
            raise _WatchStopped()

        screen_req = ScreenRequest(**screen.model_dump())
        # ATH gate must NOT hard-filter the screener for watch: dumped pumps keep
        # DexScreener spot ATH below min_ath (MEATSPIN ~$16k after ~$148k peak)
        # and would never reach hold / young ATH-probe / classify. Manual screen
        # UI still applies min_ath via _passes_primary.
        if ath_gate_enabled(cfg.screen.min_ath_mcap):
            screen_req = screen_req.model_copy(update={"min_ath_mcap": None})
        # Catch-up: budgeted near-gate hold enrich (full 4k list times out /
        # starves the cycle). Local path uses the same budget in
        # ``_catchup_refresh_hold``.
        force_enrich: list[str] | None = None
        if catchup and using_remote_screener():
            hold_snap = self._store.load_hold()
            if hold_snap and ath_gate_enabled(cfg.screen.min_ath_mcap):
                force_enrich = select_hold_enrich_batch(
                    hold_snap,
                    min_ath_mcap=float(cfg.screen.min_ath_mcap or 0.0),
                    cap=HOLD_ENRICH_CAP,
                )
                self._append_log(
                    "catchup",
                    f"Догон enrich: {len(force_enrich)}/{len(hold_snap)} "
                    f"hold (cap {HOLD_ENRICH_CAP}, near-gate)",
                    percent=3,
                )
            elif hold_snap:
                force_enrich = list(hold_snap.keys())[:HOLD_ENRICH_CAP]
        remote = using_remote_screener()
        if remote:
            self._append_log(
                "screen",
                f"Источник токенов: truegnomode ({settings.truegnomode_screener_url}) "
                f"(local fallback при сбое)",
                percent=6,
            )
        else:
            self._append_log(
                "screen",
                "Источник токенов: local token_index",
                percent=6,
            )
        screen_task = asyncio.create_task(
            fetch_screened_tokens(
                screen_req,
                on_progress=on_progress,
                force_enrich_addresses=force_enrich,
            )
        )
        stop_task = asyncio.create_task(_stop_waiter())
        try:
            done, pending = await asyncio.wait(
                {screen_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, _WatchStopped):
                    pass
            if stop_task in done:
                # Propagate stop (or swallow if screen also finished).
                try:
                    await stop_task
                except _WatchStopped:
                    self._last_message = "Остановлено во время скрининга"
                    self._append_log("stop", self._last_message)
                    return False
            feed = await screen_task
        except _WatchStopped:
            self._last_message = "Остановлено во время скрининга"
            self._append_log("stop", self._last_message)
            return False
        if self._stop_requested:
            self._last_message = "Остановлено после скрининга"
            self._append_log("stop", self._last_message)
            return False
        screened = list(feed.tokens)
        self._append_log(
            "screen",
            f"Источник: {feed.source} — {len(screened)} ток.",
            percent=18,
        )
        # Pending-rescue: empty screen must not skip durable qualify in hold.
        if not screened and ath_gate_enabled(cfg.screen.min_ath_mcap):
            pending_rescue = self._store.load_pending_parse(
                min_ath_mcap=cfg.screen.min_ath_mcap
            )
            if pending_rescue:
                self._append_log(
                    "hold",
                    f"Screen пуст — pending-rescue {len(pending_rescue)} qualify",
                    percent=19,
                )
                hold_snap = self._store.load_hold()
                screened = []
                for addr in pending_rescue:
                    ent = hold_snap.get(addr) or {}
                    screened.append(
                        ScreenedToken(
                            address=addr,
                            symbol=str(ent.get("symbol") or ""),
                            market_cap=float(ent.get("ath_mcap") or 0.0),
                            ath_mcap=float(ent.get("ath_mcap") or 0.0),
                            pair_age_hours=None,
                        )
                    )
        self._last_tokens_screened = len(screened)
        self._append_log(
            "screen",
            f"Скринер: {len(screened)} ток. (drain-all, soft UI lim={cfg.max_tokens_per_cycle})",
            percent=20,
        )

        min_ath = cfg.screen.min_ath_mcap
        gate_on = ath_gate_enabled(min_ath)
        now = time.time()
        hold_snapshot = self._store.load_hold() if gate_on else {}
        parsed_at = self._store.load_parsed_at() if gate_on else {}
        max_pair_age = cfg.screen.max_pair_age_hours
        ath_probe_task: asyncio.Task | None = None
        defer_full_probe = False
        screened_for_bg: list = []
        hold_for_bg: dict[str, dict] = {}
        if gate_on and min_ath:
            # Brief pumps can dump before DexScreener writes a lasting ATH into
            # the index. Probe Gecko/DS peaks for young under-threshold tokens
            # so dump-after-pump still qualifies while age≤max_pair_age.
            #
            # Parse-first: when unparsed qualify already wait, only a small
            # screened near-gate sync probe blocks classify; full hold-dust
            # probe runs in the background and injects newly crossed peaks.
            pending_ready = self._store.load_pending_parse(
                min_ath_mcap=float(min_ath)
            )
            if should_defer_ath_probe(len(pending_ready)):
                defer_full_probe = True
                self._append_log(
                    "hold",
                    f"Parse-first: {len(pending_ready)} qualify в hold — "
                    f"sync near-gate ATH-probe ≤{ATH_PROBE_SYNC_NEAR_GATE}, "
                    f"полный probe в фоне",
                    percent=20.5,
                )
                screened, hold_snapshot, sync_crossed = await self._probe_young_ath_peaks(
                    screened,
                    hold=hold_snapshot,
                    min_ath=float(min_ath),
                    max_pair_age_hours=max_pair_age,
                    probe_cap=ATH_PROBE_SYNC_NEAR_GATE,
                    screened_only=True,
                )
                if sync_crossed:
                    self._append_log(
                        "hold",
                        f"Sync ATH-probe: {len(sync_crossed)} пересекли порог "
                        f"(qualify в этом цикле)",
                        percent=20.7,
                    )
                screened_for_bg = list(screened)
                hold_for_bg = {k: dict(v) for k, v in hold_snapshot.items()}
            else:
                screened, hold_snapshot, _crossed = await self._probe_young_ath_peaks(
                    screened,
                    hold=hold_snapshot,
                    min_ath=float(min_ath),
                    max_pair_age_hours=max_pair_age,
                )
        # Remote donor screen has no shared local index membership. Rely on
        # hold TTL only so filtered-out tokens are not expired after 1h.
        if gate_on and using_remote_screener():
            index_addrs = None
        elif gate_on:
            index_addrs = token_index.known_addresses()
        else:
            index_addrs = None
        decision = classify_for_parse(
            screened,
            min_ath_mcap=min_ath,
            hold=hold_snapshot,
            parsed=parsed_at,
            index_addresses=index_addrs,
            now=now,
            max_pair_age_hours=max_pair_age,
        )
        if gate_on and decision.requeued_young:
            n_un = self._store.unparse_tokens(decision.requeued_young)
            self._append_log(
                "hold",
                f"Повторный парс: {n_un} молодых токенов сняты с parsed "
                f"(age≤{max_pair_age:g}h — это не «старые», cooldown истёк)",
                percent=21,
            )
        if gate_on:
            self._store.apply_qualify_updates(
                ath_updates=decision.ath_updates,
                held=decision.held,
                expired=decision.expired,
                candidates=decision.candidates,
                now=now,
            )
            self._last_tokens_held = len(decision.held)
            self._last_tokens_qualified = len(decision.candidates)
            age_note = (
                f", max_age={max_pair_age:g}h" if max_pair_age is not None else ""
            )
            self._append_log(
                "hold",
                f"ATH≥{min_ath:,.0f}: hold={len(decision.held)} "
                f"(ждут порог), qualify={len(decision.candidates)} "
                f"(к парсу), skip_parsed={decision.skipped_parsed} "
                f"(не age), parsed_set={len(parsed_at)}{age_note}"
                + (
                    f", requeue_young={len(decision.requeued_young)}"
                    if decision.requeued_young
                    else ""
                )
                + (f", expired={len(decision.expired)}" if decision.expired else ""),
                percent=22,
            )
        else:
            self._last_tokens_held = 0
            self._last_tokens_qualified = len(decision.candidates)
            self._append_log(
                "hold",
                "ATH-гейт выключен — парсим все из скринера "
                f"(qualify={len(decision.candidates)})",
                percent=22,
            )

        # Drain-all: every qualify + leftover pending from prior interrupt/restart.
        # Sort: expiring-soon → highest ATH → FIFO queued_at (hot pumps must not
        # sit behind mid-age $40k dust). max_tokens_per_cycle is NOT a hard stop.
        requeued = {a.lower() for a in decision.requeued_young}
        fresh = [t for t in decision.candidates if t.lower() not in requeued]
        retry = [t for t in decision.candidates if t.lower() in requeued]
        hold_for_queue = self._store.load_hold() if gate_on else {}
        age_by_addr: dict[str, float | None] = {}
        ath_by_addr: dict[str, float] = {}
        for row in screened:
            addr = getattr(row, "address", None)
            if not addr:
                continue
            key = str(addr).lower()
            age_by_addr[key] = row.pair_age_hours
            ath_by_addr[key] = max(
                float(row.ath_mcap or 0.0), float(row.market_cap or 0.0)
            )
        for addr, ent in hold_for_queue.items():
            ath_by_addr.setdefault(addr, float(ent.get("ath_mcap") or 0.0))

        cand_keys = {t.lower() for t in decision.candidates}
        pending_extra: list[str] = []
        if gate_on:
            for addr in self._store.load_pending_parse(min_ath_mcap=min_ath):
                if addr in cand_keys:
                    continue
                pending_extra.append(addr)

        # Soft age-out: only when screener provided a real pair_age. Falling
        # back to hold first_seen wrongly drops pending that sat under-gate for
        # days then crossed ATH (screen miss / remote omit age).
        age_dropped: list[str] = []
        max_age_f = (
            float(max_pair_age)
            if max_pair_age is not None and float(max_pair_age) > 0
            else None
        )
        if max_age_f is not None:
            for addr in list(fresh) + list(retry) + list(pending_extra):
                key = addr.lower()
                age = age_by_addr.get(key)
                if age is None:
                    continue
                if float(age) > max_age_f:
                    age_dropped.append(key)
            if age_dropped:
                drop_set = set(age_dropped)
                fresh = [t for t in fresh if t.lower() not in drop_set]
                retry = [t for t in retry if t.lower() not in drop_set]
                pending_extra = [t for t in pending_extra if t.lower() not in drop_set]
                n_drop = self._store.clear_pending_queued(age_dropped)
                self._append_log(
                    "hold",
                    f"Age-out soft: {n_drop} pending старше {max_age_f:g}h "
                    f"(только с known pair_age; ATH в hold сохранён)",
                    percent=23,
                )

        # Do NOT fill first_seen approx into age_by_addr: that fakes "known"
        # ages, floods express, and re-marks fresh in sort. Unknown → bulk/ATH.
        queue_n = len(fresh) + len(pending_extra) + len(retry)
        drain_eta_h = estimate_drain_eta_hours(queue_n)
        priority_floor = getattr(cfg, "parse_priority_min_ath", 50_000.0)

        def _parse_queue_key(addr: str) -> tuple[int, int, int, float, float, str]:
            return parse_queue_sort_key(
                addr,
                hold=hold_for_queue,
                pair_age_hours=age_by_addr,
                ath_mcap=ath_by_addr,
                max_pair_age_hours=max_pair_age,
                now=now,
                drain_eta_hours=drain_eta_h,
                priority_min_ath=priority_floor,
            )

        fresh.sort(key=_parse_queue_key)
        retry.sort(key=_parse_queue_key)
        pending_extra.sort(key=_parse_queue_key)
        # Fresh first, then prior pending, then young requeues (already parsed once).
        tokens = list(dict.fromkeys(fresh + pending_extra + retry))
        floor_f = float(priority_floor) if priority_floor is not None else 0.0
        if floor_f > 0:
            n_priority = sum(
                1
                for t in tokens
                if float(ath_by_addr.get(t.lower()) or 0.0) >= floor_f
            )
            n_deferred = len(tokens) - n_priority
        else:
            n_priority = len(tokens)
            n_deferred = 0
        express_q, bulk_q = split_express_bulk(
            tokens,
            hold=hold_for_queue,
            pair_age_hours=age_by_addr,
            max_pair_age_hours=max_pair_age,
            now=now,
        )
        self._append_log(
            "hold",
            f"Drain-all: {len(tokens)} qualify "
            f"(express={len(express_q)}, bulk={len(bulk_q)}; "
            f"fresh={len(fresh)}, pending_extra={len(pending_extra)}, "
            f"requeue={len(retry)}; "
            f"ATH≥{floor_f:,.0f} priority={n_priority} deferred={n_deferred}; "
            f"deadline→fresh→ATH≥floor→tail→FIFO; "
            f"eta≈{drain_eta_h:.1f}h)",
            percent=23,
        )
        if not tokens:
            self._last_tokens_parsed = 0
            self._last_buyers_found = 0
            self._last_buyers_new = 0
            self._last_buyers_sent = 0
            self._last_buyers_skipped = 0
            if self._last_tokens_screened == 0:
                self._last_message = "Нет токенов по фильтрам скринера"
            elif gate_on and min_ath:
                skipped = decision.skipped_parsed
                self._last_message = (
                    f"Нет токенов для парса — hold={self._last_tokens_held} "
                    f"(ATH<{min_ath:,.0f}), skip_parsed={skipped} "
                    f"(не age; скринер {self._last_tokens_screened})"
                )
            else:
                self._last_message = "Нет токенов для парса"
            self._append_log("screen", self._last_message, percent=100)
            if defer_full_probe and gate_on and min_ath:
                # Still run full probe so next cycle has peaks even with empty drain.
                try:
                    await self._probe_young_ath_peaks(
                        screened_for_bg,
                        hold=hold_for_bg,
                        min_ath=float(min_ath),
                        max_pair_age_hours=max_pair_age,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Deferred ATH-probe (empty drain) failed: %s", exc)
            return True

        threshold = (
            cfg.wallet.mcap_threshold
            if cfg.wallet.mcap_threshold is not None
            else settings.mcap_threshold
        )
        wallet_req = ParseRequest(
            tokens=tokens,
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

        rpc = RpcClient(concurrency=max(2, int(PARSE_CONCURRENCY)), sem_scope="watch_parse")
        parsed = 0
        found_total = 0
        new_total = 0
        sent_total = 0
        skipped_total = 0
        interrupted = False
        tg_failed = False
        seen = self._store.load_seen()
        # Yield RPC to follow-up getLogs when hist lag is high.
        parse_conc = max(1, int(PARSE_CONCURRENCY))
        express_workers = max(1, min(int(PARSE_EXPRESS_WORKERS), parse_conc))
        fu_lag = _followup_hist_lag_blocks()
        if fu_lag is not None and fu_lag >= 8_000:
            parse_conc = min(parse_conc, 3)
            express_workers = min(express_workers, 2)
            self._append_log(
                "parse",
                f"RPC governor: followup lag≈{fu_lag} — parse×{parse_conc} "
                f"(express={express_workers})",
                percent=21,
            )
        elif fu_lag is not None and fu_lag >= 5_000:
            parse_conc = min(parse_conc, 4)
            express_workers = min(express_workers, 3)
            self._append_log(
                "parse",
                f"RPC governor: followup lag≈{fu_lag} — parse×{parse_conc} "
                f"(express={express_workers})",
                percent=21,
            )
        parse_sem = asyncio.Semaphore(parse_conc)
        post_lock = asyncio.Lock()
        queue_lock = asyncio.Lock()
        done_count = 0
        next_idx = 0
        express = list(express_q)
        bulk = list(bulk_q)
        queued_set = {t.lower() for t in tokens}
        finished_set: set[str] = set()
        topup_injected = 0
        last_topup_done = -1
        topup_lock = asyncio.Lock()
        bg_tasks: set[asyncio.Task] = set()

        def _track_bg(coro) -> asyncio.Task:
            task = asyncio.create_task(coro)
            bg_tasks.add(task)
            task.add_done_callback(bg_tasks.discard)
            return task

        async def _inject_addrs(addrs: list[str], *, label: str) -> int:
            """Prepend addrs into express/bulk by lane; stamp queued_at via store."""
            nonlocal topup_injected
            if not addrs:
                return 0
            hold_now = self._store.load_hold() if gate_on else {}
            # Screen ages only — never invent age from first_seen for express.
            age_now = dict(age_by_addr)
            fresh_addrs = [
                a for a in addrs if a.lower() not in queued_set and a.lower() not in finished_set
            ]
            if not fresh_addrs:
                return 0
            ex, bu = split_express_bulk(
                fresh_addrs,
                hold=hold_now,
                pair_age_hours=age_now,
                max_pair_age_hours=max_pair_age,
                now=time.time(),
            )
            n = 0
            async with queue_lock:
                for addr in reversed(ex):
                    key = addr.lower()
                    if key in queued_set or key in finished_set:
                        continue
                    express.insert(0, addr)
                    queued_set.add(key)
                    n += 1
                for addr in reversed(bu):
                    key = addr.lower()
                    if key in queued_set or key in finished_set:
                        continue
                    bulk.insert(0, addr)
                    queued_set.add(key)
                    n += 1
            if n:
                topup_injected += n
                self._append_log(
                    "hold",
                    f"{label}: +{n} qualify "
                    f"(express={len(ex)}, bulk={len(bu)}; "
                    f"очередь {len(express)+len(bulk)})",
                    percent=22,
                )
            return n

        async def _maybe_topup() -> None:
            nonlocal last_topup_done, topup_injected
            if PARSE_TOPUP_EVERY <= 0:
                return
            if done_count <= 0 or done_count % PARSE_TOPUP_EVERY != 0:
                return
            async with topup_lock:
                if done_count == last_topup_done:
                    return
                last_topup_done = done_count
                try:
                    injected = await self._topup_parse_queue(
                        cfg,
                        express=express,
                        bulk=bulk,
                        queued_set=queued_set,
                        finished_set=finished_set,
                        gate_on=gate_on,
                        min_ath=min_ath,
                        max_pair_age=max_pair_age,
                        queue_lock=queue_lock,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Parse top-up failed: %s", exc)
                    return
                if injected:
                    topup_injected += injected
                    self._append_log(
                        "hold",
                        f"Top-up: +{injected} новых qualify в очередь "
                        f"(всего inject {topup_injected}, "
                        f"очередь {len(express)+len(bulk)})",
                        percent=22,
                    )

        if defer_full_probe and gate_on and min_ath:

            async def _bg_ath_probe() -> None:
                try:
                    # Let getLogs drain claim RPC first; sync near-gate already
                    # covered the hottest screened dumps this cycle.
                    await asyncio.sleep(45.0)
                    if interrupted or self._stop_requested or jobs.has_active():
                        return
                    _s, _h, crossed = await self._probe_young_ath_peaks(
                        screened_for_bg,
                        hold=hold_for_bg,
                        min_ath=float(min_ath),
                        max_pair_age_hours=max_pair_age,
                        probe_parallel=ATH_PROBE_PARALLEL_BG,
                    )
                    if crossed and not (
                        interrupted or self._stop_requested or jobs.has_active()
                    ):
                        await _inject_addrs(crossed, label="ATH-probe inject")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Background ATH-probe failed: %s", exc)

            ath_probe_task = asyncio.create_task(_bg_ath_probe())

        async def _deliver_new_buyers(
            token: str,
            new_buyers: list[BuyerRow],
            *,
            total_hint: int,
        ) -> None:
            """Follow-up ingest + Telegram — outside parse_sem (serialized)."""
            nonlocal new_total, sent_total, interrupted, tg_failed
            async with post_lock:
                # Durable before TG: cancel/crash after ingest must not lose buyers.
                self._store.enqueue_alert_outbox(token, new_buyers)
                try:
                    from .followup import followup_runner

                    ingested = await followup_runner.ingest_from_watch(new_buyers)
                    if ingested:
                        self._append_log(
                            "track",
                            f"Хвать: в follow-up {ingested} кош. по {token[:10]}…",
                            token=token,
                        )
                except Exception as exc:  # noqa: BLE001
                    # Keep outbox — flush retries ingest+TG next cycle.
                    logger.warning("Follow-up ingest failed: %s", exc)
                    self._last_error = f"Follow-up ingest: {exc}"
                    self._append_log(
                        "error",
                        f"Follow-up ingest: {exc}",
                        token=token,
                    )
                    return
                except BaseException:
                    # CancelledError / KeyboardInterrupt — outbox already durable.
                    raise

                if self._stop_requested:
                    interrupted = True
                    self._last_message = "Остановлено перед отправкой в Telegram"
                    self._append_log("stop", self._last_message)
                    return

                self._last_message = (
                    f"Telegram: отправка {len(new_buyers)} кош. "
                    f"({token[:10]}…, {done_count}/{total_hint})"
                )
                self._append_log("telegram", self._last_message, token=token)
                try:
                    header = (
                        f"Хвать · догон · {len(new_buyers)} кош."
                        if catchup
                        else f"Хвать · {len(new_buyers)} кош."
                    )
                    _msgs, sent = await send_buyers(
                        chat, new_buyers, header=header, topic_id=topic_id
                    )
                except Exception as exc:  # noqa: BLE001
                    self._last_error = str(exc)
                    self._last_message = f"Ошибка Telegram: {exc}"
                    self._append_log("error", self._last_message, token=token)
                    tg_failed = True
                    return

                if sent:
                    pairs = [(b.wallet, b.token) for b in sent]
                    self._store.mark_seen(pairs)
                    for b in sent:
                        seen.add(f"{b.wallet.lower()}:{b.token.lower()}")
                    sent_total += len(sent)
                    new_total += len(sent)
                self._last_buyers_sent = sent_total
                self._last_buyers_new = new_total
                sent_keys = {
                    f"{b.wallet.lower()}:{b.token.lower()}" for b in sent
                }
                unsent = [
                    b
                    for b in new_buyers
                    if f"{b.wallet.lower()}:{b.token.lower()}" not in sent_keys
                ]
                if unsent:
                    self._store.enqueue_alert_outbox(token, unsent)
                    self._last_error = (
                        f"Частичная отправка в Telegram: "
                        f"{len(sent)}/{len(new_buyers)}"
                    )
                    self._append_log("error", self._last_error, token=token)
                else:
                    self._store.clear_alert_outbox(token)
                    self._append_log(
                        "telegram",
                        f"Отправлено {len(sent)} кош. по {token[:10]}… "
                        f"(всего за цикл {sent_total})",
                        token=token,
                    )

        async def _parse_one(token: str) -> None:
            nonlocal parsed, found_total, new_total, sent_total, skipped_total
            nonlocal interrupted, tg_failed, done_count, next_idx
            if self._stop_requested or interrupted or jobs.has_active():
                interrupted = True
                return

            deliver: list[BuyerRow] = []
            deliver_hint = 0
            need_topup = False
            result = None
            on_tok_progress = None
            async with parse_sem:
                if self._stop_requested or interrupted or jobs.has_active():
                    interrupted = True
                    return
                async with post_lock:
                    i = next_idx
                    next_idx += 1
                total_hint = max(len(express) + len(bulk) + done_count + 1, next_idx)
                self._last_message = f"Парсинг {i + 1}/{total_hint}: {token[:10]}…"
                self._append_log(
                    "parse",
                    self._last_message,
                    percent=20 + 70 * (i / max(total_hint, 1)),
                    token=token,
                )

                async def on_tok_progress(
                    stage: str,
                    message: str,
                    percent: float,
                    _i=i,
                    _n=total_hint,
                    _token=token,
                ) -> None:
                    overall = 20 + 70 * ((_i + percent) / max(_n, 1))
                    self._last_message = message
                    self._append_log(
                        stage, message, percent=round(overall, 1), token=_token
                    )

                try:
                    # Uniswap discovery only under parse_sem — launch + wallet
                    # filters run after release so sibling workers keep getLogs.
                    result = await parse_token(
                        rpc,
                        token,
                        threshold,
                        on_progress=on_tok_progress,
                        exclude_honeypots=cfg.wallet.exclude_honeypots,
                        wallet_filters=wallet_req,
                        apply_wallet_filters=False,
                        include_launch=False,
                    )
                except asyncio.CancelledError:
                    interrupted = True
                    async with queue_lock:
                        bulk.append(token)
                    self._append_log(
                        "parse",
                        f"Парс отменён — requeue {token[:10]}…",
                        token=token,
                    )
                    raise
                except RpcSemBusy as exc:
                    async with queue_lock:
                        bulk.append(token)
                    self._append_log(
                        "parse",
                        f"RPC занят — requeue {token[:10]}… ({exc})",
                        token=token,
                    )
                    logger.warning("Watch parse requeue (RPC sem): %s %s", token, exc)
                    return
                except TimeoutError as exc:
                    msg = str(exc) or "RPC timeout"
                    async with queue_lock:
                        bulk.append(token)
                    self._append_log(
                        "parse",
                        f"RPC timeout — requeue {token[:10]}… ({msg})",
                        token=token,
                    )
                    logger.warning("Watch parse requeue (timeout): %s %s", token, msg)
                    return
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Watch parse failed for %s", token)
                    err = str(exc) or type(exc).__name__
                    self._append_log(
                        "error",
                        f"Ошибка парса {token[:10]}…: {err}",
                        token=token,
                    )
                    return

            # Outside parse_sem: launch → enrich (unique-sem) → mark → TG.
            if result is None:
                return
            filter_meta: dict = {}
            try:
                if (
                    not result.error
                    and result.pool is not None
                    and (result.stats or {}).get("launch_pending")
                ):
                    stats = dict(result.stats or {})
                    merged = await attach_launch_buyers(
                        rpc,
                        token=token,
                        pool=result.pool,
                        buyers=list(result.buyers),
                        decimals=int(stats.get("decimals") or result.decimals or 18),
                        supply_tokens=float(
                            stats.get("supply_tokens") or result.total_supply or 0.0
                        ),
                        eth_usd=float(stats.get("eth_usd") or 0.0),
                        mcap_threshold=float(threshold),
                        start_block=int(stats.get("start_block") or 0),
                        end_block=int(stats.get("end_block") or 0),
                        on_progress=on_tok_progress,
                    )
                    for b in merged:
                        if not b.token_symbol:
                            b.token_symbol = result.symbol
                    result.buyers = merged
                    stats["launch_pending"] = False
                    stats["buyers"] = len(merged)
                    result.stats = stats

                need_filters = (
                    not result.error
                    and result.buyers
                    and (
                        balance_filter_active(wallet_req)
                        or hold_time_filter_active(wallet_req)
                        or tokens_7d_filter_active(wallet_req)
                    )
                )
                if need_filters:
                    stats = dict(result.stats or {})
                    start_b = int(stats.get("start_block") or 0)
                    end_b = int(stats.get("end_block") or 0)
                    before = len(result.buyers)
                    filtered = await enrich_and_filter_buyers(
                        rpc,
                        token=token,
                        buyers=list(result.buyers),
                        req=wallet_req,
                        start_block=start_b,
                        end_block=end_b,
                        on_progress=on_tok_progress,
                        out_meta=filter_meta,
                    )
                    result.buyers = filtered
                    stats["buyers_before_wallet_filters"] = before
                    stats["buyers"] = len(filtered)
                    stats["wallet_filters_pending"] = False
                    result.stats = stats
            except Exception as exc:  # noqa: BLE001
                logger.exception("Watch post-parse failed for %s", token)
                self._append_log(
                    "error",
                    f"Ошибка post-parse {token[:10]}…: {exc}",
                    token=token,
                )
                return

            buyers = [b for b in result.buyers if int(b.buys_count or 0) >= 1]
            before_filters = int(
                (result.stats or {}).get("buyers_before_wallet_filters")
                or len(buyers)
            )
            new_buyers: list[BuyerRow] = []
            async with post_lock:
                parsed += 1
                done_count += 1
                finished_set.add(token.lower())
                for b in buyers:
                    key = f"{b.wallet.lower()}:{b.token.lower()}"
                    if key in seen:
                        skipped_total += 1
                    else:
                        new_buyers.append(b)
                # Durable outbox BEFORE mark_parsed — crash between mark and
                # bg deliver must not lose watching wallets.
                if new_buyers:
                    try:
                        self._store.enqueue_alert_outbox(token, new_buyers)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "Watch outbox enqueue failed for %s: %s",
                            token[:12],
                            exc,
                        )
                if gate_on and should_mark_parsed(
                    result.error,
                    buyers_before_filters=before_filters,
                    buyers_after_filters=len(buyers),
                ):
                    self._store.mark_token_parsed(
                        token,
                        partial_unique=bool(filter_meta.get("unique_partial")),
                        filter_wipe=bool(before_filters > 0 and not buyers),
                    )
                    if before_filters > 0 and not buyers:
                        self._append_log(
                            "hold",
                            f"{token[:10]}… parsed (cooldown) — {before_filters} "
                            "early → 0 после фильтров; повтор через "
                            f"{int(REPARSE_YOUNG_COOLDOWN_SEC // 60)}м пока age ок",
                            token=token,
                        )
                    elif filter_meta.get("unique_partial"):
                        self._append_log(
                            "hold",
                            f"{token[:10]}… partial unique "
                            f"(skip={filter_meta.get('unique_skipped', 0)}); "
                            "meta сохранена для young requeue",
                            token=token,
                        )
                found_total += len(buyers)
                self._last_tokens_parsed = parsed
                self._last_buyers_found = found_total
                self._last_buyers_skipped = skipped_total
                self._last_buyers_new = new_total + len(new_buyers)
                self._append_log(
                    "parse",
                    f"{token[:10]}… → {len(buyers)} кош. (early)"
                    + (f" ({result.error})" if result.error else ""),
                    token=token,
                )

            deliver = list(new_buyers)
            deliver_hint = max(len(express) + len(bulk) + done_count, 1)
            need_topup = (
                PARSE_TOPUP_EVERY > 0
                and done_count > 0
                and done_count % PARSE_TOPUP_EVERY == 0
            )

            if need_topup:
                _track_bg(_maybe_topup())

            if deliver:
                # Drain worker returns immediately; TG stays serialized on post_lock.
                # Awaited at end of cycle so alerts are not dropped on restart edge.
                _track_bg(
                    _deliver_new_buyers(
                        token, deliver, total_hint=deliver_hint
                    )
                )

        self._append_log(
            "parse",
            f"Параллельный парс ×{parse_conc} "
            f"(express={express_workers} prefer fresh|≤3h; "
            f"launch+enrich вне parse_sem; TG async; "
            f"очередь express={len(express)} bulk={len(bulk)}; "
            f"top-up каждые {PARSE_TOPUP_EVERY})",
            percent=21,
        )

        async def _handle(tok: str) -> None:
            await _parse_one(tok)

        await drain_express_bulk_queues(
            express=express,
            bulk=bulk,
            concurrency=parse_conc,
            express_workers=express_workers,
            handle=_handle,
            should_stop=lambda: bool(
                interrupted or self._stop_requested or jobs.has_active()
            ),
            items_lock=queue_lock,
        )

        if bg_tasks:
            results = await asyncio.gather(*list(bg_tasks), return_exceptions=True)
            for res in results:
                if isinstance(res, BaseException):
                    logger.warning("Watch bg task failed: %s", res)

        if ath_probe_task is not None:
            try:
                await ath_probe_task
            except Exception as exc:  # noqa: BLE001
                logger.warning("Background ATH-probe failed: %s", exc)

        if topup_injected:
            self._append_log(
                "hold",
                f"Top-up итог: +{topup_injected} ток. подхвачены mid-cycle",
                percent=90,
            )
        if interrupted and self._stop_requested:
            self._last_message = f"Остановлено ({parsed}/{done_count or next_idx})"
            self._append_log("stop", self._last_message)
        elif interrupted and jobs.has_active():
            self._last_message = (
                f"Остановлено — ручной парсинг ({parsed}/{done_count or next_idx})"
            )
            self._append_log("skip", self._last_message)
            logger.info("Watch cycle interrupted by manual parse")

        self._last_tokens_parsed = parsed
        self._last_buyers_found = found_total
        self._last_buyers_new = new_total
        self._last_buyers_sent = sent_total
        self._last_buyers_skipped = skipped_total

        prefix = "Догон" if catchup else "Готово"
        if interrupted:
            prefix = "Остановлено"
        hold_bit = ""
        if gate_on:
            hold_bit = f", hold {self._store.hold_count()}"
        self._last_message = (
            f"{prefix} — {parsed} ток., {found_total} кош., "
            f"{sent_total} отпр., {skipped_total} проп.{hold_bit}"
        )
        self._append_log("done", self._last_message, percent=100)
        if tg_failed and sent_total == 0:
            return False
        return not interrupted

    async def _flush_alert_outbox(
        self,
        chat: str,
        *,
        topic_id: int | None,
    ) -> int:
        """Retry follow-up ingest + Telegram for previously undelivered buyers."""
        from .models import BuyerRow

        outbox = self._store.load_alert_outbox()
        if not outbox:
            return 0
        sent_n = 0
        for token, rows in list(outbox.items()):
            if self._stop_requested or jobs.has_active():
                break
            buyers: list[BuyerRow] = []
            for row in rows:
                try:
                    buyers.append(BuyerRow.model_validate(row))
                except Exception:  # noqa: BLE001
                    continue
            # Drop already-seen (dedup across restarts).
            fresh = [
                b
                for b in buyers
                if not self._store.is_seen(b.wallet, b.token)
            ]
            if not fresh:
                self._store.clear_alert_outbox(token)
                continue
            try:
                from .followup import followup_runner

                await followup_runner.ingest_from_watch(fresh)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Alert outbox ingest %s: %s", token[:10], exc)
                self._last_error = f"Follow-up ingest (outbox): {exc}"
                continue
            try:
                _msgs, sent = await send_buyers(
                    chat,
                    fresh,
                    header=f"Хвать · outbox · {len(fresh)} кош.",
                    topic_id=topic_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Alert outbox send %s: %s", token[:10], exc)
                self._last_error = f"Telegram outbox: {exc}"
                continue
            if sent:
                self._store.mark_seen([(b.wallet, b.token) for b in sent])
                sent_n += len(sent)
            sent_keys = {
                f"{b.wallet.lower()}:{b.token.lower()}" for b in sent
            }
            left = [
                b
                for b in fresh
                if f"{b.wallet.lower()}:{b.token.lower()}" not in sent_keys
            ]
            if left:
                self._store.enqueue_alert_outbox(token, left)
            else:
                self._store.clear_alert_outbox(token)
        return sent_n

    async def _topup_parse_queue(
        self,
        cfg: WatchConfig,
        *,
        express: list[str],
        bulk: list[str],
        queued_set: set[str],
        finished_set: set[str],
        gate_on: bool,
        min_ath: float | None,
        max_pair_age: float | None,
        queue_lock: asyncio.Lock | None = None,
    ) -> int:
        """Re-screen mid-cycle and prepend newly qualified high-ATH tokens.

        Drain-all: inject **all** new qualify (no room/limit ceiling) so pumps
        like LEVCAT/ROCKET enter the head of the matching lane while older
        tokens drain.
        """
        if self._stop_requested or jobs.has_active():
            return 0
        screen_req = ScreenRequest(**cfg.screen.model_dump())
        if gate_on and ath_gate_enabled(min_ath):
            screen_req = screen_req.model_copy(update={"min_ath_mcap": None})
        # Cap results for a fast top-up poll (donor still sorts by liq/mcap).
        screen_req = screen_req.model_copy(
            update={"max_results": min(int(screen_req.max_results or 500), 800)}
        )
        feed = await fetch_screened_tokens(screen_req, on_progress=None)
        screened = list(feed.tokens)
        if not screened:
            return 0
        now = time.time()
        hold_snapshot = self._store.load_hold() if gate_on else {}
        parsed_at = self._store.load_parsed_at() if gate_on else {}
        decision = classify_for_parse(
            screened,
            min_ath_mcap=min_ath if gate_on else None,
            hold=hold_snapshot,
            parsed=parsed_at,
            index_addresses=None,
            now=now,
            max_pair_age_hours=max_pair_age,
        )
        if gate_on and decision.requeued_young:
            self._store.unparse_tokens(decision.requeued_young)
        if gate_on:
            self._store.apply_qualify_updates(
                ath_updates=decision.ath_updates,
                held=decision.held,
                expired=decision.expired,
                candidates=decision.candidates,
                now=now,
            )
        hold_now = self._store.load_hold() if gate_on else {}
        age_by: dict[str, float | None] = {
            row.address.lower(): row.pair_age_hours for row in screened
        }
        ath_by: dict[str, float] = {
            row.address.lower(): max(
                float(row.ath_mcap or 0.0), float(row.market_cap or 0.0)
            )
            for row in screened
        }
        for addr, ent in hold_now.items():
            ath_by.setdefault(addr, float(ent.get("ath_mcap") or 0.0))

        max_age_f = (
            float(max_pair_age)
            if max_pair_age is not None and float(max_pair_age) > 0
            else None
        )
        requeued = {a.lower() for a in decision.requeued_young}
        fresh: list[str] = []
        for addr in decision.candidates:
            key = addr.lower()
            if key in queued_set or key in finished_set:
                continue
            if gate_on and key in parsed_at and key not in requeued:
                continue
            age = age_by.get(key)
            if (
                max_age_f is not None
                and age is not None
                and float(age) > max_age_f
            ):
                continue
            fresh.append(addr)

        if not fresh:
            return 0
        drain_eta_h = estimate_drain_eta_hours(len(express) + len(bulk) + len(fresh))
        priority_floor = getattr(cfg, "parse_priority_min_ath", 50_000.0)
        fresh.sort(
            key=lambda a: parse_queue_sort_key(
                a,
                hold=hold_now,
                pair_age_hours=age_by,
                ath_mcap=ath_by,
                max_pair_age_hours=max_pair_age,
                now=now,
                drain_eta_hours=drain_eta_h,
                priority_min_ath=priority_floor,
            )
        )
        ex_new, bu_new = split_express_bulk(
            fresh,
            hold=hold_now,
            pair_age_hours=age_by,
            max_pair_age_hours=max_pair_age,
            now=now,
        )

        async def _inject() -> int:
            n = 0
            for addr in reversed(ex_new):
                key = addr.lower()
                if key in queued_set or key in finished_set:
                    continue
                express.insert(0, addr)
                queued_set.add(key)
                n += 1
            for addr in reversed(bu_new):
                key = addr.lower()
                if key in queued_set or key in finished_set:
                    continue
                bulk.insert(0, addr)
                queued_set.add(key)
                n += 1
            return n

        if queue_lock is not None:
            async with queue_lock:
                return await _inject()
        return await _inject()

    async def _probe_young_ath_peaks(
        self,
        screened: list,
        *,
        hold: dict[str, dict],
        min_ath: float,
        max_pair_age_hours: float | None,
        probe_cap: int = ATH_PROBE_CAP,
        screened_only: bool = False,
        probe_parallel: int = ATH_PROBE_PARALLEL,
    ) -> tuple[list, dict[str, dict], list[str]]:
        """Bump ATH for young tokens still below the gate using Gecko/DS peaks.

        Persists peaks into ``hold`` so a post-pump dump does not erase qualify.
        Tokens that cross ``min_ath`` are stamped as parse candidates
        (``queued_at``) and returned as the third tuple element.

        ``screened_only``: skip hold-only dust (sync near-gate path when drain
        is deferred).
        """
        from .followup import estimate_token_peak_mcap
        from .models import ScreenedToken

        max_age = (
            float(max_pair_age_hours)
            if max_pair_age_hours is not None and max_pair_age_hours > 0
            else 24.0
        )
        hold_out = {k: dict(v) for k, v in hold.items()}
        # addr -> (current_peak, symbol, pair_age, screen_idx | None)
        need: list[tuple[str, float, str, float | None, int | None]] = []
        seen: set[str] = set()

        for i, row in enumerate(screened):
            if not isinstance(row, ScreenedToken):
                continue
            addr = row.address.lower()
            peak = max(float(row.ath_mcap or 0.0), float(row.market_cap or 0.0))
            hold_peak = float(hold_out.get(addr, {}).get("ath_mcap") or 0.0)
            peak = max(peak, hold_peak)
            age = row.pair_age_hours
            if age is not None and float(age) > max_age:
                continue
            if peak >= min_ath:
                continue
            need.append((addr, peak, row.symbol or "", age, i))
            seen.add(addr)

        if not screened_only:
            for addr, ent in hold_out.items():
                if addr in seen:
                    continue
                peak = float(ent.get("ath_mcap") or 0.0)
                if peak >= min_ath:
                    continue
                # Hold-only: treat as young enough to probe while still on the queue.
                need.append((addr, peak, str(ent.get("symbol") or ""), None, None))

        if not need:
            return screened, hold_out, []

        now = time.time()
        probed_at = {
            addr: float(ent.get("ath_probed_at") or 0.0)
            for addr, ent in hold_out.items()
            if float(ent.get("ath_probed_at") or 0.0) > 0.0
        }
        batch = select_ath_probe_batch(
            need,
            probe_cap=probe_cap,
            now=now,
            probed_at=probed_at,
        )
        label = "sync ATH-probe" if screened_only else "ATH-probe"
        self._append_log(
            "hold",
            f"{label}: {len(batch)}/{len(need)} ток. ниже ${min_ath:,.0f}…",
            percent=18,
        )

        updates: dict[str, tuple[float, str]] = {}
        row_peaks: dict[int, float] = {}
        probe_stamp: dict[str, float] = {}
        sem = asyncio.Semaphore(max(1, int(probe_parallel)))

        async def one(
            addr: str, prev_peak: float, symbol: str, _age: float | None, idx: int | None
        ) -> tuple[str, float, str, int | None] | None:
            async with sem:
                probe_stamp[addr] = now
                try:
                    est = await estimate_token_peak_mcap(addr, min_needed=min_ath)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("young ATH probe %s: %s", addr[:10], exc)
                    return None
                if est is None or est.peak <= 0:
                    return None
                new_peak = max(prev_peak, float(est.peak))
                if new_peak <= prev_peak:
                    return None
                return addr, new_peak, symbol, idx

        results = await asyncio.gather(
            *[one(a, p, s, age, i) for a, p, s, age, i in batch]
        )
        for item in results:
            if not item:
                continue
            addr, new_peak, symbol, idx = item
            updates[addr] = (new_peak, symbol)
            if idx is not None:
                row_peaks[idx] = new_peak

        for idx, new_peak in row_peaks.items():
            if 0 <= idx < len(screened):
                row = screened[idx]
                if isinstance(row, ScreenedToken) and new_peak > float(
                    row.ath_mcap or 0.0
                ):
                    screened[idx] = row.model_copy(update={"ath_mcap": new_peak})

        crossed: list[str] = []
        if updates or probe_stamp:
            for addr, (peak, sym) in updates.items():
                ent = hold_out.get(addr) or {
                    "first_seen": now,
                    "ath_mcap": 0.0,
                    "symbol": sym,
                }
                ent["ath_mcap"] = max(float(ent.get("ath_mcap") or 0.0), peak)
                if sym and not ent.get("symbol"):
                    ent["symbol"] = sym
                hold_out[addr] = ent
                if peak >= min_ath:
                    crossed.append(addr)
            for addr, ts in probe_stamp.items():
                ent = hold_out.get(addr)
                if ent is not None:
                    ent["ath_probed_at"] = ts
            # Persist immediately so a dump mid-cycle cannot wipe the peak.
            # Stamp crossed as candidates so pending-parse survives restart.
            self._store.apply_qualify_updates(
                ath_updates=updates,
                held=list(hold_out.keys()),
                expired=[],
                candidates=crossed,
                now=now,
                probed_at=probe_stamp,
            )
            self._append_log(
                "hold",
                f"{label}: обновлено {len(updates)}, ≥порога: {len(crossed)}",
                percent=19,
            )
        return screened, hold_out, crossed

    async def _catchup_refresh_hold(
        self,
        cfg: WatchConfig,
        *,
        on_progress,
    ) -> None:
        """Ensure index is ready and force-refresh ATH for hold-queue tokens."""
        if not ath_gate_enabled(cfg.screen.min_ath_mcap):
            return
        if self._stop_requested:
            raise _WatchStopped()
        # Remote path: budgeted force_enrich goes on the truegnomode screen job.
        if using_remote_screener():
            hold_n = self._store.hold_count()
            self._append_log(
                "catchup",
                f"Догон: truegnomode enrich ≤{HOLD_ENRICH_CAP} near-gate "
                f"(hold {hold_n})",
                percent=3,
            )
            return
        self._last_message = "Догон: прогрев индекса…"
        self._append_log("catchup", self._last_message, percent=2)
        try:
            await asyncio.wait_for(
                token_index.ensure_ready(on_progress=on_progress),
                timeout=90.0,
            )
        except asyncio.TimeoutError:
            self._append_log(
                "catchup",
                "Догон: индекс не готов за 90s — продолжаем со screenable пулом",
                percent=3,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("catchup ensure_ready soft-fail: %s", exc)
            self._append_log(
                "catchup",
                f"Догон: индекс soft-fail ({type(exc).__name__}) — продолжаем",
                percent=3,
            )
        if self._stop_requested:
            raise _WatchStopped()
        hold_snap = self._store.load_hold()
        if not hold_snap:
            self._append_log("catchup", "Догон: hold пуст — только скринер", percent=4)
            return
        targets = select_hold_enrich_batch(
            hold_snap,
            min_ath_mcap=float(cfg.screen.min_ath_mcap or 0.0),
            cap=HOLD_ENRICH_CAP,
        )
        self._last_message = (
            f"Догон: re-enrich {len(targets)}/{len(hold_snap)} hold…"
        )
        self._append_log("catchup", self._last_message, percent=3)
        enriched = await token_index.force_enrich_addresses(
            targets,
            on_progress=on_progress,
        )
        if self._stop_requested:
            raise _WatchStopped()
        ath_updates = {
            addr: (max(row.ath_mcap, row.market_cap), row.symbol or "")
            for addr, row in enriched.items()
        }
        if ath_updates:
            self._store.apply_qualify_updates(
                ath_updates=ath_updates,
                held=list(hold_snap.keys()),
                expired=[],
                candidates=[],
                now=time.time(),
            )
        crossed = 0
        threshold = float(cfg.screen.min_ath_mcap or 0.0)
        for addr, (peak, _sym) in ath_updates.items():
            prev = float(hold_snap.get(addr, {}).get("ath_mcap") or 0.0)
            if peak >= threshold and prev < threshold:
                crossed += 1
        self._append_log(
            "catchup",
            f"Догон hold: обновлено {len(ath_updates)}/{len(targets)}"
            + (f", новых ATH≥порога: {crossed}" if crossed else ""),
            percent=4,
        )


watch_runner = WatchRunner()
