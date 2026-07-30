"""Scheduled watch pipeline: catch-up → screen → parse → dedup → Telegram → track."""

from __future__ import annotations

import asyncio
import logging
import time

from .chain import RpcClient
from .config import settings
from .jobs import jobs
from .launchpads.types import SniperHit
from .models import (
    BuyerRow,
    JobLogEntry,
    ParseRequest,
    ScreenRequest,
    WatchScreenFilters,
    WatchStatus,
)
from .replay import parse_token
from .screener import screen_tokens
from .sniper_score import record_sniper_trade
from .telegram import resolve_chat_id, resolve_topic_id, send_buyers, telegram_configured
from .watch_store import WatchStore, catchup_lookback_hours, watch_store

logger = logging.getLogger(__name__)

_LOG_MAX = 400
# Reloads / brief downtime must not open a useless 0.2–0.5h catch-up window.
_MIN_CATCHUP_GAP_SEC = 3600.0


async def track_one_token_buyers(
    buyers: list[BuyerRow],
    *,
    max_first_mcap: float | None = None,
) -> int:
    """Push wallets with exactly one distinct 7d token into sniper follow tracking.

    Requires tokens_traded_7d == 1 and first-buy mcap ≤ max_first_mcap.
    RayBot rule: 1 token = 1 trade_count. Returns how many new wallet+token pairs
    were recorded.
    """
    mcap_cap = (
        settings.sniper_max_first_mcap
        if max_first_mcap is None
        else float(max_first_mcap)
    )
    tracked = 0
    for b in buyers:
        if b.tokens_traded_7d is None or int(b.tokens_traded_7d) != 1:
            continue
        first_mcap = float(b.mcap_at_first_buy or 0)
        if first_mcap <= 0 or first_mcap > mcap_cap:
            continue
        try:
            ok = await record_sniper_trade(
                b.wallet,
                b.token,
                hit=SniperHit(
                    wallet=b.wallet,
                    block=int(b.first_block or 0),
                    tx=b.first_tx or "",
                    amount_usd=float(b.bought_usd or 0),
                    mcap_at_trade=first_mcap,
                ),
                min_buy_usd=0.0,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Failed to track one-token wallet %s on %s",
                b.wallet[:10],
                b.token[:10],
            )
            continue
        if ok:
            tracked += 1
    return tracked


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
        self._last_buyers_found = 0
        self._last_buyers_new = 0
        self._last_buyers_sent = 0
        self._last_buyers_skipped = 0
        self._log: list[JobLogEntry] = []

    @property
    def running(self) -> bool:
        return self._running

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
            last_buyers_found=self._last_buyers_found,
            last_buyers_new=self._last_buyers_new,
            last_buyers_sent=self._last_buyers_sent,
            last_buyers_skipped=self._last_buyers_skipped,
            seen_count=self._store.seen_count(),
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
                try:
                    await self.run_cycle(trigger=trigger, catchup=do_catchup)
                    if do_catchup and not self._stop_requested:
                        self._needs_catchup = False
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Watch cycle crashed")
                    self._last_error = str(exc)
                    self._last_message = f"Цикл упал: {exc}"
                    self._append_log("error", f"Цикл упал: {exc}")

                if self._stop_requested:
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
            logger.warning("Watch runner cancelled")
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

        screen = cfg.screen
        if catchup:
            lookback = self._catchup_lookback_hours or catchup_lookback_hours(
                self._store.load_last_success_ts()
            )
            screen = apply_catchup_to_screen(screen, lookback)
            self._last_message = (
                f"Скрининг токенов за последние {lookback:.1f} ч (догон)…"
            )
        else:
            self._last_message = "Скрининг токенов…"
        self._append_log("screen", self._last_message, percent=5)

        class _WatchStopped(Exception):
            pass

        async def on_progress(stage: str, message: str, percent: float) -> None:
            if self._stop_requested:
                raise _WatchStopped()
            self._last_message = message
            self._append_log(stage, message, percent=round(percent * 100, 1))

        async def _stop_waiter() -> None:
            while not self._stop_requested:
                await asyncio.sleep(0.25)
            raise _WatchStopped()

        screen_req = ScreenRequest(**screen.model_dump())
        screen_task = asyncio.create_task(
            screen_tokens(screen_req, on_progress=on_progress)
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
            screened = await screen_task
        except _WatchStopped:
            self._last_message = "Остановлено во время скрининга"
            self._append_log("stop", self._last_message)
            return False
        if self._stop_requested:
            self._last_message = "Остановлено после скрининга"
            self._append_log("stop", self._last_message)
            return False
        self._last_tokens_screened = len(screened)
        self._append_log(
            "screen",
            f"Скринер: {len(screened)} ток. (лимит цикла {cfg.max_tokens_per_cycle})",
            percent=20,
        )

        # MCAP tracker runs in its own background loop — don't block parse here.
        # A stuck 10k+ tracker tick previously froze the whole watch cycle.
        mt_cfg = getattr(cfg, "mcap_tracker", None)
        if mt_cfg is None or mt_cfg.enabled:
            self._append_log(
                "mcap_tracker",
                "MCAP tracker — фоновый цикл (не блокирует парсинг)",
                percent=22,
            )

        tokens = [t.address for t in screened[: cfg.max_tokens_per_cycle]]
        if not tokens:
            self._last_tokens_parsed = 0
            self._last_buyers_found = 0
            self._last_buyers_new = 0
            self._last_buyers_sent = 0
            self._last_buyers_skipped = 0
            self._last_message = "Нет токенов по фильтрам скринера"
            self._append_log("screen", self._last_message, percent=100)
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
        )
        min_buy_usd = getattr(cfg.wallet, "min_buy_usd", None)
        if min_buy_usd is None:
            min_buy_usd = settings.min_buy_usd or None

        rpc = RpcClient()
        parsed = 0
        found_total = 0
        new_total = 0
        sent_total = 0
        skipped_total = 0
        n = len(tokens)
        interrupted = False
        tg_failed = False
        seen = self._store.load_seen()

        # Parallel only when heavy wallet filters are off — otherwise RPC/Blockscout
        # contention makes the cycle look frozen (same as manual parse).
        heavy_filters = any(
            x is not None
            for x in (
                cfg.wallet.min_wallet_balance_eth,
                cfg.wallet.max_wallet_balance_eth,
                cfg.wallet.min_hold_time_minutes,
                cfg.wallet.max_hold_time_minutes,
                cfg.wallet.min_tokens_traded_7d,
                cfg.wallet.max_tokens_traded_7d,
            )
        )
        parse_conc = 1 if heavy_filters else min(2, max(1, n))
        parse_sem = asyncio.Semaphore(parse_conc)
        parse_lock = asyncio.Lock()

        async def _parse_one(i: int, token: str) -> None:
            nonlocal parsed, found_total, new_total, sent_total, skipped_total
            nonlocal interrupted, tg_failed, seen
            if self._stop_requested or interrupted or tg_failed:
                return
            if jobs.has_active():
                interrupted = True
                self._last_message = f"Остановлено — ручной парсинг ({parsed}/{n})"
                self._append_log("skip", self._last_message)
                return

            async with parse_sem:
                if self._stop_requested or interrupted or tg_failed:
                    return
                self._last_message = f"Парсинг {i + 1}/{n}: {token[:10]}…"
                self._append_log(
                    "parse",
                    self._last_message,
                    percent=20 + 70 * (i / max(n, 1)),
                    token=token,
                )

                async def on_tok_progress(
                    stage: str,
                    message: str,
                    percent: float,
                    _i=i,
                    _n=n,
                    _token=token,
                ) -> None:
                    overall = 20 + 70 * ((_i + percent) / max(_n, 1))
                    self._last_message = message
                    self._append_log(stage, message, percent=round(overall, 1), token=_token)

                try:
                    result = await parse_token(
                        rpc,
                        token,
                        threshold,
                        on_progress=on_tok_progress,
                        exclude_honeypots=cfg.wallet.exclude_honeypots,
                        wallet_filters=wallet_req,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Watch parse failed for %s", token)
                    self._append_log(
                        "error", f"Ошибка парса {token[:10]}…: {exc}", token=token
                    )
                    return

                buyers = list(result.buyers)
                async with parse_lock:
                    parsed += 1
                    found_total += len(buyers)
                    self._last_tokens_parsed = parsed
                    self._last_buyers_found = found_total
                self._append_log(
                    "parse",
                    f"{token[:10]}… → {len(buyers)} кош."
                    + (f" ({result.error})" if result.error else ""),
                    token=token,
                )

                # Eligible for Telegram + tracking (mcap / min buy only).
                eligible: list[BuyerRow] = []
                new_buyers: list[BuyerRow] = []
                local_skipped = 0
                for b in buyers:
                    if threshold is not None and b.mcap_at_first_buy > threshold:
                        local_skipped += 1
                        continue
                    if min_buy_usd and (b.bought_usd or 0) < float(min_buy_usd):
                        local_skipped += 1
                        continue
                    eligible.append(b)
                    key = f"{b.wallet.lower()}:{b.token.lower()}"
                    if key in seen:
                        local_skipped += 1
                    else:
                        new_buyers.append(b)

                # One-token wallets (7d==1, first mcap ≤ threshold) → sniper follow.
                tracked_n = await track_one_token_buyers(
                    eligible,
                    max_first_mcap=(
                        float(threshold)
                        if threshold is not None
                        else settings.sniper_max_first_mcap
                    ),
                )
                if tracked_n:
                    self._append_log(
                        "track",
                        f"На отслеживание: {tracked_n} кош. (1 токен) по {token[:10]}…",
                        token=token,
                    )

                async with parse_lock:
                    skipped_total += local_skipped
                    self._last_buyers_skipped = skipped_total
                    self._last_buyers_new = new_total + len(new_buyers)

                if not new_buyers:
                    return

                async with parse_lock:
                    if tg_failed or self._stop_requested:
                        return
                    header = (
                        f"Автопарс · догон · {len(new_buyers)} кош."
                        if catchup
                        else f"Автопарс · {len(new_buyers)} кош."
                    )
                    try:
                        _msgs, sent_buyers = await send_buyers(
                            chat, new_buyers, header=header, topic_id=topic_id
                        )
                        if sent_buyers:
                            pairs = [(b.wallet, b.token) for b in sent_buyers]
                            self._store.mark_seen(pairs)
                            for b in sent_buyers:
                                seen.add(f"{b.wallet.lower()}:{b.token.lower()}")
                            new_total += len(sent_buyers)
                            sent_total += len(sent_buyers)
                            self._last_buyers_new = new_total
                            self._last_buyers_sent = sent_total
                        partial = len(new_buyers) - len(sent_buyers)
                        if partial > 0:
                            self._last_error = (
                                f"Частичная отправка в Telegram: "
                                f"{len(sent_buyers)}/{len(new_buyers)}"
                            )
                            self._append_log("error", self._last_error, token=token)
                        else:
                            self._append_log(
                                "telegram",
                                f"Отправлено {len(sent_buyers)} кош. по {token[:10]}… "
                                f"(всего за цикл {sent_total})",
                                token=token,
                            )
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Telegram send failed")
                        self._last_error = str(exc)
                        tg_failed = True
                        self._append_log("error", f"Telegram: {exc}", token=token)

        if parse_conc == 1:
            for i, token in enumerate(tokens):
                if self._stop_requested or interrupted or tg_failed:
                    break
                await _parse_one(i, token)
        else:
            await asyncio.gather(*[_parse_one(i, t) for i, t in enumerate(tokens)])
            if jobs.has_active() and parsed < n:
                interrupted = True

        self._last_tokens_parsed = parsed
        self._last_buyers_found = found_total
        self._last_buyers_new = new_total
        self._last_buyers_sent = sent_total
        self._last_buyers_skipped = skipped_total

        prefix = "Догон" if catchup else "Готово"
        if interrupted:
            prefix = "Остановлено"
        self._last_message = (
            f"{prefix} — {parsed} ток., {found_total} кош., "
            f"{sent_total} отпр., {skipped_total} проп."
        )
        self._append_log("done", self._last_message, percent=100)
        if tg_failed and sent_total == 0:
            return False
        return not interrupted


watch_runner = WatchRunner()
