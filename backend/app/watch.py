"""Scheduled watch pipeline: catch-up → screen → parse → dedup → Telegram."""

from __future__ import annotations

import asyncio
import logging
import time

from .chain import RpcClient
from .config import settings
from .jobs import jobs
from .models import (
    BuyerRow,
    JobLogEntry,
    ParseRequest,
    ScreenRequest,
    WatchConfig,
    WatchScreenFilters,
    WatchStatus,
)
from .replay import parse_token
from .screener_feed import fetch_screened_tokens, using_remote_screener
from .telegram import resolve_chat_id, resolve_topic_id, send_buyers, telegram_configured
from .token_index import token_index
from .watch_qualify import (
    REPARSE_YOUNG_COOLDOWN_SEC,
    ath_gate_enabled,
    classify_for_parse,
    should_mark_parsed,
)
from .watch_store import WatchStore, catchup_lookback_hours, watch_store

logger = logging.getLogger(__name__)

_LOG_MAX = 400
# Reloads / brief downtime must not open a useless 0.2–0.5h catch-up window.
_MIN_CATCHUP_GAP_SEC = 3600.0


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
        # Catch-up: ask truegnomode donor to force-enrich hold addresses (local
        # path still refreshes via token_index in _catchup_refresh_hold).
        force_enrich: list[str] | None = None
        if catchup and using_remote_screener():
            hold_addrs = list(self._store.load_hold().keys())
            if hold_addrs:
                force_enrich = hold_addrs
        remote = using_remote_screener()
        if remote:
            self._append_log(
                "screen",
                f"Источник токенов: truegnomode ({settings.truegnomode_screener_url})",
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

        min_ath = cfg.screen.min_ath_mcap
        gate_on = ath_gate_enabled(min_ath)
        now = time.time()
        hold_snapshot = self._store.load_hold() if gate_on else {}
        parsed_at = self._store.load_parsed_at() if gate_on else {}
        max_pair_age = cfg.screen.max_pair_age_hours
        if gate_on and min_ath:
            # Brief pumps can dump before DexScreener writes a lasting ATH into
            # the index. Probe Gecko/DS peaks for young under-threshold tokens
            # so dump-after-pump still qualifies while age≤max_pair_age.
            screened, hold_snapshot = await self._probe_young_ath_peaks(
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

        # Never-parsed / hold-promoted first; young requeues only fill leftover
        # slots so filter-wipe retries cannot starve brand-new tokens.
        requeued = {a.lower() for a in decision.requeued_young}
        fresh = [t for t in decision.candidates if t.lower() not in requeued]
        retry = [t for t in decision.candidates if t.lower() in requeued]
        limit = max(1, int(cfg.max_tokens_per_cycle))
        retry_budget = min(len(retry), max(1, limit // 4)) if retry else 0
        tokens = (fresh + retry[:retry_budget])[:limit]
        if len(decision.candidates) > len(tokens):
            self._append_log(
                "hold",
                f"Лимит цикла {limit}: парсим {len(tokens)} из "
                f"{len(decision.candidates)} qualify "
                f"(fresh={len(fresh)}, requeue_used={min(retry_budget, len(retry))}/"
                f"{len(retry)}; остальные — следующий цикл)",
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

        for i, token in enumerate(tokens):
            if self._stop_requested:
                self._last_message = f"Остановлено ({parsed}/{n})"
                self._append_log("stop", self._last_message)
                interrupted = True
                break
            if jobs.has_active():
                self._last_message = f"Остановлено — ручной парсинг ({parsed}/{n})"
                self._append_log("skip", self._last_message)
                logger.info("Watch cycle interrupted by manual parse")
                interrupted = True
                break
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
                self._append_log("error", f"Ошибка парса {token[:10]}…: {exc}", token=token)
                continue
            parsed += 1
            buyers = [b for b in result.buyers if b.buys_count == 1]
            before_filters = int(
                (result.stats or {}).get("buyers_before_wallet_filters") or 0
            )
            if gate_on and should_mark_parsed(
                result.error,
                buyers_before_filters=before_filters,
                buyers_after_filters=len(buyers),
            ):
                self._store.mark_token_parsed(token)
                # Soft lock: young tokens requeue after cooldown (not every cycle).
                if before_filters > 0 and not buyers:
                    self._append_log(
                        "hold",
                        f"{token[:10]}… parsed (cooldown) — {before_filters} early "
                        "→ 0 после фильтров; повтор через "
                        f"{int(REPARSE_YOUNG_COOLDOWN_SEC // 60)}м пока age ок",
                        token=token,
                    )
            found_total += len(buyers)
            self._last_tokens_parsed = parsed
            self._last_buyers_found = found_total
            self._append_log(
                "parse",
                f"{token[:10]}… → {len(buyers)} кош. (buys=1)"
                + (f" ({result.error})" if result.error else ""),
                token=token,
            )

            # Dedup + send immediately per token (don't wait for the full cycle).
            new_buyers: list[BuyerRow] = []
            for b in buyers:
                key = f"{b.wallet.lower()}:{b.token.lower()}"
                if key in seen:
                    skipped_total += 1
                else:
                    new_buyers.append(b)
            self._last_buyers_skipped = skipped_total
            self._last_buyers_new = new_total + len(new_buyers)

            if not new_buyers:
                continue

            # Хвать / follow-up: track 1-buy wallets even if Telegram fails.
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
                logger.warning("Follow-up ingest failed: %s", exc)

            if self._stop_requested:
                interrupted = True
                self._last_message = "Остановлено перед отправкой в Telegram"
                self._append_log("stop", self._last_message)
                break

            self._last_message = (
                f"Telegram: отправка {len(new_buyers)} кош. "
                f"({token[:10]}…, {i + 1}/{n})"
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
                # Keep parsing other tokens; unsent pairs stay unseen for retry.
                continue

            if sent:
                pairs = [(b.wallet, b.token) for b in sent]
                self._store.mark_seen(pairs)
                for b in sent:
                    seen.add(f"{b.wallet.lower()}:{b.token.lower()}")
                sent_total += len(sent)
                new_total += len(sent)
            self._last_buyers_sent = sent_total
            self._last_buyers_new = new_total
            partial = len(new_buyers) - len(sent)
            if partial > 0:
                self._last_error = (
                    f"Частичная отправка в Telegram: {len(sent)}/{len(new_buyers)}"
                )
                self._append_log("error", self._last_error, token=token)
            else:
                self._append_log(
                    "telegram",
                    f"Отправлено {len(sent)} кош. по {token[:10]}… "
                    f"(всего за цикл {sent_total})",
                    token=token,
                )

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

    async def _probe_young_ath_peaks(
        self,
        screened: list,
        *,
        hold: dict[str, dict],
        min_ath: float,
        max_pair_age_hours: float | None,
        probe_cap: int = 24,
    ) -> tuple[list, dict[str, dict]]:
        """Bump ATH for young tokens still below the gate using Gecko/DS peaks.

        Persists peaks into ``hold`` so a post-pump dump does not erase qualify.
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

        for addr, ent in hold_out.items():
            if addr in seen:
                continue
            peak = float(ent.get("ath_mcap") or 0.0)
            if peak >= min_ath:
                continue
            # Hold-only: treat as young enough to probe while still on the queue.
            need.append((addr, peak, str(ent.get("symbol") or ""), None, None))

        if not need:
            return screened, hold_out

        # Prefer lowest known peak (most likely to miss a real pump).
        need.sort(key=lambda t: t[1])
        need = need[: max(1, probe_cap)]
        self._append_log(
            "hold",
            f"ATH-probe: {len(need)} молодых ток. ниже ${min_ath:,.0f}…",
            percent=18,
        )

        updates: dict[str, tuple[float, str]] = {}
        row_peaks: dict[int, float] = {}
        sem = asyncio.Semaphore(3)

        async def one(
            addr: str, prev_peak: float, symbol: str, _age: float | None, idx: int | None
        ) -> tuple[str, float, str, int | None] | None:
            async with sem:
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
            *[one(a, p, s, age, i) for a, p, s, age, i in need]
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

        if updates:
            now = time.time()
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
            # Persist immediately so a dump mid-cycle cannot wipe the peak.
            self._store.apply_qualify_updates(
                ath_updates=updates,
                held=list(hold_out.keys()),
                expired=[],
                candidates=[],
                now=now,
            )
            crossed = sum(1 for _a, (pk, _) in updates.items() if pk >= min_ath)
            self._append_log(
                "hold",
                f"ATH-probe: обновлено {len(updates)}, ≥порога: {crossed}",
                percent=19,
            )
        return screened, hold_out

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
        # Remote path: force_enrich_addresses is sent with the truegnomode screen job.
        if using_remote_screener():
            hold_n = self._store.hold_count()
            self._append_log(
                "catchup",
                f"Догон: truegnomode force-enrich hold ({hold_n} ток.) на screen-job",
                percent=3,
            )
            return
        self._last_message = "Догон: прогрев индекса…"
        self._append_log("catchup", self._last_message, percent=2)
        await token_index.ensure_ready(on_progress=on_progress)
        if self._stop_requested:
            raise _WatchStopped()
        hold_snap = self._store.load_hold()
        if not hold_snap:
            self._append_log("catchup", "Догон: hold пуст — только скринер", percent=4)
            return
        self._last_message = f"Догон: re-enrich hold ({len(hold_snap)} ток.)…"
        self._append_log("catchup", self._last_message, percent=3)
        enriched = await token_index.force_enrich_addresses(
            list(hold_snap.keys()),
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
            f"Догон hold: обновлено {len(ath_updates)}/{len(hold_snap)}"
            + (f", новых ATH≥порога: {crossed}" if crossed else ""),
            percent=4,
        )


watch_runner = WatchRunner()
