"""Follow-up runner: watch early buyers for 2nd/3rd new-token buys @ low mcap."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from .blockscout import scan_address_token_transfers
from .buy_gate import is_wallet_initiated_buy, method_is_non_buy
from .config import settings
from .constants import QUOTE_TOKENS
from .followup_store import FollowupStore, followup_store
from .models import (
    BuyerRow,
    FollowupConfig,
    FollowupStatus,
    JobLogEntry,
    WalletAlertFilters,
)
from .pools import fetch_dexscreener_pairs
from .raybot import RayBotClient, raybot_client, raybot_configured
from .telegram import (
    resolve_chat_id,
    resolve_topic_id,
    send_followup_deal,
    telegram_configured,
)

logger = logging.getLogger(__name__)

_LOG_MAX = 300


def _addr_hash(node: object) -> str:
    if isinstance(node, dict):
        return str(node.get("hash") or node.get("address_hash") or "").lower()
    return str(node or "").lower()


def _is_contract(node: object) -> bool:
    return isinstance(node, dict) and bool(node.get("is_contract"))


def _token_meta(item: dict[str, Any]) -> tuple[str, str]:
    tok = item.get("token") or {}
    addr = str(tok.get("address") or tok.get("address_hash") or "").lower()
    sym = str(tok.get("symbol") or "")
    return addr, sym


def should_alert_deal(
    deal_index: int,
    mcap_at_buy: float | None,
    *,
    max_mcap_alert: float,
    alert_on_deals: list[int],
    min_mcap_alert: float | None = None,
    bought_usd: float | None = None,
    min_bought_usd: float | None = None,
    max_bought_usd: float | None = None,
) -> bool:
    """True only for configured deal indices that pass native filter set.

    Mirrors RayBot-style gates without external RayBot:
    - deal index in alert_on_deals (default 2…5)
    - mcap ≤ max_mcap_alert (high mcap → no alert)
    - optional mcap ≥ min_mcap_alert
    - optional bought_usd min/max when value is known
    """
    if deal_index not in alert_on_deals:
        return False
    if mcap_at_buy is None:
        return False
    mcap = float(mcap_at_buy)
    if mcap > float(max_mcap_alert):
        return False
    if min_mcap_alert is not None and mcap < float(min_mcap_alert):
        return False
    if bought_usd is not None:
        usd = float(bought_usd)
        if min_bought_usd is not None and usd < float(min_bought_usd):
            return False
        if max_bought_usd is not None and usd > float(max_bought_usd):
            return False
    elif min_bought_usd is not None:
        # Require known size when min filter is set
        return False
    return True


def prune_settings_for_wallet(
    cfg: FollowupConfig,
    wallet_filters: WalletAlertFilters | None = None,
) -> tuple[bool, float, float]:
    """Return (enabled, min_ath_mcap_usd, after_hours) for a wallet."""
    enabled = bool(cfg.prune_enabled)
    min_ath = float(cfg.prune_min_ath_mcap or 0)
    hours = float(cfg.prune_after_hours or 48)
    if wallet_filters and wallet_filters.custom:
        if wallet_filters.prune_enabled is not None:
            enabled = bool(wallet_filters.prune_enabled)
        if wallet_filters.prune_min_ath_mcap is not None:
            min_ath = float(wallet_filters.prune_min_ath_mcap)
        if wallet_filters.prune_after_hours is not None:
            hours = float(wallet_filters.prune_after_hours)
    return enabled, min_ath, max(1.0, hours)


@dataclass(frozen=True)
class PeakMcapEstimate:
    """Peak mcap for prune decisions.

    ``reliable`` is True when we have index ATH and/or Gecko OHLCV (not spot-only).
    Spot DexScreener alone can understate a dumped ATH — do not prune on that.
    """

    peak: float
    reliable: bool


async def estimate_token_peak_mcap(
    token: str,
    *,
    min_needed: float = 0.0,
) -> PeakMcapEstimate | None:
    """Best-effort peak mcap: index ATH → DexScreener spot → Gecko OHLCV.

    Short-circuits once ``peak >= min_needed`` (skips remaining network calls).
    """
    key = (token or "").strip().lower()
    if not key.startswith("0x"):
        return None
    peak = 0.0
    reliable = False

    try:
        from .token_index import token_index

        peaks = token_index.mcap_peaks([key])
        hit = peaks.get(key)
        if hit and hit[0] > 0:
            peak = max(peak, float(hit[0]))
            # Index mixes ath + live market_cap — enough to *pass*, not to prune.
            if min_needed > 0 and peak >= min_needed:
                return PeakMcapEstimate(peak=peak, reliable=True)
    except Exception:  # noqa: BLE001
        pass

    try:
        mcap, _ = await estimate_token_quote(key)
        if mcap and mcap > 0:
            peak = max(peak, float(mcap))
            # Spot alone is enough to *pass* the gate.
            if min_needed > 0 and peak >= min_needed:
                return PeakMcapEstimate(peak=peak, reliable=True)
    except Exception:  # noqa: BLE001
        pass

    try:
        from .ath_gecko import fetch_token_ath_mcap

        res = await fetch_token_ath_mcap(key)
        if res.ath_mcap and res.ath_mcap > 0:
            peak = max(peak, float(res.ath_mcap))
            reliable = True
            if min_needed > 0 and peak >= min_needed:
                return PeakMcapEstimate(peak=peak, reliable=True)
        elif res.error:
            logger.debug("peak mcap gecko %s: %s", key[:10], res.error)
    except Exception as exc:  # noqa: BLE001
        logger.debug("peak mcap gecko %s: %s", key[:10], exc)

    if peak <= 0:
        return None
    return PeakMcapEstimate(peak=peak, reliable=reliable)


def alert_kwargs_from_config(cfg: FollowupConfig) -> dict:
    return {
        "max_mcap_alert": cfg.max_mcap_alert,
        "alert_on_deals": list(cfg.alert_on_deals or [2, 3, 4, 5]),
        "min_mcap_alert": cfg.min_mcap_alert,
        "min_bought_usd": cfg.min_bought_usd,
        "max_bought_usd": cfg.max_bought_usd,
    }


def alert_kwargs_for_wallet(
    cfg: FollowupConfig,
    wallet_filters: WalletAlertFilters | None = None,
) -> dict:
    """Merge global FollowupConfig with optional per-wallet overrides."""
    base = alert_kwargs_from_config(cfg)
    if not wallet_filters or not wallet_filters.custom:
        return base
    max_mcap = wallet_filters.max_mcap_alert
    if max_mcap is None:
        max_mcap = base["max_mcap_alert"]
    return {
        "max_mcap_alert": float(max_mcap),
        "alert_on_deals": base["alert_on_deals"],
        "min_mcap_alert": wallet_filters.min_mcap_alert,
        "min_bought_usd": wallet_filters.min_bought_usd,
        "max_bought_usd": wallet_filters.max_bought_usd,
    }


async def estimate_token_quote(token: str) -> tuple[float | None, float | None]:
    """Return (market_cap_usd, price_usd) from the highest-liquidity DexScreener pair."""
    pairs = await fetch_dexscreener_pairs(token)
    if not pairs:
        return None, None
    best_mcap: float | None = None
    best_price: float | None = None
    best_liq = -1.0
    for p in pairs:
        try:
            liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
        except (TypeError, ValueError):
            liq = 0.0
        raw = p.get("marketCap")
        if raw is None:
            raw = p.get("fdv")
        try:
            mcap = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            mcap = None
        try:
            price = float(p.get("priceUsd")) if p.get("priceUsd") is not None else None
        except (TypeError, ValueError):
            price = None
        if mcap is None and price is None:
            continue
        if liq >= best_liq:
            best_liq = liq
            best_mcap = mcap
            best_price = price
    return best_mcap, best_price


async def estimate_token_mcap(token: str) -> float | None:
    mcap, _ = await estimate_token_quote(token)
    return mcap


def _transfer_token_amount(item: dict[str, Any]) -> float | None:
    """Parse human token amount from a Blockscout token-transfer item."""
    total = item.get("total")
    raw: str | None = None
    decimals: int | None = None
    if isinstance(total, dict):
        raw = total.get("value")
        if raw is None:
            raw = total.get("token_id")
        try:
            decimals = int(total.get("decimals")) if total.get("decimals") is not None else None
        except (TypeError, ValueError):
            decimals = None
    elif total is not None:
        raw = str(total)
    if decimals is None:
        tok = item.get("token") or {}
        if isinstance(tok, dict) and tok.get("decimals") is not None:
            try:
                decimals = int(tok["decimals"])
            except (TypeError, ValueError):
                decimals = None
    if raw is None:
        return None
    try:
        value = float(str(raw).replace(",", ""))
    except (TypeError, ValueError):
        return None
    if decimals is None:
        decimals = 18
    if decimals < 0:
        return None
    return value / (10**decimals)


def estimate_bought_usd(item: dict[str, Any], price_usd: float | None) -> float | None:
    if price_usd is None or price_usd <= 0:
        return None
    amount = _transfer_token_amount(item)
    if amount is None or amount <= 0:
        return None
    return amount * float(price_usd)


async def _is_buy_like_transfer(
    item: dict[str, Any],
    wallet: str,
    *,
    buys_only: bool,
    track_transfers: bool,
) -> bool:
    """Inbound to tracked wallet; with buys_only require wallet-initiated swap."""
    to_h = _addr_hash(item.get("to"))
    if to_h != wallet.lower():
        return False
    if buys_only:
        return await is_wallet_initiated_buy(item, wallet)
    frm = item.get("from")
    if _is_contract(frm):
        return not method_is_non_buy(item.get("method"))
    return bool(track_transfers)


class FollowupRunner:
    def __init__(self, store: FollowupStore | None = None) -> None:
        self._store = store or followup_store
        self._raybot: RayBotClient = raybot_client
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._force_run = False
        self._stop_requested = False
        self._running = False
        self._next_run_ts: float | None = None
        self._last_run_ts: float | None = None
        self._last_run_duration_sec: float | None = None
        self._last_error: str | None = None
        self._last_message: str = ""
        self._last_checked = 0
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._log: list[JobLogEntry] = []

    def _append_log(self, stage: str, message: str, *, percent: float = 0.0) -> None:
        entry = JobLogEntry(
            ts=time.time(), stage=stage, message=message, percent=percent
        )
        if self._log:
            last = self._log[-1]
            if last.stage == entry.stage and last.message == entry.message:
                self._log[-1] = entry
                return
        self._log.append(entry)
        if len(self._log) > _LOG_MAX:
            self._log = self._log[-_LOG_MAX:]

    def notify_config_changed(self) -> None:
        self._wake.set()

    def status(self) -> FollowupStatus:
        from .followup_bot import followup_bot

        cfg = self._store.load_config()
        watching, done = self._store.counts()
        chat = resolve_chat_id(cfg.telegram_chat_id)
        return FollowupStatus(
            enabled=cfg.enabled,
            running=self._running,
            telegram_configured=telegram_configured(chat),
            bot_commands_enabled=cfg.bot_commands_enabled,
            bot_polling=followup_bot.polling,
            raybot_configured=raybot_configured() and cfg.raybot_enabled,
            next_run_ts=self._next_run_ts,
            last_run_ts=self._last_run_ts,
            last_run_duration_sec=self._last_run_duration_sec,
            last_error=self._last_error,
            last_message=self._last_message,
            wallets_watching=watching,
            wallets_done=done,
            last_checked=self._last_checked,
            last_new_deals=self._last_new_deals,
            last_alerts_sent=self._last_alerts_sent,
            stop_requested=self._stop_requested,
            log=list(self._log),
        )

    def reset_counters(self) -> FollowupStatus:
        self._last_error = None
        self._last_message = ""
        self._last_checked = 0
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._log.clear()
        return self.status()

    async def stop(self) -> FollowupStatus:
        self._stop_requested = True
        self._wake.set()
        return self.status()

    async def run_now(self) -> FollowupStatus:
        self._force_run = True
        self._stop_requested = False
        self._wake.set()
        return self.status()

    async def run_loop(self) -> None:
        while True:
            cfg = self._store.load_config()
            if not cfg.enabled and not self._force_run:
                self._next_run_ts = None
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass
                continue

            self._force_run = False
            self._stop_requested = False
            started = time.time()
            self._running = True
            self._next_run_ts = None
            try:
                await self.run_cycle(cfg)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Follow-up cycle failed")
                self._last_error = str(exc)
                self._last_message = f"Ошибка: {exc}"
                self._append_log("error", self._last_message)
            finally:
                self._running = False
                self._last_run_ts = time.time()
                self._last_run_duration_sec = self._last_run_ts - started

            cfg = self._store.load_config()
            if not cfg.enabled:
                continue
            # interval_sec = target period between cycle *starts*. If a cycle
            # already took longer, start the next one immediately.
            period = max(5, int(cfg.interval_sec or 5))
            sleep_for = max(0.0, period - float(self._last_run_duration_sec or 0))
            self._next_run_ts = time.time() + sleep_for
            self._wake.clear()
            while True:
                remaining = self._next_run_ts - time.time()
                if remaining <= 0 or self._force_run or self._stop_requested:
                    break
                try:
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=min(remaining, 5.0)
                    )
                except asyncio.TimeoutError:
                    pass
                if self._wake.is_set():
                    self._wake.clear()
                    cfg = self._store.load_config()
                    if not cfg.enabled and not self._force_run:
                        break

    async def run_cycle(self, cfg: FollowupConfig | None = None) -> None:
        async with self._lock:
            await self._cycle_body(cfg or self._store.load_config())

    async def ingest_from_watch(
        self,
        buyers: list[BuyerRow],
        *,
        cfg: FollowupConfig | None = None,
    ) -> int:
        """Ingest early buyers from autoparse into follow-up (idempotent)."""
        cfg = cfg or self._store.load_config()
        if not cfg.ingest_from_watch:
            return 0
        inserted = self._store.ingest_buyers(
            buyers,
            max_deals=cfg.max_deals,
            max_mcap_alert=cfg.max_mcap_alert,
        )
        if not inserted:
            return 0
        self._append_log(
            "ingest",
            f"В follow-up добавлено {len(inserted)} сделок",
        )

        # RayBot: sync wallets that just got deal #1 (new watchlist members)
        new_addrs = sorted({d.wallet for d in inserted if d.deal_index == 1})
        if cfg.raybot_enabled and new_addrs and raybot_configured():
            try:
                synced = await self._raybot.sync_wallets_low_mcap(
                    new_addrs, max_mcap_alert=cfg.max_mcap_alert
                )
                self._store.mark_raybot_synced(synced, True)
                self._append_log(
                    "raybot",
                    f"RayBot sync: {len(synced)}/{len(new_addrs)}",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("RayBot sync failed: %s", exc)
                self._append_log("raybot", f"RayBot sync ошибка: {exc}")

        # Alert immediately if watch itself produced deal #2+ @ low mcap
        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        if telegram_configured(chat):
            filters_map = self._store.get_alert_filters_map(
                sorted({d.wallet for d in inserted})
            )
            for deal in inserted:
                gate = alert_kwargs_for_wallet(cfg, filters_map.get(deal.wallet))
                if not should_alert_deal(
                    deal.deal_index,
                    deal.mcap_at_buy,
                    bought_usd=deal.bought_usd,
                    **gate,
                ):
                    continue
                if not self._store.mark_notified(deal.wallet, deal.token):
                    continue
                try:
                    hp = await send_followup_deal(
                        chat,
                        wallet=deal.wallet,
                        token=deal.token,
                        token_symbol=deal.token_symbol,
                        deal_index=deal.deal_index,
                        mcap_at_buy=deal.mcap_at_buy,
                        bought_usd=deal.bought_usd,
                        topic_id=topic_id,
                    )
                    self._append_log(
                        "telegram",
                        f"Алерт deal #{deal.deal_index} (из автопарса)"
                        + (" · HONEYPOT" if hp else ""),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up alert failed: %s", exc)
        return len(inserted)

    async def _cycle_body(self, cfg: FollowupConfig) -> None:
        wallets = self._store.list_watching()
        self._last_checked = len(wallets)
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._last_error = None
        if not wallets:
            self._last_message = "Нет кошельков в статусе watching"
            self._append_log("idle", self._last_message)
            pruned = await self._prune_stale_wallets(cfg)
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )
            return

        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        filters_map = self._store.get_alert_filters_map(wallets)

        self._last_message = f"Проверка {len(wallets)} кош…"
        self._append_log("scan", self._last_message, percent=5)

        from .chain import RpcClient

        rpc = RpcClient()
        sem = asyncio.Semaphore(max(1, int(cfg.scan_concurrency or 6)))
        done_count = 0
        skipped_alerts = 0
        progress_lock = asyncio.Lock()

        async def _alert_deals(
            wallet: str, new_deals: list[tuple[Any, str | None]]
        ) -> None:
            """Send TG as soon as this wallet's scan finishes (don't wait for all).

            Honeypot is already resolved during scan (or checked here before send).
            """
            nonlocal skipped_alerts
            if not new_deals or self._stop_requested:
                return
            gate = alert_kwargs_for_wallet(cfg, filters_map.get(wallet))
            for deal, hp_reason in new_deals:
                if self._stop_requested:
                    return
                async with progress_lock:
                    self._last_new_deals += 1
                if not should_alert_deal(
                    deal.deal_index,
                    deal.mcap_at_buy,
                    bought_usd=deal.bought_usd,
                    **gate,
                ):
                    async with progress_lock:
                        skipped_alerts += 1
                    continue
                if not tg_ok:
                    self._last_error = "Telegram не настроен"
                    continue
                if not self._store.mark_notified(deal.wallet, deal.token):
                    continue
                try:
                    # Order: honeypot already done during scan → then TG.
                    hp = await send_followup_deal(
                        chat,
                        wallet=deal.wallet,
                        token=deal.token,
                        token_symbol=deal.token_symbol,
                        deal_index=deal.deal_index,
                        mcap_at_buy=deal.mcap_at_buy,
                        bought_usd=deal.bought_usd,
                        topic_id=topic_id,
                        honeypot_reason=hp_reason,
                        check_honeypot=False,
                    )
                    async with progress_lock:
                        self._last_alerts_sent += 1
                    self._append_log(
                        "telegram",
                        f"Алерт deal #{deal.deal_index} · {wallet[:10]}…"
                        + (" · HONEYPOT" if hp else ""),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up alert failed: %s", exc)
                    self._last_error = str(exc)

        async def _scan_one(wallet: str) -> tuple[str, list]:
            nonlocal done_count
            async with sem:
                if self._stop_requested:
                    return wallet, []
                try:
                    deals = await self._scan_wallet(wallet, cfg, rpc=rpc)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up scan %s: %s", wallet[:10], exc)
                    deals = []
            async with progress_lock:
                done_count += 1
                if done_count % 5 == 0 or done_count == len(wallets):
                    pct = 5 + int(90 * done_count / max(len(wallets), 1))
                    self._last_message = (
                        f"Проверено {done_count}/{len(wallets)}, "
                        f"новых сделок {self._last_new_deals}, "
                        f"алертов {self._last_alerts_sent}"
                    )
                    self._append_log("scan", self._last_message, percent=pct)
            return wallet, deals

        tasks = [asyncio.create_task(_scan_one(w)) for w in wallets]
        try:
            for fut in asyncio.as_completed(tasks):
                if self._stop_requested:
                    break
                try:
                    item = await fut
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up gather: %s", exc)
                    continue
                wallet, new_deals = item
                await _alert_deals(wallet, new_deals)
        finally:
            if self._stop_requested:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        if self._stop_requested:
            self._last_message = "Остановлено"
            self._append_log("stop", self._last_message)
            return

        if skipped_alerts:
            self._append_log(
                "skip",
                f"Без алерта: {skipped_alerts} сделок (фильтры mcap/суммы)",
            )

        if not self._stop_requested:
            self._last_message = (
                f"Готово — {len(wallets)} кош., "
                f"{self._last_new_deals} сделок, "
                f"{self._last_alerts_sent} алертов"
            )
            self._append_log("done", self._last_message, percent=100)

        # Prune after alerts so honeypot/TG path is not blocked by ATH fetches.
        if not self._stop_requested:
            pruned = await self._prune_stale_wallets(cfg)
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )

    async def _prune_stale_wallets(self, cfg: FollowupConfig) -> int:
        """Remove wallets when discovery/#2/#3 tokens never hit ATH in time.

        - Deal #1: only if no follow-up deals exist yet (no deal_index >= 2).
        - Deals #2/#3: after created_at + window (watching or done).
        Passed ATH is persisted (ath_passed) so we do not re-hit Gecko every cycle.
        """
        rows = self._store.list_for_ath_prune()
        if not rows:
            return 0
        now = time.time()
        # (wallet, token, min_ath, reason)
        candidates: list[tuple[str, str, float, str]] = []
        passed_marks: list[tuple[str, str]] = []

        for row in rows:
            enabled, min_ath, hours = prune_settings_for_wallet(
                cfg, row.get("alert_filters")
            )
            if not enabled or min_ath <= 0:
                continue
            addr = row["address"]
            deals = list(row.get("deals") or [])
            window = hours * 3600.0
            max_idx = max((int(d.get("deal_index") or 0) for d in deals), default=0)
            has_followup = max_idx >= 2

            # Discovery prune: only before any follow-up deal exists.
            if not has_followup:
                discovered = float(row.get("discovered_at") or 0)
                if discovered > 0 and (now - discovered) >= window:
                    token = (row.get("first_token") or "").lower()
                    if not token:
                        for d in deals:
                            if int(d.get("deal_index") or 0) == 1 and d.get("token"):
                                token = str(d["token"]).lower()
                                break
                    if not token:
                        token = self._store.first_token_for_wallet(addr)
                    if token:
                        candidates.append((addr, token, min_ath, "deal#1"))

            # Follow-up #2/#3 — skip already ATH-passed deals.
            for d in deals:
                idx = int(d.get("deal_index") or 0)
                if idx not in (2, 3):
                    continue
                if d.get("ath_passed"):
                    continue
                created = float(d.get("created_at") or 0)
                if created <= 0 or (now - created) < window:
                    continue
                token = str(d.get("token") or "").lower()
                if not token:
                    continue
                candidates.append((addr, token, min_ath, f"deal#{idx}"))

        if not candidates:
            return 0

        self._last_message = f"Проверка ATH prune: {len(candidates)} проверок…"
        self._append_log("prune", self._last_message, percent=2)

        unique_tokens = sorted({t for _a, t, _m, _r in candidates})
        token_min: dict[str, float] = {}
        for _a, tok, min_ath, _r in candidates:
            token_min[tok] = max(token_min.get(tok, 0.0), min_ath)

        # Warm index peaks in one shot, then network only for misses / below threshold.
        index_peaks: dict[str, float] = {}
        try:
            from .token_index import token_index

            for tok, (peak, _sym) in token_index.mcap_peaks(unique_tokens).items():
                if peak > 0:
                    index_peaks[tok] = float(peak)
        except Exception:  # noqa: BLE001
            pass

        sem = asyncio.Semaphore(4)
        peaks: dict[str, PeakMcapEstimate | None] = {}

        async def _fetch(tok: str) -> None:
            needed = token_min.get(tok, 0.0)
            idx_peak = index_peaks.get(tok, 0.0)
            if needed > 0 and idx_peak >= needed:
                peaks[tok] = PeakMcapEstimate(peak=idx_peak, reliable=True)
                return
            async with sem:
                try:
                    peaks[tok] = await estimate_token_peak_mcap(tok, min_needed=needed)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("prune peak %s: %s", tok[:10], exc)
                    peaks[tok] = None

        await asyncio.gather(*[_fetch(t) for t in unique_tokens])

        # wallet → ordered list of failing reasons
        fails: dict[str, list[tuple[str, float, str, float]]] = {}
        for addr, token, min_ath, reason in candidates:
            est = peaks.get(token)
            if est is None:
                continue
            if est.peak >= min_ath:
                if reason.startswith("deal#") and reason != "deal#1":
                    passed_marks.append((addr, token))
                continue
            if not est.reliable:
                continue
            fails.setdefault(addr, []).append((token, min_ath, reason, est.peak))

        if passed_marks:
            self._store.mark_deals_ath_passed(passed_marks)

        removed = 0
        for addr, reasons in fails.items():
            _block, deal_count, status = self._store.get_wallet_scan_meta(addr)
            if status not in ("watching", "done"):
                continue
            for token, min_ath, reason, peak in reasons:
                if reason == "deal#1" and deal_count > 1:
                    continue
                if self._store.delete_wallet(addr):
                    removed += 1
                    self._append_log(
                        "prune",
                        f"Удалён {addr[:10]}… ({reason}) — {token[:10]}… "
                        f"peak=${peak:.0f} < ${min_ath:.0f}",
                    )
                break
        return removed

    async def _scan_wallet(
        self,
        wallet: str,
        cfg: FollowupConfig,
        *,
        rpc: Any | None = None,
    ) -> list:
        last_seen, deal_count, status = self._store.get_wallet_scan_meta(wallet)
        if status != "watching" or deal_count >= cfg.max_deals:
            return []

        known = self._store.known_tokens(wallet)
        # Newest-first pages: overwrite so the oldest post-watermark buy per token wins.
        candidates: dict[str, tuple[str, str, dict[str, Any], int]] = {}
        pages = max(1, int(cfg.scan_max_pages or 3))
        # Existing wallets after upgrade: peek tip once, set watermark, don't invent
        # deal #2+ from old transfer history.
        bootstrap = last_seen <= 0 and deal_count >= 1
        if bootstrap:
            pages = 1
        elif last_seen <= 0:
            pages = max(pages, 6)

        # Inbound-only: outbound sells used to fill newest pages and bury the buy,
        # then an aggressive watermark advance skipped that buy forever.
        items, tip_block, caught_up = await scan_address_token_transfers(
            wallet,
            max_pages=pages,
            after_block=0 if bootstrap else last_seen,
            direction="to",
        )
        max_block_seen = max(last_seen, tip_block)

        if not bootstrap:
            for item in items:
                block = _transfer_block(item)
                token, sym = _token_meta(item)
                if not token or token in QUOTE_TOKENS or token in known:
                    continue
                if not await _is_buy_like_transfer(
                    item,
                    wallet,
                    buys_only=cfg.buys_only,
                    track_transfers=cfg.track_transfers,
                ):
                    continue
                tx = str(item.get("transaction_hash") or item.get("tx_hash") or "")
                candidates[token] = (sym, tx, item, block)

        # Only advance watermark after a full catch-up (hit last_seen or no more pages).
        # Partial page windows must retry — otherwise sells bury earlier unique-token buys.
        if bootstrap:
            self._store.advance_last_seen_block(wallet, max(1, max_block_seen))
        elif caught_up and max_block_seen > last_seen:
            self._store.advance_last_seen_block(wallet, max_block_seen)

        if not candidates:
            return []

        ordered = sorted(candidates.items(), key=lambda kv: kv[1][3] or 0)
        remaining = cfg.max_deals - deal_count
        out: list[tuple[Any, str | None]] = []
        for token, (sym, tx, item, _block) in ordered:
            if self._stop_requested or remaining <= 0:
                break
            if token in self._store.known_tokens(wallet):
                continue

            async def _resolve_mcap(
                tok: str = token,
                txh: str = tx,
                it: dict[str, Any] = item,
            ) -> tuple[float | None, float | None]:
                mcap_v: float | None = None
                bought_v: float | None = None
                if txh:
                    try:
                        from .replay import estimate_entry_at_tx

                        entry = await estimate_entry_at_tx(tok, txh, rpc=rpc)
                        mcap_v = entry.mcap
                        bought_v = entry.bought_usd
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("mcap_at_tx failed: %s", exc)
                if mcap_v is None:
                    mcap_v, price = await estimate_token_quote(tok)
                    if bought_v is None:
                        bought_v = estimate_bought_usd(it, price)
                return mcap_v, bought_v

            async def _resolve_honeypot(tok: str = token) -> str | None:
                try:
                    from .security import honeypot_reason_for_token

                    return await asyncio.wait_for(
                        honeypot_reason_for_token(tok),
                        timeout=8.0,
                    )
                except Exception:  # noqa: BLE001
                    return None

            # Honeypot in parallel with mcap — still finished before TG send.
            (mcap, bought_usd), hp_reason = await asyncio.gather(
                _resolve_mcap(),
                _resolve_honeypot(),
            )
            deal = self._store.record_deal(
                wallet=wallet,
                token=token,
                token_symbol=sym,
                mcap_at_buy=mcap,
                bought_usd=bought_usd,
                tx_hash=tx,
                max_deals=cfg.max_deals,
            )
            if deal:
                out.append((deal, hp_reason))
                remaining -= 1
        return out


followup_runner = FollowupRunner()


def _transfer_block(item: dict[str, Any]) -> int:
    raw = item.get("block_number")
    if raw is None:
        raw = item.get("blockNumber")
    if raw is None:
        return 0
    try:
        if isinstance(raw, str) and raw.startswith(("0x", "0X")):
            return int(raw, 16)
        return int(raw)
    except (TypeError, ValueError):
        return 0
