"""Follow-up runner: watch early buyers for 2nd/3rd new-token buys @ low mcap."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

from .blockscout import scan_address_token_transfers
from .buy_gate import is_wallet_initiated_buy, method_is_non_buy
from .config import settings
from .constants import QUOTE_TOKENS
from .followup_schedule import (
    ScheduleConfig,
    WalletScheduleRow,
    schedule_config_from_followup,
    select_due_batch,
)
from .followup_logwatch import (
    backfill_deal_chain_times,
    fetch_inbound_transfers,
    tx_senders,
)
from .followup_store import FollowupStore, followup_store
from .gmgn_portfolio import GmgnBuy, UniqueBuysResult, fetch_unique_buys
from .models import (
    BuyerRow,
    FollowupConfig,
    FollowupDealRow,
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
    send_message,
    telegram_configured,
)
from .synth_guard import is_synthetic_evm_address
from .wallet_metrics import batch_wallet_balances

logger = logging.getLogger(__name__)

_LOG_MAX = 300
# Cap Blockscout-fallback concurrency (GMGN circuit open) — do not stampede BS.
_BS_FALLBACK_CONCURRENCY = 8
# Hot-path GMGN concurrency stays moderate even if config is higher.
_HOT_GMGN_CONCURRENCY = 4
# Hist lag above this uses burst catch-up spans (not the old min(..., 800) trap).
_HIST_BURST_LAG = 50_000


def hist_span_for_lag(lag: int, cfg: FollowupConfig) -> int:
    """Block window size for one hist getLogs attempt.

    Large lag → burst span (5k–15k class). Never clamp to a tiny hard cap:
    that made tip growth outpace catch-up forever. Shrink happens only after
    timeout/400 via the caller retry loop.
    """
    lag_i = max(0, int(lag))
    span_max = max(100, int(cfg.logwatch_max_span or 3_000))
    span_catchup = max(
        100, int(getattr(cfg, "logwatch_catchup_span", 3_000) or 3_000)
    )
    span_burst = max(
        span_catchup,
        int(getattr(cfg, "logwatch_burst_catchup_span", 10_000) or 10_000),
    )
    if lag_i > _HIST_BURST_LAG:
        return span_burst
    if lag_i > span_max * 2:
        return span_catchup
    return span_max


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


def deal_is_fresh_for_alert(
    *,
    bought_at: float | None,
    block_number: int | None,
    tip: int | None,
    now: float | None = None,
    max_buy_age_sec: float = 900.0,
    max_block_lag: int = 2_000,
) -> bool:
    """True when a deal is recent enough to warrant a Telegram alert.

    Stops live-gap / hist catch-up from blasting old buys as «#2/#3 сейчас».
    Fail-open when timestamps/tip are unknown (legacy / unit paths).
    """
    ts = time.time() if now is None else float(now)
    age_limit = max(60.0, float(max_buy_age_sec or 900.0))
    block_limit = max(100, int(max_block_lag or 2_000))
    if bought_at is not None and float(bought_at) > 0:
        age = ts - float(bought_at)
        if age > age_limit:
            return False
    if (
        tip is not None
        and block_number is not None
        and int(block_number) > 0
        and int(tip) > 0
    ):
        lag = int(tip) - int(block_number)
        if lag > block_limit:
            return False
    return True


@dataclass(frozen=True)
class GmgnRankVerdict:
    """GMGN post-seed rank for a (wallet, token) seen on logwatch."""

    uncertain: bool
    reason: str
    seed_token: str
    post_seed: tuple[GmgnBuy, ...]
    rank: int | None  # 2-based when token is in post_seed
    past_max: bool  # post-seed already fills follow-up window


def post_seed_unique_buys(
    buys: list[GmgnBuy],
    seed_token: str,
) -> tuple[GmgnBuy | None, list[GmgnBuy]]:
    """Split GMGN unique buys into (seed_buy, post-seed oldest→newest)."""
    seed_l = (seed_token or "").strip().lower()
    if not seed_l:
        return None, []
    seed_buy = next(
        (
            b
            for b in buys
            if b.token.lower() == seed_l and float(b.timestamp or 0) > 0
        ),
        None,
    )
    if seed_buy is None:
        return None, []
    seed_ts = float(seed_buy.timestamp)
    post = [
        b
        for b in buys
        if b.token
        and b.token.lower() != seed_l
        and b.token.lower() not in QUOTE_TOKENS
        and float(b.timestamp or 0) > seed_ts
    ]
    return seed_buy, post


def order_deals_for_alerts(
    new_deals: list[tuple[Any, str | None]],
) -> list[tuple[Any, str | None]]:
    """Sort newly recorded deals by ascending deal_index before Telegram.

    Cross-cycle late discoveries of a *lower* index after a higher one was
    already notified still alert (do not suppress real buys) — this only
    fixes within-batch / post-sync ordering going forward.
    """
    return sorted(
        new_deals,
        key=lambda pair: (
            int(getattr(pair[0], "deal_index", 0) or 0),
            str(getattr(pair[0], "token", "") or ""),
        ),
    )

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
        self._live_lock = asyncio.Lock()
        self._cycle_task: asyncio.Task[None] | None = None
        self._live_task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._force_run = False
        self._stop_requested = False
        self._running = False
        self._live_running = False
        self._next_run_ts: float | None = None
        self._last_run_ts: float | None = None
        self._last_run_duration_sec: float | None = None
        self._last_error: str | None = None
        self._last_message: str = ""
        self._last_checked = 0
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._last_due_count = 0
        self._last_hot_checked = 0
        self._last_warm_checked = 0
        self._last_zero_rechecked = 0
        self._last_skipped_zero_balance = 0
        self._last_hot_revisit_sec: float | None = None
        self._last_prune_ts: float = 0.0
        self._backfill_done = False
        self._logwatch_fail_streak = 0
        self._logwatch_degraded = False
        # Start the reconcile clock now so the first cycle after boot does not
        # stampede GMGN/Blockscout before logwatch has proven itself.
        self._last_reconcile_ts: float = time.time()
        self._last_pending_alerts_retried = 0
        self._last_pending_retry_ts: float = 0.0
        self._last_maintenance_ts: float = 0.0
        self._maintenance_wake = asyncio.Event()
        self._active_rpc: str = ""
        self._cursor_lag_blocks: int | None = None
        self._last_outbox_dispatched = 0
        self._live_timeout_streak = 0
        self._live_span_backoff = 0
        self._last_known_tip: int | None = None
        self._last_live_success_ts: float = 0.0
        # Fresh deals missing mcap — retried every live tick (not every 60s).
        self._mcap_micro_retry: list[tuple[str, str, float]] = []
        # Short-TTL GMGN unique-buy cache for logwatch rank (wallet → (ts, result)).
        self._gmgn_rank_cache: dict[str, tuple[float, UniqueBuysResult]] = {}
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
            running=self._running or self._live_running,
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
            last_due_count=self._last_due_count,
            last_hot_checked=self._last_hot_checked,
            last_warm_checked=self._last_warm_checked,
            last_zero_rechecked=self._last_zero_rechecked,
            last_skipped_zero_balance=self._last_skipped_zero_balance,
            last_hot_revisit_sec=self._last_hot_revisit_sec,
            logwatch_degraded=self._logwatch_degraded,
            logwatch_fail_streak=self._logwatch_fail_streak,
            last_reconcile_ts=(
                self._last_reconcile_ts if self._last_reconcile_ts > 0 else None
            ),
            last_pending_alerts_retried=self._last_pending_alerts_retried,
            active_rpc=self._active_rpc,
            cursor_lag_blocks=self._cursor_lag_blocks,
            outbox_pending=self._outbox_counts.get("pending", 0),
            outbox_failed=self._outbox_counts.get("failed", 0),
            last_outbox_dispatched=self._last_outbox_dispatched,
            log=list(self._log),
        )

    @property
    def _outbox_counts(self) -> dict[str, int]:
        try:
            return self._store.outbox_stats()
        except Exception:  # noqa: BLE001
            return {"pending": 0, "failed": 0, "sent": 0}

    def reset_counters(self) -> FollowupStatus:
        self._last_error = None
        self._last_message = ""
        self._last_checked = 0
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._last_due_count = 0
        self._last_hot_checked = 0
        self._last_warm_checked = 0
        self._last_zero_rechecked = 0
        self._last_skipped_zero_balance = 0
        self._last_hot_revisit_sec = None
        self._last_pending_alerts_retried = 0
        self._last_outbox_dispatched = 0
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
        """Run hist catch-up, live tip, and slow maintenance concurrently.

        Live tip must not wait for hist; hist must not wait for prune/reconcile.
        """
        await asyncio.gather(
            self._hist_loop(),
            self._live_loop(),
            self._maintenance_loop(),
        )

    async def _hist_loop(self) -> None:
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

            force_cycle = self._force_run
            self._force_run = False
            self._stop_requested = False
            started = time.time()
            self._running = True
            self._next_run_ts = None
            timeout = max(30, int(cfg.cycle_timeout_sec or 180))
            task = asyncio.create_task(
                self.run_cycle(cfg, force_all_due=force_cycle),
                name="followup-hist",
            )
            self._cycle_task = task
            try:
                await asyncio.wait_for(task, timeout=float(timeout))
            except asyncio.TimeoutError:
                logger.error("Follow-up hist cycle hung >%ss — watchdog abort", timeout)
                self._last_error = f"cycle watchdog timeout {timeout}s"
                self._last_message = self._last_error
                self._append_log("watchdog", self._last_message)
                self._stop_requested = True
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=8.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        self._append_log(
                            "watchdog",
                            "отменённый hist-цикл не завершился за 8s — сбрасываем lock+RPC sem",
                        )
                        self._lock = asyncio.Lock()
                        from .chain import reset_rpc_semaphores

                        n = reset_rpc_semaphores(scope="followup")
                        if n:
                            self._append_log(
                                "watchdog",
                                f"сброшено {n} followup RPC semaphore(s)",
                            )
                await self._ops_alert(
                    cfg,
                    kind="hang",
                    text=(
                        f"⚠️ Follow-up: hist-цикл завис >{timeout}s и был прерван. "
                        "Live tip продолжает работать отдельно; "
                        "hist-курсор не двигался на незавершённом проходе."
                    ),
                )
            except asyncio.CancelledError:
                task.cancel()
                raise
            except Exception as exc:  # noqa: BLE001
                from .chain import _redact_exc

                logger.exception("Follow-up hist cycle failed")
                safe = _redact_exc(exc)
                self._last_error = safe
                self._last_message = f"Ошибка: {safe}"
                self._append_log("error", self._last_message)
                await self._ops_alert(
                    cfg,
                    kind="cycle_error",
                    text=f"⚠️ Follow-up cycle error: {safe}",
                )
            finally:
                self._cycle_task = None
                self._running = False
                self._last_run_ts = time.time()
                self._last_run_duration_sec = self._last_run_ts - started
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=1.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                        pass

            cfg = self._store.load_config()
            if not cfg.enabled:
                continue
            period = max(0, int(cfg.interval_sec if cfg.interval_sec is not None else 0))
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

    async def _live_loop(self) -> None:
        """Tight tip poll: discover+enrich+alert without waiting on hist."""
        while True:
            cfg = self._store.load_config()
            if not cfg.enabled or not cfg.logwatch_enabled:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
                continue

            started = time.time()
            self._live_running = True
            timeout = max(10, int(getattr(cfg, "live_cycle_timeout_sec", 45) or 45))
            task = asyncio.create_task(
                self._live_tick(cfg), name="followup-live"
            )
            self._live_task = task
            try:
                await asyncio.wait_for(task, timeout=float(timeout))
            except asyncio.TimeoutError:
                logger.error("Follow-up live tip hung >%ss — abort tick", timeout)
                self._append_log("watchdog", f"live tip timeout {timeout}s")
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=3.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        self._live_lock = asyncio.Lock()
                        from .chain import reset_rpc_semaphores

                        reset_rpc_semaphores(scope="followup_live")
            except asyncio.CancelledError:
                task.cancel()
                raise
            except Exception as exc:  # noqa: BLE001
                from .chain import _redact_exc

                logger.warning("Follow-up live tip failed: %s", _redact_exc(exc))
                self._append_log("live", f"ошибка: {_redact_exc(exc)}")
            finally:
                self._live_task = None
                self._live_running = False
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=0.5)
                    except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                        pass

            period = float(getattr(cfg, "live_interval_sec", 1.5) or 1.5)
            elapsed = time.time() - started
            sleep_for = max(0.0, period - elapsed)
            if sleep_for > 0:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=sleep_for)
                    self._wake.clear()
                except asyncio.TimeoutError:
                    pass

    async def _maintenance_loop(self) -> None:
        """Slow side work: backfill, pending-retry, reconcile, prune, legacy.

        Must never share the hist 180s watchdog — that is what produced the
        «hist-цикл завис >180s» ops spam while live tip was healthy.
        """
        while True:
            cfg = self._store.load_config()
            if not cfg.enabled:
                self._maintenance_wake.clear()
                try:
                    await asyncio.wait_for(self._maintenance_wake.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass
                continue

            interval = float(
                getattr(cfg, "maintenance_interval_sec", 60) or 60
            )
            interval = max(15.0, interval)
            due_in = interval - (time.time() - self._last_maintenance_ts)
            if due_in > 0 and not self._maintenance_wake.is_set():
                try:
                    await asyncio.wait_for(
                        self._maintenance_wake.wait(), timeout=due_in
                    )
                except asyncio.TimeoutError:
                    pass
            self._maintenance_wake.clear()

            timeout = max(
                30, int(getattr(cfg, "maintenance_timeout_sec", 90) or 90)
            )
            task = asyncio.create_task(
                self._maintenance_pass(cfg), name="followup-maint"
            )
            try:
                await asyncio.wait_for(task, timeout=float(timeout))
            except asyncio.TimeoutError:
                logger.error(
                    "Follow-up maintenance hung >%ss — abort (hist/live OK)",
                    timeout,
                )
                self._append_log(
                    "watchdog",
                    f"maintenance timeout {timeout}s — hist/live не затронуты",
                )
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        from .chain import reset_rpc_semaphores

                        reset_rpc_semaphores(scope="followup")
            except asyncio.CancelledError:
                task.cancel()
                raise
            except Exception as exc:  # noqa: BLE001
                from .chain import _redact_exc

                logger.warning(
                    "Follow-up maintenance failed: %s", _redact_exc(exc)
                )
                self._append_log("maint", f"ошибка: {_redact_exc(exc)}")
            finally:
                self._last_maintenance_ts = time.time()
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=1.0)
                    except (
                        asyncio.TimeoutError,
                        asyncio.CancelledError,
                        Exception,
                    ):
                        pass

    async def _maintenance_pass(self, cfg: FollowupConfig) -> None:
        """Budgeted prune/retry/reconcile/backfill — off the hist critical path."""
        from .chain import RpcClient

        rpc = RpcClient(concurrency=3, sem_scope="followup")
        self._active_rpc = rpc.active_rpc_label()

        # One-shot / chunked chain-time backfill (was hanging hist on restart).
        await self._maybe_chain_backfill(cfg, rpc=rpc, budget_sec=15.0)

        catching_up = (self._cursor_lag_blocks or 0) > max(
            5_000, int(cfg.logwatch_max_span or 3_000) * 3
        )
        if catching_up:
            self._append_log(
                "catchup",
                f"догоняем cursor lag={self._cursor_lag_blocks} — "
                "retry/reconcile/prune пропущены",
            )
            return

        if self._logwatch_degraded or not cfg.logwatch_enabled:
            live_ok, live_behind = self._live_tip_healthy()
            if live_ok and cfg.logwatch_enabled:
                self._append_log(
                    "fallback",
                    f"legacy пропущен — live tip ok (behind={live_behind})",
                )
            elif (self._cursor_lag_blocks or 0) > 10_000 and cfg.logwatch_enabled:
                self._append_log(
                    "fallback",
                    f"legacy пропущен (cursor lag={self._cursor_lag_blocks})",
                )
            elif not self._stop_requested:
                try:
                    await asyncio.wait_for(
                        self._legacy_scan_pass(
                            cfg, rpc=rpc, force_all_due=False
                        ),
                        timeout=45.0,
                    )
                except asyncio.TimeoutError:
                    self._append_log(
                        "fallback", "legacy прерван по бюджету 45s"
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up legacy (maint): %s", exc)
        # Do NOT run legacy on soft/hard fail streak < threshold or while
        # live tip is healthy — that stampeded GMGN and ops spam.

        retry_interval = max(0, int(cfg.pending_retry_interval_sec or 0))
        retry_due = (
            time.time() - self._last_pending_retry_ts
        ) >= retry_interval
        if not self._stop_requested and retry_due:
            try:
                budget = float(cfg.pending_retry_time_budget_sec or 0)
                if budget > 0:
                    retried = await asyncio.wait_for(
                        self._retry_pending_alerts(cfg, rpc=rpc),
                        timeout=budget,
                    )
                else:
                    retried = await self._retry_pending_alerts(cfg, rpc=rpc)
                self._last_pending_alerts_retried = retried
                self._last_pending_retry_ts = time.time()
            except asyncio.TimeoutError:
                self._last_pending_retry_ts = time.time()
                self._append_log(
                    "retry",
                    "pending alerts прерваны по бюджету "
                    f"{float(cfg.pending_retry_time_budget_sec or 0):.0f}s",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up pending-alert retry: %s", exc)

        if (
            cfg.logwatch_enabled
            and not self._logwatch_degraded
            and not self._stop_requested
            and await self._should_safety_reconcile(cfg)
        ):
            try:
                await asyncio.wait_for(
                    self._safety_reconcile_pass(cfg, rpc=rpc),
                    timeout=40.0,
                )
            except asyncio.TimeoutError:
                self._append_log("reconcile", "прерван по бюджету 40s")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up safety reconcile: %s", exc)
                self._append_log("reconcile", f"ошибка: {exc}")

        if not self._stop_requested:
            try:
                await asyncio.wait_for(
                    self._repair_undercounted_wallets(cfg, rpc=rpc),
                    timeout=35.0,
                )
            except asyncio.TimeoutError:
                self._append_log("repair", "GMGN repair прерван по бюджету 35s")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up GMGN repair: %s", exc)

        if (
            cfg.logwatch_enabled
            and not self._logwatch_degraded
            and not self._stop_requested
        ):
            try:
                await asyncio.wait_for(
                    self._balance_only_pass(
                        cfg, rpc=rpc, force_all_due=False
                    ),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                self._append_log("balance", "прерван по бюджету 30s")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up balance pass: %s", exc)

        if not self._stop_requested:
            try:
                pruned = await asyncio.wait_for(
                    self._maybe_prune(cfg, force=False),
                    timeout=20.0,
                )
            except asyncio.TimeoutError:
                self._append_log("prune", "прерван по бюджету 20s")
                pruned = 0
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up prune: %s", exc)
                pruned = 0
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )

    async def _maybe_chain_backfill(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        budget_sec: float = 15.0,
    ) -> None:
        if self._store.get_meta("chain_times_backfill_done") == "1":
            self._backfill_done = True
            return
        if self._backfill_done:
            return
        try:
            n = await asyncio.wait_for(
                self._backfill_chain_times(cfg, rpc=rpc),
                timeout=max(5.0, float(budget_sec)),
            )
            if n:
                self._append_log(
                    "backfill",
                    f"Цепочка сделок: обновлено {n} записей, перенумерованы",
                )
            remaining = self._store.list_deals_needing_chain_backfill()
            if (
                not remaining
                and self._store.get_meta("renumber_v2_done") == "1"
            ):
                self._store.set_meta("chain_times_backfill_done", "1")
                self._backfill_done = True
        except asyncio.TimeoutError:
            self._append_log(
                "backfill",
                f"прерван по бюджету {budget_sec:.0f}s — продолжим в maint",
            )
            self._maintenance_wake.set()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Follow-up chain backfill: %s", exc)

    async def _live_tick(self, cfg: FollowupConfig) -> None:
        from .chain import RpcClient

        lock = self._live_lock
        try:
            await asyncio.wait_for(lock.acquire(), timeout=2.0)
        except asyncio.TimeoutError:
            self._append_log("live", "предыдущий live-tick ещё держит lock — skip")
            return
        try:
            rpc = RpcClient(concurrency=3, sem_scope="followup_live")
            self._active_rpc = rpc.active_rpc_label()
            # Drain outbox on the fast path so TG lag does not wait for hist.
            try:
                n = await self._dispatch_outbox(cfg, limit=5)
                if n:
                    self._last_outbox_dispatched = n
            except Exception as exc:  # noqa: BLE001
                logger.debug("live outbox: %s", exc)
            await self._live_tip_pass(cfg, rpc=rpc)
            await self._micro_retry_pending_mcap(cfg, rpc=rpc)
        finally:
            lock.release()

    async def _ops_alert(
        self,
        cfg: FollowupConfig,
        *,
        kind: str,
        text: str,
    ) -> None:
        """Rate-limited Telegram ops notice (degradation / hang / lag)."""
        cooldown = max(60, int(cfg.ops_alert_cooldown_sec or 600))
        meta_key = f"ops_alert_{kind}_ts"
        try:
            raw = self._store.get_meta(meta_key)
            last = float(raw) if raw else 0.0
        except Exception:  # noqa: BLE001
            last = 0.0
        now = time.time()
        if last > 0 and (now - last) < cooldown:
            return
        chat = resolve_chat_id(cfg.telegram_chat_id)
        if not telegram_configured(chat):
            return
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        try:
            await send_message(chat, text, topic_id=topic_id)
            self._store.set_meta(meta_key, str(now))
            self._append_log("ops", text[:160])
        except Exception as exc:  # noqa: BLE001
            logger.warning("ops alert failed: %s", exc)

    async def run_cycle(
        self,
        cfg: FollowupConfig | None = None,
        *,
        force_all_due: bool = False,
    ) -> None:
        # Capture the lock object we acquire. Watchdog may replace self._lock
        # to unblock the next tick; the zombie must release *this* instance,
        # not whatever self._lock points at later.
        lock = self._lock
        try:
            await asyncio.wait_for(lock.acquire(), timeout=5.0)
        except asyncio.TimeoutError:
            self._append_log(
                "lock",
                "предыдущий цикл ещё держит lock — пропуск этого тика",
            )
            return
        try:
            await self._cycle_body(
                cfg or self._store.load_config(),
                force_all_due=force_all_due,
            )
        finally:
            lock.release()

    async def ingest_from_watch(
        self,
        buyers: list[BuyerRow],
        *,
        cfg: FollowupConfig | None = None,
    ) -> int:
        """Ingest early buyers from autoparse into follow-up (idempotent).

        Pre-backfill semantics: watch seed is deal #1 as discovered. Do NOT
        invent silent prior buys (that inflated deal_index / deal_count and
        shipped TOKEN alerts). Multi-trade filtering belongs in watch qualify,
        not follow-up renumber-from-history.
        """
        cfg = cfg or self._store.load_config()
        if not cfg.ingest_from_watch:
            return 0
        if not buyers:
            return 0

        # Refuse pytest/synthetic fixtures even if a test forgets chat isolation.
        clean: list[BuyerRow] = []
        for b in buyers:
            w = (b.wallet or "").lower()
            t = (b.token or "").lower()
            if is_synthetic_evm_address(w) or is_synthetic_evm_address(t):
                self._append_log(
                    "ingest",
                    f"skip {w[:10]}… synthetic test address (refuse)",
                )
                continue
            clean.append(b)
        if not clean:
            return 0

        inserted = self._store.ingest_buyers(
            clean,
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
            for deal in sorted(
                inserted,
                key=lambda d: (int(d.deal_index or 0), str(d.token or "")),
            ):
                gate = alert_kwargs_for_wallet(cfg, filters_map.get(deal.wallet))
                if not should_alert_deal(
                    deal.deal_index,
                    deal.mcap_at_buy,
                    bought_usd=deal.bought_usd,
                    **gate,
                ):
                    continue
                ok = await self._deliver_deal_alert(
                    chat,
                    deal=deal,
                    topic_id=topic_id,
                    check_honeypot=True,
                )
                if ok:
                    self._append_log(
                        "telegram",
                        f"Алерт deal #{deal.deal_index} (из автопарса)",
                    )
        return len(inserted)

    async def _cycle_body(
        self,
        cfg: FollowupConfig,
        *,
        force_all_due: bool = False,
    ) -> None:
        from .chain import RpcClient

        # Isolated RPC pool: token_index/watch must not starve logwatch by
        # holding the process-wide semaphore after a cancelled getLogs.
        rpc = RpcClient(concurrency=4, sem_scope="followup")
        self._active_rpc = rpc.active_rpc_label()
        self._append_log("cycle", f"start rpc={self._active_rpc}")

        # Hist critical path: outbox + hist logwatch only.
        # Backfill / pending / reconcile / prune / legacy live in
        # ``_maintenance_loop`` so they cannot trip the 180s hist watchdog.
        try:
            dispatched = await self._dispatch_outbox(cfg)
            self._last_outbox_dispatched = dispatched
            if dispatched:
                self._append_log("outbox", f"доставлено {dispatched} из очереди")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Follow-up outbox dispatch: %s", exc)

        logwatch_ok = False
        if cfg.logwatch_enabled and not self._stop_requested:
            try:
                logwatch_ok = await self._logwatch_pass(cfg, rpc=rpc)
            except Exception as exc:  # noqa: BLE001
                from .chain import _is_retryable, _redact_exc

                safe = _redact_exc(exc)
                # Same class of blips as getLogs soft-err: do not start a
                # DEGRADED streak (and never false-flag live tip as dead).
                if _is_retryable(exc):
                    logger.warning("Follow-up logwatch soft-err: %s", safe)
                    self._append_log(
                        "logwatch", f"soft-err: {safe} — без streak"
                    )
                    logwatch_ok = True
                else:
                    logger.warning("Follow-up logwatch failed: %s", safe)
                    self._append_log("logwatch", f"ошибка: {safe}")
                    self._last_error = f"logwatch: {safe}"
                    logwatch_ok = False
        elif not cfg.logwatch_enabled:
            # Pure legacy mode — hist has nothing to do; maintenance owns scans.
            self._append_log("cycle", "logwatch выключен → maintenance/legacy")
            self._maintenance_wake.set()
            if force_all_due:
                self._maintenance_wake.set()
            return

        if logwatch_ok:
            was_degraded = self._logwatch_degraded
            had_streak = self._logwatch_fail_streak
            if was_degraded:
                self._append_log(
                    "logwatch",
                    "восстановлен после "
                    f"{had_streak} сбоев",
                )
                await self._ops_alert(
                    cfg,
                    kind="recovered",
                    text=(
                        "✅ Follow-up: logwatch восстановлен "
                        f"(было {had_streak} сбоев подряд). "
                        f"RPC={self._active_rpc}"
                    ),
                )
            elif had_streak:
                self._append_log(
                    "logwatch",
                    f"transient ok после {had_streak} сбоя(ев) — без DEGRADED",
                )
            self._logwatch_fail_streak = 0
            self._logwatch_degraded = False
            await self._maybe_alert_cursor_lag(cfg, rpc=rpc)
        else:
            self._logwatch_fail_streak += 1
            threshold = max(1, int(cfg.logwatch_fail_threshold or 3))
            # Ops TG never fires on a single blip — even if config threshold is 1.
            ops_threshold = max(3, threshold)
            became = (
                not self._logwatch_degraded
                and self._logwatch_fail_streak >= threshold
            )
            self._logwatch_degraded = self._logwatch_fail_streak >= threshold
            live_ok, live_behind = self._live_tip_healthy()
            if self._logwatch_degraded:
                self._append_log(
                    "fallback",
                    f"logwatch DEGRADED (streak={self._logwatch_fail_streak}/"
                    f"{threshold})"
                    + (
                        f"; live tip ok (behind={live_behind}) — без TG/GMGN"
                        if live_ok
                        else " → legacy через maintenance; live tip тоже отстаёт"
                    ),
                )
            else:
                self._append_log(
                    "fallback",
                    f"logwatch hard-fail streak={self._logwatch_fail_streak}/"
                    f"{threshold} — без GMGN, курсор не двигаем"
                    + (
                        f"; live tip ok (behind={live_behind})"
                        if live_ok
                        else ""
                    ),
                )
            # TG only when tip discovery is actually unhealthy (store cursor),
            # never when RPC probe for tip failed (that used to false-DEGRADE).
            if (
                became
                and not live_ok
                and self._logwatch_fail_streak >= ops_threshold
            ):
                await self._ops_alert(
                    cfg,
                    kind="degraded",
                    text=(
                        "⚠️ Follow-up DEGRADED: logwatch упал "
                        f"(streak={self._logwatch_fail_streak}). "
                        f"Live tip отстаёт на {live_behind} блоков — "
                        "fallback GMGN/Blockscout. "
                        f"RPC={self._active_rpc}"
                    ),
                )
            # Legacy only when degraded AND live is not covering tip.
            if self._logwatch_degraded and not live_ok:
                self._maintenance_wake.set()

        # After force_all_due (run_now), also nudge maintenance for prune/retry.
        if force_all_due:
            self._maintenance_wake.set()

    async def _maybe_alert_cursor_lag(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> None:
        """Ops alert when historical logwatch cursor falls far behind tip.

        Fresh deals are covered by the live tip cursor; this alert is about
        the hist backlog only and must not claim that new alerts are delayed.
        Prefer ``_live_tip_healthy`` (recent live success / last known tip)
        over a raw store live_behind that can look huge while live is fine.
        """
        try:
            tip = self._last_known_tip
            if tip is None:
                tip = int(
                    await asyncio.wait_for(rpc.block_number(), timeout=12.0)
                )
                self._last_known_tip = tip
            tip = int(tip)
            cursor = self._store.get_logwatch_cursor()
            if cursor is None:
                self._cursor_lag_blocks = None
                return
            lag = max(0, tip - int(cursor))
            self._cursor_lag_blocks = lag
            threshold = max(100, int(cfg.cursor_lag_alert_blocks or 6_000))
            if lag < threshold:
                return
            live_ok, live_behind = self._live_tip_healthy(tip=tip)
            live = self._store.get_logwatch_live_cursor()
            store_behind = (tip - int(live)) if live is not None else None
            # Prefer store watermark when known — never claim «Live tip в порядке»
            # while live_behind is huge (e.g. 128k) just because a tick ran.
            effective_behind = store_behind
            if effective_behind is None:
                effective_behind = live_behind
            near_tip = (
                effective_behind is not None and effective_behind <= 2_000
            )
            if live_ok and near_tip:
                live_note = f" (отставание {effective_behind})"
                text = (
                    f"ℹ️ Follow-up: hist-курсор отстаёт на {lag} блоков "
                    f"(cursor={cursor}, tip={tip}, порог={threshold}). "
                    f"Live tip в порядке{live_note}; "
                    "свежие алерты не ждут догона — догоняем только историю."
                )
            else:
                behind_txt = (
                    f", live_behind={effective_behind}"
                    if effective_behind is not None
                    else ", live=нет"
                )
                text = (
                    f"⚠️ Follow-up: logwatch отстаёт на {lag} блоков "
                    f"(cursor={cursor}, tip={tip}, порог={threshold}"
                    f"{behind_txt}). Live tip тоже нездоров — догоняем чанками."
                )
            await self._ops_alert(cfg, kind="cursor_lag", text=text)
        except TimeoutError:
            logger.debug("cursor lag check: block_number timeout")
        except Exception as exc:  # noqa: BLE001
            logger.debug("cursor lag check: %s", exc)

    async def _should_safety_reconcile(self, cfg: FollowupConfig) -> bool:
        interval = max(30, int(cfg.safety_reconcile_sec or 120))
        return (time.time() - self._last_reconcile_ts) >= interval

    async def _safety_reconcile_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> None:
        """Scan a small hot batch via legacy path without touching logwatch cursor."""
        now = time.time()
        sched_cfg = schedule_config_from_followup(cfg)
        # Temporarily shrink batch so reconcile stays cheap.
        tight = ScheduleConfig(
            hot_revisit_sec=sched_cfg.hot_revisit_sec,
            warm_revisit_sec=sched_cfg.warm_revisit_sec,
            zero_balance_recheck_sec=sched_cfg.zero_balance_recheck_sec,
            balance_fresh_sec=sched_cfg.balance_fresh_sec,
            hot_activity_sec=sched_cfg.hot_activity_sec,
            max_due_per_cycle=max(1, int(cfg.safety_reconcile_max or 12)),
            warm_fair_share=0.05,
        )
        rows_raw = self._store.list_watching_schedule_rows()
        rows = [
            WalletScheduleRow(
                address=r["address"],
                status=r["status"],
                deal_count=int(r["deal_count"]),
                discovered_at=float(r["discovered_at"]),
                last_activity_at=float(r["last_activity_at"]),
                last_scanned_at=r["last_scanned_at"],
                last_balance_check_at=r["last_balance_check_at"],
                wallet_balance_eth=r["wallet_balance_eth"],
            )
            for r in rows_raw
        ]
        due = select_due_batch(
            rows,
            now=now,
            max_deals=int(cfg.max_deals or 5),
            cfg=tight,
            force_all_due=False,
        )
        hot = [d for d in due if d.tier == "hot"][: int(cfg.safety_reconcile_max or 12)]
        if not hot:
            self._last_reconcile_ts = now
            return
        self._append_log(
            "reconcile",
            f"safety scan {len(hot)} hot кош. (logwatch ок, страховка)",
            percent=20,
        )
        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        filters_map = self._store.get_alert_filters_map([d.address for d in hot])
        found = 0
        alerts = 0
        for item in hot:
            if self._stop_requested:
                break
            try:
                deals, _src = await self._scan_wallet(
                    item.address, cfg, rpc=rpc, skip_gmgn=False
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("reconcile scan %s: %s", item.address[:10], exc)
                continue
            self._store.mark_scanned([item.address])
            for deal, hp_reason in order_deals_for_alerts(deals):
                found += 1
                gate = alert_kwargs_for_wallet(cfg, filters_map.get(deal.wallet))
                if not should_alert_deal(
                    deal.deal_index,
                    deal.mcap_at_buy,
                    bought_usd=deal.bought_usd,
                    **gate,
                ):
                    continue
                if not tg_ok:
                    continue
                ok = await self._deliver_deal_alert(
                    chat,
                    deal=deal,
                    topic_id=topic_id,
                    honeypot_reason=hp_reason,
                )
                if ok:
                    alerts += 1
        self._last_reconcile_ts = time.time()
        self._last_new_deals += found
        self._last_alerts_sent += alerts
        self._append_log(
            "reconcile",
            f"safety: {found} сделок, {alerts} алертов",
            percent=40,
        )

    @staticmethod
    def _deal_dedup_key(wallet: str, token: str) -> str:
        return f"deal:{(wallet or '').lower()}:{str(token or '').lower()}"

    async def _deliver_deal_alert(
        self,
        chat: str,
        *,
        deal: Any,
        topic_id: int | None,
        honeypot_reason: str | None = None,
        check_honeypot: bool = False,
    ) -> bool:
        """Claim + durably enqueue the alert, then try immediate delivery.

        Transactional outbox: the claim (``notified=1``) and the outbox row are
        committed together, so a crash between enqueue and send cannot drop the
        alert — ``_dispatch_outbox`` redelivers ``pending`` rows on a later
        cycle. Returns ``True`` when this call newly enqueued the alert.
        """
        cfg = self._store.load_config()
        deal = await self._ensure_deal_token_labels(deal)
        reason = honeypot_reason
        if check_honeypot and reason is None and deal.token:
            try:
                from .security import honeypot_reason_for_token

                reason = await asyncio.wait_for(
                    honeypot_reason_for_token(deal.token), timeout=8.0
                )
            except Exception:  # noqa: BLE001
                reason = None
        if reason and bool(getattr(cfg, "alert_skip_honeypot", True)):
            # Suppress TG; mark notified so pending-retry does not spam.
            self._store.mark_notified(deal.wallet, deal.token)
            self._append_log(
                "telegram",
                f"skip honeypot deal #{deal.deal_index} "
                f"{deal.token_symbol or deal.token[:10]}… ({reason})",
            )
            return False

        if not deal_is_fresh_for_alert(
            bought_at=getattr(deal, "bought_at", None),
            block_number=getattr(deal, "block_number", None),
            tip=self._last_known_tip,
            max_buy_age_sec=float(
                getattr(cfg, "alert_max_buy_age_sec", 900) or 900
            ),
            max_block_lag=int(
                getattr(cfg, "alert_max_block_lag", 2_000) or 2_000
            ),
        ):
            # Old hist/gap fills must not Telegram as «сейчас #2/#3».
            self._store.mark_notified(deal.wallet, deal.token)
            self._append_log(
                "telegram",
                f"skip stale deal #{deal.deal_index} "
                f"block={getattr(deal, 'block_number', None)} "
                f"tip={self._last_known_tip}",
            )
            return False

        dedup = self._deal_dedup_key(deal.wallet, deal.token)
        payload = json.dumps(
            {
                "v": 1,
                "kind": "deal",
                "chat": chat,
                "wallet": deal.wallet,
                "token": deal.token,
                "token_symbol": deal.token_symbol,
                "token_name": getattr(deal, "token_name", ""),
                "deal_index": deal.deal_index,
                "mcap_at_buy": deal.mcap_at_buy,
                "bought_usd": deal.bought_usd,
                "topic_id": topic_id,
                "honeypot_reason": reason,
                # Already resolved above when skip is off.
                "check_honeypot": False,
            }
        )
        if not self._store.claim_and_enqueue_deal(
            deal.wallet, deal.token, dedup_key=dedup, payload=payload
        ):
            return False
        # Best-effort low-latency send; failures stay pending for the dispatcher.
        await self._dispatch_outbox(cfg, limit=1, only_key=dedup)
        return True

    async def _send_outbox_payload(self, payload: dict) -> None:
        """Deliver one decoded outbox payload to its sink (raises on failure)."""
        kind = payload.get("kind", "deal")
        if kind == "deal":
            sym = str(payload.get("token_symbol") or "").strip()
            name = str(payload.get("token_name") or "").strip()
            if (not sym or not name) and payload.get("token"):
                # Late fill so failed enrich never ships the literal «TOKEN».
                @dataclass
                class _LabelDeal:
                    wallet: str
                    token: str
                    token_symbol: str = ""
                    token_name: str = ""

                tmp = _LabelDeal(
                    wallet=str(payload["wallet"]),
                    token=str(payload["token"]),
                    token_symbol=sym,
                    token_name=name,
                )
                filled = await self._ensure_deal_token_labels(tmp)
                payload = {
                    **payload,
                    "token_symbol": getattr(filled, "token_symbol", sym) or sym,
                    "token_name": getattr(filled, "token_name", name) or name,
                }
                sym = str(payload.get("token_symbol") or "").strip()
            if not sym:
                # Still unlabeled — do not spam TG with placeholder TOKEN.
                raise RuntimeError(
                    f"follow-up alert missing token_symbol for "
                    f"{str(payload.get('token') or '')[:12]}"
                )
            if bool(getattr(self._store.load_config(), "alert_skip_honeypot", True)):
                reason = payload.get("honeypot_reason")
                if not reason and payload.get("check_honeypot"):
                    try:
                        from .security import honeypot_reason_for_token

                        reason = await asyncio.wait_for(
                            honeypot_reason_for_token(payload["token"]),
                            timeout=8.0,
                        )
                    except Exception:  # noqa: BLE001
                        reason = None
                if reason:
                    logger.info(
                        "outbox skip honeypot %s (%s)",
                        str(payload.get("token", ""))[:12],
                        reason,
                    )
                    return
            await send_followup_deal(
                payload["chat"],
                wallet=payload["wallet"],
                token=payload["token"],
                token_symbol=payload.get("token_symbol", ""),
                token_name=payload.get("token_name", ""),
                deal_index=payload["deal_index"],
                mcap_at_buy=payload.get("mcap_at_buy"),
                bought_usd=payload.get("bought_usd"),
                topic_id=payload.get("topic_id"),
                honeypot_reason=payload.get("honeypot_reason"),
                check_honeypot=bool(payload.get("check_honeypot", False)),
            )
            return
        if kind == "ops":
            await send_message(
                payload["chat"], payload["text"], topic_id=payload.get("topic_id")
            )
            return
        raise ValueError(f"unknown outbox kind: {kind}")

    async def _dispatch_outbox(
        self,
        cfg: FollowupConfig,
        *,
        limit: int | None = None,
        only_key: str | None = None,
        now: float | None = None,
    ) -> int:
        """Drain due outbox rows, delivering each with capped exponential backoff.

        Crash-safe redelivery: rows stay ``pending`` (with a growing
        ``next_attempt_at``) until delivered or ``outbox_max_attempts`` is hit,
        at which point they become ``failed`` and surface in status/ops.
        """
        if not getattr(cfg, "outbox_enabled", True):
            return 0
        batch = int(limit if limit is not None else cfg.outbox_dispatch_batch)
        rows = self._store.list_due_outbox(now=now, limit=batch)
        if only_key is not None:
            rows = [r for r in rows if r.get("dedup_key") == only_key]
        if not rows:
            return 0
        max_attempts = int(cfg.outbox_max_attempts or 10)
        sent = 0
        for row in rows:
            if self._stop_requested and only_key is None:
                break
            oid = int(row["id"])
            try:
                payload = json.loads(row["payload"])
            except Exception as exc:  # noqa: BLE001
                # Poison payload: fail fast so it never blocks the queue.
                self._store.mark_outbox_failed(
                    oid,
                    error=f"bad payload: {exc}",
                    next_attempt_at=time.time() + 3600,
                    max_attempts=1,
                )
                continue
            try:
                await self._send_outbox_payload(payload)
                self._store.mark_outbox_sent(oid)
                sent += 1
            except Exception as exc:  # noqa: BLE001
                attempts = int(row.get("attempts", 0)) + 1
                backoff = min(30.0 * (2 ** attempts), 3600.0)
                safe = str(exc)[:300]
                self._store.mark_outbox_failed(
                    oid,
                    error=safe,
                    next_attempt_at=time.time() + backoff,
                    max_attempts=max_attempts,
                )
                logger.warning(
                    "outbox delivery failed (attempt %s, retry in %.0fs): %s",
                    attempts,
                    backoff,
                    safe,
                )
                self._last_error = safe
                if attempts >= max_attempts:
                    await self._ops_alert(
                        cfg,
                        kind="outbox_failed",
                        text=(
                            "⚠️ Follow-up: алерт не доставлен после "
                            f"{attempts} попыток и помечен FAILED "
                            f"({row.get('dedup_key')})."
                        ),
                    )
        return sent

    async def _retry_pending_alerts(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> int:
        """Re-attempt alerts for deals that passed the index gate but never notified."""
        chat = resolve_chat_id(cfg.telegram_chat_id)
        if not telegram_configured(chat):
            return 0
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        pending = self._store.list_pending_alert_deals(
            alert_on_deals=list(cfg.alert_on_deals or [2, 3, 4, 5]),
            limit=10,
            max_age_sec=48 * 3600,
            max_mcap_alert=float(cfg.max_mcap_alert or 0) or None,
        )
        if not pending:
            return 0
        self._append_log(
            "retry",
            f"pending alerts: {len(pending)} кандидатов",
            percent=3,
        )
        filters_map = self._store.get_alert_filters_map(
            sorted({d.wallet for d in pending})
        )
        sent = 0
        deadline = time.time() + float(cfg.pending_retry_time_budget_sec or 0)
        for deal in pending:
            if self._stop_requested:
                break
            # Soft deadline inside the loop so we stop between deals instead of
            # waiting for wait_for to cancel mid-RPC (and leave no progress).
            if (
                float(cfg.pending_retry_time_budget_sec or 0) > 0
                and time.time() >= deadline
            ):
                break
            mcap = deal.mcap_at_buy
            bought = deal.bought_usd
            if mcap is None:
                # Fast path only — full tx replay belongs to discovery, not retry.
                try:
                    mcap, _ = await asyncio.wait_for(
                        estimate_token_quote(deal.token),
                        timeout=6.0,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.debug("pending mcap refresh: %s", exc)
                if mcap is None:
                    # DexScreener still cold → on-chain reserves.
                    try:
                        from .replay import estimate_onchain_spot_mcap

                        mcap = await asyncio.wait_for(
                            estimate_onchain_spot_mcap(deal.token, rpc=rpc),
                            timeout=6.0,
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("pending onchain mcap: %s", exc)
                if mcap is not None:
                    self._store.update_deal_quote(
                        deal.wallet,
                        deal.token,
                        mcap_at_buy=mcap,
                        bought_usd=bought,
                    )
                    deal = deal.model_copy(update={"mcap_at_buy": mcap})
            deal = await self._ensure_deal_token_labels(deal, rpc=rpc)
            gate = alert_kwargs_for_wallet(cfg, filters_map.get(deal.wallet))
            if not should_alert_deal(
                deal.deal_index,
                deal.mcap_at_buy,
                bought_usd=deal.bought_usd,
                **gate,
            ):
                continue
            # Honeypot already checked on first attempt path; keep retry fast.
            ok = await self._deliver_deal_alert(
                chat,
                deal=deal,
                topic_id=topic_id,
                check_honeypot=False,
            )
            if ok:
                sent += 1
                self._append_log(
                    "telegram",
                    f"Ретрай deal #{deal.deal_index} · {deal.wallet[:10]}…",
                )
        if sent:
            self._last_alerts_sent += sent
        return sent

    async def _backfill_chain_times(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> int:
        rows = self._store.list_deals_needing_chain_backfill()
        updates = await backfill_deal_chain_times(rpc, rows) if rows else []
        n = 0
        if updates:
            n = self._store.apply_chain_backfill(
                updates, max_deals=int(cfg.max_deals or 5)
            )
        # After bought_at migration / backfill, force one renumber pass so
        # legacy «known-block sorts first» indices are corrected.
        if self._store.get_meta("renumber_v2_done") != "1":
            max_deals = int(cfg.max_deals or 5)
            for row in self._store.list_wallets(limit=1000, include_deals=False):
                self._store.renumber_wallet(row.address, max_deals=max_deals)
            # Also renumber done wallets beyond the list page.
            for row in self._store.list_wallets(
                status="done", limit=1000, include_deals=False
            ):
                self._store.renumber_wallet(row.address, max_deals=max_deals)
            self._store.set_meta("renumber_v2_done", "1")
        return n

    async def _balance_only_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        force_all_due: bool = False,
    ) -> None:
        """Refresh native balances for due wallets (no deal scan)."""
        now = time.time()
        sched_cfg = schedule_config_from_followup(cfg)
        schedule_rows_raw = self._store.list_watching_schedule_rows()
        schedule_rows = [
            WalletScheduleRow(
                address=r["address"],
                status=r["status"],
                deal_count=int(r["deal_count"]),
                discovered_at=float(r["discovered_at"]),
                last_activity_at=float(r["last_activity_at"]),
                last_scanned_at=r["last_scanned_at"],
                last_balance_check_at=r["last_balance_check_at"],
                wallet_balance_eth=r["wallet_balance_eth"],
            )
            for r in schedule_rows_raw
        ]
        due = select_due_batch(
            schedule_rows,
            now=now,
            max_deals=int(cfg.max_deals or 5),
            cfg=sched_cfg,
            force_all_due=force_all_due,
        )
        self._last_due_count = len(due)
        if not due:
            return
        refresh_addrs = [
            d.address
            for d in due
            if d.needs_balance_refresh or d.tier == "zero"
        ]
        fetched: dict[str, float | None] = {}
        if refresh_addrs:
            try:
                fetched = await batch_wallet_balances(rpc, refresh_addrs)
                self._store.update_wallet_balances(
                    fetched, checked_at=time.time()
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up balance refresh failed: %s", exc)
                fetched = {}
        self._store.mark_scanned([d.address for d in due])
        self._last_skipped_zero_balance = sum(
            1
            for bal in fetched.values()
            if bal is not None and float(bal) == 0.0
        )

    async def _enrich_transfer(
        self,
        tr: Any,
        *,
        cfg: FollowupConfig,
        rpc: Any,
        budget_sec: float | None = None,
    ) -> tuple[float | None, float | None, str | None, str, str]:
        """Resolve mcap, spend, safety flag, symbol and name for one transfer.

        Mcap paths (entry / quote / on-chain), ERC-20 meta, and honeypot run
        in parallel under a single wall budget so live tip stays ≤few seconds.
        """
        per = float(max(2, int(cfg.logwatch_enrich_timeout_sec or 8)))
        budget = float(
            budget_sec
            if budget_sec is not None
            else getattr(cfg, "live_enrich_budget_sec", 3.0) or 3.0
        )
        budget = max(1.0, min(budget, per * 3))
        step = max(1.0, min(per, budget))

        async def _entry() -> tuple[float | None, float | None, str, str]:
            if not tr.tx_hash:
                return None, None, "", ""
            try:
                from .replay import estimate_entry_at_tx

                entry = await asyncio.wait_for(
                    estimate_entry_at_tx(tr.token, tr.tx_hash, rpc=rpc),
                    timeout=step,
                )
                return (
                    entry.mcap,
                    entry.bought_usd,
                    entry.token_symbol or "",
                    entry.token_name or "",
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("logwatch mcap_at_tx %s: %s", tr.token[:10], exc)
                return None, None, "", ""

        async def _quote() -> float | None:
            try:
                mcap, _price = await asyncio.wait_for(
                    estimate_token_quote(tr.token), timeout=step
                )
                return mcap
            except Exception as exc:  # noqa: BLE001
                logger.debug("logwatch spot mcap %s: %s", tr.token[:10], exc)
                return None

        async def _onchain() -> float | None:
            try:
                from .replay import estimate_onchain_spot_mcap

                return await asyncio.wait_for(
                    estimate_onchain_spot_mcap(tr.token, rpc=rpc),
                    timeout=step,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("logwatch onchain mcap %s: %s", tr.token[:10], exc)
                return None

        async def _meta() -> tuple[str, str]:
            try:
                meta = await asyncio.wait_for(
                    rpc.token_meta(tr.token), timeout=step
                )
                return (
                    str(meta.get("symbol") or "").strip(),
                    str(meta.get("name") or "").strip(),
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("logwatch token_meta %s: %s", tr.token[:10], exc)
                return "", ""

        async def _honeypot() -> str | None:
            try:
                from .security import honeypot_reason_for_token

                return await asyncio.wait_for(
                    honeypot_reason_for_token(tr.token), timeout=step
                )
            except Exception:  # noqa: BLE001
                return None

        async def _bundle() -> tuple[
            float | None, float | None, str | None, str, str
        ]:
            entry_r, quote_r, onchain_r, meta_r, hp_r = await asyncio.gather(
                _entry(), _quote(), _onchain(), _meta(), _honeypot()
            )
            mcap_e, bought, sym_e, name_e = entry_r
            meta_sym, meta_name = meta_r
            # Prefer entry mcap (at-buy), then on-chain (fresh tokens), then quote.
            mcap = mcap_e if mcap_e is not None else (
                onchain_r if onchain_r is not None else quote_r
            )
            return (
                mcap,
                bought,
                hp_r,
                sym_e or meta_sym or "",
                name_e or meta_name or "",
            )

        try:
            return await _bundle()
        except asyncio.TimeoutError:
            self._append_log(
                "logwatch",
                f"enrich timeout — пустой результат",
            )
            return None, None, None, "", ""

    async def _ensure_deal_token_labels(
        self,
        deal: Any,
        *,
        rpc: Any | None = None,
    ) -> Any:
        """Fill empty symbol/name from on-chain ERC-20 meta before Telegram."""
        sym = str(getattr(deal, "token_symbol", "") or "").strip()
        name = str(getattr(deal, "token_name", "") or "").strip()
        if sym and name:
            return deal
        client = rpc
        if client is None:
            from .chain import RpcClient

            client = RpcClient(concurrency=2, sem_scope="followup")
        try:
            meta = await asyncio.wait_for(
                client.token_meta(deal.token), timeout=6.0
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("deal token_meta %s: %s", str(deal.token)[:10], exc)
            return deal
        new_sym = sym or str(meta.get("symbol") or "").strip()
        new_name = name or str(meta.get("name") or "").strip()
        if not new_sym and not new_name:
            return deal
        self._store.update_deal_quote(
            deal.wallet,
            deal.token,
            token_symbol=new_sym or None,
            token_name=new_name or None,
        )
        if hasattr(deal, "model_copy"):
            return deal.model_copy(
                update={"token_symbol": new_sym, "token_name": new_name}
            )
        try:
            deal.token_symbol = new_sym
            deal.token_name = new_name
        except Exception:  # noqa: BLE001
            pass
        return deal

    async def _prefetch_transfer_enrichment(
        self,
        transfers: list[Any],
        *,
        cfg: FollowupConfig,
        rpc: Any,
        sender_map: dict[str, str | None],
        budget_sec: float | None = None,
    ) -> dict[
        tuple[str, str, str],
        tuple[float | None, float | None, str | None, str, str],
    ]:
        """Enrich all candidate transfers of a batch concurrently.

        Applies the same cheap filters as the main loop so we never pay replay
        cost for transfers that will be skipped anyway. The main loop stays the
        authority on eligibility and falls back to inline enrichment on a miss.
        """
        if not transfers:
            return {}
        candidates: list[Any] = []
        for tr in transfers:
            if tr.token in self._store.known_tokens(tr.wallet):
                continue
            _, deal_count, status = self._store.get_wallet_scan_meta(tr.wallet)
            if status != "watching" or deal_count >= cfg.max_deals:
                continue
            if cfg.buys_only:
                sender = sender_map.get(tr.tx_hash.lower())
                if sender is not None and sender != tr.wallet:
                    continue
            candidates.append(tr)
        if not candidates:
            return {}

        out: dict[
            tuple[str, str, str],
            tuple[float | None, float | None, str | None, str, str],
        ] = {}
        sem = asyncio.Semaphore(max(1, int(cfg.logwatch_enrich_concurrency or 6)))
        per_budget = budget_sec

        async def one(tr: Any) -> None:
            async with sem:
                try:
                    out[(tr.wallet, tr.token, tr.tx_hash)] = (
                        await self._enrich_transfer(
                            tr, cfg=cfg, rpc=rpc, budget_sec=per_budget
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("prefetch enrich %s: %s", tr.token[:10], exc)

        started = time.time()
        if budget_sec is not None:
            # Live tip: hard wall for the whole batch.
            batch_budget = max(
                float(budget_sec),
                float(budget_sec) * max(1, (len(candidates) + 5) // 6),
            )
            batch_budget = min(batch_budget, 12.0)
        else:
            per = float(max(2, int(cfg.logwatch_enrich_timeout_sec or 8)))
            waves = max(
                1,
                (
                    len(candidates)
                    + max(1, int(cfg.logwatch_enrich_concurrency or 6))
                    - 1
                )
                // max(1, int(cfg.logwatch_enrich_concurrency or 6)),
            )
            batch_budget = min(60.0, per * waves + 4.0)
        try:
            await asyncio.wait_for(
                asyncio.gather(*[one(t) for t in candidates]),
                timeout=batch_budget,
            )
        except asyncio.TimeoutError:
            self._append_log(
                "logwatch",
                f"обогащение budget {batch_budget:.0f}s — частичный результат "
                f"{len(out)}/{len(candidates)}",
                percent=40,
            )
            return out
        self._append_log(
            "logwatch",
            f"обогащение {len(candidates)} сделок за {time.time() - started:.1f}s",
            percent=40,
        )
        return out

    def _queue_mcap_micro_retry(self, wallet: str, token: str) -> None:
        key = (wallet.lower(), token.lower())
        now = time.time()
        self._mcap_micro_retry = [
            (w, t, ts)
            for w, t, ts in self._mcap_micro_retry
            if not (w == key[0] and t == key[1]) and (now - ts) < 600
        ]
        self._mcap_micro_retry.append((key[0], key[1], now))
        if len(self._mcap_micro_retry) > 40:
            self._mcap_micro_retry = self._mcap_micro_retry[-40:]

    async def _micro_retry_pending_mcap(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> None:
        """Fast mcap backfill for fresh live deals (every live tick, not 60s)."""
        if not self._mcap_micro_retry:
            return
        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        pending = list(self._mcap_micro_retry)
        self._mcap_micro_retry = []
        sent = 0
        for wallet, token, _ts in pending[:8]:
            if self._stop_requested:
                break
            deals = [
                d
                for d in self._store.list_pending_alert_deals(
                    alert_on_deals=list(cfg.alert_on_deals or [2, 3, 4, 5]),
                    limit=20,
                    max_age_sec=600,
                    max_mcap_alert=float(cfg.max_mcap_alert or 0) or None,
                )
                if d.wallet.lower() == wallet and d.token.lower() == token
            ]
            if not deals:
                continue
            deal = deals[0]
            mcap = deal.mcap_at_buy
            if mcap is None:
                try:
                    from .replay import estimate_onchain_spot_mcap

                    mcap = await asyncio.wait_for(
                        estimate_onchain_spot_mcap(deal.token, rpc=rpc),
                        timeout=2.5,
                    )
                except Exception:  # noqa: BLE001
                    mcap = None
                if mcap is None:
                    try:
                        mcap, _ = await asyncio.wait_for(
                            estimate_token_quote(deal.token), timeout=2.5
                        )
                    except Exception:  # noqa: BLE001
                        mcap = None
                if mcap is not None:
                    self._store.update_deal_quote(
                        deal.wallet, deal.token, mcap_at_buy=mcap
                    )
                    deal = deal.model_copy(update={"mcap_at_buy": mcap})
            if mcap is None:
                self._queue_mcap_micro_retry(wallet, token)
                continue
            deal = await self._ensure_deal_token_labels(deal, rpc=rpc)
            gate = alert_kwargs_for_wallet(
                cfg, self._store.get_alert_filters_map([deal.wallet]).get(deal.wallet)
            )
            if not should_alert_deal(
                deal.deal_index,
                deal.mcap_at_buy,
                bought_usd=deal.bought_usd,
                **gate,
            ):
                continue
            if not tg_ok:
                continue
            ok = await self._deliver_deal_alert(
                chat, deal=deal, topic_id=topic_id, check_honeypot=False
            )
            if ok:
                sent += 1
                self._append_log(
                    "telegram",
                    f"micro-retry deal #{deal.deal_index} · {deal.wallet[:10]}…",
                )
        if sent:
            self._last_alerts_sent += sent

    async def _live_tip_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> bool:
        """Scan tip window with enrich+alert. Never blocked by hist catch-up."""
        watching = self._store.list_watching()
        if not watching:
            return True
        try:
            tip = int(await asyncio.wait_for(rpc.block_number(), timeout=8.0))
        except TimeoutError:
            self._append_log("live", "block_number timeout")
            return False
        self._last_known_tip = tip
        conf = max(0, int(cfg.logwatch_confirmations or 0))
        safe_tip = max(0, tip - conf)
        base_span = max(50, int(getattr(cfg, "logwatch_live_span", 300) or 300))
        if self._live_timeout_streak >= 2:
            span = min(base_span, 100)
            self._live_span_backoff = span
        else:
            span = base_span
            self._live_span_backoff = 0
        tip_from = max(0, safe_tip - span + 1)
        live_cursor = self._store.get_logwatch_live_cursor()
        if live_cursor is None:
            # First live tick: start at tip (no replay); hist covers history.
            self._store.set_logwatch_live_cursor(safe_tip)
            self._last_live_success_ts = time.time()
            self._append_log("live", f"live cursor = tip {safe_tip}")
            return True

        # Contiguous from live_cursor when inside/near window; always cover tip
        # for freshness. Never snap live_cursor over an unscanned gap.
        live_cursor_i = int(live_cursor)
        tip_scan_from = tip_from
        contiguous = live_cursor_i + 1 >= tip_from
        if contiguous:
            tip_scan_from = live_cursor_i + 1
        if tip_scan_from > safe_tip:
            self._last_live_success_ts = time.time()
            return True

        self._append_log(
            "live",
            f"tip {tip_scan_from}…{safe_tip} (wallets={len(watching)}, span={span})",
            percent=10,
        )
        fetch_timeout = min(
            12.0, float(getattr(cfg, "logwatch_fetch_timeout_sec", 45) or 45)
        )
        enrich_budget = float(getattr(cfg, "live_enrich_budget_sec", 3.0) or 3.0)
        res = await self._logwatch_scan_window(
            cfg,
            rpc=rpc,
            watching=watching,
            from_block=tip_scan_from,
            to_block=safe_tip,
            fetch_timeout=fetch_timeout,
            label="live",
            skip_enrich=False,
            enrich_budget_sec=enrich_budget,
            queue_mcap_retry=True,
        )
        if res is None:
            return False
        if not res.get("advanced"):
            self._live_timeout_streak += 1
            self._append_log(
                "live",
                f"getLogs soft-fail — live cursor не двигаем "
                f"(streak={self._live_timeout_streak})",
            )
            # Soft-fail proves the loop is alive only when already near tip.
            # Far-behind soft-fail must NOT stamp healthy (ops «в порядке» spam).
            behind_now = max(0, safe_tip - live_cursor_i)
            if behind_now <= 2_000:
                self._last_live_success_ts = time.time()
            return True
        self._live_timeout_streak = 0
        # Stamp healthy only when tip scan itself is near-tip (contiguous or
        # tip window). Huge live watermark lag still unhealthy until gap closes.
        behind_after_tip = max(0, safe_tip - live_cursor_i)
        if contiguous or behind_after_tip <= 2_000:
            self._last_live_success_ts = time.time()
        # Advance live cursor only when the scan was contiguous from the watermark.
        if contiguous:
            advance_to = res.get("advance_to")
            if advance_to is not None and int(advance_to) > live_cursor_i:
                self._store.set_logwatch_live_cursor(int(advance_to))
            else:
                self._store.set_logwatch_live_cursor(safe_tip)

        # Gap below tip window: near tip → enrich+alert; large lag → cursor-only
        # (skip_enrich) so hist replay cannot Telegram fake deal #2/#3.
        live_now = self._store.get_logwatch_live_cursor()
        live_now_i = int(live_now) if live_now is not None else live_cursor_i
        if live_now_i + 1 < tip_from:
            gap_from = live_now_i + 1
            gap_to = min(tip_from - 1, gap_from + span - 1)
            gap_behind = max(0, safe_tip - live_now_i)
            enrich_cap = max(
                2 * span,
                int(getattr(cfg, "live_gap_enrich_max_blocks", 2_000) or 2_000),
            )
            gap_skip_enrich = gap_behind > enrich_cap
            self._append_log(
                "live",
                f"gap {gap_from}…{gap_to} "
                f"(behind={gap_behind}, "
                f"{'skip_enrich' if gap_skip_enrich else 'enrich on'})",
                percent=14,
            )
            gap = await self._logwatch_scan_window(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=gap_from,
                to_block=gap_to,
                fetch_timeout=fetch_timeout,
                label="live_gap",
                skip_enrich=gap_skip_enrich,
                enrich_budget_sec=None if gap_skip_enrich else enrich_budget,
                queue_mcap_retry=not gap_skip_enrich,
            )
            if gap and gap.get("advanced") and gap.get("advance_to") is not None:
                self._store.set_logwatch_live_cursor(int(gap["advance_to"]))
                # After gap advance, refresh healthy stamp if now near tip.
                new_live = int(gap["advance_to"])
                if max(0, safe_tip - new_live) <= 2_000:
                    self._last_live_success_ts = time.time()
            elif gap and not gap.get("advanced"):
                self._live_timeout_streak += 1

        if res.get("new_deals") or res.get("alerts"):
            self._last_new_deals = int(res.get("new_deals") or 0)
            self._last_alerts_sent = int(res.get("alerts") or 0)
            self._last_message = (
                f"live {tip_scan_from}…{safe_tip}: "
                f"{res.get('new_deals', 0)} сделок, {res.get('alerts', 0)} алертов"
            )
            self._append_log("live", self._last_message, percent=100)
        return True

    async def _logwatch_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> bool:
        """Primary deal discovery via eth_getLogs. Returns False on hard failure.

        Live tip runs in a dedicated loop (fresh alerts). Hist only advances
        the catch-up cursor with ``skip_enrich`` so backlog clears without
        stalling tip discovery. Large lag → burst spans + multi-chunk; soft
        getLogs timeouts shrink-and-retry (never the old min(..., 800) trap).
        """
        watching = self._store.list_watching()
        if not watching:
            try:
                tip = int(
                    await asyncio.wait_for(rpc.block_number(), timeout=12.0)
                )
                self._last_known_tip = tip
            except TimeoutError as exc:
                live_ok, _ = self._live_tip_healthy()
                if live_ok:
                    self._append_log(
                        "logwatch",
                        "block_number timeout — live ok, soft backoff",
                    )
                    await asyncio.sleep(1.0)
                    return True
                self._append_log("logwatch", f"block_number timeout: {exc}")
                return False
            conf = max(0, int(cfg.logwatch_confirmations or 0))
            safe_tip = max(0, tip - conf)
            if self._store.get_logwatch_cursor() is None:
                self._store.set_logwatch_cursor(safe_tip)
            if self._store.get_logwatch_live_cursor() is None:
                self._store.set_logwatch_live_cursor(safe_tip)
            self._last_message = "Нет кошельков в статусе watching"
            self._append_log("idle", self._last_message)
            return True

        try:
            tip = int(
                await asyncio.wait_for(rpc.block_number(), timeout=12.0)
            )
            self._last_known_tip = tip
        except TimeoutError as exc:
            from .chain import reset_rpc_semaphores

            reset_rpc_semaphores(scope="followup")
            # Rebind client sem to the fresh pool (instance still holds old obj).
            try:
                rpc._bind_url(rpc.rpc_url)  # noqa: SLF001
            except Exception:  # noqa: BLE001
                pass
            self._append_log(
                "logwatch",
                "block_number timeout — сброс followup sem, повтор",
            )
            try:
                tip = int(
                    await asyncio.wait_for(rpc.block_number(), timeout=12.0)
                )
                self._last_known_tip = tip
            except TimeoutError:
                live_ok, _ = self._live_tip_healthy()
                if live_ok:
                    self._append_log(
                        "logwatch",
                        "block_number timeout — live ok, soft backoff "
                        "(без DEGRADED streak)",
                    )
                    await asyncio.sleep(1.5)
                    return True
                self._append_log("logwatch", f"block_number timeout: {exc}")
                return False
        conf = max(0, int(cfg.logwatch_confirmations or 0))
        safe_tip = max(0, tip - conf)
        cursor = self._store.get_logwatch_cursor()
        if cursor is None:
            # First run: do not replay history (seeds come from watch ingest).
            self._store.set_logwatch_cursor(safe_tip)
            self._store.set_logwatch_live_cursor(safe_tip)
            self._append_log(
                "logwatch",
                f"курсор = tip {safe_tip} (без реплея истории)",
                percent=5,
            )
            return True

        # Address-less Transfer getLogs: prefer public RPC — Alchemy often 400s.
        try:
            rpc._prefer_non_alchemy()  # noqa: SLF001
            self._active_rpc = rpc.active_rpc_label()
        except Exception:  # noqa: BLE001
            pass

        lag = max(0, safe_tip - cursor)
        self._cursor_lag_blocks = tip - cursor
        live_span = max(
            50, int(getattr(cfg, "logwatch_live_span", 300) or 300)
        )
        catching_up = lag > max(
            5_000, int(cfg.logwatch_max_span or 3_000) * 3
        )

        if cursor >= safe_tip:
            self._last_checked = len(watching)
            self._last_message = (
                f"hist up-to-date cursor={cursor} tip={tip}"
            )
            self._append_log("logwatch", self._last_message, percent=100)
            return True

        max_chunks = 1
        if catching_up or lag > _HIST_BURST_LAG:
            max_chunks = max(
                1,
                int(getattr(cfg, "logwatch_catchup_chunks_per_pass", 4) or 4),
            )
        elif lag > max(100, int(cfg.logwatch_max_span or 3_000)) * 2:
            max_chunks = max(
                1,
                min(
                    3,
                    int(getattr(cfg, "logwatch_catchup_chunks_per_pass", 4) or 4),
                ),
            )
        budget = float(
            getattr(cfg, "logwatch_catchup_time_budget_sec", 90.0) or 90.0
        )
        cycle_cap = float(cfg.cycle_timeout_sec or 180) * 0.55
        budget = min(budget, cycle_cap)
        t0 = time.time()

        base_fetch_timeout = float(
            getattr(cfg, "logwatch_fetch_timeout_sec", 45) or 45
        )
        # Per-window timeout: allow burst windows to finish via small RPC chunks.
        # Do NOT clamp to 20s on large lag — that froze the cursor on every try.
        fetch_timeout = min(base_fetch_timeout, 45.0)
        if lag > _HIST_BURST_LAG:
            fetch_timeout = min(max(base_fetch_timeout, 35.0), 55.0)

        total_new = 0
        total_alerts = 0
        chunks_done = 0
        last_from = cursor + 1
        last_to = cursor
        hard_fail = False
        any_advance = False

        while chunks_done < max_chunks and (time.time() - t0) < budget:
            if self._stop_requested:
                break
            cursor = self._store.get_logwatch_cursor() or cursor
            lag = max(0, safe_tip - cursor)
            self._cursor_lag_blocks = tip - cursor
            if cursor >= safe_tip:
                break

            span = hist_span_for_lag(lag, cfg)
            from_block = cursor + 1
            to_block = min(safe_tip, from_block + span - 1)
            # Stop hist before the live tip window so we do not race live alerts.
            live_cursor = self._store.get_logwatch_live_cursor()
            live_floor = max(0, safe_tip - live_span)
            if live_cursor is not None:
                to_block = min(to_block, max(live_floor, int(live_cursor)))
            else:
                to_block = min(to_block, live_floor)
            if to_block < from_block:
                snap = self._store.get_logwatch_live_cursor() or safe_tip
                if snap > cursor:
                    self._store.set_logwatch_cursor(min(snap, safe_tip))
                    any_advance = True
                self._last_message = (
                    f"hist caught live window · cursor→{snap}"
                )
                self._append_log("logwatch", self._last_message, percent=100)
                break

            last_from = from_block
            last_to = to_block
            remain = budget - (time.time() - t0)
            if remain < 5.0:
                break
            win_timeout = min(fetch_timeout, max(8.0, remain - 2.0))

            self._last_message = (
                f"hist blocks {from_block}…{to_block} "
                f"(wallets={len(watching)}, lag={lag}, span={span}, "
                f"chunk={chunks_done + 1}/{max_chunks})"
            )
            self._append_log(
                "logwatch",
                self._last_message,
                percent=10 + chunks_done * 15,
            )

            hist_res = await self._hist_scan_shrink_retry(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=from_block,
                to_block=to_block,
                fetch_timeout=win_timeout,
                cursor_floor=cursor,
                label="hist" if chunks_done == 0 else f"hist#{chunks_done + 1}",
            )
            chunks_done += 1
            if hist_res is None:
                hard_fail = True
                break
            if hist_res.get("advanced") and hist_res.get("advance_to") is not None:
                adv = int(hist_res["advance_to"])
                if adv > cursor:
                    self._store.set_logwatch_cursor(adv)
                    any_advance = True
                    last_to = adv
            total_new += int(hist_res.get("new_deals") or 0)
            total_alerts += int(hist_res.get("alerts") or 0)
            # Soft fail with no advance: shrink-retry already exhausted — stop
            # this cycle; next pass retries. Do not burn the whole budget.
            if not hist_res.get("advanced"):
                break
            # Near tip: one chunk is enough.
            if not catching_up and lag <= max(
                5_000, int(cfg.logwatch_max_span or 3_000) * 3
            ):
                break

        if hard_fail and not any_advance:
            return False

        cursor_now = self._store.get_logwatch_cursor() or cursor
        self._cursor_lag_blocks = tip - cursor_now
        self._last_new_deals = total_new
        self._last_alerts_sent = total_alerts
        self._last_checked = len(watching)
        self._last_message = (
            f"hist {last_from}…{last_to}: {total_new} сделок "
            f"(skip_enrich, alerts via live, chunks={chunks_done}"
            f", cursor→{cursor_now}, lag={self._cursor_lag_blocks})"
        )
        self._append_log("logwatch", self._last_message, percent=100)
        return True

    async def _hist_scan_shrink_retry(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        watching: list[str],
        from_block: int,
        to_block: int,
        fetch_timeout: float,
        cursor_floor: int,
        label: str,
    ) -> dict[str, Any] | None:
        """Scan ``[from_block, to_block]``; on soft timeout/400 shrink and retry.

        Hist is always ``skip_enrich``: live tip owns alerts. Shrinking keeps
        the cursor moving instead of stalling on a too-wide OR'd topic query.
        As a last resort when live tip is healthy, nudge the cursor by a tiny
        safe amount so a stuck RPC window cannot freeze catch-up forever
        (hist does not record deals during skip_enrich anyway).
        """
        cur_to = to_block
        attempt = 0
        last: dict[str, Any] | None = None
        while cur_to >= from_block:
            attempt += 1
            span_now = cur_to - from_block + 1
            res = await self._logwatch_scan_window(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=from_block,
                to_block=cur_to,
                fetch_timeout=fetch_timeout if attempt == 1 else min(fetch_timeout, 20.0),
                label=label if attempt == 1 else f"{label}-shrink{attempt}",
                cursor_floor=cursor_floor,
                skip_enrich=True,
            )
            last = res
            if res is None:
                if span_now <= 200:
                    return None
                from .chain import reset_rpc_semaphores

                reset_rpc_semaphores(scope="followup")
                cur_to = from_block + max(99, span_now // 2) - 1
                self._append_log(
                    "logwatch",
                    f"{label} hard-fail — shrink-retry "
                    f"{from_block}…{cur_to}",
                )
                continue
            if res.get("advanced"):
                return res
            # Soft timeout / retryable empty: shrink and retry.
            if span_now <= 150:
                live_ok, _ = self._live_tip_healthy()
                if live_ok:
                    # Unstick: hist skip_enrich does not record deals; live
                    # covers fresh alerts. Advance a tiny safe step.
                    nudge = min(50, span_now)
                    nudge_to = from_block + nudge - 1
                    self._append_log(
                        "logwatch",
                        f"{label} soft-fail after shrink — nudge cursor "
                        f"+{nudge} (live ok, skip_enrich) → {nudge_to}",
                    )
                    return {
                        "new_deals": 0,
                        "alerts": 0,
                        "skipped": 0,
                        "advanced": True,
                        "advance_to": nudge_to,
                    }
                return res
            from .chain import reset_rpc_semaphores

            reset_rpc_semaphores(scope="followup")
            cur_to = from_block + max(99, span_now // 2) - 1
            self._append_log(
                "logwatch",
                f"{label} soft-fail — shrink-retry {from_block}…{cur_to}",
            )
            fetch_timeout = min(fetch_timeout, 18.0)
        return last

    async def _fetch_unique_buys_cached(
        self,
        wallet: str,
        *,
        cfg: FollowupConfig,
        max_pages: int | None = None,
    ) -> UniqueBuysResult:
        """Short-TTL cache so live tip does not stampede GMGN per transfer."""
        wallet_l = wallet.lower()
        ttl = float(getattr(cfg, "gmgn_rank_cache_ttl_sec", 60.0) or 60.0)
        now = time.time()
        hit = self._gmgn_rank_cache.get(wallet_l)
        if hit is not None and (now - hit[0]) <= ttl:
            return hit[1]
        pages = int(
            max_pages
            if max_pages is not None
            else (getattr(cfg, "gmgn_rank_max_pages", 3) or 3)
        )
        result = await fetch_unique_buys(wallet_l, max_pages=max(1, pages))
        self._gmgn_rank_cache[wallet_l] = (now, result)
        if len(self._gmgn_rank_cache) > 800:
            # Drop oldest half by timestamp.
            ordered = sorted(self._gmgn_rank_cache.items(), key=lambda kv: kv[1][0])
            for key, _ in ordered[: len(ordered) // 2]:
                self._gmgn_rank_cache.pop(key, None)
        return result

    def _invalidate_gmgn_rank_cache(self, wallet: str) -> None:
        self._gmgn_rank_cache.pop(wallet.lower(), None)

    async def _gmgn_rank_verdict(
        self,
        wallet: str,
        token: str,
        cfg: FollowupConfig,
    ) -> GmgnRankVerdict:
        """Establish post-seed unique-buy rank for a newly seen transfer token.

        Fail-safe on circuit/429/seed-miss: ``uncertain=True`` — caller must
        NOT invent a local deal #2/#3 Telegram alert.
        """
        from dataclasses import replace

        from .gmgn_portfolio import gmgn_api_configured, gmgn_circuit_open

        token_l = (token or "").strip().lower()
        seed_deal = next(
            (
                d
                for d in self._store.list_deals_for_wallet(wallet)
                if int(d.get("deal_index") or 0) == 1
            ),
            None,
        )
        seed_token = str((seed_deal or {}).get("token") or "").lower()
        max_deals = max(1, int(cfg.max_deals or 5))
        empty = GmgnRankVerdict(
            uncertain=True,
            reason="unknown",
            seed_token=seed_token,
            post_seed=(),
            rank=None,
            past_max=False,
        )
        if not seed_token:
            return replace(empty, reason="no_seed")
        if not gmgn_api_configured():
            return replace(empty, reason="gmgn_unconfigured")
        if gmgn_circuit_open():
            return replace(empty, reason="gmgn_circuit")
        try:
            result = await self._fetch_unique_buys_cached(wallet, cfg=cfg)
        except Exception as exc:  # noqa: BLE001
            logger.warning("GMGN rank fetch %s: %s", wallet[:10], exc)
            return replace(empty, reason="gmgn_fetch_failed")
        if result.rate_limited or not result.ok:
            reason = "gmgn_429" if result.rate_limited else "gmgn_fetch_failed"
            return replace(empty, reason=reason)
        _seed_buy, post = post_seed_unique_buys(list(result.buys), seed_token)
        if _seed_buy is None:
            return replace(empty, reason="gmgn_seed_miss")
        past_max = len(post) >= max(0, max_deals - 1)
        # Repair / wallet-level check: no tip token — only post-seed fullness.
        if not token_l:
            return GmgnRankVerdict(
                uncertain=False,
                reason="ok",
                seed_token=seed_token,
                post_seed=tuple(post),
                rank=None,
                past_max=past_max,
            )
        rank: int | None = None
        for i, buy in enumerate(post, start=2):
            if buy.token.lower() == token_l:
                rank = i
                break
        if rank is None and past_max:
            return GmgnRankVerdict(
                uncertain=False,
                reason="past_max",
                seed_token=seed_token,
                post_seed=tuple(post),
                rank=None,
                past_max=True,
            )
        # Fresh tip buy not yet on GMGN: next slot only inside the window.
        if rank is None and not past_max:
            next_rank = len(post) + 2
            if next_rank > max_deals:
                return GmgnRankVerdict(
                    uncertain=False,
                    reason="beyond_window",
                    seed_token=seed_token,
                    post_seed=tuple(post),
                    rank=None,
                    past_max=True,
                )
            rank = next_rank
        return GmgnRankVerdict(
            uncertain=False,
            reason="ok",
            seed_token=seed_token,
            post_seed=tuple(post),
            rank=rank,
            past_max=past_max or (rank is not None and rank > max_deals),
        )

    async def _sync_wallet_gmgn_order(
        self,
        wallet: str,
        cfg: FollowupConfig,
        *,
        post_seed: list[GmgnBuy],
        tip_token: str | None = None,
        tip_symbol: str = "",
        tip_tx: str = "",
        tip_block: int = 0,
        tip_bought_at: float | None = None,
        tip_mcap: float | None = None,
        tip_bought_usd: float | None = None,
    ) -> list[Any]:
        """Apply GMGN post-seed order; optionally append a tip buy not yet on GMGN."""
        max_deals = max(1, int(cfg.max_deals or 5))
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        tip_l = (tip_token or "").lower()
        for buy in post_seed:
            tok = buy.token.lower()
            if not tok or tok in seen:
                continue
            seen.add(tok)
            rows.append(
                {
                    "token": tok,
                    "symbol": buy.symbol,
                    "tx_hash": buy.tx_hash,
                    "block_number": 0,
                    "bought_at": float(buy.timestamp) if buy.timestamp > 0 else None,
                    "mcap_at_buy": None,
                    "bought_usd": buy.cost_usd,
                }
            )
        if tip_l and tip_l not in seen and tip_l not in QUOTE_TOKENS:
            rows.append(
                {
                    "token": tip_l,
                    "symbol": tip_symbol,
                    "tx_hash": tip_tx,
                    "block_number": tip_block,
                    "bought_at": tip_bought_at,
                    "mcap_at_buy": tip_mcap,
                    "bought_usd": tip_bought_usd,
                }
            )
        inserted = self._store.apply_gmgn_buy_order(
            wallet, rows, max_deals=max_deals
        )
        self._invalidate_gmgn_rank_cache(wallet)
        return inserted

    async def _repair_undercounted_wallets(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> None:
        """Jump watching wallets to done when GMGN already has ≥ max_deals uniques."""
        from .gmgn_portfolio import gmgn_api_configured, gmgn_circuit_open

        batch = int(getattr(cfg, "gmgn_repair_batch", 8) or 0)
        if batch <= 0:
            return
        if not gmgn_api_configured() or gmgn_circuit_open():
            return
        max_deals = max(1, int(cfg.max_deals or 5))
        watching = self._store.list_watching()
        # Prefer undercounted (local deal_count well below cap).
        candidates: list[tuple[int, str]] = []
        for addr in watching:
            _seen, deal_count, status = self._store.get_wallet_scan_meta(addr)
            if status != "watching" or deal_count >= max_deals:
                continue
            candidates.append((deal_count, addr))
        candidates.sort(key=lambda x: x[0])
        repaired = 0
        for _, wallet in candidates[:batch]:
            if self._stop_requested:
                break
            verdict = await self._gmgn_rank_verdict(wallet, token="", cfg=cfg)
            # Empty token → rank always None; we only care about past_max / post_seed.
            if verdict.uncertain:
                continue
            if not verdict.post_seed and not verdict.past_max:
                continue
            local_post = max(0, self._store.get_wallet_scan_meta(wallet)[1] - 1)
            if len(verdict.post_seed) <= local_post and not verdict.past_max:
                continue
            inserted = await self._sync_wallet_gmgn_order(
                wallet, cfg, post_seed=list(verdict.post_seed)
            )
            _seen, deal_count, status = self._store.get_wallet_scan_meta(wallet)
            repaired += 1
            self._append_log(
                "repair",
                f"{wallet[:10]}… GMGN sync post={len(verdict.post_seed)} "
                f"→ deal_count={deal_count} status={status} "
                f"(+{len(inserted)} new)",
            )
            # Do NOT Telegram from repair — these are historical catch-ups.
            for deal in inserted:
                self._store.mark_notified(deal.wallet, deal.token)
        if repaired:
            self._append_log("repair", f"GMGN undercount repair: {repaired} кош.")

    def _live_tip_healthy(self, *, tip: int | None = None) -> tuple[bool, int | None]:
        """Whether live tip discovery looks fine — fail-open on unknown.

        Recent live tick success alone is NOT healthy when the live watermark
        is far behind tip (gap backfill / hang). Soft-fail must not stamp
        healthy in that case either.

        Important: do NOT re-query RPC here after a hist failure. A stuck RPC
        made ``_live_behind_blocks`` return None, which falsely meant
        «live tip тоже отстаёт» and triggered DEGRADED spam.
        """
        live = self._store.get_logwatch_live_cursor()
        tip_ref = tip if tip is not None else self._last_known_tip
        behind: int | None = None
        if tip_ref is not None and live is not None:
            behind = max(0, int(tip_ref) - int(live))
        if behind is not None and behind > 2_000:
            return False, behind
        now = time.time()
        if self._last_live_success_ts and (now - self._last_live_success_ts) <= 90.0:
            return True, behind
        if live is None or tip_ref is None:
            return True, None
        assert behind is not None
        return behind <= 2_000, behind

    async def _live_behind_blocks(self, rpc: Any) -> int | None:
        """Tip − live cursor via RPC; None if tip unavailable.

        Prefer ``_live_tip_healthy`` for DEGRADED decisions (no extra RPC).
        """
        try:
            tip = int(await asyncio.wait_for(rpc.block_number(), timeout=8.0))
            self._last_known_tip = tip
        except Exception:  # noqa: BLE001
            return None
        live = self._store.get_logwatch_live_cursor()
        if live is None:
            return None
        return max(0, tip - int(live))

    async def _logwatch_scan_window(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        watching: list[str],
        from_block: int,
        to_block: int,
        fetch_timeout: float,
        label: str,
        cursor_floor: int | None = None,
        skip_enrich: bool = False,
        enrich_budget_sec: float | None = None,
        queue_mcap_retry: bool = False,
    ) -> dict[str, Any] | None:
        """Fetch+enrich+alert one block window.

        Returns a stats dict, or None on hard failure. Soft getLogs timeout
        yields advanced=False so cursors stay put. ``skip_enrich`` skips
        tx_senders + mcap enrich (hist catch-up only advances cursor / records
        with null mcap) so hist cannot stall on RPC batches.
        """
        empty = {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
        }
        if to_block < from_block:
            return {**empty, "advanced": True, "advance_to": cursor_floor}
        try:
            window = max(1, to_block - from_block + 1)
            if skip_enrich:
                # Hist catch-up: split into small RPC chunks so each call
                # finishes under the 15s get_logs wall-timeout. A single
                # 800–10k OR'd-topic query routinely timed out and froze lag.
                rpc_chunk = max(
                    50,
                    int(getattr(cfg, "logwatch_hist_rpc_chunk", 400) or 400),
                )
                chunk_size = min(window, rpc_chunk)
            else:
                chunk_size = min(window, 2_000)
            transfers = await asyncio.wait_for(
                fetch_inbound_transfers(
                    rpc,
                    watching,
                    from_block=from_block,
                    to_block=to_block,
                    chunk_size=chunk_size,
                ),
                timeout=fetch_timeout,
            )
        except asyncio.TimeoutError:
            self._append_log(
                "logwatch",
                f"getLogs timeout {fetch_timeout:.0f}s на {from_block}…{to_block} "
                f"({label}) — курсор не двигаем",
            )
            return empty
        except Exception as exc:  # noqa: BLE001
            from .chain import _is_retryable, _redact_exc

            safe = _redact_exc(exc)
            # Alchemy 400/429/503 and connection blips are normal — treat as
            # soft (no cursor advance, no DEGRADED streak). Only unknown
            # non-retryable errors hard-fail the hist cycle.
            if _is_retryable(exc):
                self._append_log(
                    "logwatch",
                    f"getLogs soft-err ({label}): {safe} — курсор не двигаем",
                )
                return empty
            self._append_log(
                "logwatch",
                f"getLogs ошибка ({label}): {safe}",
            )
            return None

        sender_map: dict[str, str | None] = {}
        # Hist skip_enrich: do not burn RPC on tx_senders — that batch is what
        # turned a healthy tip into «hist hung >180s» when prune also ran.
        if cfg.buys_only and transfers and not skip_enrich:
            try:
                sender_map = await asyncio.wait_for(
                    tx_senders(rpc, [t.tx_hash for t in transfers]),
                    timeout=min(20.0, fetch_timeout),
                )
            except asyncio.TimeoutError:
                self._append_log(
                    "logwatch",
                    f"tx_senders timeout ({label}) — fail-open без фильтра from",
                )
                sender_map = {}

        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        filters_map = self._store.get_alert_filters_map(
            sorted({t.wallet for t in transfers})
        )

        if skip_enrich:
            # Cursor-only. Recording deals without tx_senders/enrich stamped
            # airdrops as deal #2..N (empty TOKEN, inflated deal_count).
            if transfers:
                self._append_log(
                    "logwatch",
                    f"hist skip enrich ({len(transfers)} transfers) — только курсор",
                    percent=40,
                )
            return {
                "new_deals": 0,
                "alerts": 0,
                "skipped": len(transfers),
                "advanced": True,
                "advance_to": to_block,
            }
        else:
            enrich = await self._prefetch_transfer_enrichment(
                transfers,
                cfg=cfg,
                rpc=rpc,
                sender_map=sender_map,
                budget_sec=enrich_budget_sec,
            )

        new_deals = 0
        alerts = 0
        skipped = 0
        advance_to = to_block
        stopped_early = False
        floor = cursor_floor if cursor_floor is not None else from_block - 1

        for tr in transfers:
            if self._stop_requested:
                stopped_early = True
                advance_to = max(floor, tr.block_number - 1)
                break
            if tr.token in self._store.known_tokens(tr.wallet):
                continue
            _, deal_count, status = self._store.get_wallet_scan_meta(tr.wallet)
            if status != "watching" or deal_count >= cfg.max_deals:
                continue
            if cfg.buys_only:
                sender = sender_map.get(tr.tx_hash.lower())
                if sender is not None and sender != tr.wallet:
                    skipped += 1
                    continue

            cached = enrich.get((tr.wallet, tr.token, tr.tx_hash))
            if cached is not None:
                mcap, bought_usd, hp_reason, token_symbol, token_name = cached
            else:
                (
                    mcap,
                    bought_usd,
                    hp_reason,
                    token_symbol,
                    token_name,
                ) = await self._enrich_transfer(
                    tr,
                    cfg=cfg,
                    rpc=rpc,
                    budget_sec=enrich_budget_sec,
                )

            # GMGN post-seed rank is authoritative. Local sequential record_deal
            # invented fake #2/#3 when missed unique buys never entered DB.
            from .gmgn_portfolio import gmgn_api_configured

            deal = None
            if gmgn_api_configured():
                verdict = await self._gmgn_rank_verdict(
                    tr.wallet, tr.token, cfg
                )
                if verdict.uncertain:
                    self._append_log(
                        "deal",
                        f"skip invent #{tr.token[:10]}… "
                        f"GMGN uncertain ({verdict.reason}) [{label}]",
                    )
                    skipped += 1
                    continue
                # Sync GMGN order (marks done when already ≥ max_deals uniques).
                include_tip = (
                    not verdict.past_max
                    and verdict.rank is not None
                    and verdict.rank <= int(cfg.max_deals or 5)
                )
                await self._sync_wallet_gmgn_order(
                    tr.wallet,
                    cfg,
                    post_seed=list(verdict.post_seed),
                    tip_token=tr.token if include_tip else None,
                    tip_symbol=token_symbol,
                    tip_tx=tr.tx_hash,
                    tip_block=tr.block_number,
                    tip_bought_at=tr.bought_at or None,
                    tip_mcap=mcap,
                    tip_bought_usd=bought_usd,
                )
                _seen, deal_count_now, status_now = self._store.get_wallet_scan_meta(
                    tr.wallet
                )
                if status_now != "watching" or deal_count_now >= cfg.max_deals:
                    self._append_log(
                        "deal",
                        f"GMGN past window {tr.wallet[:10]}… "
                        f"post={len(verdict.post_seed)} status={status_now} [{label}]",
                    )
                    skipped += 1
                    continue
                # Pull the row GMGN sync assigned (correct index).
                for row in self._store.list_deals_for_wallet(tr.wallet):
                    if str(row.get("token") or "").lower() == tr.token.lower():
                        deal = FollowupDealRow(
                            wallet=tr.wallet.lower(),
                            token=tr.token.lower(),
                            token_symbol=str(
                                row.get("token_symbol") or token_symbol or ""
                            ),
                            token_name=token_name,
                            deal_index=int(row.get("deal_index") or 0),
                            mcap_at_buy=(
                                float(row["mcap_at_buy"])
                                if row.get("mcap_at_buy") is not None
                                else mcap
                            ),
                            bought_usd=(
                                float(row["bought_usd"])
                                if row.get("bought_usd") is not None
                                else bought_usd
                            ),
                            tx_hash=str(row.get("tx_hash") or tr.tx_hash or ""),
                            block_number=int(
                                row.get("block_number") or tr.block_number or 0
                            ),
                            bought_at=(
                                float(row["bought_at"])
                                if row.get("bought_at")
                                else (tr.bought_at or None)
                            ),
                            notified=bool(row.get("notified")),
                            created_at=float(row.get("created_at") or time.time()),
                        )
                        break
                if deal is None:
                    # Token beyond capped GMGN prefix — not an alertable #2..N.
                    skipped += 1
                    continue
                if deal.notified:
                    skipped += 1
                    continue
                new_deals += 1
            else:
                deal = self._store.record_deal(
                    wallet=tr.wallet,
                    token=tr.token,
                    token_symbol=token_symbol,
                    token_name=token_name,
                    mcap_at_buy=mcap,
                    bought_usd=bought_usd,
                    tx_hash=tr.tx_hash,
                    block_number=tr.block_number,
                    bought_at=tr.bought_at or None,
                    max_deals=cfg.max_deals,
                )
                if not deal:
                    continue
                new_deals += 1

            self._store.advance_last_seen_block(tr.wallet, tr.block_number)
            discover_lag = (
                max(0.0, float(deal.created_at) - float(deal.bought_at))
                if deal.bought_at
                else None
            )
            self._append_log(
                "deal",
                f"#{deal.deal_index} {deal.token_symbol or deal.token[:10]}…"
                f" mcap={deal.mcap_at_buy}"
                + (
                    f" lag={discover_lag:.1f}s"
                    if discover_lag is not None
                    else ""
                )
                + (f" [{label}]" if label else ""),
            )

            gate = alert_kwargs_for_wallet(cfg, filters_map.get(tr.wallet))
            if not should_alert_deal(
                deal.deal_index,
                deal.mcap_at_buy,
                bought_usd=deal.bought_usd,
                **gate,
            ):
                if (
                    queue_mcap_retry
                    and deal.mcap_at_buy is None
                    and deal.deal_index in (cfg.alert_on_deals or [2, 3, 4, 5])
                ):
                    self._queue_mcap_micro_retry(deal.wallet, deal.token)
                skipped += 1
                continue
            tip_ref = self._last_known_tip
            if not deal_is_fresh_for_alert(
                bought_at=deal.bought_at,
                block_number=deal.block_number,
                tip=tip_ref,
                max_buy_age_sec=float(
                    getattr(cfg, "alert_max_buy_age_sec", 900) or 900
                ),
                max_block_lag=int(
                    getattr(cfg, "alert_max_block_lag", 2_000) or 2_000
                ),
            ):
                self._append_log(
                    "deal",
                    f"skip stale alert #{deal.deal_index} "
                    f"block={deal.block_number} tip={tip_ref} [{label}]",
                )
                skipped += 1
                continue
            if not tg_ok:
                self._last_error = "Telegram не настроен"
                continue
            ok = await self._deliver_deal_alert(
                chat,
                deal=deal,
                topic_id=topic_id,
                honeypot_reason=hp_reason,
            )
            if ok:
                alerts += 1
                self._append_log(
                    "telegram",
                    f"Алерт deal #{deal.deal_index} · {deal.wallet[:10]}…"
                    + (" · HONEYPOT" if hp_reason else ""),
                )

        if not stopped_early:
            advance_to = to_block
        return {
            "new_deals": new_deals,
            "alerts": alerts,
            "skipped": skipped,
            "advanced": True,
            "advance_to": advance_to,
        }

    async def _legacy_scan_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        force_all_due: bool = False,
    ) -> None:
        """Per-wallet GMGN/Blockscout scan — used when logwatch is down."""
        await self._cycle_body_legacy(
            cfg, rpc=rpc, force_all_due=force_all_due
        )

    async def _cycle_body_legacy(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        force_all_due: bool = False,
    ) -> None:
        now = time.time()
        sched_cfg = schedule_config_from_followup(cfg)
        schedule_rows_raw = self._store.list_watching_schedule_rows()
        schedule_rows = [
            WalletScheduleRow(
                address=r["address"],
                status=r["status"],
                deal_count=int(r["deal_count"]),
                discovered_at=float(r["discovered_at"]),
                last_activity_at=float(r["last_activity_at"]),
                last_scanned_at=r["last_scanned_at"],
                last_balance_check_at=r["last_balance_check_at"],
                wallet_balance_eth=r["wallet_balance_eth"],
            )
            for r in schedule_rows_raw
        ]
        watching_n = len(schedule_rows)
        due = select_due_batch(
            schedule_rows,
            now=now,
            max_deals=int(cfg.max_deals or 5),
            cfg=sched_cfg,
            force_all_due=force_all_due,
        )
        self._last_due_count = len(due)
        self._last_checked = 0
        self._last_new_deals = 0
        self._last_alerts_sent = 0
        self._last_hot_checked = 0
        self._last_warm_checked = 0
        self._last_zero_rechecked = 0
        self._last_skipped_zero_balance = 0
        self._last_hot_revisit_sec = None
        self._last_error = None

        if watching_n == 0:
            self._last_message = "Нет кошельков в статусе watching"
            self._append_log("idle", self._last_message)
            pruned = await self._maybe_prune(cfg, force=force_all_due)
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )
            return

        if not due:
            self._last_message = (
                f"Нет due кош. (watching={watching_n}, "
                f"hot≤{sched_cfg.hot_revisit_sec:.0f}s / "
                f"warm≤{sched_cfg.warm_revisit_sec:.0f}s)"
            )
            self._append_log("idle", self._last_message)
            pruned = await self._maybe_prune(cfg, force=force_all_due)
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )
            return

        row_by_addr = {r.address: r for r in schedule_rows}
        wallets = [d.address for d in due]
        tier_by_addr = {d.address: d.tier for d in due}
        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        filters_map = self._store.get_alert_filters_map(wallets)

        hot_n = sum(1 for d in due if d.tier == "hot")
        warm_n = sum(1 for d in due if d.tier == "warm")
        zero_n = sum(1 for d in due if d.tier == "zero")
        self._last_message = (
            f"Due {len(due)}/{watching_n} "
            f"(hot={hot_n} warm={warm_n} zero={zero_n})…"
        )
        self._append_log("scan", self._last_message, percent=5)

        # Refresh balances for due wallets that need it (batched multicall + TTL cache).
        refresh_addrs = [
            d.address
            for d in due
            if d.needs_balance_refresh or d.tier == "zero"
        ]
        balance_map: dict[str, float | None] = {
            addr: row_by_addr[addr].wallet_balance_eth
            for addr in wallets
            if addr in row_by_addr
        }
        if refresh_addrs and not self._stop_requested:
            try:
                fetched = await batch_wallet_balances(rpc, refresh_addrs)
                self._store.update_wallet_balances(fetched, checked_at=time.time())
                for addr, bal in fetched.items():
                    balance_map[addr] = bal
            except Exception as exc:  # noqa: BLE001
                logger.warning("Follow-up balance refresh failed: %s", exc)
                # Fail-open: leave prior values; None stays None → deal scan proceeds.

        # Confirmed zero ETH → skip network-heavy deal scan; keep watching.
        scan_wallets: list[str] = []
        skipped_zero: list[str] = []
        for addr in wallets:
            bal = balance_map.get(addr)
            if bal is not None and float(bal) == 0.0:
                skipped_zero.append(addr)
            else:
                # None (unknown/error) or >0 → scan (fail-open on None).
                scan_wallets.append(addr)

        self._last_skipped_zero_balance = len(skipped_zero)
        if skipped_zero:
            self._store.mark_scanned(skipped_zero)
            self._last_zero_rechecked = sum(
                1 for a in skipped_zero if tier_by_addr.get(a) == "zero"
            )
            self._append_log(
                "skip",
                f"skipped_zero_balance={len(skipped_zero)} "
                f"(остаются watching, recheck ~"
                f"{sched_cfg.zero_balance_recheck_sec:.0f}s)",
                percent=8,
            )

        if not scan_wallets:
            self._last_checked = len(skipped_zero)
            self._last_message = (
                f"Готово — due={len(due)}, "
                f"skipped_zero_balance={len(skipped_zero)}, сделок 0"
            )
            self._append_log("done", self._last_message, percent=100)
            if not self._stop_requested:
                pruned = await self._maybe_prune(cfg, force=force_all_due)
                if pruned:
                    self._append_log(
                        "prune",
                        f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                    )
            return

        # Prefer real Blockscout scans over empty GMGN cycles while cooling.
        gmgn_fallback_cycle = False
        try:
            from .gmgn_portfolio import (
                gmgn_api_configured,
                gmgn_circuit_open,
                wait_for_gmgn_capacity,
            )

            if gmgn_api_configured():
                ready = await wait_for_gmgn_capacity(timeout=12.0)
                if not ready or gmgn_circuit_open():
                    gmgn_fallback_cycle = True
                    self._last_message = (
                        "gmgn_empty_due_to_429, fallback_blockscout"
                    )
                    self._append_log("scan", self._last_message, percent=10)
                    logger.warning(
                        "Follow-up: GMGN circuit open — Blockscout fallback cycle"
                    )
        except Exception:  # noqa: BLE001
            pass

        conc = max(1, int(cfg.scan_concurrency or 3))
        if gmgn_fallback_cycle:
            # Blockscout-only: modest parallelism; avoid BS stampede.
            conc = min(max(conc, 1), _BS_FALLBACK_CONCURRENCY)
        else:
            # Keep GMGN pressure moderate on hot batches.
            conc = min(conc, _HOT_GMGN_CONCURRENCY)
        sem = asyncio.Semaphore(conc)
        done_count = 0
        skipped_alerts = 0
        gmgn_ok_wallets = 0
        gmgn_fallback_wallets = 0
        blockscout_wallets = 0
        progress_lock = asyncio.Lock()
        scanned_addrs: list[str] = []

        async def _alert_deals(
            wallet: str, new_deals: list[tuple[Any, str | None]]
        ) -> None:
            """Send TG as soon as this wallet's scan finishes (don't wait for all).

            Alerts go out in ascending deal_index. A late-discovered lower index
            after a higher index was already notified still alerts (no suppress).
            Honeypot is already resolved during scan (or checked here before send).
            """
            nonlocal skipped_alerts
            if not new_deals or self._stop_requested:
                return
            gate = alert_kwargs_for_wallet(cfg, filters_map.get(wallet))
            for deal, hp_reason in order_deals_for_alerts(new_deals):
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
                ok = await self._deliver_deal_alert(
                    chat,
                    deal=deal,
                    topic_id=topic_id,
                    honeypot_reason=hp_reason,
                )
                if ok:
                    async with progress_lock:
                        self._last_alerts_sent += 1
                    self._append_log(
                        "telegram",
                        f"Алерт deal #{deal.deal_index} · {wallet[:10]}…"
                        + (" · HONEYPOT" if hp_reason else ""),
                    )

        async def _scan_one(wallet: str) -> tuple[str, list]:
            nonlocal done_count, gmgn_ok_wallets, gmgn_fallback_wallets, blockscout_wallets
            async with sem:
                if self._stop_requested:
                    return wallet, []
                try:
                    deals, source = await self._scan_wallet(
                        wallet,
                        cfg,
                        rpc=rpc,
                        skip_gmgn=gmgn_fallback_cycle,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Follow-up scan %s: %s", wallet[:10], exc)
                    deals, source = [], "error"
            async with progress_lock:
                done_count += 1
                scanned_addrs.append(wallet)
                tier = tier_by_addr.get(wallet, "warm")
                if tier == "hot":
                    self._last_hot_checked += 1
                elif tier == "warm":
                    self._last_warm_checked += 1
                if source == "gmgn":
                    gmgn_ok_wallets += 1
                elif source == "blockscout_fallback":
                    gmgn_fallback_wallets += 1
                    blockscout_wallets += 1
                elif source == "blockscout":
                    blockscout_wallets += 1
                if done_count % 5 == 0 or done_count == len(scan_wallets):
                    pct = 10 + int(85 * done_count / max(len(scan_wallets), 1))
                    self._last_message = (
                        f"Проверено {done_count}/{len(scan_wallets)} due, "
                        f"hot={self._last_hot_checked} warm={self._last_warm_checked}, "
                        f"zero_skip={self._last_skipped_zero_balance}, "
                        f"новых сделок {self._last_new_deals}, "
                        f"алертов {self._last_alerts_sent}"
                        + (
                            f" · BS fallback {gmgn_fallback_wallets}"
                            if gmgn_fallback_wallets
                            else ""
                        )
                    )
                    self._append_log("scan", self._last_message, percent=pct)
            return wallet, deals

        tasks = [asyncio.create_task(_scan_one(w)) for w in scan_wallets]
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

        if scanned_addrs:
            self._store.mark_scanned(scanned_addrs)
        self._last_checked = len(scanned_addrs) + len(skipped_zero)

        # Estimate hot revisit from last_scanned_at → now for wallets we just did.
        hot_gaps: list[float] = []
        for addr in scanned_addrs:
            if tier_by_addr.get(addr) != "hot":
                continue
            prev = row_by_addr.get(addr)
            if prev and prev.last_scanned_at is not None:
                hot_gaps.append(now - float(prev.last_scanned_at))
        if hot_gaps:
            hot_gaps.sort()
            self._last_hot_revisit_sec = hot_gaps[len(hot_gaps) // 2]

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
            path_note = ""
            if gmgn_fallback_wallets or gmgn_fallback_cycle:
                path_note = (
                    f" · gmgn_empty_due_to_429, fallback_blockscout="
                    f"{gmgn_fallback_wallets}/{len(scan_wallets)}"
                )
            elif blockscout_wallets and not gmgn_ok_wallets:
                path_note = f" · blockscout={blockscout_wallets}"
            elif gmgn_ok_wallets:
                path_note = (
                    f" · gmgn_ok={gmgn_ok_wallets}"
                    + (
                        f", blockscout={blockscout_wallets}"
                        if blockscout_wallets
                        else ""
                    )
                )
            revisit_note = ""
            if self._last_hot_revisit_sec is not None:
                revisit_note = f" · hot_revisit≈{self._last_hot_revisit_sec:.0f}s"
            if (
                gmgn_fallback_cycle
                and gmgn_fallback_wallets == 0
                and gmgn_ok_wallets == 0
                and blockscout_wallets == 0
                and scan_wallets
            ):
                self._last_message = (
                    "GMGN 429 — нет скана (ни GMGN, ни Blockscout)"
                    f"{path_note}"
                )
                self._append_log("error", self._last_message, percent=100)
                logger.error(
                    "Follow-up cycle produced no scan work under GMGN 429"
                )
            else:
                self._last_message = (
                    f"Готово — due={len(due)} scanned={len(scanned_addrs)} "
                    f"hot={self._last_hot_checked} warm={self._last_warm_checked} "
                    f"zero_skip={self._last_skipped_zero_balance}, "
                    f"{self._last_new_deals} сделок, "
                    f"{self._last_alerts_sent} алертов{path_note}{revisit_note}"
                )
                self._append_log("done", self._last_message, percent=100)

        # Prune after alerts so honeypot/TG path is not blocked by ATH fetches.
        if not self._stop_requested:
            pruned = await self._maybe_prune(cfg, force=force_all_due)
            if pruned:
                self._append_log(
                    "prune",
                    f"Удалено {pruned} кош. (токен #1/#2/#3 не дошёл до ATH за срок)",
                )

    async def _maybe_prune(
        self,
        cfg: FollowupConfig,
        *,
        force: bool = False,
    ) -> int:
        """Run ATH prune at most once per ``prune_interval_sec`` (unless forced).

        Priority cycles are short; pruning every cycle stampeded Gecko into 429.
        """
        interval = float(getattr(cfg, "prune_interval_sec", 1800) or 1800)
        now = time.time()
        if not force and self._last_prune_ts > 0 and (now - self._last_prune_ts) < interval:
            return 0
        # ATH prune hits GeckoTerminal/DexScreener, which frequently 429 with
        # multi-second backoffs. Prune runs *after* discovery, so an unbounded
        # run stalls the whole cycle (up to the 180s watchdog) and delays the
        # next logwatch — i.e. adds minutes of alert latency. Time-box it: a
        # partial prune is fine, the rest resumes next interval.
        budget = float(getattr(cfg, "prune_time_budget_sec", 45) or 0)
        try:
            if budget > 0:
                pruned = await asyncio.wait_for(
                    self._prune_stale_wallets(cfg), timeout=budget
                )
            else:
                pruned = await self._prune_stale_wallets(cfg)
        except asyncio.TimeoutError:
            self._append_log(
                "prune",
                f"ATH prune прерван по бюджету {budget:.0f}s (внешние API тормозят)",
            )
            pruned = 0
        self._last_prune_ts = time.time()
        # Piggyback: keep the outbox small by dropping old delivered rows.
        try:
            self._store.prune_outbox()
        except Exception as exc:  # noqa: BLE001
            logger.debug("outbox prune: %s", exc)
        return pruned

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
        skip_gmgn: bool = False,
    ) -> tuple[list, str]:
        last_seen, deal_count, status = self._store.get_wallet_scan_meta(wallet)
        if status != "watching" or deal_count >= cfg.max_deals:
            return [], "skip"

        # GMGN provides the authoritative sequence *after the watched seed*.
        # Do not turn a wallet's full lifetime history into follow-up deals:
        # deal #1 is the token that caused watch ingestion.
        # Docs key is shared/rate-limited — only use OpenAPI in the scan loop
        # when a paid key is set; otherwise paced Blockscout (accuracy > speed).
        # On circuit / all-keys 429: empty GMGN is NOT "no new buys" — fall
        # through to Blockscout for real scan work (prefer 30–60s accurate
        # cycles over fake ~15s empty ones).
        gmgn_buys: list[GmgnBuy] = []
        gmgn_result: UniqueBuysResult | None = None
        gmgn_attempted = False
        try:
            from .gmgn_portfolio import gmgn_api_configured

            # Whole-cycle circuit open → skip per-wallet GMGN pokes (faster
            # Blockscout pass, less log noise; does not re-open the circuit).
            if gmgn_api_configured() and not skip_gmgn:
                gmgn_attempted = True
                gmgn_result = await fetch_unique_buys(wallet, max_pages=1)
                gmgn_buys = list(gmgn_result.buys)
            elif gmgn_api_configured() and skip_gmgn:
                gmgn_attempted = True
                gmgn_result = UniqueBuysResult(
                    buys=[], ok=False, rate_limited=True
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("GMGN unique buys %s: %s", wallet[:10], exc)
            gmgn_buys = []
            gmgn_attempted = True
            gmgn_result = UniqueBuysResult(
                buys=[], ok=False, rate_limited=False
            )

        seed_deal = next(
            (
                deal
                for deal in self._store.list_deals_for_wallet(wallet)
                if int(deal.get("deal_index") or 0) == 1
            ),
            None,
        )
        seed_token = str((seed_deal or {}).get("token") or "").lower()
        seed_buy = next(
            (
                buy
                for buy in gmgn_buys
                if seed_token
                and buy.token.lower() == seed_token
                and buy.timestamp > 0
            ),
            None,
        )

        use_gmgn = False
        # Without a timestamp that anchors deal #1, GMGN lifetime history
        # cannot be safely distinguished from purchases before we watched it.
        if (
            gmgn_result is not None
            and gmgn_result.ok
            and not gmgn_result.rate_limited
            and seed_buy is not None
        ):
            gmgn_buys = [
                buy
                for buy in gmgn_buys
                if buy.token
                and buy.token.lower() not in QUOTE_TOKENS
                and buy.token.lower() != seed_token
                and buy.timestamp > seed_buy.timestamp
            ][: max(0, int(cfg.max_deals) - 1)]
            # Seed-anchored GMGN answer is authoritative even when there are
            # zero new post-seed buys — do NOT fall through to Blockscout.
            if not gmgn_buys:
                return [], "gmgn"
            use_gmgn = True
        elif gmgn_attempted and (
            gmgn_result is None
            or gmgn_result.rate_limited
            or not gmgn_result.ok
            or seed_buy is None
        ):
            # CRITICAL: empty/429 must NOT look like "no new buys".
            if gmgn_result is not None and gmgn_result.rate_limited:
                reason = "gmgn_empty_due_to_429"
            elif gmgn_result is not None and gmgn_result.ok and seed_buy is None:
                reason = "gmgn_seed_miss"
            else:
                reason = "gmgn_fetch_failed"
            logger.warning(
                "%s, fallback_blockscout wallet=%s",
                reason,
                wallet[:10],
            )
            gmgn_buys = []
        else:
            gmgn_buys = []

        if use_gmgn and gmgn_buys:
            existing_by_token = {
                str(deal["token"]).lower(): deal
                for deal in self._store.list_deals_for_wallet(wallet)
            }

            async def _gmgn_deal_data(
                buy: GmgnBuy,
            ) -> dict[str, Any]:
                token = buy.token.lower()
                existing = existing_by_token.get(token)
                mcap: float | None = (
                    existing.get("mcap_at_buy") if existing else None
                )
                bought_usd: float | None = (
                    existing.get("bought_usd") if existing else buy.cost_usd
                )
                # Persist real block so later Blockscout renumber does not push
                # GMGN rows (block=0) after newer on-chain buys.
                block_number = int(
                    (existing or {}).get("block_number") or 0
                )
                if buy.tx_hash and (mcap is None or block_number <= 0):
                    try:
                        from .replay import estimate_entry_at_tx

                        entry = await estimate_entry_at_tx(
                            token, buy.tx_hash, rpc=rpc
                        )
                        if mcap is None:
                            mcap = entry.mcap
                        if bought_usd is None:
                            bought_usd = entry.bought_usd
                        if entry.block > 0:
                            block_number = int(entry.block)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("GMGN mcap_at_tx failed: %s", exc)
                    if mcap is None:
                        mcap, _price = await estimate_token_quote(token)
                elif mcap is None:
                    mcap, _price = await estimate_token_quote(token)
                return {
                    "token": token,
                    "symbol": buy.symbol,
                    "tx_hash": buy.tx_hash,
                    "bought_usd": bought_usd,
                    "mcap_at_buy": mcap,
                    "block_number": block_number,
                    "bought_at": float(buy.timestamp) if buy.timestamp > 0 else None,
                }

            gmgn_deals = await asyncio.gather(
                *[_gmgn_deal_data(buy) for buy in gmgn_buys]
            )
            inserted = self._store.apply_gmgn_buy_order(
                wallet,
                gmgn_deals,
                max_deals=cfg.max_deals,
            )

            async def _resolve_honeypot(deal: Any) -> str | None:
                try:
                    from .security import honeypot_reason_for_token

                    return await asyncio.wait_for(
                        honeypot_reason_for_token(deal.token),
                        timeout=8.0,
                    )
                except Exception:  # noqa: BLE001
                    return None

            hp_reasons = await asyncio.gather(
                *[_resolve_honeypot(deal) for deal in inserted]
            )
            # Ascending deal_index before the cycle alerter (belt + suspenders).
            paired = list(zip(inserted, hp_reasons))
            return order_deals_for_alerts(paired), "gmgn"

        # GMGN unavailable / rate-limited / seed miss → Blockscout incremental scan.
        source = (
            "blockscout_fallback"
            if gmgn_attempted
            else "blockscout"
        )
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

        if bootstrap:
            # Existing wallet after upgrade: tip watermark only — do not invent
            # deal #2+ from old transfer history.
            self._store.advance_last_seen_block(wallet, max(1, max_block_seen))
            return [], source

        if not candidates:
            if caught_up and max_block_seen > last_seen:
                self._store.advance_last_seen_block(wallet, max_block_seen)
            return [], source

        ordered = sorted(candidates.items(), key=lambda kv: kv[1][3] or 0)
        remaining = cfg.max_deals - deal_count
        out: list[tuple[Any, str | None]] = []
        recorded_blocks: list[int] = []
        stopped_early = False
        for i, (token, (sym, tx, item, block)) in enumerate(ordered):
            if self._stop_requested:
                break
            if remaining <= 0:
                stopped_early = True
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
                block_number=block,
                bought_at=None,
                max_deals=cfg.max_deals,
            )
            if deal:
                out.append((deal, hp_reason))
                remaining -= 1
                if block > 0:
                    recorded_blocks.append(block)
                _, new_count, new_status = self._store.get_wallet_scan_meta(wallet)
                if new_status != "watching" or new_count >= cfg.max_deals:
                    if i < len(ordered) - 1:
                        stopped_early = True
                    break

        # Watermark AFTER recording. Leftover candidates → last recorded block only.
        if caught_up:
            if stopped_early and recorded_blocks:
                self._store.advance_last_seen_block(wallet, max(recorded_blocks))
            elif not stopped_early and max_block_seen > last_seen:
                self._store.advance_last_seen_block(wallet, max_block_seen)
        return out, source


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
