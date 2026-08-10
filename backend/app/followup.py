"""Follow-up runner: watch early buyers for 2nd/3rd new-token buys @ low mcap."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from .blockscout import scan_address_token_transfers
from .buy_gate import (
    is_wallet_initiated_buy,
    method_is_non_buy,
    wallet_sent_quote_in_tx,
)
from .config import settings
from .constants import QUOTE_TOKENS
from .followup_schedule import (
    ScheduleConfig,
    WalletScheduleRow,
    schedule_config_from_followup,
    select_due_batch,
)
from .followup_logwatch import (
    InboundTransfer,
    backfill_deal_chain_times,
    fetch_inbound_transfers,
    fetch_inbound_transfers_result,
    topic_batch_count,
    tx_from_and_input,
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
    resolve_alert_freshness,
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
# Was 50k — lag ~45k stayed on tiny catchup spans forever (plateau).
_HIST_BURST_LAG = 15_000
# After this many hist shrinks on the same window, force-advance (skip_enrich).
_HIST_FORCE_ADVANCE_AFTER = 2
_HIST_MIN_SHRINK_SPAN = 25
# Legacy multiplier (kept for callers); live burst now triggers at enrich_cap
# so 2001–live_span×10 is not a dead zone of tip-skip-with-no-progress.
_LIVE_BURST_BEHIND_MULT = 10


def hist_span_for_lag(
    lag: int, cfg: FollowupConfig, n_wallets: int = 0
) -> int:
    """Block window size for one hist getLogs attempt.

    Large lag → burst span (5k–15k class) when the watchlist is small.
    With hundreds of OR'd wallet topics each eth_getLogs is heavy — cap the
    window so progressive sub-chunks can finish inside the fetch timeout
    instead of timing out the whole 10k window and force-advancing.
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
        base = span_burst
    elif lag_i > span_max * 2:
        base = span_catchup
    else:
        base = span_max

    n = max(0, int(n_wallets or 0))
    if n <= 0:
        return base
    batches = topic_batch_count(n)
    # Single topic batch is cheap enough for full burst/catchup windows.
    if batches <= 1:
        return base
    rpc_chunk = max(
        25, int(getattr(cfg, "logwatch_hist_rpc_chunk", 100) or 100)
    )
    # ≥3 topic batches (400+ wallets): keep each eth_getLogs small, but the
    # *window* must still outrun tip (~10 bl/s). Cap 75 made lag grow forever.
    if batches >= 3:
        rpc_chunk = min(rpc_chunk, 40)
        target_subs = max(8, 32 // batches)  # ~8–10 subcalls → 320–400 bl
        floor = 300
    else:
        rpc_chunk = min(rpc_chunk, 60)
        target_subs = max(5, 20 // batches)
        floor = 200
    cap = max(floor, rpc_chunk * target_subs)
    return min(base, cap)


def live_span_for_watchlist(base_span: int, n_wallets: int) -> int:
    """Shrink tip enrich window when OR'd wallet topics make getLogs slow.

    With 600+ wallets (≥3 topic batches) a 50–100 block tip window never
    finishes under Alchemy load before the 8–12s tip budget — purchases at
    tip stay invisible. Prefer tiny newest-tip windows that complete.
    """
    span = max(8, int(base_span or 300))
    n = max(0, int(n_wallets or 0))
    if n <= 0:
        return span
    batches = topic_batch_count(n)
    if batches <= 1:
        return span
    # 2 batches → ≤40; 3+ → ≤16 (was 100 — always timed out at 634 wallets).
    if batches >= 3:
        return max(8, min(span, 16))
    return max(12, min(span, 40))


async def classify_logwatch_buys(
    transfers: list[InboundTransfer],
    *,
    rpc: Any,
    sender_map: dict[str, str | None],
    senders_ok: bool,
    allow_quote_lookup: bool = True,
    quote_budget_sec: float = 4.0,
    method_map: dict[str, str | None] | None = None,
) -> tuple[list[InboundTransfer], list[InboundTransfer], int]:
    """Split inbound transfers into (buys, uncertain, skipped).

    Strict buy gate (Хвать):
    - Transfer.from must not be an EOA (EOA→wallet = gift/airdrop/P2P)
    - ``tx.from == wallet`` (wallet initiated) **or** wallet spent WETH/USDG in-tx
    - Reject claim/airdrop/plain-transfer selectors when known
    - Never count self-transfers / quote-token noise
    """
    if not transfers:
        return [], [], 0
    if not senders_ok:
        return [], list(transfers), 0

    methods = method_map or {}
    from_addrs = sorted(
        {
            (t.sender or "").strip().lower()
            for t in transfers
            if (t.sender or "").strip()
        }
    )
    eoa_map: dict[str, bool] = {}
    if from_addrs and hasattr(rpc, "batch_is_eoa"):
        try:
            raw = await asyncio.wait_for(rpc.batch_is_eoa(from_addrs), timeout=4.0)
            if isinstance(raw, dict):
                eoa_map = raw
        except Exception as exc:  # noqa: BLE001
            logger.debug("logwatch EOA classify: %s", exc)

    buys: list[InboundTransfer] = []
    uncertain: list[InboundTransfer] = []
    skipped = 0
    need_quote: list[InboundTransfer] = []

    for tr in transfers:
        wallet_l = (tr.wallet or "").strip().lower()
        token_l = (tr.token or "").strip().lower()
        frm = (tr.sender or "").strip().lower()
        if not wallet_l or not token_l or not frm:
            skipped += 1
            continue
        if token_l in QUOTE_TOKENS:
            skipped += 1
            continue
        if frm == wallet_l:
            skipped += 1
            continue
        # Plain transfer / claim / airdrop selectors — never a DEX buy.
        if method_is_non_buy(methods.get(tr.tx_hash.lower())):
            skipped += 1
            continue
        # EOA → wallet = gift / personal transfer / airdrop from person.
        if eoa_map.get(frm) is True:
            skipped += 1
            continue
        tx_from = sender_map.get(tr.tx_hash.lower())
        if tx_from is None:
            uncertain.append(tr)
            continue
        if tx_from == wallet_l:
            buys.append(tr)
            continue
        # Third-party tx.from: only smart-wallet / router-on-behalf with quote.
        if not allow_quote_lookup:
            skipped += 1
            continue
        need_quote.append(tr)

    if need_quote and allow_quote_lookup:
        sem = asyncio.Semaphore(4)
        started = time.time()
        budget = max(1.0, float(quote_budget_sec))

        async def _one(tr: InboundTransfer) -> tuple[InboundTransfer, bool | None]:
            if time.time() - started > budget:
                return tr, None
            async with sem:
                try:
                    left = max(0.3, budget - (time.time() - started))
                    spent = await asyncio.wait_for(
                        wallet_sent_quote_in_tx(tr.wallet, tr.tx_hash),
                        timeout=min(1.5, left),
                    )
                except Exception:  # noqa: BLE001
                    spent = None
                return tr, spent

        results = await asyncio.gather(*[_one(tr) for tr in need_quote])
        for tr, spent in results:
            if spent is True:
                buys.append(tr)
            elif spent is False:
                skipped += 1
            else:
                uncertain.append(tr)

    return buys, uncertain, skipped


def alert_filter_skip_reason(
    deal_index: int,
    mcap_at_buy: float | None,
    *,
    max_mcap_alert: float,
    alert_on_deals: list[int],
    min_mcap_alert: float | None = None,
    bought_usd: float | None = None,
    min_bought_usd: float | None = None,
    max_bought_usd: float | None = None,
) -> str | None:
    """Human reason when ``should_alert_deal`` is False; None if it would alert."""
    if deal_index not in alert_on_deals:
        return f"deal_index={deal_index} not in alert_on_deals"
    if mcap_at_buy is None:
        return "mcap=None"
    mcap = float(mcap_at_buy)
    if mcap > float(max_mcap_alert):
        return f"mcap={mcap:.0f}>max={float(max_mcap_alert):.0f}"
    if min_mcap_alert is not None and mcap < float(min_mcap_alert):
        return f"mcap={mcap:.0f}<min={float(min_mcap_alert):.0f}"
    if bought_usd is not None:
        usd = float(bought_usd)
        if min_bought_usd is not None and usd < float(min_bought_usd):
            return f"bought_usd={usd:.2f}<min={float(min_bought_usd):.2f}"
        if max_bought_usd is not None and usd > float(max_bought_usd):
            return f"bought_usd={usd:.2f}>max={float(max_bought_usd):.2f}"
    elif min_bought_usd is not None:
        return "bought_usd=None with min_bought_usd set"
    return None


def live_burst_span_for_watchlist(cfg: FollowupConfig, n_wallets: int) -> int:
    """Skip-enrich live burst chunk size — never a multi-k window under OR topics."""
    base = max(
        100,
        int(getattr(cfg, "logwatch_live_span", 300) or 300),
    )
    n = max(0, int(n_wallets or 0))
    if n <= 0:
        return min(
            2_000,
            int(getattr(cfg, "logwatch_burst_catchup_span", 10_000) or 10_000),
        )
    batches = topic_batch_count(n)
    if batches >= 3:
        return max(100, min(200, base * 2))
    if batches == 2:
        return max(150, min(400, base * 2))
    return max(300, min(1_000, base * 3))


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
    discovered_at: float | None = None,
) -> bool:
    """True when a deal is recent enough to warrant a Telegram alert.

    Stops live-gap / hist catch-up from blasting old buys as «#2/#3 сейчас».
    GMGN sync rows with neither buy time nor block are never fresh.
    """
    ts = time.time() if now is None else float(now)
    age_limit = max(60.0, float(max_buy_age_sec or 900.0))
    block_limit = max(100, int(max_block_lag or 2_000))
    has_buy_ts = bought_at is not None and float(bought_at) > 0
    has_block = block_number is not None and int(block_number) > 0
    if not has_buy_ts and not has_block:
        # Hist/GMGN ghost with no chain evidence — do not alert.
        return False
    disc_ok = False
    if discovered_at is not None and float(discovered_at) > 0:
        disc_ok = (ts - float(discovered_at)) <= age_limit
    if has_buy_ts:
        age = ts - float(bought_at)
        # Never waive wall-clock buy age via discovered_at — that let GMGN
        # hist fills (hours old) alert as if they were tip buys.
        if age > age_limit:
            return False
    if tip is not None and has_block and int(tip) > 0:
        lag = int(tip) - int(block_number)
        # discovered_at only loosens *block* lag (cursor catch-up recovery).
        eff_block_limit = block_limit
        if disc_ok:
            eff_block_limit = max(block_limit, min(block_limit * 2, 8_000))
        if lag > eff_block_limit:
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


# Ops Telegram must not depend on a healthy followup.db for rate-limits.
# Identical fatal errors (corrupt SQLite) previously re-claimed every cycle
# after a fresh/empty meta table and flooded the chat.
_OPS_FATAL_MARKERS = (
    "file is not a database",
    "database disk image is malformed",
    "malformed database schema",
    "followup db unusable",
    "disk i/o error",
)


def _is_fatal_ops_text(text: str) -> bool:
    low = (text or "").lower()
    return any(m in low for m in _OPS_FATAL_MARKERS)


def _ops_alert_fingerprint(kind: str, text: str) -> str:
    # Collapse whitespace; keep kind + short body so reworded pings stay distinct.
    body = " ".join((text or "").strip().lower().split())
    return f"{kind}|{body[:240]}"


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
        self._last_known_tip = None
        self._last_tip_ts: float = 0.0
        self._last_live_success_ts: float = 0.0
        # In-memory ops rate-limit (survives corrupt/empty SQLite meta).
        self._ops_next_allowed: dict[str, float] = {}
        self._ops_sent_count: dict[str, int] = {}
        self._fatal_error_backoff_until: float = 0.0
        # Last hist pass advanced the catch-up cursor (used to avoid Restored flap).
        self._last_hist_advanced: bool = False
        # Fresh deals missing mcap — retried every live tick (not every 60s).
        self._mcap_micro_retry: list[tuple[str, str, float]] = []
        # Transfers seen during skip_enrich catch-up — drain with enrich+alert
        # on the live path so cursor progress does not silently drop buys.
        # Also durable in SQLite (pending_tip_transfers) across restarts.
        self._pending_skip_transfers: list[tuple[InboundTransfer, float]] = []
        self._pending_skip_loaded = False
        # Short-TTL GMGN unique-buy cache for logwatch rank (wallet → (ts, result)).
        self._gmgn_rank_cache: dict[str, tuple[float, UniqueBuysResult]] = {}
        # Cap concurrent GMGN unique-buy fetches (tip bursts → 429).
        self._gmgn_rank_sem = asyncio.Semaphore(4)
        # Status() polls often — cache outbox COUNT(*) briefly.
        self._outbox_stats_cache: dict[str, int] | None = None
        self._outbox_stats_cache_ts: float = 0.0
        # Serialize pending-tip / mcap-retry mutations across hist+live+maint.
        self._pending_state_lock = threading.Lock()
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
        now = time.time()
        cached = self._outbox_stats_cache
        if cached is not None and (now - self._outbox_stats_cache_ts) < 2.0:
            return cached
        try:
            stats = self._store.outbox_stats()
        except Exception:  # noqa: BLE001
            stats = {"pending": 0, "failed": 0, "sent": 0}
        self._outbox_stats_cache = stats
        self._outbox_stats_cache_ts = now
        return stats

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
        self._load_pending_skip_transfers()
        await asyncio.gather(
            self._hist_loop(),
            self._live_loop(),
            self._maintenance_loop(),
        )

    def _load_pending_skip_transfers(self) -> None:
        """Hydrate in-memory tip_lag queue from SQLite once per process."""
        if self._pending_skip_loaded:
            return
        self._pending_skip_loaded = True
        try:
            rows = self._store.list_pending_tip_transfers(limit=200)
        except Exception as exc:  # noqa: BLE001
            logger.warning("load pending tip transfers: %s", exc)
            return
        items: list[tuple[InboundTransfer, float]] = []
        for r in rows:
            items.append(
                (
                    InboundTransfer(
                        wallet=str(r["wallet"]),
                        token=str(r["token"]),
                        sender=str(r.get("sender") or ""),
                        tx_hash=str(r.get("tx_hash") or ""),
                        block_number=int(r.get("block_number") or 0),
                        bought_at=(
                            float(r["bought_at"])
                            if r.get("bought_at") is not None
                            else 0.0
                        ),
                    ),
                    float(r.get("queued_at") or time.time()),
                )
            )
        if items:
            self._pending_skip_transfers = items
            self._append_log(
                "live",
                f"restored {len(items)} pending tip transfers from DB",
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
                        # Never replace self._lock — a zombie still holding the
                        # old Lock would race a fresh cycle on a new instance.
                        # Next tick skips via acquire timeout while we only
                        # reset RPC pressure so hung RPC waiters can unblock.
                        self._append_log(
                            "watchdog",
                            "отменённый hist-цикл не завершился за 8s — "
                            "сбрасываем RPC sem (lock не трогаем)",
                        )
                        from .chain import reset_followup_rpc_pressure

                        n = reset_followup_rpc_pressure(include_shared=True)
                        if n:
                            self._append_log(
                                "watchdog",
                                f"сброшено {n} followup/shared RPC semaphore(s)",
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
                try:
                    await self._ops_alert(
                        cfg,
                        kind="cycle_error",
                        text=f"⚠️ Follow-up cycle error: {safe}",
                    )
                except Exception as ops_exc:  # noqa: BLE001
                    logger.warning("ops alert after cycle error failed: %s", ops_exc)
                if _is_fatal_ops_text(safe):
                    # Stop the 0s-interval spin that flooded TG with the same
                    # SQLite error while followup.db was unusable.
                    self._fatal_error_backoff_until = max(
                        self._fatal_error_backoff_until, time.time() + 300.0
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
            # Tip soft-failing: do not spin hist every 0–3s just to log pause.
            if self._live_timeout_streak >= 2:
                sleep_for = max(sleep_for, 8.0)
            # Fatal DB / storage errors: hard floor so ops cannot re-fire in a tight loop.
            fatal_left = self._fatal_error_backoff_until - time.time()
            if fatal_left > 0:
                sleep_for = max(sleep_for, min(fatal_left, 300.0))
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
            # Tip getLogs (soft_partial ≤22s) + enrich + TG; keep under watchdog.
            timeout = max(45, int(getattr(cfg, "live_cycle_timeout_sec", 45) or 45))
            task = asyncio.create_task(
                self._live_tick(cfg), name="followup-live"
            )
            self._live_task = task
            try:
                await asyncio.wait_for(task, timeout=float(timeout))
            except asyncio.TimeoutError:
                logger.error("Follow-up live tip hung >%ss — abort tick", timeout)
                self._last_error = f"live tip timeout {timeout}s"
                self._last_message = self._last_error
                self._append_log("watchdog", self._last_message)
                self._live_timeout_streak += 1
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=3.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        # Do NOT swap self._live_lock (same dual-runner hazard
                        # as hist). Only reset followup_live RPC sems — never
                        # shared/token_index (that crashed Watch mid-getLogs).
                        self._append_log(
                            "watchdog",
                            "отменённый live-tick не завершился за 3s — "
                            "сбрасываем followup_live RPC sem (lock не трогаем)",
                        )
                        from .chain import reset_followup_rpc_pressure

                        reset_followup_rpc_pressure(include_shared=False)
                await self._ops_alert(
                    cfg,
                    kind="live_hang",
                    text=(
                        f"⚠️ Follow-up: live tip завис >{timeout}s и был прерван. "
                        "Hist-цикл продолжает работать отдельно."
                    ),
                )
            except asyncio.CancelledError:
                task.cancel()
                raise
            except Exception as exc:  # noqa: BLE001
                from .chain import _redact_exc

                safe = _redact_exc(exc)
                logger.warning("Follow-up live tip failed: %s", safe)
                self._last_error = f"live tip: {safe}"
                self._last_message = f"Live tip ошибка: {safe}"
                self._append_log("live", f"ошибка: {safe}")
                await self._ops_alert(
                    cfg,
                    kind="live_error",
                    text=f"⚠️ Follow-up live tip error: {safe}",
                )
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
                        from .chain import reset_followup_rpc_pressure

                        reset_followup_rpc_pressure(include_shared=True)
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

        # Live tip tick is tip-only; watermark burst + pending drain live here
        # so purchase alerts are never starved by catch-up getLogs.
        if cfg.logwatch_enabled and not self._stop_requested:
            try:
                await asyncio.wait_for(
                    self._live_catchup_pass(cfg, rpc=rpc),
                    timeout=25.0,
                )
            except asyncio.TimeoutError:
                self._append_log("catchup", "live catch-up прерван по бюджету 25s")
            except Exception as exc:  # noqa: BLE001
                logger.debug("live catch-up: %s", exc)

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

        try:
            await asyncio.wait_for(self._live_lock.acquire(), timeout=2.0)
        except asyncio.TimeoutError:
            self._append_log("live", "предыдущий live-tick ещё держит lock — skip")
            return
        try:
            rpc = RpcClient(concurrency=3, sem_scope="followup_live")
            self._active_rpc = rpc.active_rpc_label()
            tick_t0 = time.time()
            # Drain outbox on the fast path so TG lag does not wait for hist.
            try:
                n = await self._dispatch_outbox(
                    cfg, limit=2 if self._live_timeout_streak else 5
                )
                if n:
                    self._last_outbox_dispatched = n
            except Exception as exc:  # noqa: BLE001
                logger.debug("live outbox: %s", exc)
            await self._live_tip_pass(cfg, rpc=rpc)
            # Micro-retry is secondary — never push the tip watchdog over budget.
            if time.time() - tick_t0 < 28.0:
                await self._micro_retry_pending_mcap(cfg, rpc=rpc)
        finally:
            self._live_lock.release()

    async def _ops_alert(
        self,
        cfg: FollowupConfig,
        *,
        kind: str,
        text: str,
    ) -> None:
        """Rate-limited Telegram ops notice (degradation / hang / lag).

        Dedup is primarily **in-memory** so a corrupt/empty followup.db cannot
        reset the cooldown and flood the chat (e.g. ``file is not a database``
        every hist tick). Identical fatal fingerprints are one-shot per process.
        """
        chat = resolve_chat_id(cfg.telegram_chat_id)
        if not telegram_configured(chat):
            return

        now = time.time()
        fp = _ops_alert_fingerprint(kind, text)
        fatal = _is_fatal_ops_text(text)
        sent_n = int(self._ops_sent_count.get(fp, 0))
        next_ok = float(self._ops_next_allowed.get(fp, 0.0))
        if now < next_ok:
            return
        # Already told the operator once about this exact fatal — stay quiet.
        if fatal and sent_n >= 1:
            self._ops_next_allowed[fp] = now + 86_400.0
            return

        base = max(60, int(cfg.ops_alert_cooldown_sec or 600))
        if fatal:
            # One ping, then silence for a day (memory). DB claim is best-effort.
            cooldown = max(float(base), 86_400.0)
        else:
            # Escalating backoff for the same fingerprint: base, 2×, 4×… cap 6h.
            cooldown = min(float(base) * (2 ** min(sent_n, 4)), 6 * 3600.0)

        # Memory claim *before* send — survives SQLite death.
        self._ops_next_allowed[fp] = now + cooldown

        meta_key = f"ops_alert_{kind}_ts"
        db_claimed = False
        try:
            if not self._store.try_claim_ops_alert(meta_key, cooldown_sec=cooldown):
                return
            db_claimed = True
        except Exception as exc:  # noqa: BLE001
            # Corrupt DB: memory gate already holds the slot — still allow the
            # first (and only, if fatal) notification through.
            logger.warning("ops alert meta claim skipped: %s", exc)

        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        try:
            await send_message(chat, text, topic_id=topic_id)
            self._ops_sent_count[fp] = sent_n + 1
            self._append_log("ops", text[:160])
            if fatal:
                self._fatal_error_backoff_until = max(
                    self._fatal_error_backoff_until, now + 300.0
                )
        except Exception as exc:  # noqa: BLE001
            # Transport blip: free the slot soon so a later send can retry,
            # but keep a short floor so a broken bot token cannot spin-spam.
            self._ops_next_allowed[fp] = now + (300.0 if fatal else 60.0)
            if db_claimed:
                try:
                    self._store.release_ops_alert_claim(meta_key)
                except Exception:  # noqa: BLE001
                    pass
            logger.warning("ops alert failed: %s", exc)

    async def run_cycle(
        self,
        cfg: FollowupConfig | None = None,
        *,
        force_all_due: bool = False,
    ) -> None:
        # Acquire the stable lock instance (watchdog must never replace it).
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=5.0)
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
            self._lock.release()

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
        if not cfg.enabled or not cfg.ingest_from_watch:
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

        # Watch ingest only seeds deal #1. Deal #2+ alerts are owned by
        # logwatch + GMGN rank (local renumber invents junk indices).
        seeded = sum(1 for d in inserted if int(d.deal_index or 0) == 1)
        extras = [d for d in inserted if int(d.deal_index or 0) >= 2]
        if extras:
            self._append_log(
                "ingest",
                f"skip watch #2+ alerts ({len(extras)}) — GMGN/logwatch only",
            )
        return seeded if seeded else len(inserted)

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
            from .chain import _redact_exc

            safe = _redact_exc(exc)
            logger.warning("Follow-up outbox dispatch: %s", safe)
            self._last_error = f"outbox: {safe}"
            self._append_log("outbox", f"ошибка: {safe}")

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
            live_ok, live_behind = self._live_tip_healthy()
            # Soft "ok" without cursor progress must not flap Restored↔DEGRADED.
            progress_ok = bool(self._last_hist_advanced) or live_ok
            if was_degraded and progress_ok:
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
            elif had_streak and progress_ok:
                self._append_log(
                    "logwatch",
                    f"transient ok после {had_streak} сбоя(ев) — без DEGRADED",
                )
            if progress_ok:
                self._logwatch_fail_streak = 0
                self._logwatch_degraded = False
            elif was_degraded:
                self._append_log(
                    "logwatch",
                    "hist soft-ok без прогресса — DEGRADED держим "
                    f"(streak={had_streak}, live_behind={live_behind})",
                )
            await self._maybe_alert_cursor_lag(cfg, rpc=rpc)
        else:
            self._logwatch_fail_streak += 1
            threshold = max(1, int(cfg.logwatch_fail_threshold or 8))
            # Ops TG never fires on a single blip — even if config threshold is 1.
            ops_threshold = max(5, threshold)
            live_ok, live_behind = self._live_tip_healthy()
            # Hist-only failure while live covers tip: no DEGRADED, no GMGN stampede.
            if live_ok:
                self._append_log(
                    "fallback",
                    f"hist hard-fail streak={self._logwatch_fail_streak} "
                    f"но live tip ok (behind={live_behind}) — "
                    "без DEGRADED/TG/GMGN",
                )
                # Do not accumulate forever while live is fine.
                self._logwatch_fail_streak = min(
                    self._logwatch_fail_streak, threshold - 1
                )
                self._logwatch_degraded = False
            else:
                became = (
                    not self._logwatch_degraded
                    and self._logwatch_fail_streak >= threshold
                )
                self._logwatch_degraded = self._logwatch_fail_streak >= threshold
                if self._logwatch_degraded:
                    self._append_log(
                        "fallback",
                        f"logwatch DEGRADED (streak={self._logwatch_fail_streak}/"
                        f"{threshold}) → legacy через maintenance; "
                        f"live tip тоже отстаёт (behind={live_behind})",
                    )
                else:
                    self._append_log(
                        "fallback",
                        f"logwatch hard-fail streak={self._logwatch_fail_streak}/"
                        f"{threshold} — без GMGN, курсор не двигаем"
                        f"; live tip отстаёт (behind={live_behind})",
                    )
                # TG only when tip discovery is actually unhealthy.
                if became and self._logwatch_fail_streak >= ops_threshold:
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
                effective_behind is not None and effective_behind <= 3_000
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
                    check_honeypot=bool(
                        getattr(cfg, "alert_skip_honeypot", True) and hp_reason is None
                    ),
                    origin="reconcile",
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

    @staticmethod
    def _alert_origin_from_label(label: str | None) -> str:
        lab = (label or "").strip().lower()
        if lab.startswith("live") or lab in ("tip", "logwatch"):
            return "live"
        if lab.startswith("hist") or "catchup" in lab or "burst" in lab:
            return "history"
        if "drain" in lab or "skip" in lab:
            return "drain"
        if lab in ("reconcile", "legacy"):
            return "reconcile"
        return lab or "history"

    async def _deliver_deal_alert(
        self,
        chat: str,
        *,
        deal: Any,
        topic_id: int | None,
        honeypot_reason: str | None = None,
        check_honeypot: bool = False,
        origin: str = "live",
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
        # Soft DexScreener heuristics on brand-new tokens (no_sells) are often
        # false — enqueue with re-check so outbox can ship once the pair trades.
        # Hard honeypots still burn permanently (anti-spam).
        # Unresolved check (timeout) must also force outbox re-check — otherwise
        # a hard HP ships as a normal alert after enrich timed out.
        skip_hp = bool(getattr(cfg, "alert_skip_honeypot", True))
        outbox_check_honeypot = bool(check_honeypot and skip_hp and reason is None)
        outbox_honeypot_reason = reason
        if reason and skip_hp:
            soft_hp = str(reason).startswith("no_sells") or str(reason).startswith(
                "buy_sell_asymmetry"
            )
            if soft_hp:
                self._append_log(
                    "telegram",
                    f"soft honeypot → outbox recheck #{deal.deal_index} "
                    f"{deal.token_symbol or deal.token[:10]}… ({reason})",
                )
                outbox_honeypot_reason = None
                outbox_check_honeypot = True
            else:
                # Suppress TG; mark notified so pending-retry does not spam.
                self._store.mark_notified(deal.wallet, deal.token)
                self._append_log(
                    "telegram",
                    f"skip honeypot deal #{deal.deal_index} "
                    f"{deal.token_symbol or deal.token[:10]}… ({reason})",
                )
                return False
        elif skip_hp and reason is None and deal.token:
            # Claim-time check was skipped or timed out — dispatch must recheck.
            outbox_check_honeypot = True

        max_age = float(getattr(cfg, "alert_max_buy_age_sec", 900) or 900)
        if not deal_is_fresh_for_alert(
            bought_at=getattr(deal, "bought_at", None),
            block_number=getattr(deal, "block_number", None),
            tip=self._last_known_tip,
            max_buy_age_sec=max_age,
            max_block_lag=int(
                getattr(cfg, "alert_max_block_lag", 4_000) or 4_000
            ),
            discovered_at=getattr(deal, "created_at", None),
        ):
            # Truly old hist fills: suppress permanently. Near-tip lag misses
            # must NOT be burned as notified — leave room for rediscovery.
            bought = getattr(deal, "bought_at", None)
            age = (
                time.time() - float(bought)
                if bought is not None and float(bought) > 0
                else None
            )
            if age is not None and age > max_age * 2:
                self._store.mark_notified(deal.wallet, deal.token)
            self._append_log(
                "telegram",
                f"skip stale deal #{deal.deal_index} "
                f"block={getattr(deal, 'block_number', None)} "
                f"tip={self._last_known_tip}",
            )
            return False

        dedup = self._deal_dedup_key(deal.wallet, deal.token)
        sym = str(getattr(deal, "token_symbol", "") or "").strip()
        if not sym:
            # Fallback label — never claim then burn outbox on missing symbol.
            tok = str(getattr(deal, "token", "") or "")
            sym = f"{tok[:10]}…" if tok else "TOKEN"
            try:
                deal.token_symbol = sym
            except Exception:  # noqa: BLE001
                pass

        # GMGN gate before claim — invent-era local #2 must not enter outbox.
        # Uncertain (429/circuit) still claims: dispatcher re-gates with
        # soft defer (no attempt burn) so tip buys are not aged into suppress.
        probe = {
            "v": 1,
            "kind": "deal",
            "wallet": deal.wallet,
            "token": deal.token,
            "deal_index": int(deal.deal_index),
        }
        action, gated = await self._gate_outbox_deal(probe, cfg)
        if action == "discard":
            self._store.mark_notified(deal.wallet, deal.token)
            self._append_log(
                "telegram",
                f"skip GMGN discard #{deal.deal_index} "
                f"{deal.token_symbol or deal.token[:10]}… ({origin})",
            )
            return False
        if action == "defer":
            self._append_log(
                "telegram",
                f"GMGN uncertain → outbox defer #{deal.deal_index} "
                f"{deal.token_symbol or deal.token[:10]}… ({origin})",
            )
        deal_index = int(
            (gated or {}).get("deal_index")
            if gated is not None
            else deal.deal_index
        )

        payload = json.dumps(
            {
                "v": 1,
                "kind": "deal",
                "chat": chat,
                "wallet": deal.wallet,
                "token": deal.token,
                "token_symbol": sym,
                "token_name": getattr(deal, "token_name", ""),
                "deal_index": deal_index,
                "mcap_at_buy": deal.mcap_at_buy,
                "bought_usd": deal.bought_usd,
                "topic_id": topic_id,
                "honeypot_reason": outbox_honeypot_reason,
                # Soft-HP path forces re-check; otherwise already resolved.
                "check_honeypot": outbox_check_honeypot,
                "origin": (origin or "live").strip().lower() or "live",
                "bought_at": getattr(deal, "bought_at", None),
                "block_number": getattr(deal, "block_number", None),
            }
        )
        if not self._store.claim_and_enqueue_deal(
            deal.wallet, deal.token, dedup_key=dedup, payload=payload
        ):
            return False
        # Best-effort low-latency send; failures stay pending for the dispatcher.
        await self._dispatch_outbox(cfg, limit=1, only_key=dedup)
        return True

    async def _gate_outbox_deal(
        self,
        payload: dict,
        cfg: FollowupConfig,
    ) -> tuple[str, dict | None]:
        """Re-check GMGN before TG so invent-era outbox junk cannot ship.

        Returns ``(action, payload)``:
        - ``ok`` + payload (possibly rewritten deal_index)
        - ``discard`` — mark sent, no Telegram
        - ``defer`` — soft fail / retry (circuit, 429, tip lag)
        """
        if str(payload.get("kind") or "deal") != "deal":
            return "ok", payload
        wallet = str(payload.get("wallet") or "").strip()
        token = str(payload.get("token") or "").strip()
        if not wallet or not token:
            return "discard", None
        claimed = int(payload.get("deal_index") or 0)
        alert_on = {
            int(x) for x in (getattr(cfg, "alert_on_deals", None) or [2, 3, 4, 5])
        }
        verdict = await self._gmgn_rank_verdict(wallet, token, cfg)
        if verdict.uncertain:
            # Blind send = Dora/#2 invent spam. Wait for GMGN.
            return "defer", None
        if verdict.past_max or verdict.reason in (
            "past_max",
            "gmgn_seed_miss_past",
        ):
            self._store.mark_wallet_done(
                wallet, deal_count=int(getattr(cfg, "max_deals", 5) or 5)
            )
            return "discard", None
        rank = verdict.rank
        if rank is None:
            # Defensive: tip not ranked yet should already be uncertain/tip_lag.
            # Never permanently discard — rediscovery / drain can still alert.
            return "defer", None
        if rank not in alert_on:
            if alert_on and rank > max(alert_on):
                self._store.mark_wallet_done(
                    wallet, deal_count=int(getattr(cfg, "max_deals", 5) or 5)
                )
            return "discard", None
        if rank != claimed:
            self._append_log(
                "outbox",
                f"GMGN rewrite #{claimed}→#{rank} {token[:10]}… "
                f"({verdict.reason})",
            )
            return "ok", {**payload, "deal_index": rank}
        return "ok", payload

    async def _send_outbox_payload(self, payload: dict) -> str:
        """Deliver one decoded outbox payload.

        Returns ``sent`` | ``suppress`` | ``defer``. Raises on transport failure.
        Soft honeypot heuristics must ``defer`` (not look like a successful send).
        """
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
                tok = str(payload.get("token") or "")
                sym = f"{tok[:10]}…" if tok else "TOKEN"
                payload = {**payload, "token_symbol": sym}
            cfg_now = self._store.load_config()
            skip_hp = bool(getattr(cfg_now, "alert_skip_honeypot", True))
            if skip_hp:
                reason = payload.get("honeypot_reason")
                # Prefer explicit flag; default re-check when reason unknown so
                # claim-time timeouts cannot ship a hard honeypot as normal.
                should_recheck = bool(payload.get("check_honeypot")) or not reason
                if not reason and should_recheck and payload.get("token"):
                    try:
                        from .security import honeypot_reason_for_token

                        reason = await asyncio.wait_for(
                            honeypot_reason_for_token(payload["token"]),
                            timeout=8.0,
                        )
                    except Exception:  # noqa: BLE001
                        reason = None
                if reason:
                    soft_hp = str(reason).startswith("no_sells") or str(
                        reason
                    ).startswith("buy_sell_asymmetry")
                    logger.info(
                        "outbox %s honeypot %s (%s)",
                        "defer soft" if soft_hp else "skip",
                        str(payload.get("token", ""))[:12],
                        reason,
                    )
                    return "defer" if soft_hp else "suppress"
            max_age = float(getattr(cfg_now, "alert_max_buy_age_sec", 900) or 900)
            bought_at = (
                float(payload["bought_at"])
                if payload.get("bought_at") is not None
                else None
            )
            block_number = (
                int(payload["block_number"])
                if payload.get("block_number") is not None
                else None
            )
            queued_at = (
                float(payload["_queued_at"])
                if payload.get("_queued_at") is not None
                else None
            )
            has_buy_evidence = (
                (bought_at is not None and bought_at > 0)
                or (block_number is not None and int(block_number) > 0)
            )
            stale = False
            if has_buy_evidence:
                stale = not deal_is_fresh_for_alert(
                    bought_at=bought_at,
                    block_number=block_number,
                    tip=self._last_known_tip,
                    max_buy_age_sec=max_age,
                    max_block_lag=int(
                        getattr(cfg_now, "alert_max_block_lag", 4_000) or 4_000
                    ),
                )
            elif queued_at and (time.time() - float(queued_at)) > max_age * 2:
                stale = True
            if stale:
                logger.info(
                    "outbox suppress stale %s deal #%s",
                    str(payload.get("token", ""))[:12],
                    payload.get("deal_index"),
                )
                return "suppress"
            freshness = resolve_alert_freshness(
                origin=str(payload.get("origin") or "") or None,
                bought_at=bought_at,
                block_number=block_number,
                tip=self._last_known_tip,
                queued_at=queued_at,
            )
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
                honeypot_reason=None,
                # Last-line defense if outbox HP timed out above.
                check_honeypot=skip_hp,
                skip_honeypot=skip_hp,
                freshness=freshness,
            )
            return "sent"
        if kind == "ops":
            await send_message(
                payload["chat"], payload["text"], topic_id=payload.get("topic_id")
            )
            return "sent"
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
        Concurrent live+hist dispatch is serialized via lease (``sending``).
        """
        if not getattr(cfg, "outbox_enabled", True):
            return 0
        batch = int(limit if limit is not None else cfg.outbox_dispatch_batch)
        if only_key is not None:
            rows = self._store.claim_due_outbox(
                now=now, limit=max(1, batch), dedup_key=only_key
            )
        else:
            rows = self._store.claim_due_outbox(now=now, limit=batch)
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
            delivered = False
            try:
                action, gated = await self._gate_outbox_deal(payload, cfg)
                if action == "discard":
                    self._store.mark_outbox_sent(oid)
                    self._append_log(
                        "outbox",
                        f"discard invent/past-max "
                        f"{str(payload.get('token') or '')[:10]}… "
                        f"claimed=#{payload.get('deal_index')}",
                    )
                    continue
                # Soft-defer rows can sit for hours — drop once buy is too old.
                # Missing buy evidence: fall back to outbox queue age only.
                max_age = float(getattr(cfg, "alert_max_buy_age_sec", 900) or 900)
                bought_at = (
                    float(payload["bought_at"])
                    if payload.get("bought_at") is not None
                    else None
                )
                block_number = (
                    int(payload["block_number"])
                    if payload.get("block_number") is not None
                    else None
                )
                queued_at = float(row.get("created_at") or 0) or None
                has_buy_evidence = (
                    (bought_at is not None and bought_at > 0)
                    or (block_number is not None and int(block_number) > 0)
                )
                stale = False
                if has_buy_evidence:
                    stale = not deal_is_fresh_for_alert(
                        bought_at=bought_at,
                        block_number=block_number,
                        tip=self._last_known_tip,
                        max_buy_age_sec=max_age,
                        max_block_lag=int(
                            getattr(cfg, "alert_max_block_lag", 4_000) or 4_000
                        ),
                    )
                elif queued_at and (time.time() - queued_at) > max_age * 2:
                    stale = True
                if stale:
                    self._store.mark_outbox_sent(oid)
                    self._append_log(
                        "outbox",
                        f"suppress stale "
                        f"{str(payload.get('token') or '')[:10]}… "
                        f"#{payload.get('deal_index')}",
                    )
                    continue
                if action == "defer":
                    # Soft defer — do NOT burn attempts (circuit/429/tip_lag).
                    backoff = 20.0
                    self._store.mark_outbox_deferred(
                        oid,
                        error="gmgn gate defer (uncertain)",
                        next_attempt_at=time.time() + backoff,
                    )
                    self._append_log(
                        "outbox",
                        f"defer GMGN gate "
                        f"{str(payload.get('token') or '')[:10]}… "
                        f"(retry in {backoff:.0f}s)",
                    )
                    continue
                result = await self._send_outbox_payload(
                    {
                        **payload,
                        **(gated or {}),
                        "_queued_at": float(row.get("created_at") or 0)
                        or None,
                    }
                )
                if result == "defer":
                    backoff = 45.0
                    self._store.mark_outbox_deferred(
                        oid,
                        error="soft honeypot defer",
                        next_attempt_at=time.time() + backoff,
                    )
                    continue
                # sent or hard-honeypot/stale suppress — release lease either way.
                delivered = result == "sent"
                self._store.mark_outbox_sent(oid)
                if result == "sent":
                    sent += 1
            except asyncio.CancelledError:
                # CancelledError is BaseException — release lease unless TG
                # already completed (avoid double-send after HTTP ok).
                if not delivered:
                    self._store.mark_outbox_deferred(
                        oid,
                        error="cancelled during outbox",
                        next_attempt_at=time.time() + 5.0,
                    )
                else:
                    self._store.mark_outbox_sent(oid)
                raise
            except Exception as exc:  # noqa: BLE001
                # Real TG/transport failures raise; honeypot uses return codes
                # or honeypot_suppress from send_followup_deal last-line check.
                msg = str(exc)
                if msg.startswith("honeypot_suppress:"):
                    reason = msg.split(":", 1)[-1]
                    soft_hp = reason.startswith("no_sells") or reason.startswith(
                        "buy_sell_asymmetry"
                    )
                    if soft_hp:
                        self._store.mark_outbox_deferred(
                            oid,
                            error=f"soft honeypot defer ({reason})",
                            next_attempt_at=time.time() + 45.0,
                        )
                    else:
                        self._store.mark_outbox_sent(oid)
                    continue
                attempts = int(row.get("attempts", 0)) + 1
                backoff = min(30.0 * (2 ** attempts), 3600.0)
                safe = msg[:300]
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
        self._outbox_stats_cache = None
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
            # Soft-HP tip defer leaves notified=0; re-check honeypot on retry
            # so hard honeypots cannot slip through after enrich timeout.
            ok = await self._deliver_deal_alert(
                chat,
                deal=deal,
                topic_id=topic_id,
                check_honeypot=bool(getattr(cfg, "alert_skip_honeypot", True)),
                origin="pending_retry",
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
        with self._pending_state_lock:
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
        with self._pending_state_lock:
            if not self._mcap_micro_retry:
                return
            pending = list(self._mcap_micro_retry)
            self._mcap_micro_retry = []
        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        sent = 0
        batch = pending[:8]
        leftovers = pending[8:]
        processed = 0
        try:
            for wallet, token, _ts in batch:
                if self._stop_requested:
                    break
                processed += 1
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
                    chat,
                    deal=deal,
                    topic_id=topic_id,
                    check_honeypot=bool(getattr(cfg, "alert_skip_honeypot", True)),
                    origin="micro_retry",
                )
                if ok:
                    sent += 1
                    self._append_log(
                        "telegram",
                        f"micro-retry deal #{deal.deal_index} · {deal.wallet[:10]}…",
                    )
        finally:
            # Never drop the tail or unprocessed head (stop / cancel mid-loop).
            for wallet, token, _ts in batch[processed:] + leftovers:
                self._queue_mcap_micro_retry(wallet, token)
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
        tip_verified = False
        try:
            tip = int(await asyncio.wait_for(rpc.block_number(), timeout=8.0))
            tip_verified = True
        except TimeoutError:
            from .chain import reset_followup_rpc_pressure

            reset_followup_rpc_pressure()
            try:
                rpc._prefer_non_alchemy()  # noqa: SLF001
                rpc._bind_url(rpc.rpc_url)  # noqa: SLF001
            except Exception:  # noqa: BLE001
                pass
            self._append_log("live", "block_number timeout")
            try:
                tip = int(await asyncio.wait_for(rpc.block_number(), timeout=8.0))
                tip_verified = True
                self._append_log(
                    "live",
                    f"block_number ok after non-Alchemy failover "
                    f"tip={tip}",
                )
            except TimeoutError:
                # Stale tip: estimate forward so we do not rescan the same
                # 4 dead blocks forever while RPC is wedged.
                if self._last_known_tip is None:
                    return False
                verified_age = time.time() - float(self._last_tip_ts or 0)
                if verified_age > 300.0:
                    self._append_log(
                        "live",
                        f"block_number dead >{verified_age:.0f}s — tip skip",
                    )
                    return False
                # Robinhood L2 is fast; ~2 blk/s is conservative vs observed.
                base = int(self._last_known_tip)
                tip = base + max(0, int(verified_age * 2.0))
                self._append_log(
                    "live",
                    f"block_number timeout — estimated tip={tip} "
                    f"(+{tip - base} from verified {base})",
                )
        if tip_verified:
            self._last_known_tip = tip
            self._last_tip_ts = time.time()
        # else: tip is a scan-only estimate — keep verified tip/ts untouched
        conf = max(0, int(cfg.logwatch_confirmations or 0))
        safe_tip = max(0, tip - conf)
        base_span = max(8, int(getattr(cfg, "logwatch_live_span", 300) or 300))
        base_span = live_span_for_watchlist(base_span, len(watching))
        batches = topic_batch_count(len(watching))
        if self._live_timeout_streak >= 1:
            # Soft-fail → shrink harder + leave Alchemy (public RPC often OK).
            floor = 4 if batches >= 3 else 8
            span = min(base_span, floor)
            self._live_span_backoff = span
            try:
                rpc._prefer_non_alchemy()  # noqa: SLF001
                rpc._bind_url(rpc.rpc_url)  # noqa: SLF001
                self._active_rpc = rpc.active_rpc_label()
            except Exception:  # noqa: BLE001
                pass
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

        live_cursor_i = int(live_cursor)
        behind = max(0, safe_tip - live_cursor_i)
        enrich_cap = max(
            2 * max(50, base_span),
            int(getattr(cfg, "live_gap_enrich_max_blocks", 3_000) or 3_000),
        )
        _ = _LIVE_BURST_BEHIND_MULT  # retained for API/compat
        # Tip path must finish in a few seconds — never share the tick with
        # burst/gap/drain (those starved TG alerts and tripped the 45s watchdog).
        # soft_partial tip fetch uses per-batch timeouts; keep outer budget tight.
        if self._live_timeout_streak >= 2:
            fetch_timeout = 18.0 if batches >= 3 else 14.0
        else:
            fetch_timeout = 14.0 if batches >= 3 else 12.0
        enrich_budget = float(getattr(cfg, "live_enrich_budget_sec", 3.0) or 3.0)
        enrich_budget = max(3.0, min(6.0, enrich_budget))

        # Far behind: jump watermark only when tip is healthy. Jumping while
        # soft-failing skips blocks that hist cannot cover (hist is paused).
        if behind > enrich_cap and self._live_timeout_streak < 2:
            target = max(live_cursor_i, safe_tip - max(100, enrich_cap))
            if target > live_cursor_i:
                await self._queue_before_live_jump(
                    cfg,
                    rpc=rpc,
                    watching=watching,
                    from_block=live_cursor_i + 1,
                    to_block=target,
                    safe_tip=safe_tip,
                    label="live_jump",
                    fetch_timeout=min(8.0, fetch_timeout),
                )
                self._store.set_logwatch_live_cursor(target)
                self._append_log(
                    "live",
                    f"watermark jump {live_cursor_i}→{target} "
                    f"(behind was {behind}; tip enrich continues)",
                    percent=8,
                )
                live_cursor_i = target
                behind = max(0, safe_tip - live_cursor_i)
        elif behind > enrich_cap and self._live_timeout_streak >= 2:
            self._append_log(
                "live",
                f"watermark hold (behind={behind}, tip soft-fail "
                f"streak={self._live_timeout_streak})",
            )

        tip_scan_from = tip_from
        contiguous = live_cursor_i + 1 >= tip_from
        if contiguous:
            tip_scan_from = live_cursor_i + 1
        if tip_scan_from > safe_tip:
            self._last_live_success_ts = time.time()
            return True

        # Single tip window only — newest-first multi-window previously advanced
        # the contiguous cursor over unscanned older tip blocks (missed buys).
        self._append_log(
            "live",
            f"tip {tip_scan_from}…{safe_tip} "
            f"(wallets={len(watching)}, span={safe_tip - tip_scan_from + 1}, "
            f"behind={behind})",
            percent=10,
        )
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
            soft_partial=batches >= 2,
        )
        if res is None:
            return False
        total_new = int(res.get("new_deals") or 0)
        total_alerts = int(res.get("alerts") or 0)
        incomplete = bool(res.get("incomplete"))
        fetched = int(res.get("fetched") or 0)
        if not res.get("advanced"):
            # Partial topic batches may still have produced alerts — count as
            # tip progress so hist unpauses, but do not advance the cursor.
            if total_new or total_alerts:
                self._live_timeout_streak = 0
                if behind <= 2_000:
                    self._last_live_success_ts = time.time()
                self._last_new_deals = total_new
                self._last_alerts_sent = total_alerts
                self._last_message = (
                    f"live tip partial: {total_new} сделок, "
                    f"{total_alerts} алертов"
                )
                self._append_log("live", self._last_message, percent=100)
                if not contiguous and behind <= enrich_cap:
                    park = max(live_cursor_i, tip_from - 1)
                    if park > live_cursor_i:
                        self._store.set_logwatch_live_cursor(park)
                return True
            if incomplete and fetched > 0:
                # Got real logs from some batches (all filtered) — RPC ok.
                self._live_timeout_streak = 0
                if behind <= 2_000:
                    self._last_live_success_ts = time.time()
                self._append_log(
                    "live",
                    f"tip partial getLogs — {fetched} transfers filtered, "
                    "cursor hold",
                )
                if not contiguous and behind <= enrich_cap:
                    park = max(live_cursor_i, tip_from - 1)
                    if park > live_cursor_i:
                        self._store.set_logwatch_live_cursor(park)
                return True
            if incomplete and fetched == 0:
                # Empty + failed batches: do NOT reset streak — that kept hist
                # hammering Alchemy while tip stuck on a stale tip window.
                # Do NOT park the live watermark here: parking skips the gap
                # while hist is paused → tip buys in the gap age out unseen.
                self._live_timeout_streak += 1
                self._append_log(
                    "live",
                    f"tip partial empty — cursor hold "
                    f"(streak={self._live_timeout_streak})",
                )
                return True
            self._live_timeout_streak += 1
            self._append_log(
                "live",
                f"getLogs soft-fail — tip cursor не двигаем "
                f"(streak={self._live_timeout_streak})",
            )
            # Soft-fail must NOT stamp healthy — otherwise hist/drain stay
            # paused while ops think tip is fine and queued alerts age out.
            return True

        self._live_timeout_streak = 0
        if contiguous or behind <= 2_000:
            self._last_live_success_ts = time.time()
        if contiguous:
            advance_to = res.get("advance_to")
            if advance_to is not None and int(advance_to) > live_cursor_i:
                self._store.set_logwatch_live_cursor(int(advance_to))
            else:
                self._store.set_logwatch_live_cursor(safe_tip)
        elif behind <= enrich_cap:
            # Near tip but not contiguous: park live cursor at tip-enrich edge
            # so next tick becomes contiguous without waiting on hist.
            park = max(live_cursor_i, tip_from - 1)
            if park > live_cursor_i:
                self._store.set_logwatch_live_cursor(park)

        if total_new or total_alerts:
            self._last_new_deals = total_new
            self._last_alerts_sent = total_alerts
            self._last_message = (
                f"live tip: {total_new} сделок, {total_alerts} алертов"
            )
            self._append_log("live", self._last_message, percent=100)
        return True

    async def _live_catchup_pass(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
    ) -> None:
        """Off-tip watermark catch-up + pending drain (never on live tip tick)."""
        watching = self._store.list_watching()
        if not watching:
            return
        tip_soft = self._live_timeout_streak >= 2
        # Always drain already-queued tip/uncertain transfers — pausing drain
        # while tip soft-fails ages out GMGN tip_lag alerts silently.
        if self._pending_skip_transfers:
            try:
                await self._drain_pending_skip_transfers(
                    cfg,
                    rpc=rpc,
                    enrich_budget_sec=float(
                        getattr(cfg, "live_enrich_budget_sec", 3.0) or 3.0
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("maint pending drain: %s", exc)
        if tip_soft:
            self._append_log(
                "live",
                f"catchup burst pause — tip soft-fail streak="
                f"{self._live_timeout_streak}",
            )
            return
        try:
            tip = int(await asyncio.wait_for(rpc.block_number(), timeout=6.0))
            self._last_known_tip = tip
            self._last_tip_ts = time.time()
        except Exception:  # noqa: BLE001
            if self._last_known_tip is None:
                return
            tip = int(self._last_known_tip)
        conf = max(0, int(cfg.logwatch_confirmations or 0))
        safe_tip = max(0, tip - conf)
        base_span = max(8, int(getattr(cfg, "logwatch_live_span", 300) or 300))
        base_span = live_span_for_watchlist(base_span, len(watching))
        enrich_cap = max(
            2 * max(50, base_span),
            int(getattr(cfg, "live_gap_enrich_max_blocks", 3_000) or 3_000),
        )
        live_cursor = self._store.get_logwatch_live_cursor()
        if live_cursor is None:
            return
        behind = max(0, safe_tip - int(live_cursor))
        if behind > enrich_cap:
            await self._live_burst_skip_enrich(
                cfg,
                rpc=rpc,
                watching=watching,
                safe_tip=safe_tip,
                enrich_cap=enrich_cap,
                fetch_timeout=10.0,
            )

    async def _live_burst_skip_enrich(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        watching: list[str],
        safe_tip: int,
        enrich_cap: int,
        fetch_timeout: float,
    ) -> None:
        """Multi-chunk skip_enrich toward tip when live watermark is far behind.

        Leaves the last ``enrich_cap`` blocks for tip enrich+alert. No Telegram
        (skip_enrich). Force-advances stuck windows via hist shrink helper.
        """
        try:
            rpc._prefer_non_alchemy()  # noqa: SLF001
        except Exception:  # noqa: BLE001
            pass
        live_now = int(self._store.get_logwatch_live_cursor() or 0)
        target = max(live_now, safe_tip - max(100, enrich_cap))
        if live_now >= target:
            return
        behind0 = max(0, safe_tip - live_now)
        # Far behind: one watermark jump to the enrich window. Crawling +200
        # after getLogs timeouts never beats tip (~10 bl/s) with 500+ wallets.
        if behind0 > max(4_000, int(enrich_cap) * 2):
            await self._queue_before_live_jump(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=live_now + 1,
                to_block=target,
                safe_tip=safe_tip,
                label="burst_jump",
                fetch_timeout=min(8.0, fetch_timeout),
            )
            self._store.set_logwatch_live_cursor(target)
            self._append_log(
                "live",
                f"burst watermark jump {live_now}→{target} "
                f"(behind was {behind0}, skip_enrich, no TG)",
                percent=15,
            )
            if max(0, safe_tip - target) <= 2_000:
                self._last_live_success_ts = time.time()
            return
        # Watchlist-sized burst chunks (not multi-k windows that always 15s-timeout).
        burst_span = live_burst_span_for_watchlist(cfg, len(watching))
        max_chunks = max(
            3,
            min(10, int(getattr(cfg, "logwatch_catchup_chunks_per_pass", 4) or 4) * 2),
        )
        # Leave headroom for tip-enrich on the same live tick / next tick.
        budget = min(
            16.0,
            float(getattr(cfg, "live_cycle_timeout_sec", 45) or 45) * 0.35,
        )
        t0 = time.time()
        chunks = 0
        advanced_total = 0
        while live_now < target and chunks < max_chunks and (time.time() - t0) < budget:
            if self._stop_requested:
                break
            gap_from = live_now + 1
            gap_to = min(target, gap_from + burst_span - 1)
            if gap_to < gap_from:
                break
            behind = max(0, safe_tip - live_now)
            self._append_log(
                "live",
                f"burst {gap_from}…{gap_to} "
                f"(behind={behind}, skip_enrich, chunk={chunks + 1}/{max_chunks})",
                percent=12 + chunks * 5,
            )
            remain = budget - (time.time() - t0)
            win_timeout = min(fetch_timeout, max(6.0, remain - 1.0))
            res = await self._hist_scan_shrink_retry(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=gap_from,
                to_block=gap_to,
                fetch_timeout=win_timeout,
                cursor_floor=live_now,
                label=f"live_burst#{chunks + 1}",
                force_after=1,
                min_span=50,
            )
            chunks += 1
            if not res or not res.get("advanced") or res.get("advance_to") is None:
                # Jump toward tip enrich window — not a tiny +200 crawl.
                behind_now = max(0, safe_tip - live_now)
                step = max(burst_span, min(2_000, max(200, behind_now - enrich_cap)))
                jump_to = min(target, live_now + step)
                if jump_to > live_now:
                    self._append_log(
                        "live",
                        f"burst RPC dead — jump live {live_now}→{jump_to} "
                        f"(skip_enrich, no TG)",
                    )
                    self._store.set_logwatch_live_cursor(jump_to)
                    advanced_total += jump_to - live_now
                    live_now = jump_to
                    continue
                break
            adv = int(res["advance_to"])
            if adv <= live_now:
                behind_now = max(0, safe_tip - live_now)
                step = max(burst_span, min(2_000, max(200, behind_now - enrich_cap)))
                jump_to = min(target, live_now + step)
                if jump_to > live_now:
                    self._store.set_logwatch_live_cursor(jump_to)
                    advanced_total += jump_to - live_now
                    live_now = jump_to
                    continue
                break
            self._store.set_logwatch_live_cursor(adv)
            advanced_total += adv - live_now
            live_now = adv
            if max(0, safe_tip - live_now) <= 2_000:
                self._last_live_success_ts = time.time()
        if advanced_total:
            self._append_log(
                "live",
                f"burst +{advanced_total} блоков → live={live_now} "
                f"(behind={max(0, safe_tip - live_now)})",
                percent=40,
            )
        # Do NOT reset tip soft-fail streak here — burst success must not
        # re-inflate tip span while tip getLogs is still timing out.

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
        self._last_hist_advanced = False
        # Tip purchase alerts are primary, but pausing hist entirely while tip
        # soft-fails made cursor lag explode (ops spam). Keep skip_enrich
        # catch-up alive with a reduced budget.
        hist_soft = self._live_timeout_streak >= 2
        if hist_soft:
            self._append_log(
                "logwatch",
                f"hist soft-mode — tip soft-fail streak="
                f"{self._live_timeout_streak}",
            )
        if not watching:
            try:
                tip = int(
                    await asyncio.wait_for(rpc.block_number(), timeout=12.0)
                )
                self._last_known_tip = tip
                self._last_tip_ts = time.time()
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
            self._last_tip_ts = time.time()
        except TimeoutError as exc:
            from .chain import reset_followup_rpc_pressure

            reset_followup_rpc_pressure()
            # Rebind client sem to the fresh pool (instance still holds old obj).
            try:
                rpc._prefer_non_alchemy()  # noqa: SLF001
                rpc._bind_url(rpc.rpc_url)  # noqa: SLF001
            except Exception:  # noqa: BLE001
                pass
            self._append_log(
                "logwatch",
                "block_number timeout — сброс followup/shared sem, повтор",
            )
            try:
                tip = int(
                    await asyncio.wait_for(rpc.block_number(), timeout=12.0)
                )
                self._last_known_tip = tip
                self._last_tip_ts = time.time()
            except TimeoutError:
                live_ok, _ = self._live_tip_healthy()
                # Use recent tip so hist can still force-advance stuck windows.
                if (
                    self._last_known_tip is not None
                    and (time.time() - self._last_tip_ts) <= 180.0
                ):
                    tip = int(self._last_known_tip)
                    self._append_log(
                        "logwatch",
                        f"block_number timeout — stale tip={tip} "
                        "(hist catch-up продолжаем)",
                    )
                elif live_ok:
                    self._append_log(
                        "logwatch",
                        "block_number timeout — live ok, soft backoff "
                        "(без DEGRADED streak)",
                    )
                    await asyncio.sleep(1.5)
                    self._last_hist_advanced = False
                    return True
                else:
                    self._append_log("logwatch", f"block_number timeout: {exc}")
                    self._last_hist_advanced = False
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
            self._last_hist_advanced = True
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
        live_span = live_span_for_watchlist(live_span, len(watching))
        # Catch-up before the ops alert (6k). Lag 4–6k with catching_up=False
        # used to run ONE hist chunk then break → tip outruns forever.
        alert_thr = max(
            3_000, int(getattr(cfg, "cursor_lag_alert_blocks", 6_000) or 6_000)
        )
        catching_up = lag > max(4_000, alert_thr // 2)

        # Hist skip_enrich does not record deals (GMGN rank is authoritative for
        # alerts). Blocks older than alert_max_block_lag are also TG-stale.
        # Fast-forward the watermark past that floor so ops lag clears instead
        # of crawling forever under a 500+ wallet topic OR.
        alert_lag = max(
            500, int(getattr(cfg, "alert_max_block_lag", 2_000) or 2_000)
        )
        stale_floor = max(0, safe_tip - alert_lag - live_span)
        if catching_up and cursor < stale_floor and lag >= 4_000:
            self._store.set_logwatch_cursor(stale_floor)
            jumped = stale_floor - cursor
            self._append_log(
                "logwatch",
                f"hist fast-forward +{jumped} → {stale_floor} "
                f"(stale past alert_lag={alert_lag}, skip_enrich)",
                percent=8,
            )
            cursor = stale_floor
            lag = max(0, safe_tip - cursor)
            self._cursor_lag_blocks = tip - cursor
            catching_up = lag > max(4_000, alert_thr // 2)
            self._last_hist_advanced = True

        # Dual-jump: if live is also far behind, snap it to the enrich window
        # now — do not wait for the live loop under shared RPC pressure.
        live_now = self._store.get_logwatch_live_cursor()
        if live_now is not None:
            live_behind = max(0, safe_tip - int(live_now))
            enrich_cap = max(
                2 * live_span,
                int(getattr(cfg, "live_gap_enrich_max_blocks", 2_000) or 2_000),
            )
            if live_behind > max(4_000, int(enrich_cap) * 2):
                live_target = max(int(live_now), safe_tip - max(100, enrich_cap))
                if live_target > int(live_now):
                    await self._queue_before_live_jump(
                        cfg,
                        rpc=rpc,
                        watching=watching,
                        from_block=int(live_now) + 1,
                        to_block=live_target,
                        safe_tip=safe_tip,
                        label="dual_jump",
                        fetch_timeout=8.0,
                    )
                    self._store.set_logwatch_live_cursor(live_target)
                    self._append_log(
                        "logwatch",
                        f"live dual-jump {live_now}→{live_target} "
                        f"(behind was {live_behind}, skip_enrich, no TG)",
                        percent=9,
                    )
                    # Do NOT stamp _last_live_success_ts — no tip getLogs ran.
                    # Fake success suppressed DEGRADED/legacy while gap buys
                    # were skipped.

        if cursor >= safe_tip:
            self._last_checked = len(watching)
            self._last_message = (
                f"hist up-to-date cursor={cursor} tip={tip}"
            )
            self._append_log("logwatch", self._last_message, percent=100)
            self._last_hist_advanced = True
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
        # Many OR'd wallets → tiny spans; need more chunks/pass to clear lag.
        if topic_batch_count(len(watching)) >= 3 and lag > 3_000:
            max_chunks = max(max_chunks, 8)
        budget = float(
            getattr(cfg, "logwatch_catchup_time_budget_sec", 90.0) or 90.0
        )
        cycle_cap = float(cfg.cycle_timeout_sec or 180) * 0.7
        budget = min(max(budget, 60.0), cycle_cap)
        if hist_soft:
            # Tip is struggling — still clear hist lag, but don't hog RPC.
            budget = min(budget, 35.0)
            max_chunks = min(max_chunks, 4)
        t0 = time.time()

        base_fetch_timeout = float(
            getattr(cfg, "logwatch_fetch_timeout_sec", 45) or 45
        )
        # Per-window timeout: progressive sub-chunks use this as a budget.
        fetch_timeout = min(base_fetch_timeout, 45.0)
        if lag > _HIST_BURST_LAG:
            fetch_timeout = min(max(base_fetch_timeout, 35.0), 60.0)
        if topic_batch_count(len(watching)) >= 3:
            fetch_timeout = max(fetch_timeout, 40.0)

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
            # Tip soft-failed mid-pass (started healthy) — yield RPC. Soft-mode
            # hist already entered with a reduced budget; do not abort it here.
            if not hist_soft and self._live_timeout_streak >= 2:
                self._append_log(
                    "logwatch",
                    f"hist mid-pass pause — tip soft-fail streak="
                    f"{self._live_timeout_streak}",
                )
                break
            cursor = self._store.get_logwatch_cursor() or cursor
            lag = max(0, safe_tip - cursor)
            self._cursor_lag_blocks = tip - cursor
            if cursor >= safe_tip:
                break

            span = hist_span_for_lag(lag, cfg, n_wallets=len(watching))
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
            # Soft fail with no advance: near tip / inside alert horizon —
            # stop and retry (never skip unscanned alertable blocks). Far
            # behind past alert_max_block_lag — force-advance so tip cannot
            # outrun forever under a stuck OR'd getLogs window.
            if not hist_res.get("advanced"):
                high_lag = lag > max(4_000, alert_thr // 2)
                past_alert = cursor < max(0, safe_tip - alert_lag)
                if not (high_lag and past_alert):
                    # Near tip / inside alert horizon: never skip unscanned
                    # blocks, but durable-queue a tip-adjacent slice so live
                    # drain can still alert while getLogs is wedged.
                    try:
                        queued = await self._queue_before_live_jump(
                            cfg,
                            rpc=rpc,
                            watching=watching,
                            from_block=from_block,
                            to_block=to_block,
                            safe_tip=safe_tip,
                            label="hist_soft_near_tip",
                            fetch_timeout=min(8.0, win_timeout),
                        )
                        if queued:
                            self._append_log(
                                "logwatch",
                                f"hist soft-fail near tip — queued {queued} "
                                f"transfers [{from_block}…{to_block}]",
                            )
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("hist near-tip queue: %s", exc)
                    break
                step = max(
                    300,
                    int(hist_span_for_lag(lag, cfg, n_wallets=len(watching))),
                )
                # Cap at the alert floor — never jump into the TG horizon.
                alert_floor = max(0, safe_tip - alert_lag)
                jump_to = min(alert_floor, cursor + step)
                live_c = self._store.get_logwatch_live_cursor()
                if live_c is not None:
                    jump_to = min(jump_to, max(live_floor, int(live_c)))
                if jump_to <= cursor:
                    break
                self._store.set_logwatch_cursor(jump_to)
                any_advance = True
                last_to = jump_to
                self._append_log(
                    "logwatch",
                    f"hist soft-fail force-advance {cursor}→{jump_to} "
                    f"(lag={lag}, keep multi-chunk)",
                )
                continue
            # Only stop multi-chunk when near tip — the old lag<=9k break
            # aborted catch-up after ONE chunk while tip kept growing.
            near_tip = max(0, safe_tip - (self._store.get_logwatch_cursor() or cursor))
            if near_tip <= max(2_000, int(live_span) * 3):
                break

        self._last_hist_advanced = any_advance
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
        force_after: int | None = None,
        min_span: int | None = None,
    ) -> dict[str, Any] | None:
        """Scan ``[from_block, to_block]``; on soft timeout/400 shrink and retry.

        Hist/live-burst are ``skip_enrich``: alerts stay on near-tip enrich.
        Shrinking keeps the cursor moving instead of stalling on a too-wide
        OR'd topic query. After N shrinks (or min span) ALWAYS force-advance —
        even 99-block windows time out with 500+ wallet topics; skip_enrich
        means skipping a stuck RPC window cannot invent TG deal spam.
        """
        from .chain import reset_followup_rpc_pressure

        cur_to = to_block
        attempt = 0
        last: dict[str, Any] | None = None
        min_span_i = max(1, int(min_span or _HIST_MIN_SHRINK_SPAN))
        force_after_i = max(1, int(force_after or _HIST_FORCE_ADVANCE_AFTER))
        max_attempts = max(3, force_after_i + 2)

        def _force_advance(span_now: int, reason: str) -> dict[str, Any]:
            # Skip only windows already past the TG alert horizon. Near-tip
            # force-advance permanently drops buys live has not tip-scanned
            # (hist stops before tip; dual-jump can leave a gap).
            tip = self._last_known_tip
            alert_lag = max(
                500, int(getattr(cfg, "alert_max_block_lag", 4_000) or 4_000)
            )
            if tip is not None and from_block > int(tip) - alert_lag:
                self._append_log(
                    "logwatch",
                    f"{label} {reason} — refuse force-advance near tip "
                    f"(from={from_block}, tip={tip}, alert_lag={alert_lag})",
                )
                return {
                    "new_deals": 0,
                    "alerts": 0,
                    "skipped": 0,
                    "advanced": False,
                    "advance_to": None,
                }
            step = max(1, span_now)
            nudge_to = from_block + step - 1
            # Never advance past the alert floor even on far-behind skips.
            if tip is not None:
                nudge_to = min(nudge_to, max(from_block - 1, int(tip) - alert_lag))
            if nudge_to < from_block:
                return {
                    "new_deals": 0,
                    "alerts": 0,
                    "skipped": 0,
                    "advanced": False,
                    "advance_to": None,
                }
            self._append_log(
                "logwatch",
                f"{label} {reason} — force-advance +{nudge_to - from_block + 1} "
                f"(skip_enrich) → {nudge_to}",
            )
            return {
                "new_deals": 0,
                "alerts": 0,
                "skipped": 0,
                "advanced": True,
                "advance_to": nudge_to,
            }

        while cur_to >= from_block and attempt < max_attempts:
            attempt += 1
            span_now = cur_to - from_block + 1
            if attempt >= 2:
                try:
                    reset_followup_rpc_pressure()
                    rpc._prefer_non_alchemy()  # noqa: SLF001
                    rpc._bind_url(rpc.rpc_url)  # noqa: SLF001
                except Exception:  # noqa: BLE001
                    pass
            res = await self._logwatch_scan_window(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=from_block,
                to_block=cur_to,
                fetch_timeout=(
                    fetch_timeout if attempt == 1 else min(fetch_timeout, 12.0)
                ),
                label=label if attempt == 1 else f"{label}-shrink{attempt}",
                cursor_floor=cursor_floor,
                skip_enrich=True,
            )
            last = res
            if res is None:
                if span_now <= min_span_i or attempt >= force_after_i:
                    return _force_advance(
                        span_now, "hard-fail after shrink — unstick"
                    )
                reset_followup_rpc_pressure()
                next_span = max(min_span_i, span_now // 4)
                cur_to = from_block + next_span - 1
                self._append_log(
                    "logwatch",
                    f"{label} hard-fail — shrink-retry "
                    f"{from_block}…{cur_to}",
                )
                continue
            if res.get("advanced"):
                return res
            # Soft timeout / retryable empty: shrink aggressively or force-advance.
            if span_now <= min_span_i or attempt >= force_after_i:
                return _force_advance(
                    span_now, "soft-fail after shrink — unstick"
                )
            reset_followup_rpc_pressure()
            next_span = max(min_span_i, span_now // 4)
            cur_to = from_block + next_span - 1
            self._append_log(
                "logwatch",
                f"{label} soft-fail — shrink-retry {from_block}…{cur_to}",
            )
            fetch_timeout = min(fetch_timeout, 12.0)
        if last and last.get("advanced"):
            return last
        span_left = max(1, cur_to - from_block + 1) if cur_to >= from_block else 1
        return _force_advance(span_left, "exhausted shrink — unstick")

    async def _fetch_unique_buys_cached(
        self,
        wallet: str,
        *,
        cfg: FollowupConfig,
        max_pages: int | None = None,
        bypass_cache: bool = False,
    ) -> UniqueBuysResult:
        """Short-TTL cache so live tip does not stampede GMGN per transfer."""
        wallet_l = wallet.lower()
        ttl = float(getattr(cfg, "gmgn_rank_cache_ttl_sec", 60.0) or 60.0)
        now = time.time()
        if not bypass_cache:
            hit = self._gmgn_rank_cache.get(wallet_l)
            if hit is not None and (now - hit[0]) <= ttl:
                return hit[1]
        pages = int(
            max_pages
            if max_pages is not None
            else (getattr(cfg, "gmgn_rank_max_pages", 3) or 3)
        )
        async with self._gmgn_rank_sem:
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
            # Stale/wrong seed (e.g. Blockscout dust). Truncated GMGN pages can
            # omit the seed while still returning ≥max_deals buys — only close
            # the wallet when the sample is clearly a spray/past history.
            n_uniques = len(result.buys or [])
            past_floor = max(15, int(max_deals) * 3)
            if n_uniques >= past_floor:
                return GmgnRankVerdict(
                    uncertain=False,
                    reason="gmgn_seed_miss_past",
                    seed_token=seed_token,
                    post_seed=tuple(result.buys[: max(0, max_deals - 1)]),
                    rank=None,
                    past_max=True,
                )
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
        # Tip not in cached GMGN list: one forced refresh, then tip_lag.
        # Do NOT bypass cache on every tip transfer (GMGN 429 stampede).
        if rank is None and not past_max and token_l:
            try:
                result = await self._fetch_unique_buys_cached(
                    wallet, cfg=cfg, bypass_cache=True
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("GMGN tip refresh %s: %s", wallet[:10], exc)
                return replace(
                    empty,
                    reason="gmgn_tip_lag",
                    seed_token=seed_token,
                    post_seed=tuple(post),
                )
            if result.rate_limited or not result.ok:
                return replace(
                    empty,
                    reason="gmgn_tip_lag",
                    seed_token=seed_token,
                    post_seed=tuple(post),
                )
            _seed_buy, post = post_seed_unique_buys(list(result.buys), seed_token)
            if _seed_buy is None:
                return replace(empty, reason="gmgn_seed_miss")
            past_max = len(post) >= max(0, max_deals - 1)
            if past_max:
                return GmgnRankVerdict(
                    uncertain=False,
                    reason="past_max",
                    seed_token=seed_token,
                    post_seed=tuple(post),
                    rank=None,
                    past_max=True,
                )
            rank = None
            for i, buy in enumerate(post, start=2):
                if buy.token.lower() == token_l:
                    rank = i
                    break
            if rank is None:
                return replace(
                    empty,
                    reason="gmgn_tip_lag",
                    seed_token=seed_token,
                    post_seed=tuple(post),
                )
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
        # GMGN sibling fills (no chain block) must never pending-retry as tip.
        for deal in inserted:
            tok = str(getattr(deal, "token", "") or "").lower()
            if tip_l and tok == tip_l:
                continue
            if int(getattr(deal, "block_number", 0) or 0) <= 0:
                self._store.mark_notified(wallet, tok)
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
        # Burst intentionally parks near live_gap_enrich_max_blocks (~2k);
        # tip grows during the tick — allow headroom before declaring unhealthy.
        unhealthy_behind = 3_000
        if behind is not None and behind > unhealthy_behind:
            return False, behind
        now = time.time()
        if self._last_live_success_ts and (now - self._last_live_success_ts) <= 90.0:
            return True, behind
        if live is None or tip_ref is None:
            return True, None
        assert behind is not None
        return behind <= unhealthy_behind, behind

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

    def _queue_skip_enrich_transfers(
        self, transfers: list[InboundTransfer]
    ) -> None:
        """Remember transfers seen while catching up without enrich+alert."""
        if not transfers:
            return
        now = time.time()
        tip = self._last_known_tip
        max_age = 900.0
        max_block = 8_000
        with self._pending_state_lock:
            # Preserve first-seen queued_at on re-queue (tip_lag retries).
            by_key: dict[tuple[str, str], tuple[InboundTransfer, float]] = {
                (t.wallet.lower(), t.token.lower()): (t, ts)
                for t, ts in self._pending_skip_transfers
            }
            added = 0
            refreshed = 0
            for tr in transfers:
                key = (tr.wallet.lower(), tr.token.lower())
                if tr.bought_at and float(tr.bought_at) > 0:
                    if now - float(tr.bought_at) > max_age:
                        continue
                if (
                    tip is not None
                    and tr.block_number > 0
                    and int(tip) - int(tr.block_number) > max_block
                ):
                    continue
                prev = by_key.get(key)
                if prev is not None:
                    # Keep original queued_at; refresh transfer payload.
                    by_key[key] = (tr, prev[1])
                    refreshed += 1
                    continue
                by_key[key] = (tr, now)
                added += 1
            items = sorted(by_key.values(), key=lambda x: x[1])
            if len(items) > 120:
                # Persist the ones we are about to drop? Prefer keep newest for TG.
                dropped = items[:-120]
                items = items[-120:]
                try:
                    self._store.delete_pending_tip_transfers(
                        [(t.wallet, t.token) for t, _ in dropped]
                    )
                except Exception:  # noqa: BLE001
                    pass
            self._pending_skip_transfers = items
            pending_n = len(items)
            persist_rows = [
                {
                    "wallet": t.wallet,
                    "token": t.token,
                    "sender": t.sender,
                    "tx_hash": t.tx_hash,
                    "block_number": t.block_number,
                    "bought_at": t.bought_at or None,
                    "queued_at": ts,
                }
                for t, ts in items
            ]
        try:
            self._store.upsert_pending_tip_transfers(persist_rows)
        except Exception as exc:  # noqa: BLE001
            logger.debug("persist pending tip transfers: %s", exc)
        if added or refreshed:
            self._append_log(
                "live",
                f"queued {added} skip-enrich transfers "
                f"(refresh={refreshed}, pending={pending_n})",
            )

    async def _queue_before_live_jump(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        watching: list[str],
        from_block: int,
        to_block: int,
        safe_tip: int,
        label: str,
        fetch_timeout: float = 8.0,
    ) -> int:
        """Best-effort skip_enrich of the tip-adjacent slice before a watermark jump.

        Pure jumps used to skip blocks with no pending queue; hist may be far
        behind or later fast-forward past them. Scan only the newest slice still
        inside ``alert_max_block_lag`` so tip buys are not silently dropped.
        """
        if to_block < from_block or not watching:
            return 0
        alert_lag = max(
            500, int(getattr(cfg, "alert_max_block_lag", 4_000) or 4_000)
        )
        # Anything older than alert_lag from tip is TG-stale by design.
        fresh_from = max(from_block, safe_tip - alert_lag)
        if fresh_from > to_block:
            return 0
        live_span = max(50, int(getattr(cfg, "logwatch_live_span", 300) or 300))
        live_span = live_span_for_watchlist(live_span, len(watching))
        scan_span = min(to_block - fresh_from + 1, max(200, live_span * 2))
        scan_from = max(fresh_from, to_block - scan_span + 1)
        try:
            res = await self._logwatch_scan_window(
                cfg,
                rpc=rpc,
                watching=watching,
                from_block=scan_from,
                to_block=to_block,
                fetch_timeout=max(4.0, float(fetch_timeout)),
                label=label,
                skip_enrich=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("queue before jump %s: %s", label, exc)
            return 0
        if not res:
            return 0
        queued = int(res.get("skipped") or 0)
        if queued:
            self._append_log(
                "live",
                f"{label}: queued ~{queued} transfers "
                f"{scan_from}…{to_block} before watermark jump",
            )
        return queued

    async def _drain_pending_skip_transfers(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        enrich_budget_sec: float | None = None,
    ) -> dict[str, int]:
        """Enrich+alert transfers that skip_enrich previously only counted."""
        with self._pending_state_lock:
            if not self._pending_skip_transfers:
                return {"new_deals": 0, "alerts": 0}
            pending_snapshot = list(self._pending_skip_transfers)
        from .gmgn_portfolio import gmgn_circuit_open

        # Circuit open → keep queue intact and freeze queued_at so a long
        # 429 window does not age-out tip_lag buys the moment circuit clears.
        if gmgn_circuit_open():
            now_f = time.time()
            with self._pending_state_lock:
                self._pending_skip_transfers = [
                    (tr, now_f) for tr, _ts in self._pending_skip_transfers
                ]
                pending_n = len(self._pending_skip_transfers)
            try:
                self._store.touch_pending_tip_queued_at(now=now_f)
            except Exception:  # noqa: BLE001
                pass
            self._append_log(
                "live",
                f"drain pause — GMGN circuit "
                f"(pending={pending_n}, age frozen)",
            )
            return {"new_deals": 0, "alerts": 0}
        now = time.time()
        tip = self._last_known_tip
        max_age = float(getattr(cfg, "alert_max_buy_age_sec", 900) or 900)
        keep: list[tuple[InboundTransfer, float]] = []
        batch: list[InboundTransfer] = []
        batch_meta: list[tuple[InboundTransfer, float]] = []
        expired: list[tuple[str, str]] = []
        for tr, queued_at in pending_snapshot:
            if now - queued_at > max_age:
                expired.append((tr.wallet, tr.token))
                continue
            if tr.bought_at and float(tr.bought_at) > 0:
                if now - float(tr.bought_at) > max_age * 2:
                    expired.append((tr.wallet, tr.token))
                    continue
            if (
                tip is not None
                and tr.block_number > 0
                and int(tip) - int(tr.block_number)
                > int(getattr(cfg, "alert_max_block_lag", 4_000) or 4_000) * 2
            ):
                expired.append((tr.wallet, tr.token))
                continue
            if len(batch) < 12:
                batch.append(tr)
                batch_meta.append((tr, queued_at))
            else:
                keep.append((tr, queued_at))
        with self._pending_state_lock:
            # Merge: drop expired/batch from current queue; keep rest + any
            # newly queued keys that arrived while we classified.
            batch_keys = {(t.wallet.lower(), t.token.lower()) for t in batch}
            expired_keys = {(w.lower(), t.lower()) for w, t in expired}
            drop_keys = batch_keys | expired_keys
            merged: dict[tuple[str, str], tuple[InboundTransfer, float]] = {
                (t.wallet.lower(), t.token.lower()): (t, ts) for t, ts in keep
            }
            for t, ts in self._pending_skip_transfers:
                key = (t.wallet.lower(), t.token.lower())
                if key in drop_keys:
                    continue
                merged.setdefault(key, (t, ts))
            self._pending_skip_transfers = sorted(
                merged.values(), key=lambda x: x[1]
            )
        if expired:
            try:
                self._store.delete_pending_tip_transfers(expired)
            except Exception:  # noqa: BLE001
                pass
        if not batch:
            return {"new_deals": 0, "alerts": 0}
        self._append_log(
            "live",
            f"drain {len(batch)} pending skip-enrich transfers",
            percent=50,
        )
        try:
            stats = await self._process_logwatch_transfers(
                batch,
                cfg=cfg,
                rpc=rpc,
                label="pending",
                enrich_budget_sec=enrich_budget_sec,
                queue_mcap_retry=True,
                cursor_floor=None,
                from_block=min(t.block_number for t in batch),
                to_block=max(t.block_number for t in batch),
            )
            # Only drop keys that were NOT re-queued (GMGN uncertain / tip_lag).
            # Blind delete of the whole batch wiped durable rows that
            # ``_queue_skip_enrich_transfers`` just upserted — restart miss.
            with self._pending_state_lock:
                pending_keys = {
                    (t.wallet.lower(), t.token.lower())
                    for t, _ts in self._pending_skip_transfers
                }
            done_keys = [
                (t.wallet, t.token)
                for t in batch
                if (t.wallet.lower(), t.token.lower()) not in pending_keys
            ]
            if done_keys:
                try:
                    self._store.delete_pending_tip_transfers(done_keys)
                except Exception:  # noqa: BLE001
                    pass
            return stats
        except Exception:
            # Put the batch back — otherwise tip_lag items vanish on RPC blip.
            with self._pending_state_lock:
                restored = {
                    (t.wallet.lower(), t.token.lower()): (t, ts)
                    for t, ts in batch_meta
                }
                for t, ts in self._pending_skip_transfers:
                    restored.setdefault((t.wallet.lower(), t.token.lower()), (t, ts))
                self._pending_skip_transfers = sorted(
                    restored.values(), key=lambda x: x[1]
                )
            raise

    async def _skip_enrich_progressive_fetch(
        self,
        cfg: FollowupConfig,
        *,
        rpc: Any,
        watching: list[str],
        from_block: int,
        to_block: int,
        fetch_timeout: float,
        label: str,
        empty: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch hist/burst windows in sub-chunks; advance as far as budget allows.

        One outer ``wait_for`` over thousands of blocks × hundreds of OR'd wallet
        topics always timed out with zero cursor progress (lag alerts forever).
        """
        from .chain import _is_retryable, _redact_exc

        batches = topic_batch_count(len(watching))
        rpc_chunk = max(
            25, int(getattr(cfg, "logwatch_hist_rpc_chunk", 100) or 100)
        )
        # Cap start size under heavy OR topics; grow back after successes.
        max_rpc_chunk = rpc_chunk
        if batches >= 3:
            rpc_chunk = min(rpc_chunk, 25)
            max_rpc_chunk = min(max_rpc_chunk, 100)
        elif batches >= 2:
            rpc_chunk = min(rpc_chunk, 50)
            max_rpc_chunk = min(max_rpc_chunk, 100)

        deadline = time.monotonic() + max(3.0, float(fetch_timeout))
        cur = int(from_block)
        last_ok = int(from_block) - 1
        total_transfers = 0
        soft_errs = 0
        success_streak = 0

        while cur <= to_block:
            remain = deadline - time.monotonic()
            if remain < 1.5:
                break
            end = min(cur + rpc_chunk - 1, to_block)
            sub_timeout = min(remain - 0.2, max(3.0, min(12.0, float(fetch_timeout) * 0.45)))
            try:
                part = await asyncio.wait_for(
                    fetch_inbound_transfers(
                        rpc,
                        watching,
                        from_block=cur,
                        to_block=end,
                        chunk_size=max(1, end - cur + 1),
                    ),
                    timeout=sub_timeout,
                )
            except asyncio.TimeoutError:
                # Halve once inside remaining budget; else stop with partial.
                success_streak = 0
                half = max(10, (end - cur + 1) // 2)
                rpc_chunk = max(10, half)
                if half >= (end - cur + 1):
                    self._append_log(
                        "logwatch",
                        f"getLogs timeout {sub_timeout:.0f}s на {cur}…{end} "
                        f"({label}) — partial→{last_ok}",
                    )
                    break
                end = cur + half - 1
                remain = deadline - time.monotonic()
                if remain < 1.5:
                    break
                try:
                    part = await asyncio.wait_for(
                        fetch_inbound_transfers(
                            rpc,
                            watching,
                            from_block=cur,
                            to_block=end,
                            chunk_size=max(1, end - cur + 1),
                        ),
                        timeout=min(remain - 0.2, 8.0),
                    )
                except asyncio.TimeoutError:
                    success_streak = 0
                    self._append_log(
                        "logwatch",
                        f"getLogs timeout на {cur}…{end} ({label}) — "
                        f"partial→{last_ok}",
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    if _is_retryable(exc):
                        soft_errs += 1
                        success_streak = 0
                        self._append_log(
                            "logwatch",
                            f"getLogs soft-err ({label}): {_redact_exc(exc)} "
                            f"— partial→{last_ok}",
                        )
                        break
                    self._append_log(
                        "logwatch",
                        f"getLogs ошибка ({label}): {_redact_exc(exc)}",
                    )
                    if last_ok >= from_block:
                        break
                    return empty
            except Exception as exc:  # noqa: BLE001
                if _is_retryable(exc):
                    soft_errs += 1
                    success_streak = 0
                    self._append_log(
                        "logwatch",
                        f"getLogs soft-err ({label}): {_redact_exc(exc)} "
                        f"— partial→{last_ok}",
                    )
                    break
                self._append_log(
                    "logwatch",
                    f"getLogs ошибка ({label}): {_redact_exc(exc)}",
                )
                if last_ok >= from_block:
                    break
                return empty

            if part:
                self._queue_skip_enrich_transfers(part)
            total_transfers += len(part or [])
            last_ok = end
            cur = end + 1
            # Grow sub-window after consecutive successes (replay-style).
            success_streak += 1
            if success_streak >= 2 and rpc_chunk < max_rpc_chunk:
                rpc_chunk = min(max_rpc_chunk, max(rpc_chunk * 2, rpc_chunk + 25))
                success_streak = 0

        if last_ok < from_block:
            if soft_errs:
                return empty
            self._append_log(
                "logwatch",
                f"getLogs timeout {fetch_timeout:.0f}s на {from_block}…{to_block} "
                f"({label}) — курсор не двигаем",
            )
            return empty

        if total_transfers:
            self._append_log(
                "logwatch",
                f"hist skip enrich ({total_transfers} transfers) — "
                f"queued for live alert [{from_block}…{last_ok}]",
                percent=40,
            )
        elif last_ok < to_block:
            self._append_log(
                "logwatch",
                f"{label} progressive +{last_ok - from_block + 1} блоков "
                f"→ {last_ok} (budget)",
            )
        return {
            "new_deals": 0,
            "alerts": 0,
            "skipped": total_transfers,
            "advanced": True,
            "advance_to": last_ok,
        }

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
        soft_partial: bool = False,
    ) -> dict[str, Any] | None:
        """Fetch+enrich+alert one block window.

        Returns a stats dict, or None on hard failure. Soft getLogs timeout
        yields advanced=False so cursors stay put. ``skip_enrich`` skips
        tx_senders + mcap enrich (hist catch-up only advances cursor / records
        with null mcap) so hist cannot stall on RPC batches.

        ``soft_partial`` (tip): merge successful topic batches even when some
        time out — purchase alerts must not wait for a perfect 7/7 gather.
        """
        empty = {
            "new_deals": 0,
            "alerts": 0,
            "skipped": 0,
            "advanced": False,
            "advance_to": None,
            "incomplete": False,
            "fetched": 0,
        }
        if to_block < from_block:
            return {**empty, "advanced": True, "advance_to": cursor_floor}
        incomplete = False
        try:
            window = max(1, to_block - from_block + 1)
            if skip_enrich:
                # Progressive sub-chunks: a single wait_for over 10k×N-wallet
                # OR'd topics always timed out → zero progress → lag alert.
                # Advance as far as we got before the budget expires.
                return await self._skip_enrich_progressive_fetch(
                    cfg,
                    rpc=rpc,
                    watching=watching,
                    from_block=from_block,
                    to_block=to_block,
                    fetch_timeout=fetch_timeout,
                    label=label,
                    empty=empty,
                )
            chunk_size = min(window, 2_000)
            if soft_partial:
                # Per-batch timeouts + deadline merge: never outer-cancel
                # successful topic waves (that discarded buys and inflated streak).
                batch_to = min(10.0, max(6.0, float(fetch_timeout) * 0.75))
                parallel = 2 if self._live_timeout_streak >= 2 else 3
                n_batches = topic_batch_count(len(watching))
                rounds = max(1, (n_batches + parallel - 1) // parallel)
                # Leave headroom for classify/enrich under the live watchdog.
                soft_budget = min(28.0, max(float(fetch_timeout), batch_to * rounds + 1.5))
                transfers, incomplete = await fetch_inbound_transfers_result(
                    rpc,
                    watching,
                    from_block=from_block,
                    to_block=to_block,
                    chunk_size=chunk_size,
                    soft_partial=True,
                    batch_timeout_sec=batch_to,
                    batch_parallel=parallel,
                    deadline_mono=time.monotonic() + soft_budget,
                )
            else:
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

        if skip_enrich:
            # Cursor-only record path is unsafe (airdrops). Queue fresh
            # transfers for the live drain instead of dropping them.
            if transfers:
                self._queue_skip_enrich_transfers(transfers)
                self._append_log(
                    "logwatch",
                    f"hist skip enrich ({len(transfers)} transfers) — "
                    "queued for live alert",
                    percent=40,
                )
            return {
                "new_deals": 0,
                "alerts": 0,
                "skipped": len(transfers),
                "advanced": True,
                "advance_to": to_block,
                "incomplete": False,
            }

        if incomplete:
            self._append_log(
                "live",
                f"tip soft_partial: {len(transfers)} transfers "
                f"(some topic batches failed) [{from_block}…{to_block}]",
            )

        stats = await self._process_logwatch_transfers(
            transfers,
            cfg=cfg,
            rpc=rpc,
            label=label,
            enrich_budget_sec=enrich_budget_sec,
            queue_mcap_retry=queue_mcap_retry,
            cursor_floor=cursor_floor,
            from_block=from_block,
            to_block=to_block,
            fetch_timeout=fetch_timeout,
        )
        stats = {
            **stats,
            "fetched": len(transfers),
            "incomplete": bool(incomplete),
        }
        if incomplete:
            # Alerts may have fired; do not advance cursor over uncovered wallets.
            stats["advanced"] = False
        return stats

    async def _process_logwatch_transfers(
        self,
        transfers: list[InboundTransfer],
        *,
        cfg: FollowupConfig,
        rpc: Any,
        label: str,
        enrich_budget_sec: float | None = None,
        queue_mcap_retry: bool = False,
        cursor_floor: int | None = None,
        from_block: int = 0,
        to_block: int = 0,
        fetch_timeout: float = 12.0,
    ) -> dict[str, Any]:
        """Enrich + rank + alert a batch of inbound transfers."""
        if not transfers:
            return {
                "new_deals": 0,
                "alerts": 0,
                "skipped": 0,
                "advanced": True,
                "advance_to": to_block,
            }
        sender_map: dict[str, str | None] = {}
        method_map: dict[str, str | None] = {}
        senders_ok = True
        if cfg.buys_only and transfers:
            try:
                meta = await asyncio.wait_for(
                    tx_from_and_input(rpc, [t.tx_hash for t in transfers]),
                    timeout=min(12.0, fetch_timeout),
                )
                for h, (frm, inp) in meta.items():
                    sender_map[h] = frm
                    method_map[h] = inp
            except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
                senders_ok = False
                self._append_log(
                    "logwatch",
                    f"tx_senders fail ({label}) — fail-closed: "
                    f"{type(exc).__name__}",
                )
                sender_map = {}
                method_map = {}

        # Strict buys-only before enrich: drop gifts/airdrops/EOA transfers and
        # bound quote-spend lookups so live tip cannot hang on Blockscout.
        unknown_buy_senders: list[InboundTransfer] = []
        skipped = 0
        if cfg.buys_only:
            live_fast = label in ("live", "pending", "tip")
            allow_quote = True
            quote_budget = 4.0
            if live_fast and self._live_timeout_streak >= 1:
                # Under tip pressure: wallet-initiated only (no Blockscout spend).
                allow_quote = False
                quote_budget = 0.0
            elif live_fast:
                quote_budget = 3.0
            buys, uncertain, skipped_buys = await classify_logwatch_buys(
                transfers,
                rpc=rpc,
                sender_map=sender_map,
                senders_ok=senders_ok,
                allow_quote_lookup=allow_quote,
                quote_budget_sec=quote_budget,
                method_map=method_map,
            )
            skipped += skipped_buys
            unknown_buy_senders.extend(uncertain)
            transfers = buys
            if uncertain:
                self._append_log(
                    "logwatch",
                    f"buys_only: {len(buys)} buys, {skipped_buys} skip, "
                    f"{len(uncertain)} requeue ({label})",
                )

        chat = resolve_chat_id(cfg.telegram_chat_id)
        topic_id = resolve_topic_id(cfg.telegram_topic_id)
        tg_ok = telegram_configured(chat)
        filters_map = self._store.get_alert_filters_map(
            sorted({t.wallet for t in transfers})
        ) if transfers else {}
        enrich = await self._prefetch_transfer_enrichment(
            transfers,
            cfg=cfg,
            rpc=rpc,
            sender_map=sender_map,
            budget_sec=enrich_budget_sec,
        )

        new_deals = 0
        alerts = 0
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
            live_fast = label in ("live", "pending")
            if gmgn_api_configured():
                try:
                    verdict = await asyncio.wait_for(
                        self._gmgn_rank_verdict(tr.wallet, tr.token, cfg),
                        timeout=4.0 if live_fast else 12.0,
                    )
                except asyncio.TimeoutError:
                    verdict = GmgnRankVerdict(
                        uncertain=True,
                        reason="gmgn_timeout",
                        seed_token="",
                        post_seed=(),
                        rank=None,
                        past_max=False,
                    )
                if verdict.uncertain:
                    # Never invent local #2…#5 for TG. Re-queue so tip cursor
                    # advance does not permanently drop the transfer while GMGN
                    # is 429/circuit/lagging.
                    self._queue_skip_enrich_transfers([tr])
                    self._append_log(
                        "deal",
                        f"skip invent #{tr.token[:10]}… "
                        f"GMGN uncertain ({verdict.reason}) — queued [{label}]",
                    )
                    skipped += 1
                    continue
                else:
                    # Seed-miss past: close spray/wrong-seed wallets without
                    # rewriting deal rows from an unrelated GMGN prefix.
                    if verdict.past_max and verdict.reason == "gmgn_seed_miss_past":
                        self._store.mark_wallet_done(
                            tr.wallet, deal_count=int(cfg.max_deals or 5)
                        )
                        self._append_log(
                            "deal",
                            f"GMGN seed-miss past max {tr.wallet[:10]}… "
                            f"→ done [{label}]",
                        )
                        skipped += 1
                        continue
                    # Sync GMGN order (marks done when already ≥ max_deals uniques).
                    include_tip = (
                        not verdict.past_max
                        and verdict.rank is not None
                        and verdict.rank <= int(cfg.max_deals or 5)
                    )
                    inserted_rows = await self._sync_wallet_gmgn_order(
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
                    tip_newly_inserted = any(
                        str(getattr(d, "token", "") or "").lower()
                        == tr.token.lower()
                        for d in (inserted_rows or [])
                    )
                    # Tip that fills max_deals flips wallet → done. Still alert
                    # that tip (#max_deals) — do not treat "done" as skip.
                    if not include_tip:
                        _seen, deal_count_now, status_now = (
                            self._store.get_wallet_scan_meta(tr.wallet)
                        )
                        if (
                            status_now != "watching"
                            or deal_count_now >= cfg.max_deals
                        ):
                            self._append_log(
                                "deal",
                                f"GMGN past window {tr.wallet[:10]}… "
                                f"post={len(verdict.post_seed)} "
                                f"status={status_now} [{label}]",
                            )
                            skipped += 1
                            continue
                    for row in self._store.list_deals_for_wallet(tr.wallet):
                        if str(row.get("token") or "").lower() == tr.token.lower():
                            deal = FollowupDealRow(
                                wallet=tr.wallet.lower(),
                                token=tr.token.lower(),
                                token_symbol=str(
                                    row.get("token_symbol")
                                    or token_symbol
                                    or ""
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
                                tx_hash=str(
                                    row.get("tx_hash") or tr.tx_hash or ""
                                ),
                                block_number=int(
                                    row.get("block_number")
                                    or tr.block_number
                                    or 0
                                ),
                                bought_at=(
                                    float(row["bought_at"])
                                    if row.get("bought_at")
                                    else (tr.bought_at or None)
                                ),
                                notified=bool(row.get("notified")),
                                created_at=float(
                                    row.get("created_at") or time.time()
                                ),
                            )
                            break
                    if deal is None:
                        if include_tip:
                            self._append_log(
                                "deal",
                                f"tip missing after GMGN sync "
                                f"{tr.wallet[:10]}… {tr.token[:10]}… [{label}]",
                            )
                        else:
                            self._append_log(
                                "deal",
                                f"GMGN past window {tr.wallet[:10]}… "
                                f"post={len(verdict.post_seed)} "
                                f"(tip not in deal set) [{label}]",
                            )
                        skipped += 1
                        continue
                    if deal.notified:
                        skipped += 1
                        continue
                    if tip_newly_inserted:
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
                why = alert_filter_skip_reason(
                    deal.deal_index,
                    deal.mcap_at_buy,
                    bought_usd=deal.bought_usd,
                    **gate,
                )
                if live_fast or why:
                    self._append_log(
                        "deal",
                        f"skip filter #{deal.deal_index} "
                        f"{deal.token_symbol or deal.token[:10]}… "
                        f"({why or 'gate'}) [{label}]",
                    )
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
                    getattr(cfg, "alert_max_block_lag", 4_000) or 4_000
                ),
                discovered_at=deal.created_at,
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
            # Shield TG enqueue from live tip watchdog cancel — otherwise a
            # deal can be recorded and never outboxed.
            # Re-check honeypot when enrich timed out (hp_reason is None) so
            # hard honeypots cannot ship as normal follow-up alerts.
            recheck_hp = bool(
                getattr(cfg, "alert_skip_honeypot", True) and hp_reason is None
            )
            try:
                ok = await asyncio.shield(
                    self._deliver_deal_alert(
                        chat,
                        deal=deal,
                        topic_id=topic_id,
                        honeypot_reason=hp_reason,
                        check_honeypot=recheck_hp,
                        origin=self._alert_origin_from_label(label),
                    )
                )
            except asyncio.CancelledError:
                # Deliver task was shielded; still surface cancel to caller.
                raise
            if ok:
                alerts += 1
                self._append_log(
                    "telegram",
                    f"Алерт deal #{deal.deal_index} · {deal.wallet[:10]}…"
                    + (" · HONEYPOT" if hp_reason else ""),
                )

        if unknown_buy_senders:
            self._queue_skip_enrich_transfers(unknown_buy_senders)
            self._append_log(
                "logwatch",
                f"buys_only: {len(unknown_buy_senders)} transfers queued "
                f"(unknown tx.from) [{label}]",
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
                    check_honeypot=bool(
                        getattr(cfg, "alert_skip_honeypot", True) and hp_reason is None
                    ),
                    origin="legacy",
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
