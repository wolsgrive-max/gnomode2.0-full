"""Pydantic models for API and parser results."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    done = "done"
    error = "error"


class TokensUniquePeriod(str, Enum):
    """Lookback window for distinct ERC-20 token count on a wallet."""

    h12 = "12h"
    h24 = "24h"
    d1 = "1d"
    d3 = "3d"
    d7 = "7d"
    d30 = "30d"


def tokens_unique_period_hours(period: TokensUniquePeriod | str | None) -> float:
    """Map period label → hours (1d == 24h)."""
    key = (
        (period or TokensUniquePeriod.d7).value
        if isinstance(period, TokensUniquePeriod)
        else str(period or "7d")
    )
    mapping = {
        "12h": 12.0,
        "24h": 24.0,
        "1d": 24.0,
        "3d": 72.0,
        "7d": 168.0,
        "30d": 720.0,
    }
    return mapping.get(key, 168.0)


class ParseRequest(BaseModel):
    tokens: list[str] = Field(..., min_length=1)
    mcap_threshold: float | None = None
    exclude_honeypots: bool = True
    # Wallet filters (all optional min/max ranges, applied to found buyers)
    min_wallet_balance_eth: float | None = None
    max_wallet_balance_eth: float | None = None
    min_hold_time_minutes: float | None = None
    max_hold_time_minutes: float | None = None
    min_tokens_traded_7d: float | None = None
    max_tokens_traded_7d: float | None = None
    tokens_unique_period: TokensUniquePeriod = TokensUniquePeriod.d7


class MigratedToken(BaseModel):
    launchpad: str
    address: str
    name: str | None = None
    symbol: str | None = None
    image_url: str | None = None
    migrated_at: str | None = None
    pool_address: str | None = None
    source_url: str
    verification: str
    discovery_sources: list[str] = Field(default_factory=list)
    liquidity_usd: float = 0
    traders_24h: int = 0


class MigrationResponse(BaseModel):
    tokens: list[MigratedToken] = Field(default_factory=list)
    errors: dict[str, str] = Field(default_factory=dict)
    count: int = 0
    duration_ms: int = 0


class BuyerRow(BaseModel):
    wallet: str
    token: str
    token_symbol: str = ""
    bought_tokens: float
    bought_usd: float
    mcap_at_first_buy: float
    buys_count: int
    first_tx: str = ""
    first_block: int = 0
    wallet_balance_eth: float | None = None
    hold_time_minutes: float | None = None
    tokens_traded_7d: int | None = None


class PoolInfo(BaseModel):
    address: str  # pair/pool contract, or PoolManager for V4
    dex: str  # uniswap_v2 | uniswap_v3 | uniswap_v4
    quote: str
    quote_symbol: str
    token0: str
    token1: str
    fee: int | None = None
    liquidity_usd: float = 0.0
    created_block: int | None = None
    pool_id: str | None = None  # bytes32 for Uniswap V4
    pair_created_at_ms: int | None = None


class TokenParseResult(BaseModel):
    token: str
    symbol: str = ""
    name: str = ""
    decimals: int = 18
    total_supply: float = 0.0
    pool: PoolInfo | None = None
    buyers: list[BuyerRow] = Field(default_factory=list)
    error: str | None = None
    stats: dict[str, Any] = Field(default_factory=dict)


class JobProgress(BaseModel):
    stage: str = "queued"
    message: str = ""
    percent: float = 0.0
    current_token: str | None = None


class JobLogEntry(BaseModel):
    """One step in the parse/filter pipeline (shown in the UI log)."""

    ts: float
    stage: str
    message: str
    percent: float = 0.0
    token: str | None = None


class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: JobProgress
    log: list[JobLogEntry] = Field(default_factory=list)
    results: list[TokenParseResult] = Field(default_factory=list)
    error: str | None = None


class ScreenSortBy(str, Enum):
    liquidity = "liquidity"
    market_cap = "market_cap"
    traders = "traders"
    pair_age = "pair_age"


class ScreenSortOrder(str, Enum):
    asc = "asc"
    desc = "desc"


class ScreenRequest(BaseModel):
    min_liq: float | None = None
    max_liq: float | None = None
    min_mcap: float | None = None
    max_mcap: float | None = None
    # Peak/ATH mcap from the 24h index (Gecko OHLCV + DS samples). None/0 = off.
    min_ath_mcap: float | None = None
    min_traders: float | None = None
    max_traders: float | None = None
    min_pair_age_hours: float | None = None
    max_pair_age_hours: float | None = None
    exclude_honeypots: bool = True
    sort_by: ScreenSortBy = ScreenSortBy.liquidity
    sort_order: ScreenSortOrder = ScreenSortOrder.desc
    max_results: int = Field(default=500, ge=1, le=10_000)


class ScreenedToken(BaseModel):
    address: str
    symbol: str = ""
    name: str = ""
    pair_address: str = ""
    dex_id: str = ""
    price_usd: float = 0.0
    liquidity_usd: float = 0.0
    market_cap: float = 0.0
    # Peak market cap observed while the token is in the 24h index / hold queue.
    ath_mcap: float = 0.0
    traders_24h: int = 0
    buys_24h: int = 0
    sells_24h: int = 0
    pair_created_at_ms: int | None = None
    pair_age_hours: float | None = None
    url: str = ""
    gmgn_url: str = ""


class ScreenJobResponse(BaseModel):
    job_id: str
    status: JobStatus
    progress: JobProgress
    results: list[ScreenedToken] = Field(default_factory=list)
    error: str | None = None


class IndexStatus(BaseModel):
    tokens_24h: int = 0
    enriched: int = 0
    building: bool = False
    cold_started: bool = False
    refreshing: bool = False
    last_tip: int = 0
    last_scan_ts: float = 0.0
    last_refresh_ts: float = 0.0
    window_hours: int = 24


class WatchScreenFilters(BaseModel):
    # Defaults = Хвать «фильтры токенов» (screenshot): ATH 40k, age≤24h, max_results 10k.
    min_liq: float | None = 500.0
    max_liq: float | None = None
    min_mcap: float | None = None
    max_mcap: float | None = None
    # Autoparse wallet extraction only after tracked ATH ≥ this (USD).
    # None or 0 disables the gate (parse every screened token each cycle).
    min_ath_mcap: float | None = 40_000.0
    min_traders: float | None = None
    max_traders: float | None = None
    min_pair_age_hours: float | None = None
    max_pair_age_hours: float | None = 24.0
    exclude_honeypots: bool = True
    sort_by: ScreenSortBy = ScreenSortBy.liquidity
    sort_order: ScreenSortOrder = ScreenSortOrder.desc
    max_results: int = Field(default=10_000, ge=1, le=10_000)


class WatchWalletFilters(BaseModel):
    mcap_threshold: float | None = 20_000.0
    exclude_honeypots: bool = True
    min_wallet_balance_eth: float | None = None
    max_wallet_balance_eth: float | None = None
    min_hold_time_minutes: float | None = None
    max_hold_time_minutes: float | None = None
    min_tokens_traded_7d: float | None = 1.0
    max_tokens_traded_7d: float | None = 3.0
    tokens_unique_period: TokensUniquePeriod = TokensUniquePeriod.d7


class WatchConfig(BaseModel):
    enabled: bool = False
    # Default parse cadence from live Хвать token panel (12 min).
    interval_sec: int = Field(default=720, ge=60, le=86400)
    # Keep modest — qualify hits Blockscout/RPC; prefer thorough over fast.
    max_tokens_per_cycle: int = Field(default=40, ge=1, le=2000)
    # Parse drain: ATH below this goes to bulk tail (after ≥ floor), not dropped.
    # None / 0 = no priority band (pure ATH-desc within urgent/fresh).
    parse_priority_min_ath: float | None = 50_000.0
    telegram_chat_id: str = ""
    # Forum topic id (Telegram message_thread_id). Empty → no topic / General.
    telegram_topic_id: str = ""
    # Periodic tired-gnome status lines in Telegram (every ~10–15 min).
    gnome_banter_enabled: bool = True
    screen: WatchScreenFilters = Field(default_factory=WatchScreenFilters)
    wallet: WatchWalletFilters = Field(default_factory=WatchWalletFilters)


class WatchStatus(BaseModel):
    enabled: bool = False
    running: bool = False
    telegram_configured: bool = False
    next_run_ts: float | None = None
    last_run_ts: float | None = None
    last_run_duration_sec: float | None = None
    last_error: str | None = None
    last_message: str = ""
    last_tokens_screened: int = 0
    last_tokens_parsed: int = 0
    last_tokens_held: int = 0
    last_tokens_qualified: int = 0
    last_buyers_found: int = 0
    last_buyers_new: int = 0
    last_buyers_sent: int = 0
    last_buyers_skipped: int = 0
    seen_count: int = 0
    hold_count: int = 0
    parsed_token_count: int = 0
    # Catch-up after downtime (before regular interval cycles)
    needs_catchup: bool = False
    catchup_lookback_hours: float | None = None
    is_catchup_run: bool = False
    gnome_banter_enabled: bool = True
    gnome_banter_next_ts: float | None = None
    stop_requested: bool = False
    log: list[JobLogEntry] = Field(default_factory=list)


class FollowupConfig(BaseModel):
    """Watchlist of early buyers → alert on later new-token buys @ low mcap."""

    enabled: bool = False
    # Target seconds between follow-up cycle *starts*. Prefer 5–15s so we
    # do not stampede GMGN into RATE_LIMIT_BANNED; 0 = ASAP after finish.
    # Fast logwatch target period. At ~10 blocks/sec, 3s keeps normal purchase
    # alerts under ~8s end-to-end while remaining well below Alchemy limits.
    interval_sec: int = Field(default=3, ge=0, le=86400)
    # Alert only when buy mcap is at or below this (USD). High mcap → record, no alert.
    max_mcap_alert: float = Field(default=20_000.0, ge=0)
    # Optional lower bound (USD). None = no floor.
    min_mcap_alert: float | None = None
    # Optional size filters on bought_usd (when known).
    min_bought_usd: float | None = None
    max_bought_usd: float | None = None
    # Deal indices that trigger Telegram (1 = discovery in watch; 2+ = follow-up).
    alert_on_deals: list[int] = Field(default_factory=lambda: [2, 3, 4, 5])
    max_deals: int = Field(default=5, ge=1, le=20)
    # One distinct token = one deal (always). Buys-only: only inbound from contract (DEX).
    buys_only: bool = True
    # When False (default), ignore wallet↔wallet token transfers (RayBot-style EVM).
    # When True and buys_only is False, also record inbound transfers from EOAs.
    track_transfers: bool = False
    telegram_chat_id: str = ""
    telegram_topic_id: str = ""
    # Native Telegram bot commands (/status, /filters, …) via long-poll.
    bot_commands_enabled: bool = True
    # Legacy optional RayBot sync (not required — native bot replaces it).
    raybot_enabled: bool = False
    # When True, ingest early buyers from autoparse into the follow-up table.
    ingest_from_watch: bool = True
    # Do not send Telegram when the deal token is flagged honeypot.
    alert_skip_honeypot: bool = True
    # Parallel wallet scans per cycle. Keep low — shared GMGN ceiling +
    # Blockscout ~2–3 rps; high concurrency re-triggers IP bans.
    scan_concurrency: int = Field(default=3, ge=1, le=32)
    # Max Blockscout pages per wallet (newest-first). GMGN path ignores this.
    scan_max_pages: int = Field(default=3, ge=1, le=20)
    # Drop wallet if discovery token never reached this ATH mcap (USD) in time.
    prune_enabled: bool = False
    prune_min_ath_mcap: float = Field(default=50_000.0, ge=0)
    # Hours after discovery before prune check (48 = 2 days).
    prune_after_hours: float = Field(default=48.0, ge=1, le=24 * 30)
    # Priority scheduler: hot/warm revisit + zero-balance recheck (seconds).
    hot_revisit_sec: int = Field(default=20, ge=5, le=300)
    warm_revisit_sec: int = Field(default=180, ge=30, le=3600)
    zero_balance_recheck_sec: int = Field(default=900, ge=60, le=7200)
    balance_fresh_sec: int = Field(default=600, ge=60, le=3600)
    # Recent discovery/deal activity window → HOT tier.
    hot_activity_sec: int = Field(default=1800, ge=60, le=86400)
    # Max wallets scanned (deal or balance-recheck) per cycle.
    max_due_per_cycle: int = Field(default=24, ge=4, le=200)
    # Fraction of each batch reserved for overdue warm (fairness).
    warm_fair_share: float = Field(default=0.25, ge=0.05, le=0.75)
    # ATH prune is expensive (DexScreener/Gecko); with short priority cycles
    # throttle it so we do not stampede Gecko into 429 every minute.
    prune_interval_sec: int = Field(default=1800, ge=60, le=86400)
    # Primary deal discovery: eth_getLogs Transfer→watching wallets.
    # When healthy, skips the slow per-wallet GMGN/Blockscout scan.
    logwatch_enabled: bool = True
    # Max blocks per eth_getLogs span. Address-less Transfer queries with
    # hundreds of wallet topics blow up past ~5–10k blocks (Alchemy/public
    # hang or >180s), which freezes the cursor and triggers the watchdog loop.
    logwatch_max_span: int = Field(default=3_000, ge=100, le=200_000)
    # When tip−cursor is moderately large, prefer this span (still finishes
    # under the watchdog). Must NOT be clamped to a tiny hard cap — that
    # caused a death spiral where lag grew forever at ~800 bl/cycle.
    logwatch_catchup_span: int = Field(default=3_000, ge=100, le=50_000)
    # Extreme hist backlog (lag ≫ 50k): start with this large window. Shrink
    # only on getLogs timeout/400 (retry). Hist is skip_enrich; live tip
    # covers fresh alerts, so aggressive catch-up is safe.
    logwatch_burst_catchup_span: int = Field(default=10_000, ge=500, le=200_000)
    # How many hist windows to scan per cycle while lag is high / budget allows.
    logwatch_catchup_chunks_per_pass: int = Field(default=4, ge=1, le=20)
    # Wall-time budget for multi-chunk hist catch-up inside one cycle.
    logwatch_catchup_time_budget_sec: float = Field(default=90.0, ge=10.0, le=300.0)
    # eth_getLogs internal block chunk for hist (keeps each RPC under wall-timeout).
    logwatch_hist_rpc_chunk: int = Field(default=400, ge=50, le=5_000)
    # While catching up / in the dedicated live loop, scan this many tip blocks
    # with full enrich+alert. Keep small so each live tick finishes in <1–2s.
    logwatch_live_span: int = Field(default=300, ge=50, le=20_000)
    # Live gap backfill may enrich+alert only when live cursor is this close
    # to tip. Larger gaps are cursor-only (skip_enrich) — otherwise hist lag
    # replays old buys as fake «deal #2/#3» Telegram spam.
    live_gap_enrich_max_blocks: int = Field(default=2_000, ge=100, le=50_000)
    # Do not Telegram-alert deals whose buy is older than this (seconds).
    alert_max_buy_age_sec: int = Field(default=900, ge=60, le=86_400)
    # Do not Telegram-alert deals whose block is more than this behind tip.
    alert_max_block_lag: int = Field(default=2_000, ge=100, le=50_000)
    # Tip-first / live-priority threshold for hist (hist no longer embeds live).
    logwatch_live_priority_lag: int = Field(default=3_000, ge=100, le=200_000)
    # Dedicated live tip poll period (seconds). Independent of hist cycle.
    live_interval_sec: float = Field(default=1.5, ge=0.5, le=30.0)
    # Wall-time for live tip enrich (parallel mcap/meta/honeypot).
    live_enrich_budget_sec: float = Field(default=3.0, ge=1.0, le=15.0)
    # Watchdog for one live tip tick (must stay well under hist cycle_timeout).
    live_cycle_timeout_sec: int = Field(default=45, ge=10, le=180)
    # Hard wall-time for one logwatch getLogs pass (excludes enrichment).
    logwatch_fetch_timeout_sec: int = Field(default=45, ge=5, le=300)
    # Confirmations before advancing the log cursor (reorg safety).
    logwatch_confirmations: int = Field(default=2, ge=0, le=64)
    # Even when logwatch is healthy, periodically re-scan hot wallets via
    # GMGN/Blockscout so a silent RPC gap cannot drop deals forever.
    safety_reconcile_sec: int = Field(default=120, ge=30, le=3600)
    # Max hot wallets touched by one safety-reconcile pass.
    safety_reconcile_max: int = Field(default=12, ge=1, le=64)
    # Short TTL for GMGN unique-buy cache used by logwatch rank checks.
    gmgn_rank_cache_ttl_sec: float = Field(default=60.0, ge=5.0, le=600.0)
    # Pages for rank / repair fetches (accuracy > speed when numbering #2..#5).
    gmgn_rank_max_pages: int = Field(default=3, ge=1, le=10)
    # Opportunistic GMGN sync batch for undercounted watching wallets.
    gmgn_repair_batch: int = Field(default=8, ge=0, le=64)
    # After this many consecutive logwatch hard-failures, mark degraded and
    # fall back to legacy GMGN/Blockscout. Soft getLogs timeouts do not count.
    # Default 3 absorbs single Alchemy blips without DEGRADED spam.
    logwatch_fail_threshold: int = Field(default=3, ge=1, le=20)
    # Cancel a stuck cycle after this many seconds (watchdog).
    cycle_timeout_sec: int = Field(default=180, ge=30, le=3600)
    # Slow maintenance loop (backfill/prune/reconcile/legacy) — separate from hist.
    maintenance_interval_sec: float = Field(default=60.0, ge=15.0, le=3600.0)
    maintenance_timeout_sec: int = Field(default=90, ge=30, le=600)
    # Ops TG when tip−cursor exceeds this many blocks (~10 bl/s → 600≈1 min).
    cursor_lag_alert_blocks: int = Field(default=6_000, ge=100, le=500_000)
    # Min seconds between identical ops alerts (spam guard).
    ops_alert_cooldown_sec: int = Field(default=600, ge=60, le=86400)
    # Transactional outbox for deal alerts: claim + enqueue commit together so
    # a crash mid-send cannot drop an alert (a dispatcher redelivers).
    outbox_enabled: bool = True
    # Give up (mark 'failed') after this many delivery attempts per alert.
    outbox_max_attempts: int = Field(default=10, ge=1, le=100)
    # Max outbox rows drained per cycle by the dispatcher.
    outbox_dispatch_batch: int = Field(default=25, ge=1, le=500)
    # Per-deal enrichment (entry mcap replay + quote + honeypot) is resolved
    # concurrently for the whole logwatch batch: inline+sequential turned N new
    # deals into N × ~10s of alert delay.
    logwatch_enrich_concurrency: int = Field(default=6, ge=1, le=32)
    logwatch_enrich_timeout_sec: int = Field(default=8, ge=2, le=60)
    # Backfill retry for deals still missing mcap is comparatively expensive
    # (quote refetch + on-chain pool discovery). Run it on its own slower cadence
    # so it never stretches the fast logwatch loop.
    pending_retry_interval_sec: int = Field(default=60, ge=0, le=3600)
    # Wall-time cap for a pending-alert retry pass. Without this, a handful of
    # null-mcap deals (6s quote + 6s on-chain each) can push the next logwatch
    # start by a minute+ and create the ~135s discover lag seen on deal #5.
    pending_retry_time_budget_sec: int = Field(default=12, ge=0, le=120)
    # Hard cap on the post-discovery ATH prune so external-API 429 backoffs
    # cannot stall the cycle (and delay the next logwatch) for minutes.
    prune_time_budget_sec: int = Field(default=45, ge=0, le=600)


class FollowupDealRow(BaseModel):
    wallet: str
    token: str
    token_symbol: str = ""
    token_name: str = ""
    deal_index: int
    mcap_at_buy: float | None = None
    bought_usd: float | None = None
    tx_hash: str = ""
    block_number: int = 0
    # Unix seconds of the buy (chain time). Used to assign deal_index;
    # never fall back to «unknown block → sort last» (that made late
    # Blockscout hits look like deal #1/#2 ahead of the real seed).
    bought_at: float | None = None
    notified: bool = False
    created_at: float = 0.0


class WalletAlertFilters(BaseModel):
    """Per-wallet overrides for deal #2/#3 alerts and prune.

    When ``custom`` is False, global FollowupConfig filters apply.
    When True, these fields replace the global mcap/bought gates
    (``None`` on min_* means no floor; max_mcap falls back to global if None).
    Prune fields: ``None`` keeps the global prune value when custom.
    """

    custom: bool = False
    max_mcap_alert: float | None = None
    min_mcap_alert: float | None = None
    min_bought_usd: float | None = None
    max_bought_usd: float | None = None
    # Auto-drop if discovery token ATH stays below threshold after N hours.
    prune_enabled: bool | None = None
    prune_min_ath_mcap: float | None = None
    prune_after_hours: float | None = None


class FollowupWalletFiltersUpdate(BaseModel):
    """Apply alert filters to one or many tracked wallets."""

    addresses: list[str] = Field(default_factory=list, min_length=1)
    filters: WalletAlertFilters


class FollowupWalletRow(BaseModel):
    address: str
    status: str = "watching"  # watching | done | paused
    deal_count: int = 0
    wallet_balance_eth: float | None = None
    tokens_traded_7d: int | None = None
    raybot_synced: bool = False
    first_token: str = ""
    first_mcap: float | None = None
    discovered_at: float = 0.0
    updated_at: float = 0.0
    alert_filters: WalletAlertFilters = Field(default_factory=WalletAlertFilters)
    deals: list[FollowupDealRow] = Field(default_factory=list)


class FollowupStatus(BaseModel):
    enabled: bool = False
    running: bool = False
    telegram_configured: bool = False
    bot_commands_enabled: bool = True
    bot_polling: bool = False
    raybot_configured: bool = False
    next_run_ts: float | None = None
    last_run_ts: float | None = None
    last_run_duration_sec: float | None = None
    last_error: str | None = None
    last_message: str = ""
    wallets_watching: int = 0
    wallets_done: int = 0
    last_checked: int = 0
    last_new_deals: int = 0
    last_alerts_sent: int = 0
    stop_requested: bool = False
    # Priority scheduler telemetry (last completed cycle).
    last_due_count: int = 0
    last_hot_checked: int = 0
    last_warm_checked: int = 0
    last_zero_rechecked: int = 0
    last_skipped_zero_balance: int = 0
    last_hot_revisit_sec: float | None = None
    # Resilience telemetry.
    logwatch_degraded: bool = False
    logwatch_fail_streak: int = 0
    last_reconcile_ts: float | None = None
    last_pending_alerts_retried: int = 0
    active_rpc: str = ""
    cursor_lag_blocks: int | None = None
    # Transactional outbox telemetry.
    outbox_pending: int = 0
    outbox_failed: int = 0
    last_outbox_dispatched: int = 0
    log: list[JobLogEntry] = Field(default_factory=list)
