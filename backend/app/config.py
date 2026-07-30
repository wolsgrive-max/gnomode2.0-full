"""Application settings."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_APP_DIR = Path(__file__).resolve().parent
_DEFAULT_DATA = _APP_DIR / "data"
# Project root (…/gnomode 2.0) and backend/ — independent of process cwd.
_ROOT_ENV = _APP_DIR.parents[1] / ".env"
_BACKEND_ENV = _APP_DIR.parent / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(str(_ROOT_ENV), str(_BACKEND_ENV), ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    rpc_url: str = "https://rpc.mainnet.chain.robinhood.com"
    blockscout_api_key: str = ""
    # Early-buyer / watch: first buy must be under this mcap (USD).
    mcap_threshold: float = 20_000.0
    # Sniper discovery: skip wallets whose annotated first mcap exceeds this.
    sniper_max_first_mcap: float = 20_000.0
    # Larger chunks = fewer round-trips (filtered getLogs stay small)
    log_chunk_size: int = 100_000
    # Public Robinhood RPC rate-limits aggressively; keep this low.
    rpc_concurrency: int = 6
    # How many tokens to parse in parallel for a manual /api/parse job.
    parse_token_concurrency: int = 2
    # Funded EOА used as eth_call `from` for honeypot buy→sell simulation
    honeypot_sim_whale: str = ""
    host: str = "0.0.0.0"
    port: int = 8000

    # Telegram alerts for the watch pipeline
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    # Forum topic / thread id (message_thread_id). Empty = General / no topic.
    telegram_topic_id: str = ""
    watch_config_path: str = str(_DEFAULT_DATA / "watch.json")
    watch_seen_path: str = str(_DEFAULT_DATA / "watch_seen.json")
    watch_state_path: str = str(_DEFAULT_DATA / "watch_state.json")
    db_path: str = str(_DEFAULT_DATA / "gnomode.db")
    # WebSocket RPC for migration bus (derived from http rpc if empty)
    wss_rpc_url: str = ""
    migration_bus_enabled: bool = True
    # When True, also emit raw V3 PoolCreated / non-Bags V4 Initialize (noisy).
    # Default False: only Bags Migrated / BagsV4Hook / hood Graduated / Flap LaunchedToDEX.
    discover_pre_migration: bool = False
    sniper_limit: int = 10
    min_buy_usd: float = 0.0
    # RayBot follow-up: track trade #2/#3 of discovered snipers
    sniper_follow_enabled: bool = True
    sniper_follow_interval_sec: int = 12
    sniper_alert_max_trade: int = 3  # alert on trade_count 2..this
    sniper_scan_lookback_blocks: int = 1_728_000  # ~2d @ 10 bps
    telegram_bot_polling: bool = True
    # Default RayBot filter defaults (per-chat overrides in user_filters)
    sniper_default_max_mcap_usd: float = 150_000.0
    sniper_default_min_buy_usd: float = 50.0
    sniper_default_exclude_honeypots: bool = True
    sniper_default_min_liq_usd: float = 0.0
    sniper_default_max_liq_usd: float = 0.0  # 0 = no max

    # MCAP tracker: follow sub-50k tokens until they hit the target
    mcap_tracker_enabled: bool = True
    mcap_tracker_interval_sec: int = 300
    mcap_tracker_target: float = 50_000.0
    mcap_tracker_max_age_days: int = 7
    mcap_tracker_min_growth_pct: float = 20.0
    mcap_tracker_dead_pct: float = 70.0


settings = Settings()
