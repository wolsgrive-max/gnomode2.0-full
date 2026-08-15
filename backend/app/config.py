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
    mcap_threshold: float = 20_000.0
    # Larger chunks = fewer round-trips (filtered getLogs stay small)
    log_chunk_size: int = 100_000
    # Public Robinhood RPC rate-limits aggressively; keep this low.
    rpc_concurrency: int = 6
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
    watch_hold_path: str = str(_DEFAULT_DATA / "watch_hold.json")

    # Follow-up watchlist (SQLite) + optional RayBot EVM sync
    followup_db_path: str = str(_DEFAULT_DATA / "followup.db")
    followup_config_path: str = str(_DEFAULT_DATA / "followup.json")
    raybot_api_user: str = ""
    raybot_api_token: str = ""
    # Bot number from RayBot docs (1 = @ray_purple_bot). 0 = webhook destination.
    raybot_bot: int = 1
    raybot_base_url: str = "https://webapi.raybot.app"
    raybot_webhook_auth: str = ""


settings = Settings()
