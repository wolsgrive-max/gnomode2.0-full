"""Pytest fixtures for gnomode backend tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))


@pytest.fixture(autouse=True)
def _isolate_telegram_credentials(monkeypatch):
    """Never let unit tests inherit live TELEGRAM_* from the developer .env.

    ``app.telegram.resolve_chat_id("")`` falls back to env/settings; without
    isolation, ingest tests that leave ``telegram_chat_id=""`` will send real
    alerts (fixture wallet 0xaaa… / SEED already hit production once).
    """
    for key in (
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_TOPIC_ID",
    ):
        monkeypatch.setenv(key, "")
    from app.config import settings

    monkeypatch.setattr(settings, "telegram_bot_token", "")
    monkeypatch.setattr(settings, "telegram_chat_id", "")
    monkeypatch.setattr(settings, "telegram_topic_id", "")


@pytest.fixture
def tmp_watch_paths(tmp_path, monkeypatch):
    config_path = tmp_path / "watch.json"
    seen_path = tmp_path / "watch_seen.json"
    hold_path = tmp_path / "watch_hold.json"
    from app.config import settings

    monkeypatch.setattr(settings, "watch_config_path", str(config_path))
    monkeypatch.setattr(settings, "watch_seen_path", str(seen_path))
    monkeypatch.setattr(settings, "watch_hold_path", str(hold_path))
    return config_path, seen_path
