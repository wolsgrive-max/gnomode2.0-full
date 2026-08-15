"""API + bot command tests for Follow-up (native Telegram bot)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "followup.db"
    cfg_path = tmp_path / "followup.json"
    store = FollowupStore(db_path=str(db_path), config_path=str(cfg_path))

    from app.config import settings

    monkeypatch.setattr(settings, "telegram_bot_token", "test-bot")
    monkeypatch.setattr(settings, "telegram_chat_id", "100")
    monkeypatch.setattr(settings, "followup_db_path", str(db_path))
    monkeypatch.setattr(settings, "followup_config_path", str(cfg_path))

    import app.followup as followup_mod
    import app.followup_bot as bot_mod
    import app.followup_store as store_mod
    import app.main as main
    import app.watch as watch_mod

    monkeypatch.setattr(store_mod, "followup_store", store)
    monkeypatch.setattr(followup_mod, "followup_store", store)
    monkeypatch.setattr(bot_mod, "followup_store", store)
    followup_mod.followup_runner._store = store
    monkeypatch.setattr(main, "followup_store", store)
    monkeypatch.setattr(main, "followup_runner", followup_mod.followup_runner)

    async def _noop_loop():
        return None

    monkeypatch.setattr(main.token_index, "run_refresh_loop", _noop_loop)
    monkeypatch.setattr(watch_mod.watch_runner, "run_loop", _noop_loop)
    monkeypatch.setattr(followup_mod.followup_runner, "run_loop", _noop_loop)
    monkeypatch.setattr(bot_mod.followup_bot, "run_loop", _noop_loop)

    with TestClient(main.app) as c:
        yield c, store


def test_get_put_followup(client):
    c, store = client
    res = c.get("/api/followup")
    assert res.status_code == 200
    assert res.json()["enabled"] is False
    assert res.json()["bot_commands_enabled"] is True

    payload = FollowupConfig(
        enabled=True,
        interval_sec=180,
        max_mcap_alert=12_000,
        min_mcap_alert=500,
        bot_commands_enabled=True,
        telegram_chat_id="555",
    ).model_dump(mode="json")
    res = c.put("/api/followup", json=payload)
    assert res.status_code == 200
    body = res.json()
    assert body["enabled"] is True
    assert body["max_mcap_alert"] == 12_000
    assert body["min_mcap_alert"] == 500
    assert store.load_config().telegram_chat_id == "555"

    res = c.get("/api/followup/status")
    assert res.status_code == 200
    st = res.json()
    assert st["enabled"] is True
    assert st["telegram_configured"] is True
    assert "bot_polling" in st
    assert "bot_commands_enabled" in st


def test_followup_wallets_after_ingest(client):
    c, store = client
    buyers = [
        BuyerRow(
            wallet="0xAAA0000000000000000000000000000000000001",
            token="0xBBB0000000000000000000000000000000000001",
            token_symbol="T1",
            bought_tokens=1.0,
            bought_usd=100.0,
            mcap_at_first_buy=8_000.0,
            buys_count=1,
        )
    ]
    inserted = store.ingest_buyers(buyers, max_mcap_alert=15_000)
    assert len(inserted) == 1

    res = c.get("/api/followup/wallets")
    assert res.status_code == 200
    rows = res.json()
    assert len(rows) == 1
    assert rows[0]["deal_count"] == 1
    assert rows[0]["deals"][0]["deal_index"] == 1


def test_followup_run_stop(client):
    c, _store = client
    res = c.post("/api/followup/run")
    assert res.status_code == 200
    res = c.post("/api/followup/stop")
    assert res.status_code == 200
    assert res.json()["stop_requested"] is True


@pytest.mark.asyncio
async def test_bot_commands_set_max_mcap(tmp_path, monkeypatch):
    store = FollowupStore(
        db_path=str(tmp_path / "f.db"),
        config_path=str(tmp_path / "f.json"),
    )
    store.save_config(FollowupConfig(max_mcap_alert=15_000))

    import app.followup as followup_mod
    import app.followup_bot as bot_mod

    monkeypatch.setattr(bot_mod, "followup_store", store)
    monkeypatch.setattr(followup_mod, "followup_store", store)
    followup_mod.followup_runner._store = store

    bot = bot_mod.FollowupBot()
    reply = await bot._handle("/set_max_mcap", ["9000"])
    assert "9000" in reply.replace(",", "")
    assert store.load_config().max_mcap_alert == 9000.0

    reply = await bot._handle("/filters", [])
    assert "max_mcap" in reply
    reply = await bot._handle("/on", [])
    assert store.load_config().enabled is True
    reply = await bot._handle("/off", [])
    assert store.load_config().enabled is False

    reply = await bot._handle("/set_min_bought", ["50"])
    assert store.load_config().min_bought_usd == 50.0
    reply = await bot._handle("/set_max_bought", ["off"])
    assert store.load_config().max_bought_usd is None
    reply = await bot._handle("/set_buys_only", ["off"])
    assert store.load_config().buys_only is False
    reply = await bot._handle("/set_transfers", ["on"])
    assert store.load_config().track_transfers is True
    reply = await bot._handle("/filters", [])
    assert "track_transfers" in reply
    assert "bought_usd" in reply


def test_bot_chat_allowed_private_and_configured(tmp_path, monkeypatch):
    store = FollowupStore(
        db_path=str(tmp_path / "f.db"),
        config_path=str(tmp_path / "f.json"),
    )
    store.save_config(
        FollowupConfig(telegram_chat_id="-100111", bot_commands_enabled=True)
    )
    import app.followup_bot as bot_mod
    from app.models import WatchConfig

    class _FakeWatchStore:
        def load_config(self):
            return WatchConfig(telegram_chat_id="-100333")

    monkeypatch.setattr(bot_mod, "followup_store", store)
    monkeypatch.setattr(bot_mod, "resolve_chat_id", lambda override=None: (override or "-100222"))
    monkeypatch.setitem(__import__("sys").modules, "app.watch_store", type("M", (), {"watch_store": _FakeWatchStore()})())
    # Patch the late import inside _chat_allowed
    import app.watch_store as ws_mod

    monkeypatch.setattr(ws_mod, "watch_store", _FakeWatchStore())
    bot = bot_mod.FollowupBot()
    assert bot._chat_allowed("999", chat_type="private") is True
    assert bot._chat_allowed("-100111", chat_type="supergroup") is True
    assert bot._chat_allowed("-100222", chat_type="supergroup") is True
    assert bot._chat_allowed("-100333", chat_type="supergroup") is True
    assert bot._chat_allowed("-100999", chat_type="supergroup") is False
