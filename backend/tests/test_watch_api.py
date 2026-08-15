"""API tests for /api/watch endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.models import WatchConfig
from app.watch_store import WatchStore


@pytest.fixture
def client(tmp_path, monkeypatch):
    config_path = tmp_path / "watch.json"
    seen_path = tmp_path / "seen.json"
    state_path = tmp_path / "state.json"
    hold_path = tmp_path / "hold.json"
    store = WatchStore(
        config_path=config_path,
        seen_path=seen_path,
        state_path=state_path,
        hold_path=hold_path,
    )

    from app.config import settings

    monkeypatch.setattr(settings, "telegram_bot_token", "test-bot")
    monkeypatch.setattr(settings, "telegram_chat_id", "100")
    monkeypatch.setattr(settings, "watch_config_path", str(config_path))
    monkeypatch.setattr(settings, "watch_seen_path", str(seen_path))
    monkeypatch.setattr(settings, "watch_state_path", str(state_path))
    monkeypatch.setattr(settings, "watch_hold_path", str(hold_path))

    import app.main as main
    import app.watch as watch_mod
    import app.watch_store as store_mod

    monkeypatch.setattr(store_mod, "watch_store", store)
    monkeypatch.setattr(watch_mod, "watch_store", store)
    watch_mod.watch_runner._store = store
    monkeypatch.setattr(main, "watch_store", store)
    monkeypatch.setattr(main, "watch_runner", watch_mod.watch_runner)

    async def _noop_loop():
        return None

    monkeypatch.setattr(main.token_index, "run_refresh_loop", _noop_loop)
    monkeypatch.setattr(watch_mod.watch_runner, "run_loop", _noop_loop)

    with TestClient(main.app) as c:
        yield c, store


def test_get_put_watch(client):
    c, store = client
    res = c.get("/api/watch")
    assert res.status_code == 200
    body = res.json()
    assert body["enabled"] is False

    payload = WatchConfig(
        enabled=True,
        interval_sec=300,
        max_tokens_per_cycle=7,
        telegram_chat_id="555",
    ).model_dump(mode="json")
    res = c.put("/api/watch", json=payload)
    assert res.status_code == 200
    assert res.json()["enabled"] is True
    assert res.json()["interval_sec"] == 300
    assert res.json()["max_tokens_per_cycle"] == 7
    assert store.load_config().telegram_chat_id == "555"

    res = c.get("/api/watch/status")
    assert res.status_code == 200
    st = res.json()
    assert st["enabled"] is True
    assert st["telegram_configured"] is True
    assert st["hold_count"] == 0
    assert st["parsed_token_count"] == 0
    assert "last_tokens_held" in st
    assert "last_tokens_qualified" in st

    res = c.get("/api/watch")
    assert res.json()["screen"]["min_ath_mcap"] == 40_000.0


def test_clear_seen(client):
    c, store = client
    store.mark_seen([("0xaaa", "0xbbb")])
    assert store.seen_count() == 1
    res = c.post("/api/watch/clear-seen")
    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert store.seen_count() == 0


def test_clear_pending(client):
    c, store = client
    store.apply_qualify_updates(
        ath_updates={"0xabc": (55_000.0, "ABC"), "0xdef": (80_000.0, "DEF")},
        held=[],
        expired=[],
        candidates=["0xabc", "0xdef"],
        now=100.0,
    )
    assert len(store.load_pending_parse(min_ath_mcap=40_000)) == 2
    res = c.post("/api/watch/clear-pending")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["cleared"] == 2
    assert body["pending_count"] == 0
    hold = store.load_hold()
    assert hold["0xabc"]["ath_mcap"] == 55_000.0
    assert float(hold["0xabc"].get("queued_at") or 0) == 0.0


def test_run_now_sets_force_flag(client):
    c, _store = client
    import app.watch as watch_mod

    assert watch_mod.watch_runner._force_run is False
    res = c.post("/api/watch/run")
    assert res.status_code == 200
    assert watch_mod.watch_runner._force_run is True


def test_stop_sets_flag_and_clears_force(client):
    c, _store = client
    import app.watch as watch_mod

    watch_mod.watch_runner._force_run = True
    res = c.post("/api/watch/stop")
    assert res.status_code == 200
    assert watch_mod.watch_runner._stop_requested is True
    assert watch_mod.watch_runner._force_run is False
    body = res.json()
    assert body["stop_requested"] is True
    assert isinstance(body.get("log"), list)


def test_max_tokens_up_to_2000(client):
    c, _store = client
    payload = WatchConfig(max_tokens_per_cycle=500).model_dump(mode="json")
    res = c.put("/api/watch", json=payload)
    assert res.status_code == 200
    assert res.json()["max_tokens_per_cycle"] == 500


def test_reset_counters(client):
    c, _store = client
    import app.watch as watch_mod

    watch_mod.watch_runner._last_tokens_parsed = 9
    watch_mod.watch_runner._last_buyers_sent = 4
    watch_mod.watch_runner._last_error = "boom"
    res = c.post("/api/watch/reset-counters")
    assert res.status_code == 200
    body = res.json()
    assert body["last_tokens_parsed"] == 0
    assert body["last_buyers_sent"] == 0
    assert body["last_error"] is None
    assert body["last_message"] == "Счётчики сброшены"
