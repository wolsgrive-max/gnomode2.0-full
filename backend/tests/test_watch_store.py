"""Tests for watch config / seen-set / ATH hold persistence and dedup keys."""

from __future__ import annotations

from app.models import WatchConfig, WatchScreenFilters
from app.watch_store import WatchStore, seen_key


def _store(tmp_path) -> WatchStore:
    return WatchStore(
        config_path=tmp_path / "watch.json",
        seen_path=tmp_path / "seen.json",
        state_path=tmp_path / "state.json",
        hold_path=tmp_path / "hold.json",
    )


def test_seen_key_normalizes():
    assert seen_key(" 0xAbC ", "0xDeF") == "0xabc:0xdef"


def test_config_roundtrip(tmp_path):
    store = _store(tmp_path)
    assert store.load_config().enabled is False

    cfg = WatchConfig(
        enabled=True,
        interval_sec=600,
        max_tokens_per_cycle=5,
        telegram_chat_id="42",
        screen=WatchScreenFilters(min_liq=1000.0, min_ath_mcap=50_000.0),
    )
    store.save_config(cfg)
    loaded = store.load_config()
    assert loaded.enabled is True
    assert loaded.interval_sec == 600
    assert loaded.max_tokens_per_cycle == 5
    assert loaded.telegram_chat_id == "42"
    assert loaded.screen.min_liq == 1000.0
    assert loaded.screen.min_ath_mcap == 50_000.0


def test_seen_dedup_and_clear(tmp_path):
    store = _store(tmp_path)
    w1, t1 = "0xAAA", "0xTTT"
    w2, t2 = "0xBBB", "0xTTT"

    assert store.is_seen(w1, t1) is False
    assert store.mark_seen([(w1, t1)]) == 1
    assert store.is_seen(w1, t1) is True
    assert store.is_seen(w1.lower(), t1.lower()) is True
    assert store.mark_seen([(w1, t1)]) == 0
    assert store.mark_seen([(w2, t2)]) == 1
    assert store.seen_count() == 2

    # Persist across new store instance
    store2 = _store(tmp_path)
    assert store2.is_seen(w1, t1) is True
    assert store2.seen_count() == 2

    store2.clear_seen()
    assert store2.seen_count() == 0
    assert store2.is_seen(w1, t1) is False


def test_hold_upsert_promote_and_mark_parsed(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xabc": (12_000.0, "ABC")},
        held=["0xabc"],
        expired=[],
        candidates=[],
        now=100.0,
    )
    assert store.hold_count() == 1
    assert store.load_hold()["0xabc"]["ath_mcap"] == 12_000.0

    store.apply_qualify_updates(
        ath_updates={"0xabc": (55_000.0, "ABC")},
        held=[],
        expired=[],
        candidates=["0xabc"],
        now=200.0,
    )
    # Still on hold until mark_parsed (retry if parse fails).
    assert store.hold_count() == 1
    assert store.load_hold()["0xabc"]["ath_mcap"] == 55_000.0

    store.mark_token_parsed("0xAbC", at=123.0)
    assert store.is_token_parsed("0xabc") is True
    assert store.hold_count() == 0
    assert store.parsed_token_count() == 1
    assert store.load_parsed_at()["0xabc"] == 123.0

    store2 = _store(tmp_path)
    assert store2.is_token_parsed("0xabc") is True
    assert store2.load_parsed_at()["0xabc"] == 123.0
    assert store2.hold_count() == 0

    assert store2.unparse_tokens(["0xAbC"]) == 1
    assert store2.is_token_parsed("0xabc") is False


def test_legacy_parsed_list_loads_with_zero_ts(tmp_path):
    import json

    hold_path = tmp_path / "hold.json"
    hold_path.write_text(
        json.dumps({"hold": {}, "parsed": ["0xLegacyToken"]}),
        encoding="utf-8",
    )
    store = _store(tmp_path)
    assert store.is_token_parsed("0xlegacytoken")
    assert store.load_parsed_at()["0xlegacytoken"] == 0.0


def test_hold_expired_removed(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xold": (5_000.0, "OLD")},
        held=["0xold"],
        expired=[],
        now=1.0,
    )
    store.apply_qualify_updates(
        ath_updates={},
        held=[],
        expired=["0xold"],
        candidates=[],
        now=2.0,
    )
    assert store.hold_count() == 0
