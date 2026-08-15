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
    assert store.load_hold()["0xabc"]["queued_at"] == 200.0

    store.mark_token_parsed("0xAbC", at=123.0)
    assert store.is_token_parsed("0xabc") is True
    # Requeue meta kept (ATH + first_seen); not counted as waiting hold.
    assert store.hold_count() == 0
    meta = store.load_hold()["0xabc"]
    assert meta["ath_mcap"] == 55_000.0
    assert meta["first_seen"] == 100.0
    assert float(meta.get("queued_at") or 0.0) == 0.0
    assert store.parsed_token_count() == 1
    assert store.load_parsed_at()["0xabc"] == 123.0

    store2 = _store(tmp_path)
    assert store2.is_token_parsed("0xabc") is True
    assert store2.load_parsed_at()["0xabc"] == 123.0
    assert store2.load_hold()["0xabc"]["ath_mcap"] == 55_000.0

    assert store2.unparse_tokens(["0xAbC"]) == 1
    assert store2.is_token_parsed("0xabc") is False
    assert float(store2.load_hold()["0xabc"].get("queued_at") or 0.0) > 0.0


def test_alert_outbox_roundtrip(tmp_path):
    from app.models import BuyerRow

    store = _store(tmp_path)
    b = BuyerRow(
        wallet="0x" + "1" * 40,
        token="0x" + "a" * 40,
        token_symbol="T",
        bought_tokens=1.0,
        bought_usd=1.0,
        mcap_at_first_buy=1000.0,
        buys_count=1,
        first_tx="0x" + "b" * 64,
    )
    assert store.enqueue_alert_outbox(b.token, [b]) == 1
    assert store.alert_outbox_count() == 1
    loaded = store.load_alert_outbox()
    assert b.token.lower() in loaded
    store.clear_alert_outbox(b.token)
    assert store.alert_outbox_count() == 0


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


def test_candidate_persists_queued_at_fifo(tmp_path):
    """Parse candidates get a hold row + queued_at so drain-all can resume."""
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xlev": (80_000.0, "LEVCAT")},
        held=[],
        expired=[],
        candidates=["0xlev"],
        now=500.0,
    )
    ent = store.load_hold()["0xlev"]
    assert ent["ath_mcap"] == 80_000.0
    assert ent["queued_at"] == 500.0

    # Second cycle must not reset FIFO stamp.
    store.apply_qualify_updates(
        ath_updates={"0xlev": (90_000.0, "LEVCAT")},
        held=[],
        expired=[],
        candidates=["0xlev"],
        now=900.0,
    )
    ent2 = store.load_hold()["0xlev"]
    assert ent2["ath_mcap"] == 90_000.0
    assert ent2["queued_at"] == 500.0


def test_pending_survives_wipe_when_absent_from_decision(tmp_path):
    """Unparsed qualify must not be deleted when not in current held∪candidates."""
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xhot": (200_000.0, "HOT"), "0xdust": (5_000.0, "DUST")},
        held=["0xdust"],
        expired=[],
        candidates=["0xhot"],
        now=100.0,
    )
    assert "0xhot" in store.load_pending_parse(min_ath_mcap=40_000)
    assert store.load_hold()["0xhot"]["queued_at"] == 100.0

    # Later classify omits 0xhot (not on screen) but still has other dust.
    store.apply_qualify_updates(
        ath_updates={"0xdust": (6_000.0, "DUST")},
        held=["0xdust"],
        expired=[],
        candidates=[],
        now=200.0,
    )
    assert "0xhot" in store.load_hold()
    assert store.load_hold()["0xhot"]["queued_at"] == 100.0
    assert "0xhot" in store.load_pending_parse(min_ath_mcap=40_000)
    # Non-pending dust absent from held would be wiped — dust is still held.
    assert "0xdust" in store.load_hold()


def test_pending_queued_at_survives_reload(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xabc": (55_000.0, "ABC")},
        held=[],
        expired=[],
        candidates=["0xabc"],
        now=777.0,
    )
    store2 = _store(tmp_path)
    ent = store2.load_hold()["0xabc"]
    assert ent["queued_at"] == 777.0
    assert store2.load_pending_parse(min_ath_mcap=40_000) == ["0xabc"]


def test_remove_hold_tokens_age_drop(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xold": (80_000.0, "OLD")},
        held=[],
        expired=[],
        candidates=["0xold"],
        now=1.0,
    )
    assert store.remove_hold_tokens(["0xOLD"]) == 1
    assert store.hold_count() == 0
    assert store.load_pending_parse() == []


def test_clear_pending_queued_keeps_ath(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xold": (148_000.0, "MEAT")},
        held=[],
        expired=[],
        candidates=["0xold"],
        now=1.0,
    )
    assert store.load_pending_parse(min_ath_mcap=40_000) == ["0xold"]
    assert store.clear_pending_queued(["0xOLD"]) == 1
    hold = store.load_hold()
    assert "0xold" in hold
    assert hold["0xold"]["ath_mcap"] == 148_000.0
    assert float(hold["0xold"].get("queued_at") or 0) == 0.0
    assert store.load_pending_parse(min_ath_mcap=40_000) == []
    # Second clear is a no-op.
    assert store.clear_pending_queued(["0xold"]) == 0


def test_clear_all_pending_queued_keeps_ath(tmp_path):
    store = _store(tmp_path)
    store.apply_qualify_updates(
        ath_updates={"0xaa": (50_000.0, "A"), "0xbb": (60_000.0, "B")},
        held=[],
        expired=[],
        candidates=["0xaa", "0xbb"],
        now=10.0,
    )
    assert len(store.load_pending_parse(min_ath_mcap=40_000)) == 2
    assert store.clear_all_pending_queued() == 2
    hold = store.load_hold()
    assert hold["0xaa"]["ath_mcap"] == 50_000.0
    assert hold["0xbb"]["ath_mcap"] == 60_000.0
    assert store.load_pending_parse(min_ath_mcap=40_000) == []
    assert store.clear_all_pending_queued() == 0
