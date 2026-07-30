"""RayBot sniper pipeline unit tests (DB + filters, no live RPC)."""

from __future__ import annotations

import asyncio

from app.database import Database
from app.launchpads.types import SniperHit
from app.sniper_score import record_sniper_hits


def test_schema_user_filters_and_wallet_fields(tmp_path):
    db = Database(tmp_path / "t.db")
    cols = {r[1] for r in db._conn.execute("PRAGMA table_info(tracked_wallets)")}
    assert "first_token" in cols
    assert "first_mcap" in cols
    assert "is_active" in cols
    assert db.get_user_filters("123") is None
    row = db.upsert_user_filters("123", min_buy_usd=100, max_mcap_usd=80_000)
    assert row["min_buy_usd"] == 100
    assert row["max_mcap_usd"] == 80_000
    assert db.get_user_filters("123")["exclude_honeypots"] in (0, 1)


def test_raybot_trade_number_and_first_token(tmp_path):
    db = Database(tmp_path / "t2.db")
    w = "0x" + "ab" * 20
    t1 = "0x" + "11" * 20
    t2 = "0x" + "22" * 20
    assert db.insert_trade(wallet=w, token=t1, mcap_at_trade=12_000, amount_usd=40)
    wal = db.get_wallet(w)
    assert wal is not None
    assert wal["first_token"] == t1
    assert wal["first_mcap"] == 12_000
    assert wal["trade_count"] == 1
    assert db.insert_trade(wallet=w, token=t2, mcap_at_trade=9_000)
    assert not db.insert_trade(wallet=w, token=t2)  # DCA ignored
    wal = db.get_wallet(w)
    assert wal["trade_count"] == 2
    trades = db.get_trades(wallet=w)
    assert len(trades) == 2
    assert {t.get("trade_number") for t in trades} == {1, 2}


def test_record_sniper_hits_async(tmp_path):
    db = Database(tmp_path / "t3.db")
    token = "0x" + "33" * 20
    hits = [
        SniperHit(wallet="0x" + "aa" * 20, block=1, tx="0x1", mcap_at_trade=1000),
        SniperHit(wallet="0x" + "bb" * 20, block=2, tx="0x2", mcap_at_trade=1100),
    ]
    n = asyncio.run(record_sniper_hits(token, hits, min_buy_usd=0, db=db))
    assert n == 2
    assert len(db.get_snipers_by_trade_count()) == 2


def test_token_by_pool_id(tmp_path):
    db = Database(tmp_path / "t4.db")
    pool = "0x" + "cd" * 32
    db.insert_token(
        address="0x" + "44" * 20,
        symbol="TEST",
        name="Test",
        launchpad_id="bags",
        dex="uniswap_v4",
        pool_id=pool,
    )
    found = db.get_token_by_pool_id(pool)
    assert found is not None
    assert found["symbol"] == "TEST"


def test_track_one_token_buyers_only_exactly_one(tmp_path, monkeypatch):
    from app.models import BuyerRow
    from app import watch as watch_mod
    from app.sniper_score import record_sniper_trade

    db = Database(tmp_path / "track.db")

    async def _rec(wallet, token, **kwargs):
        kwargs.pop("db", None)
        return await record_sniper_trade(wallet, token, db=db, **kwargs)

    monkeypatch.setattr(watch_mod, "record_sniper_trade", _rec)

    async def _run():
        buyers = [
            BuyerRow(
                wallet="0x" + "aa" * 20,
                token="0x" + "11" * 20,
                bought_tokens=1,
                bought_usd=10,
                mcap_at_first_buy=5000,
                buys_count=1,
                first_tx="0x1",
                first_block=10,
                tokens_traded_7d=1,
            ),
            BuyerRow(
                wallet="0x" + "dd" * 20,
                token="0x" + "11" * 20,
                bought_tokens=1,
                bought_usd=10,
                mcap_at_first_buy=25_000,  # over 20k cap
                buys_count=1,
                tokens_traded_7d=1,
            ),
            BuyerRow(
                wallet="0x" + "bb" * 20,
                token="0x" + "11" * 20,
                bought_tokens=1,
                bought_usd=10,
                mcap_at_first_buy=5000,
                buys_count=1,
                tokens_traded_7d=2,
            ),
            BuyerRow(
                wallet="0x" + "cc" * 20,
                token="0x" + "11" * 20,
                bought_tokens=1,
                bought_usd=10,
                mcap_at_first_buy=5000,
                buys_count=1,
                tokens_traded_7d=None,
            ),
        ]
        n = await watch_mod.track_one_token_buyers(buyers, max_first_mcap=20_000)
        assert n == 1
        w = db.get_wallet("0x" + "aa" * 20)
        assert w is not None
        assert w["trade_count"] == 1
        assert w["first_token"] == ("0x" + "11" * 20)
        assert db.get_wallet("0x" + "bb" * 20) is None
        assert db.get_wallet("0x" + "dd" * 20) is None

    asyncio.run(_run())


def test_deactivate_wallets_above_first_mcap(tmp_path):
    db = Database(tmp_path / "prune.db")
    db.insert_trade(wallet="0x" + "aa" * 20, token="0x" + "11" * 20, mcap_at_trade=10_000)
    db.insert_trade(wallet="0x" + "bb" * 20, token="0x" + "22" * 20, mcap_at_trade=30_000)
    n = db.deactivate_wallets_above_first_mcap(20_000)
    assert n == 1
    assert db.get_wallet("0x" + "aa" * 20)["is_active"] in (1, True)
    assert db.get_wallet("0x" + "bb" * 20)["is_active"] in (0, False)

