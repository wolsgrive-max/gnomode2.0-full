"""Tests for launchpad registry, log parsers, DB RayBot scoring, GoPlus."""

from __future__ import annotations

from app.constants import BAGS_MIGRATED_TOPIC, BAGS_V4_HOOK, HOODFUN_GRADUATED_TOPIC
from app.database import Database
from app.goplus import classify_security
from app.launchpads.bags import parse_bags_migrated_log
from app.launchpads.hoodfun import parse_hoodfun_graduated_log
from app.launchpads.registry import classify_pool
from app.ws_migration import parse_v4_initialize_log


def test_classify_bags_by_hooks():
    spec = classify_pool(hooks=BAGS_V4_HOOK, dex="uniswap_v4")
    assert spec.id == "bags"
    assert spec.kind == "curve_v4"


def test_classify_unknown_v3():
    spec = classify_pool(dex="uniswap_v3")
    assert spec.id == "unknown_v3"


def test_parse_v4_initialize_bags_hooks():
    # RH layout: topics[2]=WETH, topics[3]=token; data: fee, tickSpacing, hooks, ...
    weth = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
    token = "0x1111111111111111111111111111111111111111"
    hooks = BAGS_V4_HOOK

    def topic_addr(a: str) -> str:
        return "0x" + a.lower().replace("0x", "").rjust(64, "0")

    def pad_word(hex40: str) -> str:
        return hex40.lower().replace("0x", "").rjust(64, "0")

    data = "0x" + ("0" * 64) + ("0" * 64) + pad_word(hooks) + ("0" * 64) + ("0" * 64)
    log = {
        "topics": [
            "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
            "0x" + "ab" * 32,
            topic_addr(weth),
            topic_addr(token),
        ],
        "data": data,
        "blockNumber": 100,
        "transactionHash": "0x" + "cd" * 32,
    }
    ev = parse_v4_initialize_log(log)
    assert ev is not None
    assert ev.token.lower() == token.lower()
    assert ev.launchpad_id == "bags"
    assert ev.dex == "uniswap_v4"


def test_parse_bags_migrated():
    token = "0x2222222222222222222222222222222222222222"
    creator = "0x3333333333333333333333333333333333333333"
    admin = "0x4444444444444444444444444444444444444444"

    def topic_addr(a: str) -> str:
        return "0x" + a.lower().replace("0x", "").rjust(64, "0")

    log = {
        "topics": [
            BAGS_MIGRATED_TOPIC,
            topic_addr(creator),
            topic_addr(admin),
            topic_addr(token),
        ],
        "data": "0x" + ("00" * 32) + ("00" * 32) + ("ef" * 32) + ("00" * 32),
        "blockNumber": 200,
        "transactionHash": "0x" + "aa" * 32,
    }
    ev = parse_bags_migrated_log(log)
    assert ev is not None
    assert ev.launchpad_id == "bags"
    assert ev.token.lower() == token.lower()
    assert ev.pool_id.startswith("0x")


def test_parse_hoodfun_graduated():
    token = "0x5555555555555555555555555555555555555555"
    pool = "0x6666666666666666666666666666666666666666"

    def topic_addr(a: str) -> str:
        return "0x" + a.lower().replace("0x", "").rjust(64, "0")

    log = {
        "topics": [
            HOODFUN_GRADUATED_TOPIC,
            topic_addr(token),
            topic_addr(pool),
        ],
        "data": "0x",
        "blockNumber": 300,
        "transactionHash": "0x" + "bb" * 32,
    }
    ev = parse_hoodfun_graduated_log(log)
    assert ev is not None
    assert ev.launchpad_id == "hoodfun"
    assert ev.dex == "uniswap_v3"
    assert ev.pool.lower() == pool.lower()


def test_goplus_flags():
    blocked = classify_security(
        "0xabc",
        {"is_honeypot": "0", "sell_tax": "0.15", "buy_tax": "0"},
    )
    assert blocked.blocked
    assert blocked.reason and "sell_tax" in blocked.reason

    ok = classify_security("0xabc", {"is_honeypot": "0", "sell_tax": "0.05"})
    assert not ok.blocked

    sd = classify_security("0xabc", {"selfdestruct": "1"})
    assert sd.blocked


def test_raybot_unique_pair(tmp_path):
    db = Database(tmp_path / "t.db")
    w = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    t1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    t2 = "0xcccccccccccccccccccccccccccccccccccccccc"
    assert db.insert_trade(wallet=w, token=t1, amount_usd=10, block=1)
    assert not db.insert_trade(wallet=w, token=t1, amount_usd=20, block=2)
    assert db.insert_trade(wallet=w, token=t2, amount_usd=5, block=3)
    snipers = db.get_snipers_by_trade_count()
    assert len(snipers) == 1
    assert snipers[0]["trade_count"] == 2
    db.close()
