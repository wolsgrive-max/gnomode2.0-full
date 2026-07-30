"""Flap LaunchedToDEX parser tests."""

from __future__ import annotations

from app.constants import FLAP_LAUNCHED_TO_DEX_TOPIC, FLAP_LAUNCHPAD
from app.launchpads.flap import parse_flap_launched_log


def test_parse_flap_launched_to_dex():
    token = "0xedcae4d41af50c602cff856aa673533856057777"
    pool = "0x8c3c52f1236c7f1e52ddeb5caed15febe262cf5a"
    data = (
        "0x"
        + token[2:].rjust(64, "0")
        + pool[2:].rjust(64, "0")
        + "0" * 64
        + "0" * 64
    )
    log = {
        "address": FLAP_LAUNCHPAD,
        "topics": [FLAP_LAUNCHED_TO_DEX_TOPIC],
        "data": data,
        "blockNumber": 22877526,
        "transactionHash": "0x" + "ab" * 32,
    }
    ev = parse_flap_launched_log(log)
    assert ev is not None
    assert ev.launchpad_id == "flap"
    assert ev.dex == "uniswap_v2"
    assert ev.token.lower() == token.lower()
    assert ev.pool and ev.pool.lower() == pool.lower()
