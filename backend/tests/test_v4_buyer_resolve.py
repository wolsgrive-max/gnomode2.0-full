"""V4 Universal-Router buyer resolution (PM → UR → EOA hop)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from hexbytes import HexBytes

from app.constants import (
    TRANSFER_TOPIC,
    UNI_V4_POOL_MANAGER,
    UNIVERSAL_ROUTER,
    V4_SWAP_TOPIC,
)
from app.replay import _norm_tx_hash, _resolve_buyers_batch, _transfer_from_ok


TOKEN = "0x5ae8d07763d74ca5bd22f8a5b26c6d953e61dfe2"
WALLET = "0x952b61bd0185533e926154f0e4e98452ee1f1186"
TX = "0x1f0707a41ad0b9a7bcb9b31b139db4e439d9d36bd27bac120103ff5326cbf278"
POOL_ID = "0x38798d55dafe55ab2521eca75b197b577d177da9990d950f45ecc8b59c5b0472"
AMOUNT = 89_044_957_318_009_828_498_139_485


def _topic_addr(addr: str) -> HexBytes:
    return HexBytes("0x" + "0" * 24 + addr.lower().replace("0x", ""))


def _transfer_log(*, frm: str, to: str, amount: int, log_index: int, tx: str = TX) -> dict:
    return {
        "transactionHash": HexBytes(tx),
        "logIndex": log_index,
        "address": TOKEN,
        "topics": [
            HexBytes(TRANSFER_TOPIC),
            _topic_addr(frm),
            _topic_addr(to),
        ],
        "data": HexBytes("0x" + amount.to_bytes(32, "big").hex()),
    }


def _v4_swap_log(*, tx: str = TX) -> dict:
    return {
        "transactionHash": HexBytes(tx),
        "logIndex": 0,
        "address": UNI_V4_POOL_MANAGER,
        "topics": [
            HexBytes(V4_SWAP_TOPIC),
            HexBytes(POOL_ID),
            _topic_addr(UNIVERSAL_ROUTER),
        ],
        "data": HexBytes("0x" + "00" * 192),
    }


@pytest.mark.asyncio
async def test_resolve_v2_helper_contract_hop_via_receipt() -> None:
    """MEOW-class: pool→helper-contract→EOA must resolve to the EOA, not the relay."""
    pool = "0x8366a39CC670B4001A1121B8F6A443A643e40951"
    helper = "0xc812a7c831fb52cc5E2aD748cad142134523291C"
    eoa = "0xEB0469Fb8a57C8321B4d97bB1E696A5EE0136E1B"
    amount = 70_561_990_530_469_518_962_777_811
    swap = {
        "transactionHash": HexBytes(TX),
        "logIndex": 0,
        "topics": [
            HexBytes("0xd78ad95fa46c994b6551d0da85fc275feebd9f49f3b6bfa8bbdd1d0c0c0c0c0c"),
            _topic_addr(eoa),
            _topic_addr(eoa),
        ],
        "data": HexBytes("0x" + "00" * 128),
    }
    # Topic-filtered getLogs only sees pool→helper (as V2 replay does).
    xfers_by_tx = {
        _norm_tx_hash(TX): [
            _transfer_log(frm=pool, to=helper, amount=amount, log_index=1),
        ]
    }
    receipt = {
        "logs": [
            {
                "address": TOKEN,
                "topics": [
                    HexBytes(TRANSFER_TOPIC),
                    _topic_addr(pool),
                    _topic_addr(helper),
                ],
                "data": HexBytes("0x" + amount.to_bytes(32, "big").hex()),
                "logIndex": 1,
            },
            {
                "address": TOKEN,
                "topics": [
                    HexBytes(TRANSFER_TOPIC),
                    _topic_addr(helper),
                    _topic_addr(eoa),
                ],
                "data": HexBytes("0x" + amount.to_bytes(32, "big").hex()),
                "logIndex": 2,
            },
        ]
    }

    def _is_eoa(addrs, cache=None):
        out = {}
        for a in addrs:
            al = a.lower()
            out[al] = al == eoa.lower()
        if cache is not None:
            cache.update(out)
            return cache
        return out

    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(side_effect=_is_eoa),
        batch_get_receipts=AsyncMock(
            return_value={_norm_tx_hash(TX): receipt}
        ),
        _jsonrpc_batch=AsyncMock(return_value=[]),
    )

    got = await _resolve_buyers_batch(
        rpc,  # type: ignore[arg-type]
        token=TOKEN,
        pool_or_manager=pool,
        early_swaps=[swap],
        xfers_by_tx=xfers_by_tx,
    )
    txh = _norm_tx_hash(TX)
    assert txh in got
    buyer, amt = got[txh]
    assert buyer.lower() == eoa.lower()
    assert amt == amount
    assert buyer.lower() != helper.lower()


def test_norm_tx_hash_adds_0x_for_hexbytes() -> None:
    raw = HexBytes(TX)
    assert raw.hex() == TX[2:]  # no 0x
    assert _norm_tx_hash(raw) == TX.lower()
    assert _norm_tx_hash(TX[2:]) == TX.lower()
    assert _norm_tx_hash(TX) == TX.lower()


def test_transfer_from_ok_allows_universal_router() -> None:
    assert _transfer_from_ok(UNI_V4_POOL_MANAGER, UNI_V4_POOL_MANAGER)
    assert _transfer_from_ok(UNIVERSAL_ROUTER, UNI_V4_POOL_MANAGER)
    assert not _transfer_from_ok(WALLET, UNI_V4_POOL_MANAGER)


@pytest.mark.asyncio
async def test_resolve_v4_universal_router_hop() -> None:
    """FROGLET-class: PM→UR→EOA must resolve to the wallet, not drop the tx."""
    swap = _v4_swap_log()
    xfers = [
        _transfer_log(frm=UNI_V4_POOL_MANAGER, to=UNIVERSAL_ROUTER, amount=AMOUNT, log_index=1),
        _transfer_log(frm=UNIVERSAL_ROUTER, to=WALLET, amount=AMOUNT, log_index=2),
    ]
    txh = _norm_tx_hash(TX)
    xfers_by_tx = {txh: xfers}

    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(
            side_effect=lambda addrs, cache=None: {a.lower(): a.lower() == WALLET.lower() for a in addrs}
        ),
        batch_get_receipts=AsyncMock(return_value={}),
        _jsonrpc_batch=AsyncMock(return_value=[]),
    )

    # Avoid creator-launch scan (would RPC).
    import app.replay as replay_mod

    async def _no_skip(_rpc, hashes):
        return set()

    monkey = pytest.MonkeyPatch()
    monkey.setattr(replay_mod, "_creator_launch_tx_hashes", _no_skip)
    try:
        got = await _resolve_buyers_batch(
            rpc,  # type: ignore[arg-type]
            token=TOKEN,
            pool_or_manager=UNI_V4_POOL_MANAGER,
            early_swaps=[swap],
            xfers_by_tx=xfers_by_tx,
        )
    finally:
        monkey.undo()

    assert txh in got
    buyer, amt = got[txh]
    assert buyer.lower() == WALLET.lower()
    assert amt == AMOUNT
    rpc.batch_get_receipts.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_receipt_fallback_with_bare_hex_hash() -> None:
    """Receipt fallback must work even when HexBytes.hex() omitted 0x historically."""
    swap = _v4_swap_log()
    # No useful xfers → force receipt path
    xfers_by_tx: dict = {}

    receipt = {
        "logs": [
            {
                "address": UNI_V4_POOL_MANAGER,
                "topics": [HexBytes(V4_SWAP_TOPIC), HexBytes(POOL_ID), _topic_addr(UNIVERSAL_ROUTER)],
                "data": "0x0",
                "logIndex": "0x0",
            },
            {
                "address": TOKEN,
                "topics": [
                    HexBytes(TRANSFER_TOPIC),
                    _topic_addr(UNI_V4_POOL_MANAGER),
                    _topic_addr(UNIVERSAL_ROUTER),
                ],
                "data": "0x" + AMOUNT.to_bytes(32, "big").hex(),
                "logIndex": "0x1",
            },
            {
                "address": TOKEN,
                "topics": [
                    HexBytes(TRANSFER_TOPIC),
                    _topic_addr(UNIVERSAL_ROUTER),
                    _topic_addr(WALLET),
                ],
                "data": "0x" + AMOUNT.to_bytes(32, "big").hex(),
                "logIndex": "0x2",
            },
        ]
    }

    rpc = SimpleNamespace(
        batch_is_eoa=AsyncMock(
            side_effect=lambda addrs, cache=None: {
                a.lower(): a.lower() == WALLET.lower() for a in addrs
            }
        ),
        batch_get_receipts=AsyncMock(return_value={TX.lower(): receipt}),
        _jsonrpc_batch=AsyncMock(return_value=[]),
    )

    import app.replay as replay_mod

    async def _no_skip(_rpc, hashes):
        return set()

    monkey = pytest.MonkeyPatch()
    monkey.setattr(replay_mod, "_creator_launch_tx_hashes", _no_skip)
    try:
        got = await _resolve_buyers_batch(
            rpc,  # type: ignore[arg-type]
            token=TOKEN,
            pool_or_manager=UNI_V4_POOL_MANAGER,
            early_swaps=[swap],
            xfers_by_tx=xfers_by_tx,
        )
    finally:
        monkey.undo()

    assert TX.lower() in got
    assert got[TX.lower()][0].lower() == WALLET.lower()
    # Must request receipts with 0x-prefixed hash
    called_with = rpc.batch_get_receipts.await_args.args[0]
    assert all(h.startswith("0x") for h in called_with)
