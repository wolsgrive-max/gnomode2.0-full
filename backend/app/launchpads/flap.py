"""Flap.sh bonding-curve launchpad: LaunchedToDEX graduation → Uniswap V2."""

from __future__ import annotations

import logging
from typing import Any

from eth_hash.auto import keccak

from ..chain import RpcClient, checksum
from ..constants import FLAP_LAUNCHED_TO_DEX_TOPIC, FLAP_LAUNCHPAD, TRANSFER_TOPIC, ZERO
from .base import data_word_addr, topic_to_addr
from .types import MigrationEvent, SniperHit

logger = logging.getLogger(__name__)

# TokenStatus.DEX in Flap Portal (getTokenV8Safe)
_FLAP_STATUS_DEX = 2
_GET_TOKEN_V8_SAFE_SEL = "0x" + keccak(b"getTokenV8Safe(address)")[:4].hex()


class FlapAdapter:
    id = "flap"

    async def resolve_curve(self, rpc: RpcClient, token: str) -> str | None:
        del rpc, token
        return checksum(FLAP_LAUNCHPAD)

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]:
        """First unique Transfer recipients where from = Flap portal (curve sells)."""
        to_block = event.block
        from_block = max(1, to_block - 2_000_000)
        pad_topic = "0x" + "0" * 24 + FLAP_LAUNCHPAD.lower().replace("0x", "")
        try:
            logs = await rpc.get_logs_chunked(
                address=event.token,
                topics=[TRANSFER_TOPIC, pad_topic],
                from_block=from_block,
                to_block=to_block,
                parallel=2,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Flap Transfer scan failed: %s", exc)
            return []

        ordered = sorted(
            logs,
            key=lambda lg: (int(lg["blockNumber"]), int(lg.get("logIndex") or 0)),
        )
        seen: set[str] = set()
        hits: list[SniperHit] = []
        for lg in ordered:
            topics = lg.get("topics") or []
            if len(topics) < 3:
                continue
            to_addr = topic_to_addr(topics[2]).lower()
            if to_addr in seen or to_addr == ZERO.lower():
                continue
            seen.add(to_addr)
            tx = lg.get("transactionHash")
            tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
            if not tx_hex.startswith("0x"):
                tx_hex = "0x" + tx_hex
            hits.append(
                SniperHit(wallet=checksum(to_addr), block=int(lg["blockNumber"]), tx=tx_hex)
            )
            if len(hits) >= limit:
                break
        return hits


def parse_flap_launched_log(log: dict[str, Any]) -> MigrationEvent | None:
    """Parse Flap ``LaunchedToDEX(token, pool, amount, eth)`` (args in data)."""
    topics = log.get("topics") or []
    if not topics:
        return None
    topic0 = topics[0]
    t0 = topic0.hex() if isinstance(topic0, (bytes, bytearray)) else str(topic0)
    if not t0.startswith("0x"):
        t0 = "0x" + t0
    if t0.lower() != FLAP_LAUNCHED_TO_DEX_TOPIC.lower():
        return None
    data = log.get("data") or "0x"
    if isinstance(data, (bytes, bytearray)):
        data = "0x" + data.hex()
    token = data_word_addr(data, 0)
    pool = data_word_addr(data, 1)
    if not token or set(token[2:]) <= {"0"}:
        return None
    tx = log.get("transactionHash")
    tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
    if not tx_hex.startswith("0x"):
        tx_hex = "0x" + tx_hex
    return MigrationEvent(
        token=checksum(token),
        launchpad_id="flap",
        dex="uniswap_v2",
        block=int(log["blockNumber"]),
        tx=tx_hex,
        pool=checksum(pool) if pool and not set(pool[2:]) <= {"0"} else None,
        curve_address=checksum(str(log.get("address") or FLAP_LAUNCHPAD)),
        source="ws",
    )


async def flap_is_migrated(rpc: RpcClient, token: str) -> bool:
    """True if Portal reports TokenStatus.DEX (or call fails open for event-sourced)."""
    data = _GET_TOKEN_V8_SAFE_SEL + "0" * 24 + token.lower().replace("0x", "")
    try:
        raw = await rpc._call(
            lambda: rpc.w3.eth.call(
                {"to": checksum(FLAP_LAUNCHPAD), "data": data}
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("flap getTokenV8Safe %s: %s", token[:12], exc)
        return False
    if not raw or len(raw) < 64:
        return False
    # First word of returned struct is typically status (uint8/uint256)
    status = int.from_bytes(raw[:32], "big")
    return status == _FLAP_STATUS_DEX


flap_adapter = FlapAdapter()
