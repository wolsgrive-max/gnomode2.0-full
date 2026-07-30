"""hood.fun launchpad: Graduated event + Transfer snipers from launchpad."""

from __future__ import annotations

import logging
from typing import Any

from ..chain import RpcClient, checksum, topic_address
from ..constants import (
    HOODFUN_GRADUATED_TOPIC,
    HOODFUN_LAUNCHPAD,
    HOODFUN_LAUNCHPAD_LEGACY,
    TRANSFER_TOPIC,
    ZERO,
)
from .base import topic_to_addr
from .types import MigrationEvent, SniperHit

logger = logging.getLogger(__name__)

_LAUNCHPADS = (HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY)


class HoodfunAdapter:
    id = "hoodfun"

    async def resolve_curve(self, rpc: RpcClient, token: str) -> str | None:
        # hood.fun keeps curve state on the singleton launchpad, not a per-token curve.
        del rpc, token
        return checksum(HOODFUN_LAUNCHPAD)

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]:
        """First unique Transfer recipients where from ∈ hood.fun launchpads."""
        to_block = event.block
        from_block = max(1, to_block - 2_000_000)
        hits: list[SniperHit] = []
        seen: set[str] = set()

        for pad in _LAUNCHPADS:
            pad_topic = "0x" + "0" * 24 + pad.lower().replace("0x", "")
            try:
                logs = await rpc.get_logs_chunked(
                    address=event.token,
                    topics=[TRANSFER_TOPIC, pad_topic],
                    from_block=from_block,
                    to_block=to_block,
                    parallel=2,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("hood.fun Transfer scan failed: %s", exc)
                continue
            ordered = sorted(
                logs,
                key=lambda lg: (int(lg["blockNumber"]), int(lg.get("logIndex") or 0)),
            )
            for lg in ordered:
                topics = lg.get("topics") or []
                if len(topics) < 3:
                    continue
                to_addr = topic_address(topics[2]).lower()
                if to_addr in seen or to_addr == ZERO.lower():
                    continue
                seen.add(to_addr)
                tx = lg.get("transactionHash")
                tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
                if not tx_hex.startswith("0x"):
                    tx_hex = "0x" + tx_hex
                hits.append(
                    SniperHit(
                        wallet=checksum(to_addr),
                        block=int(lg["blockNumber"]),
                        tx=tx_hex,
                    )
                )
                if len(hits) >= limit:
                    return hits
        return hits


def parse_hoodfun_graduated_log(log: dict[str, Any]) -> MigrationEvent | None:
    topics = log.get("topics") or []
    if len(topics) < 3:
        return None
    topic0 = topics[0]
    t0 = topic0.hex() if isinstance(topic0, (bytes, bytearray)) else str(topic0)
    if not t0.startswith("0x"):
        t0 = "0x" + t0
    if t0.lower() != HOODFUN_GRADUATED_TOPIC.lower():
        return None
    token = topic_to_addr(topics[1])
    pool = topic_to_addr(topics[2])
    tx = log.get("transactionHash")
    tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
    if not tx_hex.startswith("0x"):
        tx_hex = "0x" + tx_hex
    return MigrationEvent(
        token=checksum(token),
        launchpad_id="hoodfun",
        dex="uniswap_v3",
        block=int(log["blockNumber"]),
        tx=tx_hex,
        pool=checksum(pool),
        source="ws",
    )


hoodfun_adapter = HoodfunAdapter()
