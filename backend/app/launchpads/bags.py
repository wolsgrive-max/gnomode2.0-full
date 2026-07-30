"""Bags launchpad: Lens state + Transfer-from-curve snipers."""

from __future__ import annotations

import logging
from typing import Any

from ..chain import RpcClient, checksum, topic_address
from ..constants import (
    BAGS_DEPLOY_BLOCK,
    BAGS_FACTORY,
    BAGS_FACTORY_ABI,
    BAGS_LENS,
    BAGS_LENS_ABI,
    BAGS_MIGRATED_TOPIC,
    TRANSFER_TOPIC,
    ZERO,
)
from .base import data_word_bytes32, topic_to_addr
from .types import MigrationEvent, SniperHit

logger = logging.getLogger(__name__)


class BagsAdapter:
    id = "bags"

    async def resolve_curve(self, rpc: RpcClient, token: str) -> str | None:
        try:
            lens = rpc.w3.eth.contract(
                address=checksum(BAGS_LENS), abi=BAGS_LENS_ABI
            )
            state = await rpc._call(
                lambda: lens.functions.getTokenState(checksum(token)).call()
            )
            # tuple or AttributeDict
            exists = state[0] if isinstance(state, (tuple, list)) else state.exists
            if not exists:
                return None
            curve = state[2] if isinstance(state, (tuple, list)) else state.curve
            if not curve or str(curve).lower() == ZERO.lower():
                return None
            return checksum(curve)
        except Exception as exc:  # noqa: BLE001
            logger.debug("BagsLens getTokenState failed for %s: %s", token[:12], exc)
            try:
                factory = rpc.w3.eth.contract(
                    address=checksum(BAGS_FACTORY), abi=BAGS_FACTORY_ABI
                )
                curve = await rpc._call(
                    lambda: factory.functions.curveForToken(checksum(token)).call()
                )
                if curve and str(curve).lower() != ZERO.lower():
                    return checksum(curve)
            except Exception:  # noqa: BLE001
                pass
            return None

    async def resolve_token_state(self, rpc: RpcClient, token: str) -> dict[str, Any] | None:
        try:
            lens = rpc.w3.eth.contract(
                address=checksum(BAGS_LENS), abi=BAGS_LENS_ABI
            )
            state = await rpc._call(
                lambda: lens.functions.getTokenState(checksum(token)).call()
            )
            if isinstance(state, (tuple, list)):
                exists, migrated, curve, fee_share, pool_id = (
                    state[0],
                    state[1],
                    state[2],
                    state[3],
                    state[4],
                )
            else:
                exists = state.exists
                migrated = state.migrated
                curve = state.curve
                fee_share = state.feeShare
                pool_id = state.poolId
            if not exists:
                return None
            pool_hex = pool_id.hex() if isinstance(pool_id, (bytes, bytearray)) else str(pool_id)
            if not pool_hex.startswith("0x"):
                pool_hex = "0x" + pool_hex
            return {
                "exists": bool(exists),
                "migrated": bool(migrated),
                "curve": checksum(curve) if curve else None,
                "fee_share": checksum(fee_share) if fee_share else None,
                "pool_id": pool_hex.lower(),
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("Bags resolve_token_state %s: %s", token[:12], exc)
            return None

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]:
        from ..config import settings
        from ..constants import BLOCKS_PER_SECOND

        curve = event.curve_address or await self.resolve_curve(rpc, event.token)
        if not curve:
            logger.info("Bags: no curve for %s", event.token[:12])
            return []

        to_block = max(event.block, BAGS_DEPLOY_BLOCK)
        lookback = max(50_000, int(settings.sniper_scan_lookback_blocks))
        from_block = max(BAGS_DEPLOY_BLOCK, to_block - lookback)
        curve_topic = "0x" + "0" * 24 + curve.lower().replace("0x", "")
        try:
            logs = await rpc.get_logs_chunked(
                address=event.token,
                topics=[TRANSFER_TOPIC, curve_topic],
                from_block=from_block,
                to_block=to_block,
                parallel=2,
                chunk_size=min(settings.log_chunk_size, 50_000),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Bags getLogs Transfer failed: %s", exc)
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
            to_addr = topic_address(topics[2]).lower()
            if to_addr in seen or to_addr == ZERO.lower():
                continue
            seen.add(to_addr)
            tx = lg.get("transactionHash")
            tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
            hits.append(
                SniperHit(
                    wallet=checksum(to_addr),
                    block=int(lg["blockNumber"]),
                    tx=tx_hex if tx_hex.startswith("0x") else "0x" + tx_hex,
                )
            )
            if len(hits) >= limit:
                break
        logger.debug(
            "Bags snipers %s: window %s→%s (~%.1fh) hits=%s",
            event.token[:10],
            from_block,
            to_block,
            lookback / max(BLOCKS_PER_SECOND, 1) / 3600,
            len(hits),
        )
        return hits


def parse_bags_migrated_log(log: dict[str, Any]) -> MigrationEvent | None:
    """Parse BagsBondingCurve Migrated log → MigrationEvent."""
    topics = log.get("topics") or []
    if len(topics) < 4:
        return None
    topic0 = topics[0]
    t0 = topic0.hex() if isinstance(topic0, (bytes, bytearray)) else str(topic0)
    if not t0.startswith("0x"):
        t0 = "0x" + t0
    if t0.lower() != BAGS_MIGRATED_TOPIC.lower():
        return None
    token = topic_to_addr(topics[3])
    pool_id = data_word_bytes32(log.get("data") or "0x", 2)
    tx = log.get("transactionHash")
    tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx)
    if not tx_hex.startswith("0x"):
        tx_hex = "0x" + tx_hex
    curve = None
    try:
        addr = log.get("address")
        if addr:
            curve = checksum(str(addr))
    except Exception:  # noqa: BLE001
        curve = None
    return MigrationEvent(
        token=checksum(token),
        launchpad_id="bags",
        dex="uniswap_v4",
        block=int(log["blockNumber"]),
        tx=tx_hex,
        pool_id=pool_id,
        curve_address=curve,
        source="ws",
    )


bags_adapter = BagsAdapter()
