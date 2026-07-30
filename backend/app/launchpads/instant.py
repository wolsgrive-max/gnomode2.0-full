"""Instant DEX launchpads — reuse replay early buyers."""

from __future__ import annotations

import logging

from ..chain import RpcClient
from ..config import settings
from .types import MigrationEvent, SniperHit

logger = logging.getLogger(__name__)


class InstantAdapter:
    id = "instant"

    async def resolve_curve(self, rpc: RpcClient, token: str) -> str | None:
        del rpc, token
        return None

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]:
        from ..replay import parse_token

        threshold = settings.mcap_threshold
        try:
            result = await parse_token(
                rpc,
                event.token,
                threshold,
                exclude_honeypots=False,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("instant scan_snipers failed for %s: %s", event.token[:12], exc)
            return []
        hits: list[SniperHit] = []
        for b in result.buyers[:limit]:
            hits.append(
                SniperHit(
                    wallet=b.wallet,
                    block=b.first_block,
                    tx=b.first_tx or "",
                    amount_usd=float(b.bought_usd or 0),
                    mcap_at_trade=float(b.mcap_at_first_buy or 0),
                )
            )
        return hits


instant_adapter = InstantAdapter()
