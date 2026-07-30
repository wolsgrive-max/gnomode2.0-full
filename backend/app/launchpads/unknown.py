"""Unknown pools — classify + delegate to instant scanner."""

from __future__ import annotations

from ..chain import RpcClient
from .instant import InstantAdapter
from .types import MigrationEvent, SniperHit


class UnknownAdapter(InstantAdapter):
    id = "unknown"

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]:
        return await super().scan_snipers(rpc, event, limit=limit)


unknown_adapter = UnknownAdapter()
