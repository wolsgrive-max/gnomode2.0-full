"""Migration pipeline: classify → store → background sniper discovery."""

from __future__ import annotations

import logging
from typing import Any

from .chain import RpcClient, checksum
from .constants import FLAP_LAUNCHPAD, HOODFUN_ABI, HOODFUN_LAUNCHPAD
from .launchpads.bags import bags_adapter
from .launchpads.flap import flap_is_migrated
from .launchpads.types import MigrationEvent
from .migration_scan import store_migration
from .migration_validate import hoodfun_is_migrated

logger = logging.getLogger(__name__)


async def detect_launchpad(
    rpc: RpcClient, token: str, hint: str | None = None
) -> tuple[str, str, str | None, str | None]:
    if hint:
        dex = {
            "bags": "uniswap_v4",
            "flap": "uniswap_v2",
            "hoodfun": "uniswap_v3",
        }.get(hint, "uniswap_v3")
        return hint, dex, None, None

    state = await bags_adapter.resolve_token_state(rpc, token)
    if state and state.get("exists"):
        if not state.get("migrated"):
            return (
                "bags_bonding",
                "uniswap_v4",
                state.get("curve"),
                state.get("pool_id"),
            )
        return (
            "bags",
            "uniswap_v4",
            state.get("curve"),
            state.get("pool_id"),
        )

    try:
        hood = rpc.w3.eth.contract(
            address=checksum(HOODFUN_LAUNCHPAD), abi=HOODFUN_ABI
        )
        is_hood = await rpc._call(
            lambda: hood.functions.isHoodToken(checksum(token)).call()
        )
        if is_hood:
            if await hoodfun_is_migrated(rpc, token):
                return "hoodfun", "uniswap_v3", checksum(HOODFUN_LAUNCHPAD), None
            return "hoodfun_bonding", "uniswap_v3", checksum(HOODFUN_LAUNCHPAD), None
    except Exception:  # noqa: BLE001
        pass

    if await flap_is_migrated(rpc, token):
        return "flap", "uniswap_v2", checksum(FLAP_LAUNCHPAD), None

    return "unknown_v4", "uniswap_v4", None, None


async def handle_migration(event: MigrationEvent) -> dict[str, Any]:
    """WS/API entry: store listing; snipers discovered async in store_migration."""
    if event.launchpad_id in ("bags_bonding", "hoodfun_bonding"):
        return {
            "ok": False,
            "token": event.token,
            "launchpad_id": event.launchpad_id,
            "dex": event.dex,
            "honeypot": False,
            "snipers": 0,
            "new_pairs": 0,
            "message": "not_migrated_yet",
            "skipped": True,
        }
    return await store_migration(event)


async def parse_token_migration(
    token: str, *, launchpad_id: str | None = None
) -> dict[str, Any]:
    rpc = RpcClient(concurrency=2)
    tip = await rpc.block_number()
    token = checksum(token)
    lp, dex, curve, pool_id = await detect_launchpad(rpc, token, launchpad_id)
    if lp.endswith("_bonding"):
        return {
            "ok": False,
            "token": token,
            "launchpad_id": lp,
            "dex": dex,
            "honeypot": False,
            "snipers": 0,
            "new_pairs": 0,
            "message": "Токен ещё на bonding curve — миграция не завершена",
            "skipped": True,
        }
    event = MigrationEvent(
        token=token,
        launchpad_id=lp,
        dex=dex,
        block=tip,
        tx="0x" + "0" * 64,
        pool_id=pool_id,
        curve_address=curve,
        source="manual",
    )
    return await store_migration(event, rpc=rpc)
