"""Validate that a token has actually graduated / migrated (not still bonding)."""

from __future__ import annotations

import logging
import re
from typing import Any

from eth_hash.auto import keccak

from .chain import RpcClient, checksum
from .constants import HOODFUN_ABI, HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY, QUOTE_TOKENS, ZERO
from .launchpads.bags import bags_adapter
from .launchpads.flap import flap_is_migrated
from .launchpads.types import MigrationEvent

logger = logging.getLogger(__name__)

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
_KNOWN_QUOTES = {a.lower() for a in QUOTE_TOKENS} | {ZERO.lower()}
_JUNK_NAMES = {"", "???", "unknown", "test", "null", "none"}

# ERC-8056 / RH stock-token shaped interfaces
_UI_MULTIPLIER_SEL = "0x" + keccak(b"uiMultiplier()")[:4].hex()
_BALANCE_OF_UI_SEL = "0x" + keccak(b"balanceOfUI(address)")[:4].hex()


def is_plausible_token_address(addr: str) -> bool:
    if not addr or not _ADDR_RE.match(addr):
        return False
    low = addr.lower()
    if low in _KNOWN_QUOTES:
        return False
    if set(low[2:]) <= {"0"}:
        return False
    return True


async def bags_is_migrated(rpc: RpcClient, token: str) -> tuple[bool, dict[str, Any] | None]:
    state = await bags_adapter.resolve_token_state(rpc, token)
    if not state or not state.get("exists"):
        return False, state
    return bool(state.get("migrated")), state


async def hoodfun_is_migrated(rpc: RpcClient, token: str) -> bool:
    for pad in (HOODFUN_LAUNCHPAD, HOODFUN_LAUNCHPAD_LEGACY):
        try:
            hood = rpc.w3.eth.contract(address=checksum(pad), abi=HOODFUN_ABI)
            try:
                is_hood = await rpc._call(
                    lambda: hood.functions.isHoodToken(checksum(token)).call()
                )
                if not is_hood:
                    continue
            except Exception:  # noqa: BLE001
                pass
            curve = await rpc._call(
                lambda: hood.functions.curves(checksum(token)).call()
            )
            if isinstance(curve, (tuple, list)) and len(curve) >= 8:
                if bool(curve[6]) or bool(curve[7]):
                    return True
                continue
            graduated = bool(getattr(curve, "graduated", False))
            migrated = bool(getattr(curve, "migrated", False))
            if graduated or migrated:
                return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("hoodfun_is_migrated %s @%s: %s", token[:12], pad[:10], exc)
    return False


async def token_has_meta(rpc: RpcClient, token: str) -> tuple[bool, str, str, int]:
    try:
        meta = await rpc.token_meta(token)
    except Exception:  # noqa: BLE001
        return False, "", "", 0
    symbol = str(meta.get("symbol") or "").strip()
    name = str(meta.get("name") or "").strip()
    supply = int(meta.get("total_supply_raw") or 0)
    if supply <= 0:
        return False, symbol, name, supply
    if symbol.lower() in _JUNK_NAMES and name.lower() in _JUNK_NAMES:
        return False, symbol, name, supply
    if not symbol and not name:
        return False, symbol, name, supply
    # Reject fake "meta" that is just a truncated address
    if symbol.lower() == token.lower()[:10].lower() or symbol.lower() == token.lower():
        return False, symbol, name, supply
    return True, symbol, name, supply


async def is_erc8056_rwa(rpc: RpcClient, token: str) -> bool:
    """Heuristic: contracts exposing ERC-8056 UI share helpers are stock/RWA, not memes."""
    addr = checksum(token)

    async def _call(data: str) -> bytes | None:
        try:
            return await rpc._call(
                lambda: rpc.w3.eth.call({"to": addr, "data": data})
            )
        except Exception:  # noqa: BLE001
            return None

    ui_mul = await _call(_UI_MULTIPLIER_SEL)
    if ui_mul and len(ui_mul) >= 32:
        return True
    # balanceOfUI(address) with zero arg
    bal = await _call(_BALANCE_OF_UI_SEL + ("0" * 64))
    if bal and len(bal) >= 32:
        return True
    return False


async def verify_migration_event(
    rpc: RpcClient, event: MigrationEvent
) -> tuple[bool, str, dict[str, Any]]:
    """
    Return (ok, reason, enrich).
    Rejects: not-yet-migrated, empty/meta-less, quote tokens, RWA, junk addresses.
    """
    enrich: dict[str, Any] = {}
    token = event.token
    if not is_plausible_token_address(token):
        return False, "bad_address", enrich

    if event.launchpad_id.endswith("_bonding"):
        return False, "not_migrated_yet", enrich

    ok_meta, symbol, name, supply = await token_has_meta(rpc, token)
    enrich["symbol"] = symbol
    enrich["name"] = name
    enrich["total_supply_raw"] = supply
    if not ok_meta:
        return False, "empty_token", enrich

    try:
        if await is_erc8056_rwa(rpc, token):
            return False, "erc8056_rwa", enrich
    except Exception as exc:  # noqa: BLE001
        logger.debug("erc8056 check %s: %s", token[:12], exc)

    lp = (event.launchpad_id or "").lower()

    if lp == "bags":
        migrated, state = await bags_is_migrated(rpc, token)
        if not state or not state.get("exists"):
            return False, "not_bags", enrich
        enrich["curve"] = state.get("curve")
        enrich["pool_id"] = state.get("pool_id")
        enrich["launchpad_id"] = "bags"
        if not migrated:
            return False, "not_migrated_bags", enrich
        return True, "ok", enrich

    if lp == "hoodfun":
        if not await hoodfun_is_migrated(rpc, token):
            return False, "not_migrated_hoodfun", enrich
        enrich["launchpad_id"] = "hoodfun"
        return True, "ok", enrich

    if lp == "flap":
        # Event-sourced feed is enough; view-call confirms when available
        if event.source in ("scan", "ws", "gap"):
            enrich["launchpad_id"] = "flap"
            return True, "ok", enrich
        if await flap_is_migrated(rpc, token):
            enrich["launchpad_id"] = "flap"
            return True, "ok", enrich
        return False, "not_migrated_flap", enrich

    # Auto-detect if launchpad unknown: Bags migrated? hood graduated? Flap DEX?
    migrated, state = await bags_is_migrated(rpc, token)
    if state and state.get("exists"):
        enrich["curve"] = state.get("curve")
        enrich["pool_id"] = state.get("pool_id")
        enrich["launchpad_id"] = "bags"
        if not migrated:
            return False, "not_migrated_bags", enrich
        return True, "ok", enrich

    if await hoodfun_is_migrated(rpc, token):
        enrich["launchpad_id"] = "hoodfun"
        return True, "ok", enrich

    if await flap_is_migrated(rpc, token):
        enrich["launchpad_id"] = "flap"
        return True, "ok", enrich

    # Feed events without on-chain graduation proof are noise
    if event.source in ("scan", "ws", "gap"):
        return False, "skip_unverified_feed", enrich

    # Manual parse of non-curve pads still allowed for known instant pads
    if lp.startswith("unknown"):
        return False, "unverified_unknown", enrich

    if lp in (
        "hoodrich",
        "recurve",
        "clanker",
        "launchhood",
        "virtuals",
        "klik",
        "bankr",
        "apestore",
        "instant",
    ):
        return True, "ok", enrich

    return False, f"unverified:{lp}", enrich
