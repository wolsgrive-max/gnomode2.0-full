"""Shared types for launchpad migration detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

LaunchpadKind = Literal[
    "curve_v4",
    "curve_v3",
    "curve_v2",
    "instant_v3",
    "instant_v4",
    "unknown",
]


@dataclass(frozen=True)
class LaunchpadSpec:
    id: str
    kind: LaunchpadKind
    factories: tuple[str, ...]
    v4_hooks: tuple[str, ...] = ()
    migrate_topics: tuple[str, ...] = ()
    create_topics: tuple[str, ...] = ()
    label: str = ""


@dataclass
class MigrationEvent:
    token: str
    launchpad_id: str
    dex: str  # uniswap_v3 | uniswap_v4
    block: int
    tx: str
    pool: str = ""
    pool_id: str | None = None
    curve_address: str | None = None
    hooks: str | None = None
    source: str = "ws"  # ws | poll | gap
    extra: dict = field(default_factory=dict)


@dataclass
class SniperHit:
    wallet: str
    block: int
    tx: str
    amount_usd: float = 0.0
    mcap_at_trade: float = 0.0
