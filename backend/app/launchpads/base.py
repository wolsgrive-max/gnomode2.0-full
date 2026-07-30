"""Launchpad adapter protocol + helpers."""

from __future__ import annotations

from typing import Protocol

from ..chain import RpcClient
from .types import MigrationEvent, SniperHit


class LaunchpadAdapter(Protocol):
    id: str

    async def resolve_curve(
        self, rpc: RpcClient, token: str
    ) -> str | None: ...

    async def scan_snipers(
        self,
        rpc: RpcClient,
        event: MigrationEvent,
        *,
        limit: int = 10,
    ) -> list[SniperHit]: ...


def topic_to_addr(topic: object) -> str:
    if isinstance(topic, (bytes, bytearray)):
        h = topic.hex()
    else:
        h = str(topic)
    if h.startswith("0x"):
        h = h[2:]
    return "0x" + h[-40:].lower()


def data_word_addr(data: object, index: int) -> str:
    if isinstance(data, (bytes, bytearray)):
        h = data.hex()
    else:
        h = str(data)
    if h.startswith("0x"):
        h = h[2:]
    word = h[index * 64 : (index + 1) * 64]
    return "0x" + word[-40:].lower()


def data_word_bytes32(data: object, index: int) -> str:
    if isinstance(data, (bytes, bytearray)):
        h = data.hex()
    else:
        h = str(data)
    if h.startswith("0x"):
        h = h[2:]
    word = h[index * 64 : (index + 1) * 64]
    return "0x" + word.lower()
