"""Resolve adapter instance by launchpad id."""

from __future__ import annotations

from .bags import bags_adapter
from .flap import flap_adapter
from .hoodfun import hoodfun_adapter
from .instant import InstantAdapter, instant_adapter
from .unknown import unknown_adapter

_ADAPTERS = {
    "bags": bags_adapter,
    "hoodfun": hoodfun_adapter,
    "flap": flap_adapter,
    "clanker": InstantAdapter(),
    "launchhood": InstantAdapter(),
    "virtuals": InstantAdapter(),
    "klik": InstantAdapter(),
    "bankr": InstantAdapter(),
    "apestore": InstantAdapter(),
    "unknown_v3": unknown_adapter,
    "unknown_v4": unknown_adapter,
    "unknown": unknown_adapter,
    "instant": instant_adapter,
}


def get_adapter(launchpad_id: str):
    return _ADAPTERS.get(launchpad_id) or unknown_adapter
