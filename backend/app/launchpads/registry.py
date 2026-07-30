"""Launchpad registry: classify by V4 hooks / factory address."""

from __future__ import annotations

from ..constants import (
    APE_STORE,
    BAGS_FACTORY,
    BAGS_MIGRATED_TOPIC,
    BAGS_TOKEN_CREATED_TOPIC,
    BAGS_V4_HOOK,
    BANKR_BOT,
    CLANKER_LAUNCHPAD,
    FLAP_LAUNCHED_TO_DEX_TOPIC,
    FLAP_LAUNCHPAD,
    FLAP_VAULT_PORTAL,
    HOODFUN_GRADUATED_TOPIC,
    HOODFUN_LAUNCHPAD,
    HOODFUN_LAUNCHPAD_LEGACY,
    HOODFUN_TOKEN_CREATED_TOPIC,
    HOODRICH_CURVE,
    KLIK_FINANCE,
    LAUNCHHOOD,
    RECURVE_LAUNCHPAD,
    VIRTUALS,
)
from .types import LaunchpadSpec

LAUNCHPADS: dict[str, LaunchpadSpec] = {
    "bags": LaunchpadSpec(
        id="bags",
        kind="curve_v4",
        factories=(BAGS_FACTORY.lower(),),
        v4_hooks=(BAGS_V4_HOOK.lower(),),
        migrate_topics=(BAGS_MIGRATED_TOPIC.lower(),),
        create_topics=(BAGS_TOKEN_CREATED_TOPIC.lower(),),
        label="Bags",
    ),
    "hoodfun": LaunchpadSpec(
        id="hoodfun",
        kind="curve_v3",
        factories=(HOODFUN_LAUNCHPAD.lower(), HOODFUN_LAUNCHPAD_LEGACY.lower()),
        migrate_topics=(HOODFUN_GRADUATED_TOPIC.lower(),),
        create_topics=(HOODFUN_TOKEN_CREATED_TOPIC.lower(),),
        label="hood.fun",
    ),
    "flap": LaunchpadSpec(
        id="flap",
        kind="curve_v2",
        factories=(FLAP_LAUNCHPAD.lower(), FLAP_VAULT_PORTAL.lower()),
        migrate_topics=(FLAP_LAUNCHED_TO_DEX_TOPIC.lower(),),
        label="Flap.sh",
    ),
    "hoodrich": LaunchpadSpec(
        id="hoodrich",
        kind="curve_v3",
        factories=(HOODRICH_CURVE.lower(),),
        label="HoodRich Curve",
    ),
    "recurve": LaunchpadSpec(
        id="recurve",
        kind="curve_v3",
        factories=(RECURVE_LAUNCHPAD.lower(),),
        label="Recurve",
    ),
    "clanker": LaunchpadSpec(
        id="clanker",
        kind="instant_v3",
        factories=(CLANKER_LAUNCHPAD.lower(),),
        label="Clanker",
    ),
    "launchhood": LaunchpadSpec(
        id="launchhood",
        kind="instant_v3",
        factories=(LAUNCHHOOD.lower(),),
        label="LaunchHood",
    ),
    "virtuals": LaunchpadSpec(
        id="virtuals",
        kind="instant_v3",
        factories=(VIRTUALS.lower(),),
        label="Virtuals",
    ),
    "klik": LaunchpadSpec(
        id="klik",
        kind="instant_v3",
        factories=(KLIK_FINANCE.lower(),),
        label="Klik Finance",
    ),
    "bankr": LaunchpadSpec(
        id="bankr",
        kind="instant_v3",
        factories=(BANKR_BOT.lower(),),
        label="Bankr Bot",
    ),
    "apestore": LaunchpadSpec(
        id="apestore",
        kind="instant_v3",
        factories=(APE_STORE.lower(),),
        label="Ape.store",
    ),
    "unknown_v3": LaunchpadSpec(
        id="unknown_v3",
        kind="unknown",
        factories=(),
        label="Unknown V3",
    ),
    "unknown_v4": LaunchpadSpec(
        id="unknown_v4",
        kind="unknown",
        factories=(),
        label="Unknown V4",
    ),
}

_HOOK_INDEX: dict[str, LaunchpadSpec] = {}
_FACTORY_INDEX: dict[str, LaunchpadSpec] = {}
for _spec in LAUNCHPADS.values():
    for h in _spec.v4_hooks:
        _HOOK_INDEX[h.lower()] = _spec
    for f in _spec.factories:
        _FACTORY_INDEX[f.lower()] = _spec


def get_spec(launchpad_id: str) -> LaunchpadSpec:
    return LAUNCHPADS.get(launchpad_id) or LAUNCHPADS["unknown_v4"]


def classify_by_hooks(hooks: str | None) -> LaunchpadSpec:
    if not hooks:
        return LAUNCHPADS["unknown_v4"]
    return _HOOK_INDEX.get(hooks.lower()) or LAUNCHPADS["unknown_v4"]


def classify_by_factory(factory: str | None) -> LaunchpadSpec | None:
    if not factory:
        return None
    return _FACTORY_INDEX.get(factory.lower())


def classify_pool(
    *,
    hooks: str | None = None,
    factory_hint: str | None = None,
    dex: str = "uniswap_v4",
) -> LaunchpadSpec:
    by_hook = classify_by_hooks(hooks) if hooks else None
    if by_hook and by_hook.id not in ("unknown_v3", "unknown_v4"):
        return by_hook
    by_factory = classify_by_factory(factory_hint)
    if by_factory:
        return by_factory
    return LAUNCHPADS["unknown_v3"] if dex == "uniswap_v3" else LAUNCHPADS["unknown_v4"]
