"""Priority scheduler for follow-up wallet scans (hot / warm / zero-balance).

Durable state lives in SQLite (``last_scanned_at``, ``last_balance_check_at``,
``wallet_balance_eth``). This module only classifies tiers and picks a fair
due batch — it never talks to the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Tier = Literal["hot", "warm", "done", "zero"]


@dataclass(frozen=True)
class ScheduleConfig:
    """Revisit / fairness knobs (seconds unless noted)."""

    hot_revisit_sec: float = 20.0
    warm_revisit_sec: float = 180.0
    # Confirmed zero ETH → skip deal scans; recheck balance on this cadence.
    zero_balance_recheck_sec: float = 900.0
    # Positive / unknown balance may be trusted this long before RPC refresh.
    balance_fresh_sec: float = 600.0
    # Recent discovery or deal activity → HOT.
    hot_activity_sec: float = 1800.0
    max_due_per_cycle: int = 24
    # Fraction of each batch reserved for warm when warm is overdue.
    warm_fair_share: float = 0.25


@dataclass(frozen=True)
class WalletScheduleRow:
    address: str
    status: str
    deal_count: int
    discovered_at: float
    last_activity_at: float
    last_scanned_at: float | None
    last_balance_check_at: float | None
    wallet_balance_eth: float | None


@dataclass(frozen=True)
class DueWallet:
    address: str
    tier: Tier
    next_due: float
    needs_balance_refresh: bool
    # True when confirmed balance is 0 and only a balance recheck is due.
    zero_balance_skip: bool


def classify_tier(
    row: WalletScheduleRow,
    *,
    now: float,
    max_deals: int,
    cfg: ScheduleConfig,
) -> Tier:
    """Classify watching wallet into hot / warm / done / zero."""
    if row.status != "watching" or row.deal_count >= max_deals:
        return "done"
    # Confirmed zero with a recent balance check → zero tier (balance-only).
    if (
        row.wallet_balance_eth is not None
        and float(row.wallet_balance_eth) == 0.0
        and row.last_balance_check_at is not None
    ):
        return "zero"
    activity = max(float(row.last_activity_at or 0), float(row.discovered_at or 0))
    if activity > 0 and (now - activity) <= cfg.hot_activity_sec:
        return "hot"
    if float(row.discovered_at or 0) > 0 and (now - float(row.discovered_at)) <= cfg.hot_activity_sec:
        return "hot"
    return "warm"


def _revisit_for_tier(tier: Tier, cfg: ScheduleConfig) -> float:
    if tier == "hot":
        return cfg.hot_revisit_sec
    if tier == "zero":
        return cfg.zero_balance_recheck_sec
    return cfg.warm_revisit_sec


def next_due_at(
    row: WalletScheduleRow,
    *,
    now: float,
    max_deals: int,
    cfg: ScheduleConfig,
) -> tuple[Tier, float]:
    """Return (tier, next_due_ts). Never-scanned wallets are immediately due."""
    tier = classify_tier(row, now=now, max_deals=max_deals, cfg=cfg)
    if tier == "done":
        return tier, float("inf")
    if tier == "zero":
        base = row.last_balance_check_at
        if base is None:
            base = row.last_scanned_at
        if base is None:
            return tier, 0.0
        return tier, float(base) + cfg.zero_balance_recheck_sec
    last = row.last_scanned_at
    if last is None:
        return tier, 0.0
    return tier, float(last) + _revisit_for_tier(tier, cfg)


def needs_balance_refresh(
    row: WalletScheduleRow,
    *,
    now: float,
    cfg: ScheduleConfig,
) -> bool:
    """Whether we should refresh native balance before a deal scan.

    Fail-open: unknown / None balance always refreshes when selected.
    Confirmed zero uses the longer zero-recheck cadence (caller selects due).
    Positive balance is trusted for ``balance_fresh_sec``.
    """
    checked = row.last_balance_check_at
    bal = row.wallet_balance_eth
    if bal is None or checked is None:
        return True
    age = now - float(checked)
    if float(bal) == 0.0:
        return age >= cfg.zero_balance_recheck_sec
    return age >= cfg.balance_fresh_sec


def is_confirmed_zero(row: WalletScheduleRow) -> bool:
    """True only when balance is known and exactly 0 (not None / error)."""
    return row.wallet_balance_eth is not None and float(row.wallet_balance_eth) == 0.0


def select_due_batch(
    rows: list[WalletScheduleRow],
    *,
    now: float,
    max_deals: int,
    cfg: ScheduleConfig | None = None,
    force_all_due: bool = False,
) -> list[DueWallet]:
    """Pick a fair due batch: hot first, warm not starved.

    ``force_all_due`` (manual run) still respects due times but raises the
    batch cap so a catch-up cycle can drain a larger overdue backlog.
    """
    cfg = cfg or ScheduleConfig()
    cap = int(cfg.max_due_per_cycle)
    if force_all_due:
        cap = max(cap * 3, 64)

    due: list[DueWallet] = []
    for row in rows:
        tier, due_ts = next_due_at(row, now=now, max_deals=max_deals, cfg=cfg)
        if tier == "done" or due_ts > now:
            continue
        due.append(
            DueWallet(
                address=row.address,
                tier=tier,
                next_due=due_ts,
                needs_balance_refresh=needs_balance_refresh(row, now=now, cfg=cfg),
                zero_balance_skip=False,
            )
        )

    hot = sorted(
        [d for d in due if d.tier == "hot"],
        key=lambda d: (d.next_due, d.address),
    )
    warm = sorted(
        [d for d in due if d.tier == "warm"],
        key=lambda d: (d.next_due, d.address),
    )
    zero = sorted(
        [d for d in due if d.tier == "zero"],
        key=lambda d: (d.next_due, d.address),
    )

    selected: list[DueWallet] = []
    warm_slots = max(1, int(round(cap * cfg.warm_fair_share))) if warm else 0
    # Leave room for at least one zero-balance recheck when present.
    zero_slots = min(2, len(zero)) if zero else 0
    hot_slots = max(0, cap - warm_slots - zero_slots)

    selected.extend(hot[:hot_slots])
    selected.extend(warm[:warm_slots])
    selected.extend(zero[:zero_slots])

    # Fill remaining capacity: hot → warm → zero (fairness already reserved).
    remaining = cap - len(selected)
    if remaining > 0:
        for pool in (hot[hot_slots:], warm[warm_slots:], zero[zero_slots:]):
            for item in pool:
                if remaining <= 0:
                    break
                selected.append(item)
                remaining -= 1
            if remaining <= 0:
                break

    # Stable order within batch: hot, then warm, then zero; older due first.
    tier_rank = {"hot": 0, "warm": 1, "zero": 2, "done": 3}
    selected.sort(key=lambda d: (tier_rank.get(d.tier, 9), d.next_due, d.address))
    return selected[:cap]


def schedule_config_from_followup(cfg: object) -> ScheduleConfig:
    """Build ScheduleConfig from FollowupConfig (optional fields with defaults)."""
    return ScheduleConfig(
        hot_revisit_sec=float(getattr(cfg, "hot_revisit_sec", 20) or 20),
        warm_revisit_sec=float(getattr(cfg, "warm_revisit_sec", 180) or 180),
        zero_balance_recheck_sec=float(
            getattr(cfg, "zero_balance_recheck_sec", 900) or 900
        ),
        balance_fresh_sec=float(getattr(cfg, "balance_fresh_sec", 600) or 600),
        hot_activity_sec=float(getattr(cfg, "hot_activity_sec", 1800) or 1800),
        max_due_per_cycle=int(getattr(cfg, "max_due_per_cycle", 24) or 24),
        warm_fair_share=float(getattr(cfg, "warm_fair_share", 0.25) or 0.25),
    )
