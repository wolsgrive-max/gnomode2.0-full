"""ATH gate + hold-queue classification for the watch autoparse cycle."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import ScreenedToken

# Drop hold entries older than this if they never qualify.
HOLD_TTL_SEC = 48 * 3600.0
# Young tokens (still inside max_pair_age) can be re-parsed after this cooldown
# so a bad unique/filter pass cannot permanently hide wallets.
REPARSE_YOUNG_COOLDOWN_SEC = 20 * 60.0


@dataclass(frozen=True)
class QualifyDecision:
    """Result of splitting screened (+ hold) tokens into parse vs hold."""

    candidates: list[str]
    held: list[str]
    # token(lower) -> (ath_mcap, symbol)
    ath_updates: dict[str, tuple[float, str]]
    # tokens removed from hold due to TTL / leave-index without qualify
    expired: list[str]
    # skipped because in parsed set and not eligible for young requeue
    skipped_parsed: int = 0
    # previously parsed, still ≤ max_pair_age, cooldown elapsed → parse again
    requeued_young: list[str] = field(default_factory=list)


def ath_gate_enabled(min_ath_mcap: float | None) -> bool:
    return min_ath_mcap is not None and min_ath_mcap > 0


def _as_parsed_at(parsed: set[str] | dict[str, float] | None) -> dict[str, float]:
    if not parsed:
        return {}
    if isinstance(parsed, dict):
        return {str(k).lower(): float(v) for k, v in parsed.items()}
    return {str(a).lower(): 0.0 for a in parsed}


def classify_for_parse(
    screened: list[ScreenedToken],
    *,
    min_ath_mcap: float | None,
    hold: dict[str, dict],
    parsed: set[str] | dict[str, float],
    index_addresses: set[str] | None = None,
    now: float,
    hold_ttl_sec: float = HOLD_TTL_SEC,
    max_pair_age_hours: float | None = None,
    reparse_cooldown_sec: float = REPARSE_YOUNG_COOLDOWN_SEC,
) -> QualifyDecision:
    """Split tokens into parse candidates vs hold queue.

    When the ATH gate is disabled (``min_ath_mcap`` None/0), every screened
    token that is not already parsed is a candidate (legacy re-parse behaviour
    is restored by the caller not using ``parsed``).

    Young tokens already in ``parsed`` (pair age ≤ ``max_pair_age_hours``) are
    requeued after ``reparse_cooldown_sec`` — age and parsed are different:
    parsed means "parsed recently", not "too old for the screener".
    """
    ath_updates: dict[str, tuple[float, str]] = {}
    held: list[str] = []
    candidates: list[str] = []
    seen_candidate: set[str] = set()
    index_keys = index_addresses if index_addresses is not None else set()
    parsed_at = _as_parsed_at(parsed)
    skipped_parsed = 0
    requeued_young: list[str] = []
    max_age = (
        float(max_pair_age_hours)
        if max_pair_age_hours is not None and max_pair_age_hours > 0
        else None
    )

    def bump(addr: str, ath: float, symbol: str = "") -> float:
        prev = hold.get(addr, {})
        prev_ath = float(prev.get("ath_mcap") or 0.0)
        already = float(ath_updates.get(addr, (0.0, ""))[0])
        # Discard hold ATH inflated by price×1e9 on low-supply tokens.
        if prev_ath >= 1_000_000_000.0 and ath > 0 and prev_ath > ath * 50:
            prev_ath = 0.0
        if already >= 1_000_000_000.0 and ath > 0 and already > ath * 50:
            already = 0.0
        peak = max(prev_ath, already, ath)
        prev_sym = str(prev.get("symbol") or "")
        already_sym = ath_updates.get(addr, (0.0, ""))[1] if addr in ath_updates else ""
        sym = symbol or already_sym or prev_sym
        ath_updates[addr] = (peak, sym)
        return peak

    def still_young(pair_age_hours: float | None) -> bool:
        return (
            max_age is not None
            and pair_age_hours is not None
            and float(pair_age_hours) <= max_age
        )

    def cooled(addr: str) -> bool:
        return (now - float(parsed_at.get(addr, 0.0))) >= float(reparse_cooldown_sec)

    gate_on = ath_gate_enabled(min_ath_mcap)
    threshold = float(min_ath_mcap or 0.0)

    for row in screened:
        addr = row.address.lower()
        peak = bump(addr, max(row.ath_mcap, row.market_cap), row.symbol or "")
        if gate_on and addr in parsed_at:
            if (
                peak >= threshold
                and still_young(row.pair_age_hours)
                and cooled(addr)
            ):
                requeued_young.append(addr)
                # fall through — treat as fresh candidate
            else:
                skipped_parsed += 1
                continue
        if not gate_on:
            if addr not in seen_candidate:
                candidates.append(addr)
                seen_candidate.add(addr)
            continue
        if peak >= threshold:
            if addr not in seen_candidate:
                candidates.append(addr)
                seen_candidate.add(addr)
        else:
            held.append(addr)

    expired: list[str] = []
    if gate_on:
        for addr, ent in list(hold.items()):
            if addr in parsed_at and addr not in requeued_young:
                continue
            first_seen = float(ent.get("first_seen") or now)
            peak = bump(addr, float(ent.get("ath_mcap") or 0.0), str(ent.get("symbol") or ""))
            # Promote hold entries that crossed ATH even if they fell out of the
            # current screener slice (still parseable by address).
            if peak >= threshold:
                if addr not in seen_candidate:
                    candidates.append(addr)
                    seen_candidate.add(addr)
                continue
            age = now - first_seen
            left_index = bool(index_keys) and addr not in index_keys
            if age >= hold_ttl_sec or (left_index and age >= 3600.0):
                expired.append(addr)
                continue
            if addr not in held:
                held.append(addr)

    return QualifyDecision(
        candidates=candidates,
        held=held,
        ath_updates=ath_updates,
        expired=expired,
        skipped_parsed=skipped_parsed,
        requeued_young=requeued_young,
    )


def should_mark_parsed(
    error: str | None,
    *,
    buyers_before_filters: int | None = None,
    buyers_after_filters: int | None = None,
) -> bool:
    """Whether a finished parse should enter the parsed set (with timestamp).

    Retry hard discovery failures (no pool) without marking. When early buyers
    existed under mcap but wallet filters wiped everyone, still mark parsed —
    ``classify_for_parse`` requeues young tokens after ``REPARSE_YOUNG_COOLDOWN_SEC``.
    Leaving them unmarked caused every cycle to re-parse the same hopeless
    tokens and starve never-seen candidates (no new wallets).
    """
    if error:
        low = error.lower()
        if "no uniswap" in low or "no pool" in low:
            return False
        return True
    # buyers_before/after kept for call-site compatibility / logging.
    _ = (buyers_before_filters, buyers_after_filters)
    return True