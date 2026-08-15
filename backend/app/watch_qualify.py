"""ATH gate + hold-queue classification for the watch autoparse cycle."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import ScreenedToken

# Drop hold entries older than this if they never qualify.
HOLD_TTL_SEC = 48 * 3600.0
# Far-below-gate dust (< this fraction of min_ath) expires sooner so catch-up
# force-enrich / ATH-probe budgets are not dominated by forever-$1k spam.
HOLD_DUST_ATH_FRAC = 0.25
HOLD_DUST_TTL_SEC = 6 * 3600.0
# Young tokens (still inside max_pair_age) can be re-parsed after this cooldown
# so a bad unique/filter pass cannot permanently hide wallets.
REPARSE_YOUNG_COOLDOWN_SEC = 20 * 60.0
# Hold-only ATH probes rotate after this so near-gate pumps are not starved by
# forever-low dust (LEGATE: DS ~31k, Gecko ~226k, never probed under lowest-first).
ATH_PROBE_COOLDOWN_SEC = 45 * 60.0
# Default Gecko/DS probe budget per watch cycle.
ATH_PROBE_CAP = 64
# When this many unparsed qualify already wait in hold, skip *full* blocking
# ATH-probe (hold dust). A small screened near-gate sync probe still runs.
ATH_PROBE_DEFER_PENDING_MIN = 12
# Sync near-gate probe size when drain is deferred (dump-after-pump same cycle).
ATH_PROBE_SYNC_NEAR_GATE = 12
# Cap catch-up DexScreener / truegnomode force-enrich (near-gate first).
HOLD_ENRICH_CAP = 200
# Watch parse parallelism (shared RPC/Blockscout — keep modest under Pro).
PARSE_CONCURRENCY = 6
# Workers that prefer the express lane (fresh / strict ≤3h remaining).
# 4 of 6 parse slots prefer express; rest drain bulk (same total RPC pool).
PARSE_EXPRESS_WORKERS = 4
# About-to-expire window: these jump ahead of high-ATH but still-young tokens.
PARSE_URGENT_REMAINING_HOURS = 3.0
# Assumed wall-seconds per token when sizing deadline-aware urgency.
PARSE_AVG_SEC_PER_TOKEN = 90.0
# Slack added on top of drain-ETA when deciding deadline urgency (hours).
PARSE_DEADLINE_SLACK_HOURS = 0.5
# Cap how far ETA may expand the urgent window (hours) on the bulk lane only.
PARSE_DEADLINE_ETA_CAP_HOURS = 6.0
# Fresh pumps (pair age ≤ this) parse before older high-ATH backlog.
PARSE_FRESH_PAIR_AGE_HOURS = 6.0
# Legacy: unknown-age + recent queued no longer marks express/fresh (flooded the
# lane when hold first_seen/queued_at were mass-stamped). Kept for callers.
PARSE_FRESH_QUEUED_HOURS = 2.0
# Stop wallet unique/enrich after this many passers (Хвать needs some, not all).
PARSE_MAX_PASSING_BUYERS = 100
# Base wall-clock budget for unique lookups per token (after unique-slot acquire).
PARSE_UNIQUE_WALL_SEC = 55.0
# Scale wall with shortlist size so late unique=1 wallets are not skipped while
# earlier rejects burn the fixed 55s (CUSSY: index 65/73 never examined).
PARSE_UNIQUE_WALL_PER_WALLET_SEC = 2.5
PARSE_UNIQUE_WALL_MAX_SEC = 150.0
# Max tokens concurrently running Blockscout unique enrich (parse ×6 otherwise
# stampede BS → 429/500 and trip the wall with fewer wallets examined).
# Pro BS sem=4 — keep unique-conc ≤3 so room remains for launch/other BS.
PARSE_UNIQUE_CONCURRENCY = 3
# In-flight wallet unique lookups inside one token's batch (≤ Pro BS conc).
PARSE_UNIQUE_WALLET_FANOUT = 3
# Unique Blockscout lookups per batch before re-checking the pass-cap.
PARSE_UNIQUE_BATCH = 40
# Floor for adaptive unique batches (avoid tiny 1–2 wallet BS round-trips).
PARSE_UNIQUE_BATCH_MIN = 8
# Outer parallel Gecko/DS peak probes (bg drain path uses 1 to avoid 429 storms).
ATH_PROBE_PARALLEL = 3
ATH_PROBE_PARALLEL_BG = 1
# Re-screen and inject newly qualified pumps every N completed parses.
PARSE_TOPUP_EVERY = 6
# Skip Blockscout launch supplement only once Uniswap already found this many
# early buyers (pad-only wallets otherwise get missed).
LAUNCH_BS_SKIP_MIN_UNISWAP_BUYERS = 8


def unique_wall_sec(n_wallets: int) -> float:
    """Wall budget after unique-slot acquire.

    Fixed 55s was too short for ~60–80 balance survivors: pass-rate under
    unique=1/30d is near zero, so the wall trips before the tail is examined
    and valid wallets are silently dropped (not rejected).
    """
    n = max(0, int(n_wallets))
    base = max(0.01, float(PARSE_UNIQUE_WALL_SEC))
    if n <= 0:
        return base
    scaled = float(PARSE_UNIQUE_WALL_PER_WALLET_SEC) * float(n)
    cap = max(base, float(PARSE_UNIQUE_WALL_MAX_SEC))
    return max(base, min(cap, scaled))


def should_defer_ath_probe(
    pending_qualify: int,
    *,
    min_pending: int = ATH_PROBE_DEFER_PENDING_MIN,
) -> bool:
    """True → do not block the cycle on full hold-dust ATH-probe.

    Already-gated hold rows (``queued_at`` + ATH≥gate) can drain immediately;
    probing thousands of under-gate dust must not starve CATSTRAT-class tokens.
    A small screened near-gate sync probe still runs before classify.
    """
    return int(pending_qualify) >= max(1, int(min_pending))


def estimate_drain_eta_hours(
    queue_len: int,
    *,
    concurrency: int = PARSE_CONCURRENCY,
    avg_sec_per_token: float = PARSE_AVG_SEC_PER_TOKEN,
) -> float:
    """Rough wall-hours to drain ``queue_len`` at current parse parallelism."""
    n = max(0, int(queue_len))
    slots = max(1, int(concurrency))
    sec = max(1.0, float(avg_sec_per_token))
    return (n / slots) * sec / 3600.0


def resolve_pair_age_hours(
    addr: str,
    *,
    hold: dict[str, dict],
    pair_age_hours: dict[str, float | None],
    now: float,
) -> float | None:
    """Screen age, else hold ``first_seen`` approx (remote/pending-rescue)."""
    key = addr.lower()
    age = pair_age_hours.get(key)
    if age is not None:
        return float(age)
    ent = hold.get(key) or {}
    fs = float(ent.get("first_seen") or 0.0)
    if fs <= 0.0:
        return None
    return max(0.0, (now - fs) / 3600.0)


def is_express_candidate(
    addr: str,
    *,
    hold: dict[str, dict],
    pair_age_hours: dict[str, float | None],
    max_pair_age_hours: float | None,
    now: float,
    fresh_pair_age_hours: float = PARSE_FRESH_PAIR_AGE_HOURS,
    fresh_queued_hours: float = PARSE_FRESH_QUEUED_HOURS,
    urgent_remaining_hours: float = PARSE_URGENT_REMAINING_HOURS,
) -> bool:
    """True → express lane (never blocked by ETA-expanded bulk urgency).

    Express = **known** screen pair_age ≤6h OR remaining ≤3h. Unknown age goes
    to bulk (still parsed) so mass-stamped ``first_seen``/``queued_at`` cannot
    flood the hot lane. ``fresh_queued_hours`` is unused (API compat).
    """
    _ = (hold, now, fresh_queued_hours)
    key = addr.lower()
    age = pair_age_hours.get(key)
    if age is None:
        return False
    age_f = float(age)
    if age_f <= float(fresh_pair_age_hours):
        return True
    max_age = (
        float(max_pair_age_hours)
        if max_pair_age_hours is not None and float(max_pair_age_hours) > 0
        else None
    )
    if max_age is not None:
        remaining = max(0.0, max_age - age_f)
        if remaining <= float(urgent_remaining_hours):
            return True
    return False


def split_express_bulk(
    tokens: list[str],
    *,
    hold: dict[str, dict],
    pair_age_hours: dict[str, float | None],
    max_pair_age_hours: float | None,
    now: float,
) -> tuple[list[str], list[str]]:
    """Split ordered tokens into express vs bulk lanes (stable within each)."""
    express: list[str] = []
    bulk: list[str] = []
    for addr in tokens:
        if is_express_candidate(
            addr,
            hold=hold,
            pair_age_hours=pair_age_hours,
            max_pair_age_hours=max_pair_age_hours,
            now=now,
        ):
            express.append(addr)
        else:
            bulk.append(addr)
    return express, bulk


def unique_lookup_batch_size(
    *,
    pass_cap: int,
    n_survivors: int,
    max_batch: int = PARSE_UNIQUE_BATCH,
    min_batch: int = PARSE_UNIQUE_BATCH_MIN,
) -> int:
    """Next unique-lookup batch size given remaining pass slots.

    Shrinks toward the end so a high pass-rate token does not pull a full
    ``max_batch`` of Blockscout wallets when only a few passers are still needed.
    """
    remaining = max(0, int(pass_cap) - int(n_survivors))
    if remaining <= 0:
        return 0
    cap = max(1, int(max_batch))
    floor = max(1, min(int(min_batch), cap))
    return min(cap, max(floor, remaining * 2))


# (addr, peak, symbol, pair_age_hours|None, screen_idx|None)
AthProbeNeed = tuple[str, float, str, float | None, int | None]


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

    def still_young(addr: str, pair_age_hours: float | None) -> bool:
        """True when the token is still inside max_pair_age for young reparse.

        Remote / pending-rescue rows often omit ``pair_age_hours``. Fall back to
        hold ``first_seen`` so a successful parse cannot permanently hide a
        still-young pump from cooldown requeue.
        """
        if max_age is None:
            return False
        if pair_age_hours is not None:
            return float(pair_age_hours) <= max_age
        ent = hold.get(addr) or {}
        fs = float(ent.get("first_seen") or 0.0)
        if fs <= 0.0:
            return False
        return ((now - fs) / 3600.0) <= max_age

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
                and still_young(addr, row.pair_age_hours)
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
            dust = peak < (threshold * HOLD_DUST_ATH_FRAC) if gate_on else False
            dust_expired = dust and age >= HOLD_DUST_TTL_SEC
            if (
                dust_expired
                or age >= hold_ttl_sec
                or (left_index and age >= 3600.0)
            ):
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


def select_ath_probe_batch(
    need: list[AthProbeNeed],
    *,
    probe_cap: int = ATH_PROBE_CAP,
    now: float,
    probed_at: dict[str, float] | None = None,
    reprobe_cooldown_sec: float = ATH_PROBE_COOLDOWN_SEC,
) -> list[AthProbeNeed]:
    """Pick ATH-probe targets without starving near-gate pumps.

    Priority:
    1. Tokens in the current screener slice (nearest-to-gate first) — live
       pumps must be probed the same cycle they appear, not behind dust.
    2. Hold-only never-probed / cooled: ~2/3 nearest-to-gate, ~1/3 lowest
       peak (hard DS undercount after a full dump).
    """
    cap = max(1, int(probe_cap))
    probed = probed_at or {}

    def cooled(addr: str) -> bool:
        ts = float(probed.get(addr) or 0.0)
        if ts <= 0.0:
            return True
        return (now - ts) >= float(reprobe_cooldown_sec)

    def never_probed(addr: str) -> bool:
        return float(probed.get(addr) or 0.0) <= 0.0

    screened = [t for t in need if t[4] is not None]
    # Screened may re-probe on a short leash; hold-only must rotate.
    screened_ready = [t for t in screened if cooled(t[0]) or never_probed(t[0])]
    # Prefer never-probed screened, then nearest to gate.
    screened_ready.sort(key=lambda t: (0 if never_probed(t[0]) else 1, -t[1]))

    hold_only = [t for t in need if t[4] is None and cooled(t[0])]
    hold_never = [t for t in hold_only if never_probed(t[0])]
    hold_retry = [t for t in hold_only if not never_probed(t[0])]

    picked: list[AthProbeNeed] = []
    seen: set[str] = set()

    def take(items: list[AthProbeNeed], n: int) -> None:
        for item in items:
            if n <= 0 or len(picked) >= cap:
                return
            addr = item[0]
            if addr in seen:
                continue
            picked.append(item)
            seen.add(addr)
            n -= 1

    take(screened_ready, cap)
    remaining = cap - len(picked)
    if remaining <= 0:
        return picked

    def fill_from(pool: list[AthProbeNeed], slots: int) -> None:
        nonlocal remaining
        if slots <= 0 or remaining <= 0 or not pool:
            return
        near_n = max(1, (slots * 2 + 2) // 3) if slots > 1 else slots
        low_n = slots - near_n
        by_high = sorted(pool, key=lambda t: t[1], reverse=True)
        by_low = sorted(pool, key=lambda t: t[1])
        before = len(picked)
        take(by_high, near_n)
        take(by_low, low_n)
        # Unused near/low slots → whatever is left in this pool.
        leftover = slots - (len(picked) - before)
        if leftover > 0:
            take(by_high, leftover)
        remaining = cap - len(picked)

    # Never-probed hold first so new near-gate pumps rotate in; retries fill gaps.
    fill_from(hold_never, remaining)
    if remaining > 0:
        fill_from(hold_retry, remaining)
    return picked


def select_hold_enrich_batch(
    hold: dict[str, dict],
    *,
    min_ath_mcap: float,
    cap: int = HOLD_ENRICH_CAP,
) -> list[str]:
    """Pick hold addresses for catch-up DexScreener / donor force-enrich.

    Prefers never-probed + nearest-to-gate peaks so a 4k hold queue cannot
    stall the cycle or blow the truegnomode 180s screen timeout.
    """
    threshold = float(min_ath_mcap or 0.0)
    cap_n = max(1, int(cap))
    rows: list[tuple[int, float, float, str]] = []
    for addr, ent in hold.items():
        peak = float(ent.get("ath_mcap") or 0.0)
        if threshold > 0 and peak >= threshold:
            continue  # already qualify — screen/classify will pick them up
        never = 0 if float(ent.get("ath_probed_at") or 0.0) <= 0.0 else 1
        # Near gate first (high peak), then never-probed, then oldest first_seen.
        first_seen = float(ent.get("first_seen") or 0.0)
        rows.append((never, -peak, first_seen, str(addr).lower()))
    rows.sort()
    return [addr for *_rest, addr in rows[:cap_n]]


def parse_queue_sort_key(
    addr: str,
    *,
    hold: dict[str, dict],
    pair_age_hours: dict[str, float | None],
    ath_mcap: dict[str, float],
    max_pair_age_hours: float | None,
    now: float,
    urgent_remaining_hours: float = PARSE_URGENT_REMAINING_HOURS,
    fresh_pair_age_hours: float = PARSE_FRESH_PAIR_AGE_HOURS,
    fresh_queued_hours: float = PARSE_FRESH_QUEUED_HOURS,
    drain_eta_hours: float | None = None,
    deadline_slack_hours: float = PARSE_DEADLINE_SLACK_HOURS,
    priority_min_ath: float | None = 50_000.0,
) -> tuple[int, int, int, float, float, str]:
    """Parse order: deadline/urgent → fresh → ATH≥priority → lower ATH tail → FIFO.

    Pure ATH-desc buried CATSTRAT-class (~$75k, age~4h) behind mega-ATH dust
    still inside the 24h window. Freshness/urgent bands use **known** screen
    pair_age only (no first_seen / queued fallback) so mass-stamped hold meta
    cannot fake freshness. Unknown-age sorts by ATH then queue time in bulk.

    ``priority_min_ath`` (default 50k): tokens below the floor sort after all
    tokens at/above it within the same urgent/fresh band (not dropped).
    ``None`` / ``<=0`` disables the band.
    """
    _ = fresh_queued_hours
    key = addr.lower()
    ent = hold.get(key) or {}
    queued = float(ent.get("queued_at") or ent.get("first_seen") or 0.0)
    if queued <= 0.0:
        queued = now
    ath = float(ath_mcap.get(key) or ent.get("ath_mcap") or 0.0)
    # Known screen age only — do not approximate from first_seen for bands.
    age = pair_age_hours.get(key)
    max_age = (
        float(max_pair_age_hours)
        if max_pair_age_hours is not None and float(max_pair_age_hours) > 0
        else None
    )
    urgent = 1
    if max_age is not None and age is not None:
        remaining = max(0.0, max_age - float(age))
        deadline = float(urgent_remaining_hours)
        if drain_eta_hours is not None and float(drain_eta_hours) > 0:
            expanded = float(drain_eta_hours) + float(deadline_slack_hours)
            # Cap ETA expansion so bulk mid-age cannot claim "urgent" forever.
            deadline = max(
                deadline,
                min(expanded, float(PARSE_DEADLINE_ETA_CAP_HOURS)),
            )
        if remaining <= deadline:
            urgent = 0
    fresh = 1
    if age is not None and float(age) <= float(fresh_pair_age_hours):
        fresh = 0
    floor = float(priority_min_ath) if priority_min_ath is not None else 0.0
    deferred = 0 if floor <= 0.0 or ath >= floor else 1
    return (urgent, fresh, deferred, -ath, queued, key)


def should_mark_parsed(
    error: str | None,
    *,
    buyers_before_filters: int | None = None,
    buyers_after_filters: int | None = None,
) -> bool:
    """Whether a finished parse should enter the parsed set (with timestamp).

    Retry hard discovery / soft-infra failures without marking. When early buyers
    existed under mcap but wallet filters wiped everyone, still mark parsed —
    ``classify_for_parse`` requeues young tokens after ``REPARSE_YOUNG_COOLDOWN_SEC``.
    Leaving them unmarked caused every cycle to re-parse the same hopeless
    tokens and starve never-seen candidates (no new wallets).
    """
    if error:
        low = error.lower()
        # Hard discovery — retry next cycle.
        if "no uniswap" in low or "no pool" in low:
            return False
        # Transient / false-positive skips — do not permanently park the token.
        if "honeypot" in low:
            return False
        if "usd price unavailable" in low or "price unavailable" in low:
            return False
        return True
    # buyers_before/after kept for call-site compatibility / logging.
    _ = (buyers_before_filters, buyers_after_filters)
    return True