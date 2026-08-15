"""Persistent watch config, seen-set, ATH hold queue, and last-success timestamp."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from .config import settings
from .models import WatchConfig

logger = logging.getLogger(__name__)

# Cap seen keys so the file cannot grow without bound.
_SEEN_MAX = 50_000
_PARSED_MAX = 20_000
_HOLD_MAX = 10_000
_MAX_CATCHUP_HOURS = 24.0


def seen_key(wallet: str, token: str) -> str:
    return f"{wallet.strip().lower()}:{token.strip().lower()}"


def catchup_lookback_hours(last_success_ts: float | None, *, now: float | None = None) -> float:
    """Hours of token age to cover since last successful watch run.

    Never ran, or gap ≥ 24h → 24h. Otherwise the exact downtime gap.
    """
    now_ts = time.time() if now is None else now
    if last_success_ts is None or last_success_ts <= 0:
        return _MAX_CATCHUP_HOURS
    gap_h = (now_ts - last_success_ts) / 3600.0
    if gap_h >= _MAX_CATCHUP_HOURS:
        return _MAX_CATCHUP_HOURS
    # Tiny floor so a near-instant re-enable still has a non-zero window.
    return max(gap_h, 1.0 / 60.0)


class WatchStore:
    def __init__(
        self,
        config_path: str | Path | None = None,
        seen_path: str | Path | None = None,
        state_path: str | Path | None = None,
        hold_path: str | Path | None = None,
        alert_path: str | Path | None = None,
    ) -> None:
        self._config_path = Path(config_path or settings.watch_config_path)
        self._seen_path = Path(seen_path or settings.watch_seen_path)
        self._state_path = Path(state_path or settings.watch_state_path)
        self._hold_path = Path(hold_path or settings.watch_hold_path)
        self._alert_path = Path(
            alert_path
            or (self._hold_path.parent / "watch_alert_outbox.json")
        )
        self._lock = threading.Lock()
        self._seen: set[str] | None = None
        self._hold: dict[str, dict[str, Any]] | None = None
        # token -> unix parsed_at (0.0 = legacy / unknown → eligible for young requeue)
        self._parsed: dict[str, float] | None = None
        self._alerts: dict[str, list[dict[str, Any]]] | None = None
        self._last_success_ts: float | None | object = _UNSET

    def load_config(self) -> WatchConfig:
        with self._lock:
            if not self._config_path.is_file():
                return WatchConfig()
            try:
                raw = json.loads(self._config_path.read_text(encoding="utf-8"))
                return WatchConfig.model_validate(raw)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load watch config %s: %r", self._config_path, exc)
                return WatchConfig()

    def save_config(self, cfg: WatchConfig) -> WatchConfig:
        with self._lock:
            self._config_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._config_path.with_suffix(self._config_path.suffix + ".tmp")
            payload = cfg.model_dump(mode="json")
            tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            tmp.replace(self._config_path)
            return cfg

    def _ensure_seen_loaded(self) -> set[str]:
        if self._seen is not None:
            return self._seen
        seen: set[str] = set()
        if self._seen_path.is_file():
            try:
                raw = json.loads(self._seen_path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    seen = {str(x).lower() for x in raw if x}
                elif isinstance(raw, dict) and isinstance(raw.get("keys"), list):
                    seen = {str(x).lower() for x in raw["keys"] if x}
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load watch seen %s: %r", self._seen_path, exc)
        self._seen = seen
        return seen

    def _persist_seen(self) -> None:
        assert self._seen is not None
        keys = list(self._seen)
        if len(keys) > _SEEN_MAX:
            keys = keys[-_SEEN_MAX:]
            self._seen = set(keys)
        self._seen_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._seen_path.with_suffix(self._seen_path.suffix + ".tmp")
        tmp.write_text(json.dumps({"keys": keys}, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self._seen_path)

    def load_seen(self) -> set[str]:
        with self._lock:
            return set(self._ensure_seen_loaded())

    def is_seen(self, wallet: str, token: str) -> bool:
        with self._lock:
            return seen_key(wallet, token) in self._ensure_seen_loaded()

    def mark_seen(self, pairs: list[tuple[str, str]]) -> int:
        """Mark wallet+token pairs as seen. Returns number of newly added keys."""
        if not pairs:
            return 0
        with self._lock:
            seen = self._ensure_seen_loaded()
            before = len(seen)
            for wallet, token in pairs:
                seen.add(seen_key(wallet, token))
            added = len(seen) - before
            if added:
                self._persist_seen()
            return added

    def clear_seen(self) -> None:
        with self._lock:
            self._seen = set()
            self._persist_seen()

    def seen_count(self) -> int:
        with self._lock:
            return len(self._ensure_seen_loaded())

    def _ensure_state_loaded(self) -> float | None:
        if self._last_success_ts is not _UNSET:
            return self._last_success_ts  # type: ignore[return-value]
        ts: float | None = None
        if self._state_path.is_file():
            try:
                raw = json.loads(self._state_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    val = raw.get("last_success_ts")
                    if isinstance(val, (int, float)) and val > 0:
                        ts = float(val)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load watch state %s: %r", self._state_path, exc)
        self._last_success_ts = ts
        return ts

    def load_last_success_ts(self) -> float | None:
        with self._lock:
            return self._ensure_state_loaded()

    def save_last_success_ts(self, ts: float) -> None:
        with self._lock:
            self._last_success_ts = float(ts)
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
            payload = {"last_success_ts": self._last_success_ts}
            tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            tmp.replace(self._state_path)

    # ---------------------------------------------------------------- hold / ATH

    def _ensure_hold_loaded(self) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
        if self._hold is not None and self._parsed is not None:
            return self._hold, self._parsed
        hold: dict[str, dict[str, Any]] = {}
        parsed: dict[str, float] = {}
        if self._hold_path.is_file():
            try:
                raw = json.loads(self._hold_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    raw_hold = raw.get("hold")
                    if isinstance(raw_hold, dict):
                        for k, v in raw_hold.items():
                            if not isinstance(v, dict):
                                continue
                            addr = str(k).lower()
                            ent: dict[str, Any] = {
                                "first_seen": float(v.get("first_seen") or time.time()),
                                "ath_mcap": float(v.get("ath_mcap") or 0.0),
                                "symbol": str(v.get("symbol") or ""),
                            }
                            # Pending-parse / probe stamps must survive reload
                            # (drain-all resumes mid-cycle after restart).
                            try:
                                queued = float(v.get("queued_at") or 0.0)
                            except (TypeError, ValueError):
                                queued = 0.0
                            if queued > 0.0:
                                ent["queued_at"] = queued
                            try:
                                probed = float(v.get("ath_probed_at") or 0.0)
                            except (TypeError, ValueError):
                                probed = 0.0
                            if probed > 0.0:
                                ent["ath_probed_at"] = probed
                            if v.get("partial_unique"):
                                ent["partial_unique"] = True
                            if v.get("filter_wipe"):
                                ent["filter_wipe"] = True
                            try:
                                last_p = float(v.get("last_parsed_at") or 0.0)
                            except (TypeError, ValueError):
                                last_p = 0.0
                            if last_p > 0.0:
                                ent["last_parsed_at"] = last_p
                            hold[addr] = ent
                    raw_at = raw.get("parsed_at")
                    if isinstance(raw_at, dict):
                        for k, v in raw_at.items():
                            if not k:
                                continue
                            try:
                                parsed[str(k).lower()] = float(v or 0.0)
                            except (TypeError, ValueError):
                                parsed[str(k).lower()] = 0.0
                    raw_parsed = raw.get("parsed")
                    if isinstance(raw_parsed, list):
                        # Legacy list: unknown timestamp → 0 so young tokens
                        # become immediately eligible for requeue.
                        for x in raw_parsed:
                            if not x:
                                continue
                            addr = str(x).lower()
                            parsed.setdefault(addr, 0.0)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load watch hold %s: %r", self._hold_path, exc)
        self._hold = hold
        self._parsed = parsed
        return hold, parsed

    def _persist_hold(self) -> None:
        assert self._hold is not None and self._parsed is not None
        # Bound growth: keep newest hold / parsed tails.
        if len(self._hold) > _HOLD_MAX:
            items = sorted(
                self._hold.items(),
                key=lambda kv: float(kv[1].get("first_seen") or 0.0),
                reverse=True,
            )
            self._hold = dict(items[:_HOLD_MAX])
        if len(self._parsed) > _PARSED_MAX:
            newest = sorted(self._parsed.items(), key=lambda kv: kv[1])[-_PARSED_MAX:]
            self._parsed = dict(newest)
        parsed_list = list(self._parsed.keys())
        payload = {
            "hold": self._hold,
            "parsed": parsed_list,
            "parsed_at": self._parsed,
        }
        self._hold_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._hold_path.with_suffix(self._hold_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(self._hold_path)

    def load_hold(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            hold, _ = self._ensure_hold_loaded()
            return {k: dict(v) for k, v in hold.items()}

    def load_parsed_tokens(self) -> set[str]:
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            return set(parsed.keys())

    def load_parsed_at(self) -> dict[str, float]:
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            return dict(parsed)

    def hold_count(self) -> int:
        """Hold rows that are not merely parsed requeue-meta (queued or under-gate)."""
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            n = 0
            for addr, ent in hold.items():
                if addr in parsed and float(ent.get("queued_at") or 0.0) <= 0.0:
                    continue
                n += 1
            return n

    def parsed_token_count(self) -> int:
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            return len(parsed)

    def load_pending_parse(
        self, *, min_ath_mcap: float | None = None
    ) -> list[str]:
        """Unparsed qualify addresses waiting for drain-all parse.

        A hold row becomes pending when it first crosses the ATH gate
        (``queued_at`` stamped). Survives classify wipe and process restart.
        """
        thr = float(min_ath_mcap or 0.0)
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            out: list[str] = []
            for addr, ent in hold.items():
                if addr in parsed:
                    continue
                if float(ent.get("queued_at") or 0.0) <= 0.0:
                    continue
                if thr > 0.0 and float(ent.get("ath_mcap") or 0.0) < thr:
                    continue
                out.append(addr)
            return out

    def remove_hold_tokens(self, tokens: list[str] | set[str]) -> int:
        """Drop hold rows (TTL dust / left-index expire, etc.)."""
        keys = {str(t).strip().lower() for t in tokens if t}
        if not keys:
            return 0
        with self._lock:
            hold, _ = self._ensure_hold_loaded()
            n = 0
            for key in keys:
                if key in hold:
                    del hold[key]
                    n += 1
            if n:
                self._persist_hold()
            return n

    def clear_pending_queued(self, tokens: list[str] | set[str]) -> int:
        """Soft age-out: clear ``queued_at`` but keep hold row + ATH peak.

        Aged pending must leave the drain queue (early buyers outside the
        window), but wiping the row erased Gecko/DS peaks so dump-after-pump
        tokens could never re-qualify.
        """
        keys = {str(t).strip().lower() for t in tokens if t}
        if not keys:
            return 0
        with self._lock:
            hold, _ = self._ensure_hold_loaded()
            n = 0
            for key in keys:
                ent = hold.get(key)
                if not ent:
                    continue
                if float(ent.get("queued_at") or 0.0) <= 0.0:
                    continue
                ent["queued_at"] = 0.0
                n += 1
            if n:
                self._persist_hold()
            return n

    def clear_all_pending_queued(self) -> int:
        """Soft-clear every pending-parse stamp (``queued_at``); keep hold/ATH.

        Used by ops to drop a dead backlog without wiping peaks so tokens can
        re-qualify on the next screen/ATH probe.
        """
        with self._lock:
            hold, _ = self._ensure_hold_loaded()
            n = 0
            for ent in hold.values():
                if float(ent.get("queued_at") or 0.0) <= 0.0:
                    continue
                ent["queued_at"] = 0.0
                n += 1
            if n:
                self._persist_hold()
            return n

    def apply_qualify_updates(
        self,
        *,
        ath_updates: dict[str, tuple[float, str]],
        held: list[str],
        expired: list[str],
        candidates: list[str] | None = None,
        now: float | None = None,
        probed_at: dict[str, float] | None = None,
    ) -> None:
        """Persist ATH peaks, upsert hold entries, drop expired ones.

        Waiting tokens (``held``) always get a hold row. Parse candidates are
        stamped with ``queued_at`` (pending-parse) so drain-all / restart can
        resume without losing qualify that were not in this screen slice.

        Unparsed pending (``queued_at`` set) is **not** wiped when absent from
        the current ``held ∪ candidates`` set — only ``expired``, ``parsed``,
        or explicit ``remove_hold_tokens`` clears it.

        ``probed_at`` stamps ``ath_probed_at`` on hold rows so Gecko probe
        budget rotates instead of sticky-reprobing the same dust forever.
        """
        now_ts = time.time() if now is None else now
        held_set = {a.lower() for a in held}
        cand_set = {a.lower() for a in (candidates or [])}
        expired_set = {a.lower() for a in expired}
        probe_stamp = {
            str(k).strip().lower(): float(v)
            for k, v in (probed_at or {}).items()
            if k and float(v) > 0.0
        }
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            dirty = False

            for key in expired_set:
                if key in hold:
                    del hold[key]
                    dirty = True

            def _upsert(key: str, *, create: bool, as_candidate: bool = False) -> None:
                nonlocal dirty
                ath, sym = ath_updates.get(key, (0.0, ""))
                if key in parsed:
                    # Keep ATH/first_seen for young requeue; never wipe meta.
                    ent = hold.get(key)
                    if ent is None:
                        if not create and not ath:
                            return
                        hold[key] = {
                            "first_seen": now_ts,
                            "ath_mcap": float(ath),
                            "symbol": sym or "",
                            "queued_at": 0.0,
                        }
                        dirty = True
                        return
                    new_ath = max(float(ent.get("ath_mcap") or 0.0), float(ath))
                    new_sym = sym or str(ent.get("symbol") or "")
                    if new_ath != ent.get("ath_mcap") or new_sym != ent.get("symbol"):
                        ent["ath_mcap"] = new_ath
                        ent["symbol"] = new_sym
                        dirty = True
                    # Candidates while still parsed are handled via unparse_tokens.
                    return
                ent = hold.get(key)
                if ent is None:
                    if not create:
                        return
                    hold[key] = {
                        "first_seen": now_ts,
                        "ath_mcap": float(ath),
                        "symbol": sym or "",
                    }
                    dirty = True
                    ent = hold[key]
                else:
                    new_ath = max(float(ent.get("ath_mcap") or 0.0), float(ath))
                    new_sym = sym or str(ent.get("symbol") or "")
                    if new_ath != ent.get("ath_mcap") or new_sym != ent.get("symbol"):
                        ent["ath_mcap"] = new_ath
                        ent["symbol"] = new_sym
                        dirty = True
                if as_candidate and float(ent.get("queued_at") or 0.0) <= 0.0:
                    # First time this addr crossed ATH gate → pending drain.
                    ent["queued_at"] = now_ts
                    dirty = True
                if key in probe_stamp:
                    ts = probe_stamp[key]
                    if float(ent.get("ath_probed_at") or 0.0) != ts:
                        ent["ath_probed_at"] = ts
                        dirty = True

            for key in held_set:
                _upsert(key, create=True)

            for key in cand_set:
                # Persist candidates so drain-all / restart never drops qualify.
                _upsert(key, create=True, as_candidate=True)

            # Stamp probes even when the addr is only in ath_updates / probed set.
            for key, ts in probe_stamp.items():
                ent = hold.get(key)
                if ent is None or key in parsed:
                    continue
                if float(ent.get("ath_probed_at") or 0.0) != ts:
                    ent["ath_probed_at"] = ts
                    dirty = True

            for key in list(hold.keys()):
                if key in expired_set:
                    del hold[key]
                    dirty = True
                    continue
                # Parsed requeue-meta rows must survive classify wipe.
                if key in parsed:
                    continue
                ent = hold[key]
                # Keep unparsed pending-parse across partial classify / restart.
                if float(ent.get("queued_at") or 0.0) > 0.0:
                    continue
                if key not in held_set and key not in cand_set:
                    del hold[key]
                    dirty = True

            if dirty:
                self._persist_hold()

    def mark_token_parsed(
        self,
        token: str,
        *,
        at: float | None = None,
        partial_unique: bool = False,
        filter_wipe: bool = False,
    ) -> None:
        """Stamp parsed_at and clear pending queue flag — keep ATH/first_seen.

        Hold meta must survive so young requeue (``still_young`` via
        ``first_seen``) works even when the screener omits pair_age. Wiping the
        row after a successful parse permanently hid pumps from cooldown reparse.
        """
        key = token.strip().lower()
        if not key:
            return
        ts = time.time() if at is None else float(at)
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            parsed[key] = ts
            ent = hold.get(key)
            if ent is None:
                ent = {
                    "first_seen": ts,
                    "ath_mcap": 0.0,
                    "symbol": "",
                }
                hold[key] = ent
            ent["queued_at"] = 0.0
            ent["last_parsed_at"] = ts
            if partial_unique:
                ent["partial_unique"] = True
            if filter_wipe:
                ent["filter_wipe"] = True
            self._persist_hold()

    def unparse_tokens(self, tokens: list[str] | set[str]) -> int:
        """Remove tokens from the parsed set so they can be parsed again."""
        keys = {str(t).strip().lower() for t in tokens if t}
        if not keys:
            return 0
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            n = 0
            for key in keys:
                if key in parsed:
                    del parsed[key]
                    n += 1
                ent = hold.get(key)
                if ent is not None:
                    # Allow drain-all to pick them up again if still qualify.
                    if float(ent.get("queued_at") or 0.0) <= 0.0:
                        ent["queued_at"] = time.time()
                    ent.pop("partial_unique", None)
                    ent.pop("filter_wipe", None)
            if n:
                self._persist_hold()
            return n

    def is_token_parsed(self, token: str) -> bool:
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            return token.strip().lower() in parsed

    def clear_hold(self) -> None:
        """Clear ATH hold queue and parsed-token set (does not touch wallet seen)."""
        with self._lock:
            self._hold = {}
            self._parsed = {}
            self._persist_hold()

    # ---------------------------------------------------------------- alert outbox (TG retry, no re-parse)

    def _ensure_alerts_loaded(self) -> dict[str, list[dict[str, Any]]]:
        if self._alerts is not None:
            return self._alerts
        alerts: dict[str, list[dict[str, Any]]] = {}
        if self._alert_path.is_file():
            try:
                raw = json.loads(self._alert_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        if not k or not isinstance(v, list):
                            continue
                        rows = [x for x in v if isinstance(x, dict)]
                        if rows:
                            alerts[str(k).lower()] = rows[:80]
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to load alert outbox %s: %r", self._alert_path, exc)
        self._alerts = alerts
        return alerts

    def _persist_alerts(self) -> None:
        assert self._alerts is not None
        # Bound: newest tokens only.
        if len(self._alerts) > 80:
            items = list(self._alerts.items())[-80:]
            self._alerts = dict(items)
        self._alert_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._alert_path.with_suffix(self._alert_path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(self._alerts, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp.replace(self._alert_path)

    def enqueue_alert_outbox(
        self, token: str, buyers: list[Any]
    ) -> int:
        """Persist undelivered buyers for a later TG-only flush (no re-parse)."""
        key = token.strip().lower()
        if not key or not buyers:
            return 0
        rows: list[dict[str, Any]] = []
        for b in buyers:
            if hasattr(b, "model_dump"):
                rows.append(b.model_dump(mode="json"))
            elif isinstance(b, dict):
                rows.append(dict(b))
        if not rows:
            return 0
        with self._lock:
            alerts = self._ensure_alerts_loaded()
            prev = alerts.get(key) or []
            seen_w = {
                str(x.get("wallet") or "").lower()
                for x in prev
                if isinstance(x, dict)
            }
            added = 0
            for row in rows:
                w = str(row.get("wallet") or "").lower()
                if not w or w in seen_w:
                    continue
                prev.append(row)
                seen_w.add(w)
                added += 1
            alerts[key] = prev[:80]
            self._persist_alerts()
            return added

    def load_alert_outbox(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            return {
                k: [dict(x) for x in v]
                for k, v in self._ensure_alerts_loaded().items()
            }

    def clear_alert_outbox(self, token: str) -> None:
        key = token.strip().lower()
        if not key:
            return
        with self._lock:
            alerts = self._ensure_alerts_loaded()
            if key in alerts:
                del alerts[key]
                self._persist_alerts()

    def alert_outbox_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._ensure_alerts_loaded().values())

_UNSET = object()

watch_store = WatchStore()
