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
    ) -> None:
        self._config_path = Path(config_path or settings.watch_config_path)
        self._seen_path = Path(seen_path or settings.watch_seen_path)
        self._state_path = Path(state_path or settings.watch_state_path)
        self._hold_path = Path(hold_path or settings.watch_hold_path)
        self._lock = threading.Lock()
        self._seen: set[str] | None = None
        self._hold: dict[str, dict[str, Any]] | None = None
        # token -> unix parsed_at (0.0 = legacy / unknown → eligible for young requeue)
        self._parsed: dict[str, float] | None = None
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
                            hold[addr] = {
                                "first_seen": float(v.get("first_seen") or time.time()),
                                "ath_mcap": float(v.get("ath_mcap") or 0.0),
                                "symbol": str(v.get("symbol") or ""),
                            }
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
        with self._lock:
            hold, _ = self._ensure_hold_loaded()
            return len(hold)

    def parsed_token_count(self) -> int:
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            return len(parsed)

    def apply_qualify_updates(
        self,
        *,
        ath_updates: dict[str, tuple[float, str]],
        held: list[str],
        expired: list[str],
        candidates: list[str] | None = None,
        now: float | None = None,
    ) -> None:
        """Persist ATH peaks, upsert hold entries, drop expired ones.

        Waiting tokens (``held``) always get a hold row. Candidates that were
        already on hold stay until ``mark_token_parsed`` so a failed parse
        (no pool) can retry next cycle.
        """
        now_ts = time.time() if now is None else now
        held_set = {a.lower() for a in held}
        cand_set = {a.lower() for a in (candidates or [])}
        expired_set = {a.lower() for a in expired}
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            dirty = False

            for key in expired_set:
                if key in hold:
                    del hold[key]
                    dirty = True

            def _upsert(key: str, *, create: bool) -> None:
                nonlocal dirty
                if key in parsed:
                    if key in hold:
                        del hold[key]
                        dirty = True
                    return
                ath, sym = ath_updates.get(key, (0.0, ""))
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
                    return
                new_ath = max(float(ent.get("ath_mcap") or 0.0), float(ath))
                new_sym = sym or str(ent.get("symbol") or "")
                if new_ath != ent.get("ath_mcap") or new_sym != ent.get("symbol"):
                    ent["ath_mcap"] = new_ath
                    ent["symbol"] = new_sym
                    dirty = True

            for key in held_set:
                _upsert(key, create=True)

            for key in cand_set:
                # Keep prior hold rows for retry; do not create new ones.
                _upsert(key, create=False)

            for key in list(hold.keys()):
                if key in parsed or key in expired_set:
                    del hold[key]
                    dirty = True
                    continue
                if key not in held_set and key not in cand_set:
                    del hold[key]
                    dirty = True

            if dirty:
                self._persist_hold()

    def mark_token_parsed(self, token: str, *, at: float | None = None) -> None:
        key = token.strip().lower()
        if not key:
            return
        ts = time.time() if at is None else float(at)
        with self._lock:
            hold, parsed = self._ensure_hold_loaded()
            parsed[key] = ts
            if key in hold:
                del hold[key]
            self._persist_hold()

    def unparse_tokens(self, tokens: list[str] | set[str]) -> int:
        """Remove tokens from the parsed set so they can be parsed again."""
        keys = {str(t).strip().lower() for t in tokens if t}
        if not keys:
            return 0
        with self._lock:
            _, parsed = self._ensure_hold_loaded()
            n = 0
            for key in keys:
                if key in parsed:
                    del parsed[key]
                    n += 1
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


_UNSET = object()

watch_store = WatchStore()
