"""Durable SQLite cache for exact unique-token (tokens traded) counts.

Only ``exact=True`` counts are stored. Early-exit lower/upper bounds must never
be persisted — a later tighter filter would otherwise reuse an incomplete count.

Lookup order for callers: in-memory → this SQLite layer → Blockscout.
Optional soft seed from followup.db when wallet ``updated_at`` is still fresh.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

from .config import settings

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS unique_counts (
    wallet TEXT NOT NULL,
    period_hours INTEGER NOT NULL,
    count INTEGER NOT NULL,
    exact INTEGER NOT NULL DEFAULT 1,
    updated_at REAL NOT NULL,
    PRIMARY KEY (wallet, period_hours)
);
CREATE INDEX IF NOT EXISTS idx_unique_updated
    ON unique_counts(updated_at);
"""

_DEFAULT_PERIOD_HOURS = 168
_lock = threading.Lock()
_conn: sqlite3.Connection | None = None
_db_path: Path | None = None
_path_override: Path | None = None
_seeded = False


def _ttl_sec() -> float:
    return max(float(getattr(settings, "unique_cache_ttl_sec", 6 * 3600) or 0), 0.0)


def _cache_path() -> Path:
    if _path_override is not None:
        return _path_override
    raw = getattr(settings, "unique_cache_path", None) or ""
    if str(raw).strip():
        return Path(str(raw))
    return Path(settings.followup_db_path).resolve().parent / "wallet_unique.db"


def _connect() -> sqlite3.Connection:
    global _conn, _db_path
    path = _cache_path()
    if _conn is not None and _db_path == path:
        return _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:  # noqa: BLE001
            pass
        _conn = None
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    _conn = conn
    _db_path = path
    return conn


def reset_for_tests(path: Path | None = None) -> None:
    """Close connection and optionally point at a fresh temp DB (tests)."""
    global _conn, _db_path, _path_override, _seeded
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:  # noqa: BLE001
                pass
        _conn = None
        _db_path = None
        _path_override = path
        _seeded = False
        if path is not None:
            _connect()

def get_exact(wallet: str, period_hours: int, *, now: float | None = None) -> int | None:
    """Return cached exact count if present and within TTL, else ``None``."""
    wallet_l = wallet.lower()
    period = max(int(period_hours), 1)
    ttl = _ttl_sec()
    stamp = time.time() if now is None else float(now)
    with _lock:
        conn = _connect()
        row = conn.execute(
            "SELECT count, exact, updated_at FROM unique_counts "
            "WHERE wallet=? AND period_hours=?",
            (wallet_l, period),
        ).fetchone()
        if row is None:
            return None
        if int(row["exact"] or 0) != 1:
            return None
        if ttl > 0 and stamp - float(row["updated_at"]) > ttl:
            return None
        return int(row["count"])


def put_exact(
    wallet: str,
    period_hours: int,
    count: int,
    *,
    exact: bool = True,
    now: float | None = None,
) -> bool:
    """Store an exact count. Refuses ``exact=False`` (returns False)."""
    if not exact:
        return False
    wallet_l = wallet.lower()
    period = max(int(period_hours), 1)
    stamp = time.time() if now is None else float(now)
    with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO unique_counts(wallet, period_hours, count, exact, updated_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(wallet, period_hours) DO UPDATE SET "
            "count=excluded.count, exact=1, updated_at=excluded.updated_at",
            (wallet_l, period, int(count), 1, stamp),
        )
        conn.commit()
    return True


def invalidate(wallet: str, period_hours: int | None = None) -> int:
    """Drop cached rows for ``wallet`` (one period or all). Returns rows deleted."""
    wallet_l = wallet.lower()
    with _lock:
        conn = _connect()
        if period_hours is None:
            cur = conn.execute(
                "DELETE FROM unique_counts WHERE wallet=?", (wallet_l,)
            )
        else:
            cur = conn.execute(
                "DELETE FROM unique_counts WHERE wallet=? AND period_hours=?",
                (wallet_l, max(int(period_hours), 1)),
            )
        conn.commit()
        return int(cur.rowcount or 0)


def seed_from_followup(
    *,
    followup_db: Path | str | None = None,
    period_hours: int = _DEFAULT_PERIOD_HOURS,
    now: float | None = None,
    force: bool = False,
) -> int:
    """Soft-seed exact counts from followup ``wallets.tokens_traded_7d``.

    Only rows with non-NULL ``tokens_traded_7d`` and ``updated_at`` within TTL
    are copied — stale followup values must not poison the unique cache.
    Returns number of rows upserted. Idempotent; runs once per process unless
    ``force=True``.
    """
    global _seeded
    if _seeded and not force:
        return 0
    path = Path(followup_db) if followup_db else Path(settings.followup_db_path)
    if not path.is_file():
        _seeded = True
        return 0
    ttl = _ttl_sec()
    stamp = time.time() if now is None else float(now)
    period = max(int(period_hours), 1)
    n = 0
    try:
        src = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10.0)
        src.row_factory = sqlite3.Row
    except Exception as exc:  # noqa: BLE001
        logger.warning("unique cache seed: cannot open followup db: %s", exc)
        _seeded = True
        return 0
    try:
        try:
            rows = src.execute(
                "SELECT address, tokens_traded_7d, updated_at FROM wallets "
                "WHERE tokens_traded_7d IS NOT NULL"
            ).fetchall()
        except sqlite3.Error as exc:
            logger.warning("unique cache seed: followup query failed: %s", exc)
            _seeded = True
            return 0
        with _lock:
            conn = _connect()
            for row in rows:
                updated = float(row["updated_at"] or 0.0)
                if ttl > 0 and (updated <= 0 or stamp - updated > ttl):
                    continue
                count = int(row["tokens_traded_7d"])
                # Never seed count≤1 as exact — goes stale the moment the wallet
                # buys a second token and falsely passes Хвать unique=1.
                if count <= 1:
                    continue
                wallet = str(row["address"] or "").lower()
                if not wallet:
                    continue
                prev = conn.execute(
                    "SELECT updated_at FROM unique_counts "
                    "WHERE wallet=? AND period_hours=?",
                    (wallet, period),
                ).fetchone()
                if prev is not None and float(prev["updated_at"]) >= updated:
                    continue
                conn.execute(
                    "INSERT INTO unique_counts"
                    "(wallet, period_hours, count, exact, updated_at) "
                    "VALUES(?,?,?,?,?) "
                    "ON CONFLICT(wallet, period_hours) DO UPDATE SET "
                    "count=excluded.count, exact=1, updated_at=excluded.updated_at "
                    "WHERE excluded.updated_at >= unique_counts.updated_at",
                    (wallet, period, count, 1, updated),
                )
                n += 1
            if n:
                conn.commit()
    finally:
        src.close()
    _seeded = True
    if n:
        logger.info("unique cache seeded %d wallets from followup (period=%dh)", n, period)
    return n


def maybe_seed() -> None:
    """Best-effort one-shot seed (safe to call on every batch)."""
    try:
        seed_from_followup()
    except Exception as exc:  # noqa: BLE001
        logger.warning("unique cache seed failed: %s", exc)
