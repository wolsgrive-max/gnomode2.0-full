"""SQLite store for follow-up wallets (WAL, durable, lightweight)."""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import struct
import threading
import time
from pathlib import Path
from typing import Any

from .config import settings
from .models import (
    BuyerRow,
    FollowupConfig,
    FollowupDealRow,
    FollowupWalletRow,
    WalletAlertFilters,
)

logger = logging.getLogger(__name__)

_SQLITE_MAGIC = b"SQLite format 3\x00"
_HEADER_REPAIR_PAGE_SIZES = (4096, 8192, 2048, 1024, 512)
_DB_OPEN_ERRORS = (
    "file is not a database",
    "database disk image is malformed",
    "malformed database schema",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallets (
    address TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'watching',
    deal_count INTEGER NOT NULL DEFAULT 0,
    wallet_balance_eth REAL,
    tokens_traded_7d INTEGER,
    raybot_synced INTEGER NOT NULL DEFAULT 0,
    first_token TEXT NOT NULL DEFAULT '',
    first_mcap REAL,
    discovered_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    alert_filters TEXT,
    last_seen_block INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS deals (
    wallet TEXT NOT NULL,
    token TEXT NOT NULL,
    token_symbol TEXT NOT NULL DEFAULT '',
    token_name TEXT NOT NULL DEFAULT '',
    deal_index INTEGER NOT NULL,
    mcap_at_buy REAL,
    bought_usd REAL,
    tx_hash TEXT NOT NULL DEFAULT '',
    block_number INTEGER NOT NULL DEFAULT 0,
    bought_at REAL,
    notified INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    ath_passed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (wallet, token)
);
CREATE INDEX IF NOT EXISTS idx_deals_wallet ON deals(wallet);
CREATE INDEX IF NOT EXISTS idx_wallets_status ON wallets(status);
CREATE TABLE IF NOT EXISTS alert_log (
    wallet TEXT NOT NULL,
    token TEXT NOT NULL,
    kind TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (wallet, token, kind)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS alert_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedup_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL DEFAULT 'deal',
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    sent_at REAL
);
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON alert_outbox(status, next_attempt_at);
"""


def _is_sqlite_open_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(marker in msg for marker in _DB_OPEN_ERRORS)


def _has_sqlite_magic(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            return fh.read(16) == _SQLITE_MAGIC
    except OSError:
        return False


def _sqlite_usable(path: Path) -> bool:
    """True if path opens as SQLite and answers a trivial pragma."""
    if not path.is_file() or path.stat().st_size <= 0:
        return False
    if not _has_sqlite_magic(path):
        return False
    try:
        with sqlite3.connect(str(path), timeout=5.0) as conn:
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
        return True
    except sqlite3.Error:
        return False


def _rotate_aside(path: Path, *, suffix: str) -> Path | None:
    """Rename path out of the way. Never unlink without keeping a copy."""
    if not path.exists():
        return None
    ts = time.strftime("%Y%m%d-%H%M%S")
    dest = path.with_name(f"{path.name}.{suffix}-{ts}")
    n = 0
    while dest.exists():
        n += 1
        dest = path.with_name(f"{path.name}.{suffix}-{ts}-{n}")
    path.rename(dest)
    for side_suffix in ("-wal", "-shm"):
        side_path = path.with_name(f"{path.name}{side_suffix}")
        if side_path.exists():
            side_dest = Path(str(dest) + side_suffix)
            try:
                side_path.rename(side_dest)
            except OSError:
                logger.warning("followup db: could not rotate sidecar %s", side_path)
    return dest


def _try_repair_overwritten_header(path: Path) -> Path | None:
    """Best-effort repair when page-1 header bytes were overwritten.

    Observed failure mode: first ~100 bytes garbage (looks TLS-like) while the
    rest of a 4KiB-paged DB remains readable. Returns path to a clean dump, or
    None if repair is not possible.
    """
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if not raw or raw.startswith(_SQLITE_MAGIC):
        return None
    if b"CREATE TABLE wallets" not in raw and b"TABLE wallets" not in raw:
        return None

    for page_size in _HEADER_REPAIR_PAGE_SIZES:
        if len(raw) % page_size != 0:
            continue
        total_pages = len(raw) // page_size
        hdr = bytearray(100)
        hdr[0:16] = _SQLITE_MAGIC
        struct.pack_into(">H", hdr, 16, page_size)
        hdr[18] = 1
        hdr[19] = 1
        hdr[21] = 64
        hdr[22] = 32
        hdr[23] = 32
        struct.pack_into(">I", hdr, 28, total_pages)
        struct.pack_into(">I", hdr, 44, 4)
        struct.pack_into(">I", hdr, 56, 1)
        candidate = bytearray(raw)
        candidate[0:100] = hdr
        tmp = path.with_name(f"{path.name}.hdr-repair-tmp")
        clean = path.with_name(f"{path.name}.hdr-repaired")
        try:
            tmp.write_bytes(candidate)
            if clean.exists():
                clean.unlink()
            src = sqlite3.connect(str(tmp))
            dst = sqlite3.connect(str(clean))
            try:
                for line in src.iterdump():
                    dst.execute(line)
                dst.commit()
                check = dst.execute("PRAGMA integrity_check").fetchone()
                if not check or str(check[0]).lower() != "ok":
                    raise sqlite3.DatabaseError(
                        f"repaired integrity_check={check!r}"
                    )
                # Must look like our schema, not an empty shell.
                n = dst.execute("SELECT COUNT(*) FROM wallets").fetchone()
                if n is None:
                    raise sqlite3.DatabaseError("repaired DB missing wallets")
            finally:
                dst.close()
                src.close()
            return clean
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "followup db header repair page_size=%s failed: %s",
                page_size,
                exc,
            )
            for p in (tmp, clean):
                try:
                    if p.exists():
                        p.unlink()
                except OSError:
                    pass
    return None


class FollowupStore:
    def __init__(
        self,
        db_path: str | None = None,
        config_path: str | None = None,
    ) -> None:
        self._db_path = Path(db_path or settings.followup_db_path)
        self._config_path = Path(config_path or settings.followup_config_path)
        self._lock = threading.Lock()
        self._ensured = False
        self._db_prepared = False

    def _bak_path(self) -> Path:
        return self._db_path.with_suffix(self._db_path.suffix + ".bak")

    def _refresh_bak(self) -> None:
        """Copy a known-good DB to ``*.bak`` (best-effort, never raises)."""
        try:
            if not _sqlite_usable(self._db_path):
                return
            bak = self._bak_path()
            shutil.copy2(self._db_path, bak)
        except OSError as exc:
            logger.warning("followup db: failed to refresh .bak: %s", exc)

    def _prepare_db_file(self) -> None:
        """Refuse to open/clobber a non-SQLite file; restore or rotate instead."""
        if self._db_prepared:
            return
        path = self._db_path
        path.parent.mkdir(parents=True, exist_ok=True)

        if not path.exists():
            # Prefer restoring from backup before creating an empty schema.
            bak = self._bak_path()
            if _sqlite_usable(bak):
                shutil.copy2(bak, path)
                logger.warning(
                    "followup db missing at %s — restored from %s",
                    path,
                    bak,
                )
            self._db_prepared = True
            return

        size = path.stat().st_size
        if size == 0:
            rotated = _rotate_aside(path, suffix="empty")
            logger.error(
                "followup db at %s was empty (0 bytes); rotated to %s. "
                "Will restore .bak or create a fresh schema (wallets may be lost).",
                path,
                rotated,
            )
            bak = self._bak_path()
            if _sqlite_usable(bak):
                shutil.copy2(bak, path)
                logger.warning("followup db restored from backup %s", bak)
            self._db_prepared = True
            return

        if _sqlite_usable(path):
            self._db_prepared = True
            return

        # Non-empty but not a usable SQLite DB — never overwrite in place.
        logger.error(
            "followup db unusable at %s (size=%s, magic_ok=%s). "
            "Rotating aside; attempting header repair / .bak restore before "
            "creating a fresh schema.",
            path,
            size,
            _has_sqlite_magic(path),
        )
        repaired = _try_repair_overwritten_header(path)
        rotated = _rotate_aside(path, suffix="corrupt")
        logger.error("followup db rotated corrupt file to %s", rotated)

        if repaired is not None and _sqlite_usable(repaired):
            shutil.move(str(repaired), str(path))
            logger.warning(
                "followup db recovered via header repair into %s "
                "(corrupt original kept at %s)",
                path,
                rotated,
            )
            self._db_prepared = True
            return

        bak = self._bak_path()
        if _sqlite_usable(bak):
            shutil.copy2(bak, path)
            logger.warning(
                "followup db restored from backup %s after corruption "
                "(corrupt original at %s)",
                bak,
                rotated,
            )
            self._db_prepared = True
            return

        logger.error(
            "followup db: no usable .bak at %s — creating empty schema at %s. "
            "Wallet/deal history from the corrupt file may be lost; inspect %s.",
            bak,
            path,
            rotated,
        )
        self._db_prepared = True

    def _connect(self) -> sqlite3.Connection:
        self._prepare_db_file()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            return conn
        except sqlite3.Error as exc:
            if not _is_sqlite_open_error(exc):
                raise
            # Race / late detection: prepare again once, then fail clearly.
            self._db_prepared = False
            logger.error(
                "followup db open failed at %s: %s — re-preparing file",
                self._db_path,
                exc,
            )
            self._prepare_db_file()
            try:
                conn = sqlite3.connect(str(self._db_path), timeout=30.0)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=5000")
                return conn
            except sqlite3.Error as exc2:
                raise sqlite3.DatabaseError(
                    f"followup db unusable at {self._db_path}: {exc2}. "
                    f"Refusing to clobber; check *.corrupt-* / *.bak alongside it."
                ) from exc2

    def _ensure(self) -> None:
        if self._ensured:
            return
        with self._lock:
            if self._ensured:
                return
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
                cols = {r[1] for r in conn.execute("PRAGMA table_info(wallets)")}
                if "alert_filters" not in cols:
                    conn.execute("ALTER TABLE wallets ADD COLUMN alert_filters TEXT")
                if "last_seen_block" not in cols:
                    conn.execute(
                        "ALTER TABLE wallets ADD COLUMN last_seen_block INTEGER NOT NULL DEFAULT 0"
                    )
                dcols = {r[1] for r in conn.execute("PRAGMA table_info(deals)")}
                if "ath_passed" not in dcols:
                    conn.execute(
                        "ALTER TABLE deals ADD COLUMN ath_passed INTEGER NOT NULL DEFAULT 0"
                    )
                if "block_number" not in dcols:
                    conn.execute(
                        "ALTER TABLE deals ADD COLUMN block_number INTEGER NOT NULL DEFAULT 0"
                    )
                if "bought_at" not in dcols:
                    conn.execute("ALTER TABLE deals ADD COLUMN bought_at REAL")
                    # Best-effort seed: prefer created_at so unknown-block rows
                    # do not sort after later Blockscout hits with a real block.
                    conn.execute(
                        "UPDATE deals SET bought_at = created_at "
                        "WHERE bought_at IS NULL OR bought_at <= 0"
                    )
                if "token_name" not in dcols:
                    conn.execute(
                        "ALTER TABLE deals ADD COLUMN token_name TEXT NOT NULL DEFAULT ''"
                    )
                # Priority scheduler + zero-balance skip (durable across restarts).
                if "last_scanned_at" not in cols:
                    conn.execute(
                        "ALTER TABLE wallets ADD COLUMN last_scanned_at REAL"
                    )
                if "last_balance_check_at" not in cols:
                    conn.execute(
                        "ALTER TABLE wallets ADD COLUMN last_balance_check_at REAL"
                    )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS meta ("
                    "key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_wallets_scan "
                    "ON wallets(status, last_scanned_at)"
                )
                conn.commit()
            self._ensured = True
            self._refresh_bak()

    @staticmethod
    def _parse_alert_filters(raw: object) -> WalletAlertFilters:
        if raw is None or raw == "":
            return WalletAlertFilters()
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            return WalletAlertFilters.model_validate(data)
        except Exception:  # noqa: BLE001
            return WalletAlertFilters()

    @staticmethod
    def _dump_alert_filters(filters: WalletAlertFilters) -> str | None:
        if not filters.custom:
            return None
        return filters.model_dump_json()

    def set_wallet_alert_filters(
        self,
        addresses: list[str],
        filters: WalletAlertFilters,
    ) -> list[str]:
        """Set alert filters for wallets that exist. Returns updated addresses."""
        self._ensure()
        addrs = sorted({a.strip().lower() for a in addresses if a and a.strip()})
        if not addrs:
            return []
        payload = self._dump_alert_filters(filters)
        now = time.time()
        updated: list[str] = []
        with self._lock:
            with self._connect() as conn:
                for addr in addrs:
                    cur = conn.execute(
                        "UPDATE wallets SET alert_filters=?, updated_at=? WHERE address=?",
                        (payload, now, addr),
                    )
                    if cur.rowcount:
                        updated.append(addr)
                conn.commit()
        return updated

    def get_alert_filters_map(self, addresses: list[str]) -> dict[str, WalletAlertFilters]:
        self._ensure()
        addrs = [a.lower() for a in addresses if a]
        if not addrs:
            return {}
        out: dict[str, WalletAlertFilters] = {}
        with self._connect() as conn:
            # sqlite has a limit on variables; chunk if needed
            for i in range(0, len(addrs), 400):
                chunk = addrs[i : i + 400]
                placeholders = ",".join("?" * len(chunk))
                rows = conn.execute(
                    f"SELECT address, alert_filters FROM wallets WHERE address IN ({placeholders})",
                    chunk,
                ).fetchall()
                for r in rows:
                    out[r["address"]] = self._parse_alert_filters(r["alert_filters"])
        return out

    # --- config (JSON, same atomic pattern as watch) ---

    def load_config(self) -> FollowupConfig:
        path = self._config_path
        if not path.is_file():
            return FollowupConfig()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return FollowupConfig.model_validate(data)
        except Exception:  # noqa: BLE001
            return FollowupConfig()

    def save_config(self, cfg: FollowupConfig) -> FollowupConfig:
        path = self._config_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            cfg.model_dump_json(indent=2),
            encoding="utf-8",
        )
        tmp.replace(path)
        return cfg

    # --- wallets / deals ---

    @staticmethod
    def _renumber_deals(
        conn: sqlite3.Connection,
        wallet_l: str,
        *,
        max_deals: int,
        now: float | None = None,
    ) -> int:
        """Assign deal_index by buy time. Return count.

        Sort key is chain buy time (``bought_at``), then block, then token.
        Missing ``bought_at`` falls back to ``created_at`` — never push
        unknown-block rows after later known-block buys (that bug made a
        fresh Blockscout hit look like deal #2 ahead of the watch seed).
        """
        rows = conn.execute(
            "SELECT token FROM deals WHERE wallet=? "
            "ORDER BY COALESCE(NULLIF(bought_at, 0), created_at) ASC, "
            "CASE WHEN block_number IS NULL OR block_number <= 0 "
            "THEN 0 ELSE block_number END ASC, token ASC",
            (wallet_l,),
        ).fetchall()
        for i, row in enumerate(rows, 1):
            conn.execute(
                "UPDATE deals SET deal_index=? WHERE wallet=? AND token=?",
                (i, wallet_l, row["token"]),
            )
        n = len(rows)
        ts = time.time() if now is None else now
        status = "done" if n >= max_deals else "watching"
        # Keep tokens_traded_7d ≥ deal_count so unique cache seed / UI stay
        # coherent after a new deal (BS inbound unique often undercounts Relay).
        conn.execute(
            "UPDATE wallets SET deal_count=?, status=?, updated_at=?, "
            "tokens_traded_7d=CASE "
            "WHEN tokens_traded_7d IS NULL OR tokens_traded_7d < ? THEN ? "
            "ELSE tokens_traded_7d END "
            "WHERE address=?",
            (n, status, ts, n, n, wallet_l),
        )
        # Deal set changed — never let a stale unique=1 admit the next buy.
        FollowupStore._bust_unique_caches(wallet_l)
        return n

    @staticmethod
    def _bust_unique_caches(wallet: str) -> None:
        """Drop durable + in-memory unique counts after deal set changes."""
        wallet_l = (wallet or "").lower()
        if not wallet_l:
            return
        try:
            from . import wallet_unique_cache as uc

            uc.invalidate(wallet_l)
        except Exception:  # noqa: BLE001
            pass
        try:
            from . import wallet_metrics as wm

            dead = [
                k
                for k in list(wm._tokens7d_cache)
                if k.endswith(f":{wallet_l}")
            ]
            for k in dead:
                wm._tokens7d_cache.pop(k, None)
        except Exception:  # noqa: BLE001
            pass

    def ingest_buyers(
        self,
        buyers: list[BuyerRow],
        *,
        max_deals: int = 3,
        max_mcap_alert: float | None = None,
    ) -> list[FollowupDealRow]:
        """Insert deals for early buyers (skip if wallet+token exists).

        Returns newly inserted deal rows (for RayBot sync + optional alerts).
        """
        self._ensure()
        now = time.time()
        inserted: list[FollowupDealRow] = []
        with self._lock:
            with self._connect() as conn:
                for b in buyers:
                    wallet = b.wallet.lower()
                    token = b.token.lower()
                    if max_mcap_alert is not None and b.mcap_at_first_buy > max_mcap_alert:
                        continue
                    existing = conn.execute(
                        "SELECT 1 FROM deals WHERE wallet=? AND token=?",
                        (wallet, token),
                    ).fetchone()
                    if existing:
                        continue
                    wrow = conn.execute(
                        "SELECT deal_count, status FROM wallets WHERE address=?",
                        (wallet,),
                    ).fetchone()
                    seed_block = int(b.first_block or 0)
                    if wrow is None:
                        conn.execute(
                            "INSERT INTO wallets ("
                            "address, status, deal_count, wallet_balance_eth, "
                            "tokens_traded_7d, raybot_synced, first_token, first_mcap, "
                            "discovered_at, updated_at, last_seen_block"
                            ") VALUES (?, 'watching', 0, ?, ?, 0, ?, ?, ?, ?, ?)",
                            (
                                wallet,
                                b.wallet_balance_eth,
                                b.tokens_traded_7d,
                                token,
                                b.mcap_at_first_buy,
                                now,
                                now,
                                seed_block,
                            ),
                        )
                    else:
                        if wrow["status"] == "done":
                            continue
                        if int(wrow["deal_count"] or 0) >= max_deals:
                            continue
                        conn.execute(
                            "UPDATE wallets SET updated_at=?, "
                            "wallet_balance_eth=COALESCE(?, wallet_balance_eth), "
                            "tokens_traded_7d=COALESCE(?, tokens_traded_7d), "
                            "last_seen_block=MAX(last_seen_block, ?) "
                            "WHERE address=?",
                            (
                                now,
                                b.wallet_balance_eth,
                                b.tokens_traded_7d,
                                seed_block,
                                wallet,
                            ),
                        )
                    conn.execute(
                        "INSERT INTO deals ("
                        "wallet, token, token_symbol, deal_index, mcap_at_buy, "
                        "bought_usd, tx_hash, block_number, bought_at, notified, created_at"
                        ") VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, 0, ?)",
                        (
                            wallet,
                            token,
                            b.token_symbol or "",
                            b.mcap_at_first_buy,
                            b.bought_usd,
                            b.first_tx or "",
                            seed_block,
                            now,
                            now,
                        ),
                    )
                    self._renumber_deals(conn, wallet, max_deals=max_deals, now=now)
                    idx_row = conn.execute(
                        "SELECT deal_index FROM deals WHERE wallet=? AND token=?",
                        (wallet, token),
                    ).fetchone()
                    deal_index = int(idx_row["deal_index"]) if idx_row else 0
                    inserted.append(
                        FollowupDealRow(
                            wallet=wallet,
                            token=token,
                            token_symbol=b.token_symbol or "",
                            deal_index=deal_index,
                            mcap_at_buy=b.mcap_at_first_buy,
                            bought_usd=b.bought_usd,
                            tx_hash=b.first_tx or "",
                            block_number=seed_block,
                            bought_at=now,
                            notified=False,
                            created_at=now,
                        )
                    )
                conn.commit()
        return inserted

    def record_deal(
        self,
        *,
        wallet: str,
        token: str,
        token_symbol: str = "",
        token_name: str = "",
        mcap_at_buy: float | None,
        bought_usd: float | None = None,
        tx_hash: str = "",
        block_number: int = 0,
        bought_at: float | None = None,
        max_deals: int = 3,
    ) -> FollowupDealRow | None:
        """Record a new distinct-token deal. Returns row if inserted, else None.

        ``deal_index`` is assigned by buy time (``bought_at``) among this
        wallet's deals — not by insert time — so late-seen earlier buys
        renumber correctly.
        """
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        now = time.time()
        block = max(0, int(block_number or 0))
        buy_ts = float(bought_at) if bought_at and float(bought_at) > 0 else now
        with self._lock:
            with self._connect() as conn:
                if conn.execute(
                    "SELECT 1 FROM deals WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                ).fetchone():
                    return None
                wrow = conn.execute(
                    "SELECT deal_count, status FROM wallets WHERE address=?",
                    (wallet_l,),
                ).fetchone()
                if wrow is None:
                    return None
                if wrow["status"] != "watching":
                    return None
                if int(wrow["deal_count"] or 0) >= max_deals:
                    return None
                conn.execute(
                    "INSERT INTO deals ("
                    "wallet, token, token_symbol, token_name, deal_index, mcap_at_buy, "
                    "bought_usd, tx_hash, block_number, bought_at, notified, created_at"
                    ") VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0, ?)",
                    (
                        wallet_l,
                        token_l,
                        token_symbol,
                        token_name,
                        mcap_at_buy,
                        bought_usd,
                        tx_hash,
                        block,
                        buy_ts,
                        now,
                    ),
                )
                self._renumber_deals(conn, wallet_l, max_deals=max_deals, now=now)
                idx_row = conn.execute(
                    "SELECT deal_index, notified FROM deals WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                ).fetchone()
                if idx_row is None:
                    conn.commit()
                    return None
                deal_index = int(idx_row["deal_index"])
                conn.commit()
                return FollowupDealRow(
                    wallet=wallet_l,
                    token=token_l,
                    token_symbol=token_symbol,
                    token_name=token_name,
                    deal_index=deal_index,
                    mcap_at_buy=mcap_at_buy,
                    bought_usd=bought_usd,
                    tx_hash=tx_hash,
                    block_number=block,
                    bought_at=buy_ts,
                    notified=False,
                    created_at=now,
                )

    def delete_deal(
        self,
        wallet: str,
        token: str,
        *,
        max_deals: int = 5,
    ) -> bool:
        """Remove a deal (e.g. airdrop false positive) and renumber the rest."""
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM deals WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                )
                if cur.rowcount <= 0:
                    return False
                self._renumber_deals(
                    conn, wallet_l, max_deals=max_deals, now=time.time()
                )
                conn.commit()
                return True

    def set_deal_block(
        self,
        wallet: str,
        token: str,
        block_number: int,
        *,
        bought_at: float | None = None,
        max_deals: int = 5,
        renumber: bool = True,
    ) -> bool:
        """Update ``block_number`` / ``bought_at`` for a deal; optionally renumber."""
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        block = max(0, int(block_number or 0))
        with self._lock:
            with self._connect() as conn:
                if bought_at is not None and float(bought_at) > 0:
                    cur = conn.execute(
                        "UPDATE deals SET block_number=?, bought_at=? "
                        "WHERE wallet=? AND token=?",
                        (block, float(bought_at), wallet_l, token_l),
                    )
                else:
                    cur = conn.execute(
                        "UPDATE deals SET block_number=? WHERE wallet=? AND token=?",
                        (block, wallet_l, token_l),
                    )
                if cur.rowcount <= 0:
                    return False
                if renumber:
                    self._renumber_deals(
                        conn, wallet_l, max_deals=max_deals, now=time.time()
                    )
                conn.commit()
                return True

    def get_meta(self, key: str) -> str | None:
        self._ensure()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)
            ).fetchone()
        return str(row["value"]) if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._ensure()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES(?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, str(value)),
                )
                conn.commit()

    def get_logwatch_cursor(self) -> int | None:
        raw = self.get_meta("logwatch_cursor")
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def set_logwatch_cursor(self, block: int) -> None:
        self.set_meta("logwatch_cursor", str(max(0, int(block))))

    def get_logwatch_live_cursor(self) -> int | None:
        """Tip-priority scan cursor (independent of historical catch-up)."""
        raw = self.get_meta("logwatch_live_cursor")
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    def set_logwatch_live_cursor(self, block: int) -> None:
        self.set_meta("logwatch_live_cursor", str(max(0, int(block))))

    def list_deals_needing_chain_backfill(self) -> list[dict[str, Any]]:
        """Deals with a tx_hash but missing block and/or bought_at."""
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT wallet, token, tx_hash, block_number, bought_at, created_at "
                "FROM deals WHERE tx_hash IS NOT NULL AND tx_hash != '' "
                "AND (block_number IS NULL OR block_number <= 0 "
                "     OR bought_at IS NULL OR bought_at <= 0)"
            ).fetchall()
        return [dict(r) for r in rows]

    def apply_chain_backfill(
        self,
        updates: list[tuple[str, str, int, float]],
        *,
        max_deals: int = 5,
    ) -> int:
        """Apply (wallet, token, block_number, bought_at) and renumber wallets.

        Returns number of deal rows updated.
        """
        if not updates:
            return 0
        self._ensure()
        touched: set[str] = set()
        n = 0
        with self._lock:
            with self._connect() as conn:
                for wallet, token, block, bought_at in updates:
                    wallet_l = wallet.lower()
                    token_l = token.lower()
                    block_i = max(0, int(block or 0))
                    ts = float(bought_at) if bought_at and float(bought_at) > 0 else 0.0
                    if block_i <= 0 and ts <= 0:
                        continue
                    if block_i > 0 and ts > 0:
                        cur = conn.execute(
                            "UPDATE deals SET block_number=?, bought_at=?, "
                            "tx_hash=CASE "
                            "WHEN tx_hash NOT LIKE '0x%' AND tx_hash != '' "
                            "THEN '0x' || tx_hash ELSE tx_hash END "
                            "WHERE wallet=? AND token=?",
                            (block_i, ts, wallet_l, token_l),
                        )
                    elif block_i > 0:
                        cur = conn.execute(
                            "UPDATE deals SET block_number=?, "
                            "tx_hash=CASE "
                            "WHEN tx_hash NOT LIKE '0x%' AND tx_hash != '' "
                            "THEN '0x' || tx_hash ELSE tx_hash END "
                            "WHERE wallet=? AND token=?",
                            (block_i, wallet_l, token_l),
                        )
                    else:
                        cur = conn.execute(
                            "UPDATE deals SET bought_at=? WHERE wallet=? AND token=?",
                            (ts, wallet_l, token_l),
                        )
                    if cur.rowcount:
                        n += 1
                        touched.add(wallet_l)
                now = time.time()
                for wallet_l in sorted(touched):
                    self._renumber_deals(
                        conn, wallet_l, max_deals=max_deals, now=now
                    )
                conn.commit()
        return n

    def renumber_wallet(self, wallet: str, *, max_deals: int = 5) -> int:
        """Re-assign deal_index by block for one wallet. Returns deal count."""
        self._ensure()
        wallet_l = wallet.lower()
        with self._lock:
            with self._connect() as conn:
                n = self._renumber_deals(
                    conn, wallet_l, max_deals=max_deals, now=time.time()
                )
                conn.commit()
                return n

    def mark_notified(self, wallet: str, token: str, kind: str = "deal") -> bool:
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        "INSERT INTO alert_log (wallet, token, kind, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (wallet_l, token_l, kind, now),
                    )
                except sqlite3.IntegrityError:
                    return False
                conn.execute(
                    "UPDATE deals SET notified=1 WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                )
                conn.commit()
                return True

    def unmark_notified(self, wallet: str, token: str, kind: str = "deal") -> None:
        """Roll back a failed Telegram send so the deal can be retried."""
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM alert_log WHERE wallet=? AND token=? AND kind=?",
                    (wallet_l, token_l, kind),
                )
                conn.execute(
                    "UPDATE deals SET notified=0 WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                )
                conn.commit()

    def claim_and_enqueue_deal(
        self,
        wallet: str,
        token: str,
        *,
        dedup_key: str,
        payload: str,
        kind: str = "deal",
    ) -> bool:
        """Atomically claim a deal (notified=1) and durably enqueue its alert.

        Transactional outbox: the business write (notified) and the outbox row
        commit together, so a crash after this point still leaves a ``pending``
        outbox row that the dispatcher redelivers. Returns ``False`` when the
        deal was already claimed (idempotent).
        """
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        "INSERT INTO alert_log (wallet, token, kind, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (wallet_l, token_l, kind, now),
                    )
                except sqlite3.IntegrityError:
                    return False
                conn.execute(
                    "UPDATE deals SET notified=1 WHERE wallet=? AND token=?",
                    (wallet_l, token_l),
                )
                # INSERT OR IGNORE: a leftover row for the same key must not
                # break the claim; the dispatcher will still deliver it.
                conn.execute(
                    "INSERT OR IGNORE INTO alert_outbox "
                    "(dedup_key, kind, payload, status, attempts, "
                    "created_at, updated_at, next_attempt_at) "
                    "VALUES (?, ?, ?, 'pending', 0, ?, ?, 0)",
                    (dedup_key, kind, payload, now, now),
                )
                conn.commit()
                return True

    def enqueue_outbox(
        self,
        *,
        dedup_key: str,
        payload: str,
        kind: str = "ops",
    ) -> bool:
        """Enqueue a standalone alert (no deal claim). Idempotent on dedup_key."""
        self._ensure()
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO alert_outbox "
                    "(dedup_key, kind, payload, status, attempts, "
                    "created_at, updated_at, next_attempt_at) "
                    "VALUES (?, ?, ?, 'pending', 0, ?, ?, 0)",
                    (dedup_key, kind, payload, now, now),
                )
                conn.commit()
                return cur.rowcount > 0

    def list_due_outbox(self, *, now: float | None = None, limit: int = 25) -> list[dict]:
        """Pending outbox rows ready for (re)delivery, oldest first."""
        self._ensure()
        ts = float(now if now is not None else time.time())
        limit = max(1, min(int(limit), 500))
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, dedup_key, kind, payload, attempts "
                "FROM alert_outbox "
                "WHERE status='pending' AND next_attempt_at <= ? "
                "ORDER BY id ASC LIMIT ?",
                (ts, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_outbox_sent(self, outbox_id: int) -> None:
        self._ensure()
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE alert_outbox SET status='sent', sent_at=?, "
                    "updated_at=?, last_error=NULL WHERE id=?",
                    (now, now, int(outbox_id)),
                )
                conn.commit()

    def mark_outbox_failed(
        self,
        outbox_id: int,
        *,
        error: str,
        next_attempt_at: float,
        max_attempts: int = 10,
    ) -> None:
        """Bump attempts; keep ``pending`` until ``max_attempts``, then ``failed``."""
        self._ensure()
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT attempts FROM alert_outbox WHERE id=?",
                    (int(outbox_id),),
                ).fetchone()
                attempts = (int(row["attempts"]) if row else 0) + 1
                status = "failed" if attempts >= max(1, int(max_attempts)) else "pending"
                conn.execute(
                    "UPDATE alert_outbox SET attempts=?, status=?, "
                    "last_error=?, updated_at=?, next_attempt_at=? WHERE id=?",
                    (
                        attempts,
                        status,
                        (error or "")[:400],
                        now,
                        float(next_attempt_at),
                        int(outbox_id),
                    ),
                )
                conn.commit()

    def outbox_stats(self) -> dict[str, int]:
        """Counts by status for telemetry (pending / failed / sent)."""
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM alert_outbox GROUP BY status"
            ).fetchall()
        out = {"pending": 0, "failed": 0, "sent": 0}
        for r in rows:
            out[str(r["status"])] = int(r["n"])
        return out

    def prune_outbox(self, *, keep_sent_sec: float = 7 * 24 * 3600) -> int:
        """Drop old delivered rows so the table stays small."""
        self._ensure()
        cutoff = time.time() - float(keep_sent_sec)
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM alert_outbox WHERE status='sent' AND "
                    "COALESCE(sent_at, updated_at) < ?",
                    (cutoff,),
                )
                conn.commit()
                return cur.rowcount

    def list_pending_alert_deals(
        self,
        *,
        alert_on_deals: list[int] | None = None,
        limit: int = 50,
        max_age_sec: float | None = 48 * 3600,
        max_mcap_alert: float | None = None,
    ) -> list[FollowupDealRow]:
        """Deals that look alert-worthy but were never successfully notified.

        Includes ``mcap_at_buy IS NULL`` (quote was down at record time) so a
        later cycle can re-fetch mcap and still fire. Old backlog is capped by
        ``max_age_sec`` so a restart does not stampede Telegram/honeypot.
        High-mcap rows are excluded when ``max_mcap_alert`` is set.
        """
        self._ensure()
        indices = [int(i) for i in (alert_on_deals or [2, 3, 4, 5]) if int(i) >= 1]
        if not indices:
            return []
        limit = max(1, min(int(limit), 200))
        placeholders = ",".join("?" * len(indices))
        params: list[Any] = list(indices)
        age_sql = ""
        if max_age_sec is not None and float(max_age_sec) > 0:
            age_sql = (
                "AND COALESCE(NULLIF(d.bought_at, 0), d.created_at) "
                ">= ?"
            )
            params.append(time.time() - float(max_age_sec))
        mcap_sql = ""
        if max_mcap_alert is not None and float(max_mcap_alert) > 0:
            # Known-high mcap never alerts. Unknown mcap only if fresh (2h) so
            # we retry quote once soon after discovery, not forever.
            mcap_sql = (
                "AND ("
                "(d.mcap_at_buy IS NOT NULL AND d.mcap_at_buy <= ?) "
                "OR (d.mcap_at_buy IS NULL AND "
                "COALESCE(NULLIF(d.bought_at, 0), d.created_at) >= ?)"
                ")"
            )
            params.append(float(max_mcap_alert))
            params.append(time.time() - 2 * 3600)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT d.* FROM deals d "
                f"JOIN wallets w ON w.address = d.wallet "
                f"WHERE d.notified = 0 AND d.deal_index IN ({placeholders}) "
                f"AND w.status IN ('watching', 'done') "
                f"{age_sql} {mcap_sql} "
                f"ORDER BY COALESCE(NULLIF(d.bought_at, 0), d.created_at) DESC "
                f"LIMIT ?",
                params,
            ).fetchall()
        out: list[FollowupDealRow] = []
        for d in rows:
            out.append(
                FollowupDealRow(
                    wallet=d["wallet"],
                    token=d["token"],
                    token_symbol=d["token_symbol"] or "",
                    deal_index=int(d["deal_index"]),
                    mcap_at_buy=d["mcap_at_buy"],
                    bought_usd=d["bought_usd"],
                    tx_hash=d["tx_hash"] or "",
                    block_number=int(d["block_number"] or 0)
                    if "block_number" in d.keys()
                    else 0,
                    bought_at=(
                        float(d["bought_at"])
                        if "bought_at" in d.keys() and d["bought_at"] is not None
                        else None
                    ),
                    notified=bool(d["notified"]),
                    created_at=float(d["created_at"]),
                )
            )
        return out

    def update_deal_quote(
        self,
        wallet: str,
        token: str,
        *,
        mcap_at_buy: float | None = None,
        bought_usd: float | None = None,
        token_symbol: str | None = None,
        token_name: str | None = None,
    ) -> bool:
        """Fill missing mcap/bought_usd/symbol/name on an existing deal."""
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        sym = (token_symbol or "").strip()
        name = (token_name or "").strip()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "UPDATE deals SET "
                    "mcap_at_buy=CASE WHEN mcap_at_buy IS NULL AND ? IS NOT NULL "
                    "THEN ? ELSE mcap_at_buy END, "
                    "bought_usd=CASE WHEN bought_usd IS NULL AND ? IS NOT NULL "
                    "THEN ? ELSE bought_usd END, "
                    "token_symbol=CASE WHEN ?!='' AND (token_symbol IS NULL OR token_symbol='') "
                    "THEN ? ELSE token_symbol END, "
                    "token_name=CASE WHEN ?!='' AND (token_name IS NULL OR token_name='') "
                    "THEN ? ELSE token_name END "
                    "WHERE wallet=? AND token=?",
                    (
                        mcap_at_buy,
                        mcap_at_buy,
                        bought_usd,
                        bought_usd,
                        sym,
                        sym,
                        name,
                        name,
                        wallet_l,
                        token_l,
                    ),
                )
                conn.commit()
                return bool(cur.rowcount)

    def mark_raybot_synced(self, addresses: list[str], synced: bool = True) -> None:
        if not addresses:
            return
        self._ensure()
        flag = 1 if synced else 0
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                for addr in addresses:
                    conn.execute(
                        "UPDATE wallets SET raybot_synced=?, updated_at=? WHERE address=?",
                        (flag, now, addr.lower()),
                    )
                conn.commit()

    def delete_wallet(self, address: str) -> bool:
        """Remove wallet + its deals/alerts. Returns True if a row was deleted."""
        self._ensure()
        wallet_l = address.strip().lower()
        if not wallet_l:
            return False
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT 1 FROM wallets WHERE address=?", (wallet_l,)
                ).fetchone()
                if cur is None:
                    return False
                conn.execute("DELETE FROM deals WHERE wallet=?", (wallet_l,))
                conn.execute("DELETE FROM alert_log WHERE wallet=?", (wallet_l,))
                conn.execute("DELETE FROM wallets WHERE address=?", (wallet_l,))
                conn.commit()
                return True

    def list_for_ath_prune(self) -> list[dict]:
        """Watching + done wallets with deals for ATH prune (#1 / #2 / #3)."""
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT address, discovered_at, first_token, alert_filters, status, deal_count "
                "FROM wallets WHERE status IN ('watching', 'done') "
                "ORDER BY discovered_at"
            ).fetchall()
            if not rows:
                return []
            addrs = [r["address"] for r in rows]
            deals_by: dict[str, list[dict]] = {a: [] for a in addrs}
            for i in range(0, len(addrs), 400):
                chunk = addrs[i : i + 400]
                placeholders = ",".join("?" * len(chunk))
                drows = conn.execute(
                    f"SELECT wallet, token, deal_index, created_at, "
                    f"COALESCE(ath_passed, 0) AS ath_passed FROM deals "
                    f"WHERE wallet IN ({placeholders}) ORDER BY wallet, deal_index",
                    chunk,
                ).fetchall()
                for d in drows:
                    deals_by.setdefault(d["wallet"], []).append(
                        {
                            "token": str(d["token"] or "").lower(),
                            "deal_index": int(d["deal_index"]),
                            "created_at": float(d["created_at"] or 0),
                            "ath_passed": bool(d["ath_passed"]),
                        }
                    )
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "address": r["address"],
                    "discovered_at": float(r["discovered_at"] or 0),
                    "first_token": (r["first_token"] or "").lower(),
                    "alert_filters": self._parse_alert_filters(r["alert_filters"]),
                    "status": r["status"],
                    "deal_count": int(r["deal_count"] or 0),
                    "deals": deals_by.get(r["address"], []),
                }
            )
        return out

    def list_watching_for_prune(self) -> list[dict]:
        """Backward-compatible alias — prefer ``list_for_ath_prune``."""
        return [
            row
            for row in self.list_for_ath_prune()
            if row.get("status") == "watching"
        ]

    def mark_deals_ath_passed(self, pairs: list[tuple[str, str]]) -> int:
        """Mark (wallet, token) deals as ATH-passed so prune skips them next cycle."""
        if not pairs:
            return 0
        self._ensure()
        n = 0
        with self._lock:
            with self._connect() as conn:
                for wallet, token in pairs:
                    cur = conn.execute(
                        "UPDATE deals SET ath_passed=1 WHERE wallet=? AND token=? AND ath_passed=0",
                        (wallet.lower(), token.lower()),
                    )
                    n += int(cur.rowcount or 0)
                conn.commit()
        return n

    def first_token_for_wallet(self, wallet: str) -> str:
        self._ensure()
        wallet_l = wallet.lower()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT first_token FROM wallets WHERE address=?",
                (wallet_l,),
            ).fetchone()
            if row and row["first_token"]:
                return str(row["first_token"]).lower()
            d = conn.execute(
                "SELECT token FROM deals WHERE wallet=? AND deal_index=1 LIMIT 1",
                (wallet_l,),
            ).fetchone()
            return str(d["token"]).lower() if d else ""

    def list_watching(self) -> list[str]:
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT address FROM wallets WHERE status='watching' ORDER BY discovered_at"
            ).fetchall()
        return [r["address"] for r in rows]

    def list_watching_schedule_rows(self) -> list[dict[str, Any]]:
        """Watching wallets with scan/balance/activity fields for the scheduler."""
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT w.address, w.status, w.deal_count, w.discovered_at,
                       w.last_scanned_at, w.last_balance_check_at, w.wallet_balance_eth,
                       COALESCE(
                           (SELECT MAX(d.created_at) FROM deals d WHERE d.wallet = w.address),
                           w.discovered_at
                       ) AS last_activity_at
                FROM wallets w
                WHERE w.status = 'watching'
                ORDER BY w.discovered_at
                """
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "address": str(r["address"]),
                    "status": str(r["status"] or "watching"),
                    "deal_count": int(r["deal_count"] or 0),
                    "discovered_at": float(r["discovered_at"] or 0),
                    "last_activity_at": float(r["last_activity_at"] or 0),
                    "last_scanned_at": (
                        float(r["last_scanned_at"])
                        if r["last_scanned_at"] is not None
                        else None
                    ),
                    "last_balance_check_at": (
                        float(r["last_balance_check_at"])
                        if r["last_balance_check_at"] is not None
                        else None
                    ),
                    "wallet_balance_eth": (
                        float(r["wallet_balance_eth"])
                        if r["wallet_balance_eth"] is not None
                        else None
                    ),
                }
            )
        return out

    def mark_scanned(
        self,
        addresses: list[str],
        *,
        scanned_at: float | None = None,
    ) -> None:
        """Persist last_scanned_at for scheduler revisit timing."""
        addrs = sorted({a.strip().lower() for a in addresses if a and a.strip()})
        if not addrs:
            return
        self._ensure()
        ts = float(scanned_at if scanned_at is not None else time.time())
        with self._lock:
            with self._connect() as conn:
                for addr in addrs:
                    conn.execute(
                        "UPDATE wallets SET last_scanned_at=?, updated_at=? WHERE address=?",
                        (ts, ts, addr),
                    )
                conn.commit()

    def update_wallet_balances(
        self,
        balances: dict[str, float | None],
        *,
        checked_at: float | None = None,
    ) -> None:
        """Write refreshed native balances. ``None`` values are left unchanged."""
        if not balances:
            return
        self._ensure()
        ts = float(checked_at if checked_at is not None else time.time())
        with self._lock:
            with self._connect() as conn:
                for addr, bal in balances.items():
                    if bal is None:
                        continue
                    conn.execute(
                        "UPDATE wallets SET wallet_balance_eth=?, "
                        "last_balance_check_at=?, updated_at=? WHERE address=?",
                        (float(bal), ts, ts, addr.lower()),
                    )
                conn.commit()

    def get_wallet_scan_meta(self, wallet: str) -> tuple[int, int, str]:
        """Return (last_seen_block, deal_count, status)."""
        self._ensure()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_seen_block, deal_count, status FROM wallets WHERE address=?",
                (wallet.lower(),),
            ).fetchone()
        if not row:
            return 0, 0, ""
        return int(row["last_seen_block"] or 0), int(row["deal_count"] or 0), str(row["status"] or "")

    def advance_last_seen_block(self, wallet: str, block: int) -> None:
        if block <= 0:
            return
        self._ensure()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE wallets SET last_seen_block=MAX(last_seen_block, ?), "
                    "updated_at=? WHERE address=?",
                    (int(block), time.time(), wallet.lower()),
                )
                conn.commit()

    def known_tokens(self, wallet: str) -> set[str]:
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT token FROM deals WHERE wallet=?",
                (wallet.lower(),),
            ).fetchall()
        return {r["token"] for r in rows}

    def list_deals_for_wallet(self, wallet: str) -> list[dict[str, Any]]:
        """Deal rows for one wallet ordered by deal_index."""
        self._ensure()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT token, token_symbol, deal_index, block_number, bought_at, "
                "mcap_at_buy, tx_hash FROM deals WHERE wallet=? "
                "ORDER BY deal_index",
                (wallet.lower(),),
            ).fetchall()
        return [dict(r) for r in rows]

    def apply_gmgn_buy_order(
        self,
        wallet: str,
        post_seed_buys: list[dict[str, Any]],
        *,
        max_deals: int = 5,
    ) -> list[FollowupDealRow]:
        """Upsert post-seed GMGN buys with explicit follow-up indices.

        ``post_seed_buys`` is oldest→newest, excludes the seed token, and each
        item has token, symbol?, tx_hash?, block_number?, bought_at?, mcap_at_buy?,
        and bought_usd?.  The seed remains deal #1; these buys begin at deal #2.

        Any existing deal for this wallet that is not the seed and not in the
        capped GMGN prefix is deleted (stale Blockscout dust must not keep a
        colliding ``deal_index``).

        Returns only **newly inserted** deal rows (for Telegram alerts).
        """
        self._ensure()
        wallet_l = wallet.lower()
        limit = max(1, int(max_deals))
        if limit <= 1:
            return []
        # Be defensive: the GMGN client already returns unique tokens, but this
        # store boundary must never assign duplicate ranks if a caller regresses.
        unique: list[dict[str, Any]] = []
        seen_tokens: set[str] = set()
        for raw in post_seed_buys:
            token = str(raw.get("token") or "").strip().lower()
            if not token or token in seen_tokens:
                continue
            seen_tokens.add(token)
            unique.append(raw)
            if len(unique) >= limit - 1:
                break
        capped = unique
        if not capped:
            return []
        now = time.time()
        inserted: list[FollowupDealRow] = []
        with self._lock:
            with self._connect() as conn:
                wrow = conn.execute(
                    "SELECT status FROM wallets WHERE address=?",
                    (wallet_l,),
                ).fetchone()
                if wrow is None:
                    return []
                seed = conn.execute(
                    "SELECT token FROM deals WHERE wallet=? AND deal_index=1",
                    (wallet_l,),
                ).fetchone()
                if seed is None:
                    return []
                seed_token = str(seed["token"]).lower()
                # Seed + GMGN post-seed prefix are the only rows allowed to keep
                # a deal_index. Stale Blockscout dust/airdrops (e.g. CASHCAT)
                # previously stayed at an old rank and collided with GMGN #2.
                keep_tokens: set[str] = {seed_token}
                rank = 2
                for raw in capped:
                    token = str(raw.get("token") or "").lower()
                    if not token or token == seed_token:
                        continue
                    i = rank
                    rank += 1
                    keep_tokens.add(token)
                    sym = str(raw.get("symbol") or raw.get("token_symbol") or "")
                    tx = str(raw.get("tx_hash") or "")
                    block = max(0, int(raw.get("block_number") or 0))
                    mcap = raw.get("mcap_at_buy")
                    bought = raw.get("bought_usd")
                    raw_bought_at = raw.get("bought_at")
                    try:
                        buy_ts = (
                            float(raw_bought_at)
                            if raw_bought_at is not None and float(raw_bought_at) > 0
                            else 0.0
                        )
                    except (TypeError, ValueError):
                        buy_ts = 0.0
                    existing = conn.execute(
                        "SELECT deal_index, tx_hash, mcap_at_buy, bought_usd FROM deals "
                        "WHERE wallet=? AND token=?",
                        (wallet_l, token),
                    ).fetchone()
                    if existing:
                        conn.execute(
                            "UPDATE deals SET deal_index=?, "
                            "token_symbol=CASE WHEN ?!='' THEN ? ELSE token_symbol END, "
                            "tx_hash=CASE WHEN ?!='' THEN ? ELSE tx_hash END, "
                            "block_number=CASE WHEN ?>0 THEN ? ELSE block_number END, "
                            "bought_at=CASE WHEN ?>0 AND (bought_at IS NULL OR bought_at<=0) "
                            "THEN ? ELSE bought_at END, "
                            "mcap_at_buy=CASE WHEN mcap_at_buy IS NULL AND ? IS NOT NULL "
                            "THEN ? ELSE mcap_at_buy END, "
                            "bought_usd=CASE WHEN bought_usd IS NULL AND ? IS NOT NULL "
                            "THEN ? ELSE bought_usd END "
                            "WHERE wallet=? AND token=?",
                            (
                                i,
                                sym,
                                sym,
                                tx,
                                tx,
                                block,
                                block,
                                buy_ts,
                                buy_ts,
                                mcap,
                                mcap,
                                bought,
                                bought,
                                wallet_l,
                                token,
                            ),
                        )
                        continue
                    conn.execute(
                        "INSERT INTO deals ("
                        "wallet, token, token_symbol, deal_index, mcap_at_buy, "
                        "bought_usd, tx_hash, block_number, bought_at, notified, created_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                        (
                            wallet_l,
                            token,
                            sym,
                            i,
                            mcap,
                            bought,
                            tx,
                            block,
                            buy_ts if buy_ts > 0 else now,
                            now,
                        ),
                    )
                    inserted.append(
                        FollowupDealRow(
                            wallet=wallet_l,
                            token=token,
                            token_symbol=sym,
                            deal_index=i,
                            mcap_at_buy=float(mcap) if mcap is not None else None,
                            bought_usd=float(bought) if bought is not None else None,
                            tx_hash=tx,
                            block_number=block,
                            bought_at=buy_ts if buy_ts > 0 else now,
                            notified=False,
                            created_at=now,
                        )
                    )
                # Drop Blockscout-only / pre-GMGN ghosts so deal_index stays unique
                # and matches GMGN post-seed chronology (not discovery time).
                orphans = conn.execute(
                    "SELECT token FROM deals WHERE wallet=?",
                    (wallet_l,),
                ).fetchall()
                for row in orphans:
                    tok = str(row["token"]).lower()
                    if tok not in keep_tokens:
                        conn.execute(
                            "DELETE FROM deals WHERE wallet=? AND token=?",
                            (wallet_l, tok),
                        )
                # GMGN's post-seed order is authoritative for this sync.
                max_idx = rank - 1
                status = "done" if max_idx >= limit else "watching"
                conn.execute(
                    "UPDATE wallets SET deal_count=?, status=?, updated_at=? "
                    "WHERE address=?",
                    (max_idx, status, now, wallet_l),
                )
                conn.commit()
        return inserted

    def counts(self) -> tuple[int, int]:
        self._ensure()
        with self._connect() as conn:
            watching = conn.execute(
                "SELECT COUNT(*) AS c FROM wallets WHERE status='watching'"
            ).fetchone()["c"]
            done = conn.execute(
                "SELECT COUNT(*) AS c FROM wallets WHERE status='done'"
            ).fetchone()["c"]
        return int(watching), int(done)

    def reopen_under_max_deals(self, max_deals: int) -> int:
        """Move ``done`` wallets back to ``watching`` when max_deals was raised."""
        self._ensure()
        limit = max(1, int(max_deals))
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE wallets SET status='watching', updated_at=? "
                "WHERE status='done' AND deal_count < ?",
                (time.time(), limit),
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def list_wallets(
        self,
        *,
        status: str | None = None,
        limit: int = 200,
        offset: int = 0,
        include_deals: bool = True,
    ) -> list[FollowupWalletRow]:
        self._ensure()
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))
        with self._connect() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM wallets WHERE status=? "
                    "ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (status, limit, offset),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM wallets ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
            deals_by_wallet: dict[str, list[FollowupDealRow]] = {r["address"]: [] for r in rows}
            if include_deals and deals_by_wallet:
                addrs = list(deals_by_wallet.keys())
                for i in range(0, len(addrs), 400):
                    chunk = addrs[i : i + 400]
                    placeholders = ",".join("?" * len(chunk))
                    drows = conn.execute(
                        f"SELECT * FROM deals WHERE wallet IN ({placeholders}) "
                        "ORDER BY wallet, deal_index",
                        chunk,
                    ).fetchall()
                    for d in drows:
                        deals_by_wallet.setdefault(d["wallet"], []).append(
                            FollowupDealRow(
                                wallet=d["wallet"],
                                token=d["token"],
                                token_symbol=d["token_symbol"] or "",
                                deal_index=int(d["deal_index"]),
                                mcap_at_buy=d["mcap_at_buy"],
                                bought_usd=d["bought_usd"],
                                tx_hash=d["tx_hash"] or "",
                                block_number=int(d["block_number"] or 0)
                                if "block_number" in d.keys()
                                else 0,
                                bought_at=(
                                    float(d["bought_at"])
                                    if "bought_at" in d.keys()
                                    and d["bought_at"] is not None
                                    else None
                                ),
                                notified=bool(d["notified"]),
                                created_at=float(d["created_at"]),
                            )
                        )
            out: list[FollowupWalletRow] = []
            for r in rows:
                out.append(
                    FollowupWalletRow(
                        address=r["address"],
                        status=r["status"],
                        deal_count=int(r["deal_count"]),
                        wallet_balance_eth=r["wallet_balance_eth"],
                        tokens_traded_7d=r["tokens_traded_7d"],
                        raybot_synced=bool(r["raybot_synced"]),
                        first_token=r["first_token"] or "",
                        first_mcap=r["first_mcap"],
                        discovered_at=float(r["discovered_at"]),
                        updated_at=float(r["updated_at"]),
                        alert_filters=self._parse_alert_filters(
                            r["alert_filters"] if "alert_filters" in r.keys() else None
                        ),
                        deals=deals_by_wallet.get(r["address"], []),
                    )
                )
        return out


followup_store = FollowupStore()
