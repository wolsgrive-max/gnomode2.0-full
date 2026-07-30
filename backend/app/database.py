"""SQLite WAL store for migrated tokens, snipers, trades, blacklist."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import settings

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or settings.db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(
            str(self.path),
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS migrated_tokens (
                address TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                launchpad_id TEXT,
                dex TEXT,
                pool_id TEXT,
                curve_address TEXT,
                deploy_block INTEGER,
                migration_block INTEGER,
                migration_tx TEXT,
                honeypot INTEGER DEFAULT 0,
                start_mcap REAL,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS tracked_wallets (
                address TEXT PRIMARY KEY,
                first_seen TEXT,
                trade_count INTEGER DEFAULT 1,
                winrate REAL
            );
            CREATE TABLE IF NOT EXISTS wallet_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet TEXT NOT NULL,
                token TEXT NOT NULL,
                mcap_at_trade REAL,
                amount_usd REAL,
                tx_hash TEXT,
                block INTEGER,
                created_at TEXT,
                UNIQUE(wallet, token)
            );
            CREATE TABLE IF NOT EXISTS blacklist (
                address TEXT PRIMARY KEY,
                reason TEXT,
                source TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS token_mcap_tracker (
                address TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                launchpad_id TEXT,
                dex TEXT,
                pool_id TEXT,
                first_seen_mcap REAL,
                current_mcap REAL,
                peak_mcap REAL,
                last_checked_at TEXT,
                trend TEXT DEFAULT 'unknown',
                trend_since TEXT,
                added_at TEXT,
                target_reached_at TEXT
            );
            CREATE TABLE IF NOT EXISTS mcap_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                mcap REAL NOT NULL,
                price_usd REAL,
                liquidity_usd REAL,
                checked_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_wallet ON wallet_trades(wallet);
            CREATE INDEX IF NOT EXISTS idx_trades_token ON wallet_trades(token);
            CREATE INDEX IF NOT EXISTS idx_tokens_launchpad ON migrated_tokens(launchpad_id);
            CREATE INDEX IF NOT EXISTS idx_mcap_snapshots_addr_time
                ON mcap_snapshots(token_address, checked_at);
            CREATE TABLE IF NOT EXISTS user_filters (
                chat_id TEXT PRIMARY KEY,
                min_buy_usd REAL DEFAULT 50,
                max_mcap_usd REAL DEFAULT 150000,
                exclude_honeypots INTEGER DEFAULT 1,
                min_liq_usd REAL DEFAULT 0,
                max_liq_usd REAL DEFAULT 0,
                updated_at TEXT
            );
            """
        )
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Additive migrations for RayBot columns (safe on existing DBs)."""
        tw_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(tracked_wallets)")}
        alters: list[str] = []
        if "first_token" not in tw_cols:
            alters.append("ALTER TABLE tracked_wallets ADD COLUMN first_token TEXT")
        if "first_mcap" not in tw_cols:
            alters.append("ALTER TABLE tracked_wallets ADD COLUMN first_mcap REAL")
        if "is_active" not in tw_cols:
            alters.append(
                "ALTER TABLE tracked_wallets ADD COLUMN is_active INTEGER DEFAULT 1"
            )
        mt_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(migrated_tokens)")}
        if "liquidity_usd" not in mt_cols:
            alters.append("ALTER TABLE migrated_tokens ADD COLUMN liquidity_usd REAL")
        if "mcap_usd" not in mt_cols:
            alters.append("ALTER TABLE migrated_tokens ADD COLUMN mcap_usd REAL")
        wt_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(wallet_trades)")}
        if "trade_number" not in wt_cols:
            alters.append("ALTER TABLE wallet_trades ADD COLUMN trade_number INTEGER")
        for sql in alters:
            try:
                self._conn.execute(sql)
            except sqlite3.OperationalError as exc:
                logger.debug("schema migrate skip %s: %s", sql, exc)
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tokens_pool_id
            ON migrated_tokens(pool_id)
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_wallets_active
            ON tracked_wallets(is_active, trade_count)
            """
        )

    async def _run(self, fn):
        async with self._lock:
            return await asyncio.to_thread(fn)

    def insert_token(
        self,
        *,
        address: str,
        symbol: str = "",
        name: str = "",
        launchpad_id: str = "",
        dex: str = "",
        pool_id: str | None = None,
        curve_address: str | None = None,
        deploy_block: int | None = None,
        migration_block: int | None = None,
        migration_tx: str | None = None,
        honeypot: bool = False,
        start_mcap: float | None = None,
    ) -> None:
        addr = address.lower()
        self._conn.execute(
            """
            INSERT INTO migrated_tokens (
                address, symbol, name, launchpad_id, dex, pool_id, curve_address,
                deploy_block, migration_block, migration_tx, honeypot, start_mcap, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                symbol=COALESCE(excluded.symbol, migrated_tokens.symbol),
                name=COALESCE(excluded.name, migrated_tokens.name),
                launchpad_id=COALESCE(excluded.launchpad_id, migrated_tokens.launchpad_id),
                dex=COALESCE(excluded.dex, migrated_tokens.dex),
                pool_id=COALESCE(excluded.pool_id, migrated_tokens.pool_id),
                curve_address=COALESCE(excluded.curve_address, migrated_tokens.curve_address),
                deploy_block=COALESCE(excluded.deploy_block, migrated_tokens.deploy_block),
                migration_block=COALESCE(excluded.migration_block, migrated_tokens.migration_block),
                migration_tx=COALESCE(excluded.migration_tx, migrated_tokens.migration_tx),
                honeypot=excluded.honeypot,
                start_mcap=COALESCE(excluded.start_mcap, migrated_tokens.start_mcap)
            """,
            (
                addr,
                symbol,
                name,
                launchpad_id,
                dex,
                pool_id,
                curve_address.lower() if curve_address else None,
                deploy_block,
                migration_block,
                migration_tx,
                1 if honeypot else 0,
                start_mcap,
                _utc_now(),
            ),
        )

    async def ainsert_token(self, **kwargs: Any) -> None:
        await self._run(lambda: self.insert_token(**kwargs))

    def purge_non_graduated_tokens(self) -> int:
        """Remove listings that are not bonding-curve graduations (legacy junk)."""
        cur = self._conn.execute(
            """
            DELETE FROM migrated_tokens
            WHERE lower(COALESCE(launchpad_id, '')) NOT IN ('bags', 'hoodfun', 'flap')
               OR length(TRIM(COALESCE(symbol, ''))) = 0
               OR lower(TRIM(COALESCE(symbol, ''))) LIKE '0x%'
            """
        )
        self._conn.commit()
        return int(cur.rowcount or 0)

    async def apurge_non_graduated_tokens(self) -> int:
        return await self._run(self.purge_non_graduated_tokens)

    def get_token(self, address: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM migrated_tokens WHERE address = ?",
            (address.lower(),),
        ).fetchone()
        return dict(row) if row else None

    async def aget_token(self, address: str) -> dict[str, Any] | None:
        return await self._run(lambda: self.get_token(address))

    def insert_wallet(
        self,
        address: str,
        *,
        trade_count: int = 1,
        first_token: str | None = None,
        first_mcap: float | None = None,
        is_active: bool = True,
    ) -> None:
        addr = address.lower()
        self._conn.execute(
            """
            INSERT INTO tracked_wallets
                (address, first_seen, trade_count, winrate, first_token, first_mcap, is_active)
            VALUES (?, ?, ?, NULL, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                first_token=COALESCE(tracked_wallets.first_token, excluded.first_token),
                first_mcap=COALESCE(tracked_wallets.first_mcap, excluded.first_mcap),
                is_active=COALESCE(excluded.is_active, tracked_wallets.is_active)
            """,
            (
                addr,
                _utc_now(),
                trade_count,
                first_token.lower() if first_token else None,
                first_mcap,
                1 if is_active else 0,
            ),
        )

    async def ainsert_wallet(self, address: str, **kwargs: Any) -> None:
        await self._run(lambda: self.insert_wallet(address, **kwargs))

    def has_wallet_token_trade(self, wallet: str, token: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM wallet_trades WHERE wallet = ? AND token = ? LIMIT 1",
            (wallet.lower(), token.lower()),
        ).fetchone()
        return row is not None

    async def ahas_wallet_token_trade(self, wallet: str, token: str) -> bool:
        return await self._run(lambda: self.has_wallet_token_trade(wallet, token))

    def increment_trade_count(self, address: str) -> None:
        self._conn.execute(
            """
            UPDATE tracked_wallets
            SET trade_count = trade_count + 1
            WHERE address = ?
            """,
            (address.lower(),),
        )

    def insert_trade(
        self,
        *,
        wallet: str,
        token: str,
        mcap_at_trade: float | None = None,
        amount_usd: float | None = None,
        tx_hash: str | None = None,
        block: int | None = None,
    ) -> bool:
        """Insert trade. Returns True if new wallet+token pair (1 token = 1 trade)."""
        w, t = wallet.lower(), token.lower()
        if self.has_wallet_token_trade(w, t):
            return False
        self.insert_wallet(
            w,
            trade_count=1,
            first_token=t,
            first_mcap=mcap_at_trade,
        )
        # Provisional trade_number = current distinct tokens + 1
        n_before = self._conn.execute(
            "SELECT COUNT(*) FROM wallet_trades WHERE wallet = ?", (w,)
        ).fetchone()[0]
        trade_number = int(n_before) + 1
        cur = self._conn.execute(
            """
            INSERT OR IGNORE INTO wallet_trades
                (wallet, token, mcap_at_trade, amount_usd, tx_hash, block, created_at, trade_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (w, t, mcap_at_trade, amount_usd, tx_hash, block, _utc_now(), trade_number),
        )
        if cur.rowcount:
            n_trades = self._conn.execute(
                "SELECT COUNT(*) FROM wallet_trades WHERE wallet = ?",
                (w,),
            ).fetchone()[0]
            self._conn.execute(
                "UPDATE tracked_wallets SET trade_count = ? WHERE address = ?",
                (n_trades, w),
            )
            return True
        return False

    async def ainsert_trade(self, **kwargs: Any) -> bool:
        return await self._run(lambda: self.insert_trade(**kwargs))

    def get_wallet(self, address: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM tracked_wallets WHERE address = ?",
            (address.lower(),),
        ).fetchone()
        return dict(row) if row else None

    async def aget_wallet(self, address: str) -> dict[str, Any] | None:
        return await self._run(lambda: self.get_wallet(address))

    def get_active_wallets(self, *, max_trade_count: int | None = None) -> list[dict[str, Any]]:
        if max_trade_count is None:
            rows = self._conn.execute(
                """
                SELECT * FROM tracked_wallets
                WHERE COALESCE(is_active, 1) = 1
                ORDER BY trade_count DESC, first_seen ASC
                """
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM tracked_wallets
                WHERE COALESCE(is_active, 1) = 1
                  AND trade_count <= ?
                ORDER BY trade_count DESC, first_seen ASC
                """,
                (max_trade_count,),
            ).fetchall()
        return [dict(r) for r in rows]

    async def aget_active_wallets(self, **kwargs: Any) -> list[dict[str, Any]]:
        return await self._run(lambda: self.get_active_wallets(**kwargs))

    def deactivate_wallets_above_first_mcap(self, max_mcap: float) -> int:
        """Mark tracked wallets inactive when first_mcap exceeds the cap."""
        cur = self._conn.execute(
            """
            UPDATE tracked_wallets
            SET is_active = 0
            WHERE COALESCE(is_active, 1) = 1
              AND first_mcap IS NOT NULL
              AND first_mcap > ?
            """,
            (float(max_mcap),),
        )
        return int(cur.rowcount or 0)

    async def adeactivate_wallets_above_first_mcap(self, max_mcap: float) -> int:
        return await self._run(
            lambda: self.deactivate_wallets_above_first_mcap(max_mcap)
        )

    def get_token_by_pool_id(self, pool_id: str) -> dict[str, Any] | None:
        pid = pool_id.lower()
        if not pid.startswith("0x"):
            pid = "0x" + pid
        row = self._conn.execute(
            "SELECT * FROM migrated_tokens WHERE lower(pool_id) = ? LIMIT 1",
            (pid,),
        ).fetchone()
        return dict(row) if row else None

    async def aget_token_by_pool_id(self, pool_id: str) -> dict[str, Any] | None:
        return await self._run(lambda: self.get_token_by_pool_id(pool_id))

    def update_token_market(
        self,
        address: str,
        *,
        mcap_usd: float | None = None,
        liquidity_usd: float | None = None,
    ) -> None:
        self._conn.execute(
            """
            UPDATE migrated_tokens SET
                mcap_usd = COALESCE(?, mcap_usd),
                liquidity_usd = COALESCE(?, liquidity_usd)
            WHERE address = ?
            """,
            (mcap_usd, liquidity_usd, address.lower()),
        )

    async def aupdate_token_market(self, address: str, **kwargs: Any) -> None:
        await self._run(lambda: self.update_token_market(address, **kwargs))

    def get_snipers_by_trade_count(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT * FROM tracked_wallets
            ORDER BY trade_count DESC, first_seen ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    async def aget_snipers_by_trade_count(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return await self._run(lambda: self.get_snipers_by_trade_count(limit=limit))

    def get_user_filters(self, chat_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM user_filters WHERE chat_id = ?",
            (str(chat_id).strip(),),
        ).fetchone()
        return dict(row) if row else None

    async def aget_user_filters(self, chat_id: str) -> dict[str, Any] | None:
        return await self._run(lambda: self.get_user_filters(chat_id))

    def upsert_user_filters(
        self,
        chat_id: str,
        *,
        min_buy_usd: float | None = None,
        max_mcap_usd: float | None = None,
        exclude_honeypots: bool | None = None,
        min_liq_usd: float | None = None,
        max_liq_usd: float | None = None,
    ) -> dict[str, Any]:
        cid = str(chat_id).strip()
        cur = self.get_user_filters(cid) or {}
        from .config import settings as _settings

        vals = {
            "min_buy_usd": (
                float(min_buy_usd)
                if min_buy_usd is not None
                else float(cur.get("min_buy_usd") or _settings.sniper_default_min_buy_usd)
            ),
            "max_mcap_usd": (
                float(max_mcap_usd)
                if max_mcap_usd is not None
                else float(cur.get("max_mcap_usd") or _settings.sniper_default_max_mcap_usd)
            ),
            "exclude_honeypots": (
                1
                if (
                    exclude_honeypots
                    if exclude_honeypots is not None
                    else bool(cur.get("exclude_honeypots", 1))
                )
                else 0
            ),
            "min_liq_usd": (
                float(min_liq_usd)
                if min_liq_usd is not None
                else float(cur.get("min_liq_usd") or _settings.sniper_default_min_liq_usd)
            ),
            "max_liq_usd": (
                float(max_liq_usd)
                if max_liq_usd is not None
                else float(cur.get("max_liq_usd") or _settings.sniper_default_max_liq_usd)
            ),
        }
        self._conn.execute(
            """
            INSERT INTO user_filters (
                chat_id, min_buy_usd, max_mcap_usd, exclude_honeypots,
                min_liq_usd, max_liq_usd, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                min_buy_usd=excluded.min_buy_usd,
                max_mcap_usd=excluded.max_mcap_usd,
                exclude_honeypots=excluded.exclude_honeypots,
                min_liq_usd=excluded.min_liq_usd,
                max_liq_usd=excluded.max_liq_usd,
                updated_at=excluded.updated_at
            """,
            (
                cid,
                vals["min_buy_usd"],
                vals["max_mcap_usd"],
                vals["exclude_honeypots"],
                vals["min_liq_usd"],
                vals["max_liq_usd"],
                _utc_now(),
            ),
        )
        return self.get_user_filters(cid) or vals

    async def aupsert_user_filters(self, chat_id: str, **kwargs: Any) -> dict[str, Any]:
        return await self._run(lambda: self.upsert_user_filters(chat_id, **kwargs))

    def list_user_filter_chats(self) -> list[str]:
        rows = self._conn.execute("SELECT chat_id FROM user_filters").fetchall()
        return [str(r[0]) for r in rows]

    async def alist_user_filter_chats(self) -> list[str]:
        return await self._run(self.list_user_filter_chats)
    def get_top_tokens(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT * FROM migrated_tokens
            WHERE address NOT LIKE '0x00000000000000000000000000000000000000%'
            ORDER BY (migration_block IS NULL), migration_block DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    async def aget_top_tokens(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return await self._run(lambda: self.get_top_tokens(limit=limit))

    def get_trades(
        self,
        *,
        wallet: str | None = None,
        token: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if wallet:
            clauses.append("wallet = ?")
            params.append(wallet.lower())
        if token:
            clauses.append("token = ?")
            params.append(token.lower())
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM wallet_trades{where} ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [dict(r) for r in rows]

    async def aget_trades(self, **kwargs: Any) -> list[dict[str, Any]]:
        return await self._run(lambda: self.get_trades(**kwargs))

    def is_blacklisted(self, address: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM blacklist WHERE address = ? LIMIT 1",
            (address.lower(),),
        ).fetchone()
        return row is not None

    async def ais_blacklisted(self, address: str) -> bool:
        return await self._run(lambda: self.is_blacklisted(address))

    def add_blacklist(
        self, address: str, *, reason: str = "", source: str = "manual"
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO blacklist (address, reason, source, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                reason=excluded.reason,
                source=excluded.source
            """,
            (address.lower(), reason, source, _utc_now()),
        )

    async def aadd_blacklist(
        self, address: str, *, reason: str = "", source: str = "manual"
    ) -> None:
        await self._run(
            lambda: self.add_blacklist(address, reason=reason, source=source)
        )

    def remove_blacklist(self, address: str) -> None:
        self._conn.execute(
            "DELETE FROM blacklist WHERE address = ?", (address.lower(),)
        )

    async def aremove_blacklist(self, address: str) -> None:
        await self._run(lambda: self.remove_blacklist(address))

    def list_blacklist(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM blacklist ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    async def alist_blacklist(self) -> list[dict[str, Any]]:
        return await self._run(self.list_blacklist)

    def delete_token(self, address: str) -> None:
        self._conn.execute(
            "DELETE FROM migrated_tokens WHERE address = ?",
            (address.lower(),),
        )

    def purge_empty_tokens(self) -> int:
        """Remove rows with empty symbol+name (junk from earlier scans)."""
        cur = self._conn.execute(
            """
            DELETE FROM migrated_tokens
            WHERE (symbol IS NULL OR TRIM(symbol) = '')
              AND (name IS NULL OR TRIM(name) = '')
            """
        )
        return int(cur.rowcount or 0)

    async def apurge_empty_tokens(self) -> int:
        return await self._run(self.purge_empty_tokens)

    # ------------------------------------------------------------------ mcap tracker

    def insert_mcap_tracker(
        self,
        *,
        address: str,
        symbol: str = "",
        name: str = "",
        launchpad_id: str = "",
        dex: str = "",
        pool_id: str = "",
        first_seen_mcap: float = 0.0,
    ) -> None:
        now = _utc_now()
        self._conn.execute(
            """
            INSERT OR IGNORE INTO token_mcap_tracker (
                address, symbol, name, launchpad_id, dex, pool_id,
                first_seen_mcap, current_mcap, peak_mcap,
                last_checked_at, trend, trend_since, added_at, target_reached_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unknown', ?, ?, NULL)
            """,
            (
                address.lower(),
                symbol,
                name,
                launchpad_id,
                dex,
                pool_id or None,
                first_seen_mcap,
                first_seen_mcap,
                first_seen_mcap,
                now,
                now,
                now,
            ),
        )

    async def ainsert_mcap_tracker(self, **kwargs: Any) -> None:
        await self._run(lambda: self.insert_mcap_tracker(**kwargs))

    def update_mcap_tracker(
        self,
        address: str,
        *,
        current_mcap: float,
        peak_mcap: float | None = None,
        last_checked_at: str,
        trend: str,
        trend_since: str | None,
    ) -> None:
        if peak_mcap is None:
            self._conn.execute(
                """
                UPDATE token_mcap_tracker
                SET current_mcap = ?,
                    peak_mcap = MAX(COALESCE(peak_mcap, 0), ?),
                    last_checked_at = ?,
                    trend = ?,
                    trend_since = ?
                WHERE address = ?
                """,
                (
                    current_mcap,
                    current_mcap,
                    last_checked_at,
                    trend,
                    trend_since,
                    address.lower(),
                ),
            )
        else:
            self._conn.execute(
                """
                UPDATE token_mcap_tracker
                SET current_mcap = ?,
                    peak_mcap = ?,
                    last_checked_at = ?,
                    trend = ?,
                    trend_since = ?
                WHERE address = ?
                """,
                (
                    current_mcap,
                    peak_mcap,
                    last_checked_at,
                    trend,
                    trend_since,
                    address.lower(),
                ),
            )

    async def aupdate_mcap_tracker(self, address: str, **kwargs: Any) -> None:
        await self._run(lambda: self.update_mcap_tracker(address, **kwargs))

    def get_mcap_tracker_pending(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT * FROM token_mcap_tracker
            WHERE target_reached_at IS NULL
            ORDER BY
              CASE WHEN last_checked_at IS NULL THEN 0 ELSE 1 END,
              last_checked_at ASC,
              added_at DESC
        """
        if limit is not None and limit > 0:
            rows = self._conn.execute(sql + " LIMIT ?", (int(limit),)).fetchall()
        else:
            rows = self._conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    async def aget_mcap_tracker_pending(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        return await self._run(lambda: self.get_mcap_tracker_pending(limit=limit))

    def count_mcap_tracker_pending(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) FROM token_mcap_tracker WHERE target_reached_at IS NULL"
        ).fetchone()
        return int(row[0] if row else 0)

    async def acount_mcap_tracker_pending(self) -> int:
        return await self._run(self.count_mcap_tracker_pending)

    def apply_mcap_check_batch(
        self,
        updates: list[dict[str, Any]],
        *,
        since_iso: str,
        growth_pct: float,
        dead_pct: float,
    ) -> list[dict[str, Any]]:
        """Apply snapshot+tracker updates in one thread hop; return rows that hit target."""
        from .mcap_checker import _detect_trend

        hit_target: list[dict[str, Any]] = []
        for u in updates:
            addr = str(u["address"]).lower()
            mcap = float(u["mcap"])
            checked_at = str(u["checked_at"])
            self.insert_mcap_snapshot(
                token_address=addr,
                mcap=mcap,
                price_usd=u.get("price_usd"),
                liquidity_usd=u.get("liquidity_usd"),
                checked_at=checked_at,
            )
            snaps = self.get_mcap_snapshots(addr, since_iso=since_iso)
            trend = _detect_trend(snaps, growth_pct=growth_pct, dead_pct=dead_pct)
            trend_since = u.get("trend_since")
            if trend != u.get("prev_trend"):
                trend_since = checked_at
            self.update_mcap_tracker(
                addr,
                current_mcap=mcap,
                peak_mcap=float(u["peak_mcap"]),
                last_checked_at=checked_at,
                trend=trend,
                trend_since=trend_since,
            )
            if mcap >= float(u.get("target") or 0):
                hit_target.append({"address": addr, "mcap": mcap})
        return hit_target

    async def aapply_mcap_check_batch(
        self,
        updates: list[dict[str, Any]],
        *,
        since_iso: str,
        growth_pct: float,
        dead_pct: float,
    ) -> list[dict[str, Any]]:
        return await self._run(
            lambda: self.apply_mcap_check_batch(
                updates,
                since_iso=since_iso,
                growth_pct=growth_pct,
                dead_pct=dead_pct,
            )
        )

    def get_mcap_tracker_all(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT * FROM token_mcap_tracker
            ORDER BY
              CASE WHEN target_reached_at IS NULL THEN 0 ELSE 1 END,
              COALESCE(current_mcap, 0) DESC,
              added_at DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    async def aget_mcap_tracker_all(self) -> list[dict[str, Any]]:
        return await self._run(self.get_mcap_tracker_all)

    def get_mcap_tracker_one(self, address: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM token_mcap_tracker WHERE address = ?",
            (address.lower(),),
        ).fetchone()
        return dict(row) if row else None

    async def aget_mcap_tracker_one(self, address: str) -> dict[str, Any] | None:
        return await self._run(lambda: self.get_mcap_tracker_one(address))

    def insert_mcap_snapshot(
        self,
        *,
        token_address: str,
        mcap: float,
        price_usd: float | None = None,
        liquidity_usd: float | None = None,
        checked_at: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO mcap_snapshots (
                token_address, mcap, price_usd, liquidity_usd, checked_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (token_address.lower(), mcap, price_usd, liquidity_usd, checked_at),
        )

    async def ainsert_mcap_snapshot(self, **kwargs: Any) -> None:
        await self._run(lambda: self.insert_mcap_snapshot(**kwargs))

    def get_mcap_snapshots(
        self, token_address: str, since_iso: str
    ) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT * FROM mcap_snapshots
            WHERE token_address = ? AND checked_at >= ?
            ORDER BY checked_at ASC
            """,
            (token_address.lower(), since_iso),
        ).fetchall()
        return [dict(r) for r in rows]

    async def aget_mcap_snapshots(
        self, token_address: str, since_iso: str
    ) -> list[dict[str, Any]]:
        return await self._run(
            lambda: self.get_mcap_snapshots(token_address, since_iso)
        )

    def update_mcap_target_reached(
        self, address: str, target_reached_at: str
    ) -> None:
        self._conn.execute(
            """
            UPDATE token_mcap_tracker
            SET target_reached_at = ?
            WHERE address = ?
            """,
            (target_reached_at, address.lower()),
        )

    async def aupdate_mcap_target_reached(
        self, address: str, target_reached_at: str
    ) -> None:
        await self._run(
            lambda: self.update_mcap_target_reached(address, target_reached_at)
        )

    def delete_mcap_tracker(self, address: str) -> None:
        addr = address.lower()
        self._conn.execute(
            "DELETE FROM token_mcap_tracker WHERE address = ?", (addr,)
        )
        self._conn.execute(
            "DELETE FROM mcap_snapshots WHERE token_address = ?", (addr,)
        )

    async def adelete_mcap_tracker(self, address: str) -> None:
        await self._run(lambda: self.delete_mcap_tracker(address))

    def cleanup_mcap_tracker(
        self, *, max_age_days: int = 7, dead_hours: int = 24
    ) -> int:
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        max_age_cutoff = (now - timedelta(days=max_age_days)).isoformat()
        dead_cutoff = (now - timedelta(hours=dead_hours)).isoformat()
        cur = self._conn.execute(
            """
            DELETE FROM token_mcap_tracker
            WHERE target_reached_at IS NULL
              AND (
                (trend = 'dead' AND COALESCE(last_checked_at, added_at) < ?)
                OR (COALESCE(added_at, last_checked_at) < ?)
              )
            """,
            (dead_cutoff, max_age_cutoff),
        )
        deleted = int(cur.rowcount or 0)
        self._conn.execute(
            """
            DELETE FROM mcap_snapshots
            WHERE token_address NOT IN (SELECT address FROM token_mcap_tracker)
            """
        )
        return deleted

    async def acleanup_mcap_tracker(self, **kwargs: Any) -> int:
        return await self._run(lambda: self.cleanup_mcap_tracker(**kwargs))

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            pass


# Lazy singleton
_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db
