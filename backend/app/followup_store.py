"""SQLite store for follow-up wallets (WAL, durable, lightweight)."""

from __future__ import annotations

import json
import sqlite3
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
    deal_index INTEGER NOT NULL,
    mcap_at_buy REAL,
    bought_usd REAL,
    tx_hash TEXT NOT NULL DEFAULT '',
    block_number INTEGER NOT NULL DEFAULT 0,
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
"""


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

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

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
                conn.commit()
            self._ensured = True

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
        """Assign deal_index by chain time (block, then created_at). Return count."""
        rows = conn.execute(
            "SELECT token FROM deals WHERE wallet=? "
            "ORDER BY CASE WHEN block_number IS NULL OR block_number <= 0 "
            "THEN 1 ELSE 0 END, block_number ASC, created_at ASC, token ASC",
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
        conn.execute(
            "UPDATE wallets SET deal_count=?, status=?, updated_at=? WHERE address=?",
            (n, status, ts, wallet_l),
        )
        return n

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
                        "bought_usd, tx_hash, block_number, notified, created_at"
                        ") VALUES (?, ?, ?, 0, ?, ?, ?, ?, 0, ?)",
                        (
                            wallet,
                            token,
                            b.token_symbol or "",
                            b.mcap_at_first_buy,
                            b.bought_usd,
                            b.first_tx or "",
                            seed_block,
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
        mcap_at_buy: float | None,
        bought_usd: float | None = None,
        tx_hash: str = "",
        block_number: int = 0,
        max_deals: int = 3,
    ) -> FollowupDealRow | None:
        """Record a new distinct-token deal. Returns row if inserted, else None.

        ``deal_index`` is assigned by on-chain block order among this wallet's
        deals — not by insert time — so late-seen earlier buys renumber correctly.
        """
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        now = time.time()
        block = max(0, int(block_number or 0))
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
                    "wallet, token, token_symbol, deal_index, mcap_at_buy, "
                    "bought_usd, tx_hash, block_number, notified, created_at"
                    ") VALUES (?, ?, ?, 0, ?, ?, ?, ?, 0, ?)",
                    (
                        wallet_l,
                        token_l,
                        token_symbol,
                        mcap_at_buy,
                        bought_usd,
                        tx_hash,
                        block,
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
                    deal_index=deal_index,
                    mcap_at_buy=mcap_at_buy,
                    bought_usd=bought_usd,
                    tx_hash=tx_hash,
                    block_number=block,
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
        max_deals: int = 5,
        renumber: bool = True,
    ) -> bool:
        """Update ``block_number`` for a deal; optionally renumber the wallet."""
        self._ensure()
        wallet_l = wallet.lower()
        token_l = token.lower()
        block = max(0, int(block_number or 0))
        with self._lock:
            with self._connect() as conn:
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
                "SELECT token, token_symbol, deal_index, block_number, "
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
        item has token, symbol?, tx_hash?, block_number?, mcap_at_buy?, and
        bought_usd?.  The seed remains deal #1; these buys begin at deal #2.

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
                        "bought_usd, tx_hash, block_number, notified, created_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
                        (
                            wallet_l,
                            token,
                            sym,
                            i,
                            mcap,
                            bought,
                            tx,
                            block,
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
