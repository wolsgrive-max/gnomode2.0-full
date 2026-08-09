"""followup.db corruption detection: rotate, restore .bak, header repair."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from app.followup_store import FollowupStore, _try_repair_overwritten_header


def _seed_store(db: Path, cfg: Path, address: str = "0xaaa0000000000000000000000000000000000001") -> FollowupStore:
    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    store._ensure()
    now = 1_700_000_000.0
    with store._connect() as conn:
        conn.execute(
            "INSERT INTO wallets(address, status, deal_count, discovered_at, updated_at) "
            "VALUES (?, 'watching', 0, ?, ?)",
            (address, now, now),
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    store._refresh_bak()
    return store


def test_empty_db_restores_from_bak(tmp_path: Path) -> None:
    db = tmp_path / "followup.db"
    cfg = tmp_path / "followup.json"
    _seed_store(db, cfg)
    assert db.with_suffix(".db.bak").exists() or (tmp_path / "followup.db.bak").exists()
    bak = Path(str(db) + ".bak")
    assert bak.is_file()

    db.write_bytes(b"")
    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    store._ensure()
    rows = store.list_wallets()
    assert any(w.address.startswith("0xaaa") for w in rows)
    empties = list(tmp_path.glob("followup.db.empty-*"))
    assert empties, "empty file should be rotated aside"


def test_garbage_db_restores_from_bak_without_clobber(tmp_path: Path) -> None:
    db = tmp_path / "followup.db"
    cfg = tmp_path / "followup.json"
    _seed_store(db, cfg, address="0xbbb0000000000000000000000000000000000002")
    bak = Path(str(db) + ".bak")
    assert bak.is_file()

    db.write_bytes(b"\x17\x03\x03not-a-database" + b"\x00" * 200)
    for side in (Path(str(db) + "-wal"), Path(str(db) + "-shm")):
        if side.exists():
            side.unlink()
    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    store._ensure()
    rows = store.list_wallets()
    assert any(w.address.startswith("0xbbb") for w in rows)
    corrupt = [
        p
        for p in tmp_path.glob("followup.db.corrupt-*")
        if not str(p).endswith(("-wal", "-shm"))
    ]
    assert corrupt, "garbage file must be rotated, not overwritten in place"
    assert b"not-a-database" in corrupt[0].read_bytes()
    assert db.read_bytes()[:15] == b"SQLite format 3"


def test_garbage_without_bak_creates_fresh_after_rotate(tmp_path: Path) -> None:
    db = tmp_path / "followup.db"
    cfg = tmp_path / "followup.json"
    db.write_bytes(b"definitely-not-sqlite-payload" * 40)
    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    store._ensure()
    assert store.list_wallets() == []
    assert list(tmp_path.glob("followup.db.corrupt-*"))
    assert db.is_file() and db.read_bytes()[:15] == b"SQLite format 3"


def test_header_repair_recovers_wallets(tmp_path: Path) -> None:
    db = tmp_path / "followup.db"
    cfg = tmp_path / "followup.json"
    addr = "0xccc0000000000000000000000000000000000003"
    _seed_store(db, cfg, address=addr)
    # Drop bak so repair path is exercised (not bak restore).
    bak = Path(str(db) + ".bak")
    if bak.exists():
        bak.unlink()

    raw = bytearray(db.read_bytes())
    page_size = 4096
    # Pad to full pages if needed.
    if len(raw) % page_size:
        raw.extend(b"\x00" * (page_size - (len(raw) % page_size)))
    # Overwrite header like production incident (TLS-looking garbage).
    garbage = bytes.fromhex("1703030013380703be4d3ee892e592d5") + b"\x00" * 84
    raw[0:100] = garbage[:100]
    db.write_bytes(raw)
    assert db.read_bytes()[:15] != b"SQLite format 3"

    repaired = _try_repair_overwritten_header(db)
    assert repaired is not None
    with sqlite3.connect(str(repaired)) as conn:
        n = conn.execute("SELECT COUNT(*) FROM wallets WHERE address=?", (addr,)).fetchone()[0]
        assert n == 1
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    store._ensure()
    rows = store.list_wallets()
    assert any(w.address == addr for w in rows)
    assert list(tmp_path.glob("followup.db.corrupt-*"))


def test_healthy_db_still_opens(tmp_path: Path) -> None:
    db = tmp_path / "followup.db"
    cfg = tmp_path / "followup.json"
    _seed_store(db, cfg)
    store = FollowupStore(db_path=str(db), config_path=str(cfg))
    assert len(store.list_wallets()) >= 1
