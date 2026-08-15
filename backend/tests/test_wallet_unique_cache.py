"""Durable unique-token SQLite cache: hit/miss/TTL / exact-only."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from app import wallet_unique_cache as uc


@pytest.fixture()
def cache_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "wallet_unique.db"
    monkeypatch.setattr(
        "app.wallet_unique_cache.settings.unique_cache_ttl_sec", 6 * 3600
    )
    uc.reset_for_tests(path)
    yield path
    uc.reset_for_tests(None)


def test_put_get_exact(cache_db: Path) -> None:
    assert uc.put_exact("0xAbc", 168, 2, exact=True) is True
    assert uc.get_exact("0xabc", 168) == 2


def test_refuse_inexact(cache_db: Path) -> None:
    assert uc.put_exact("0xabc", 168, 5, exact=False) is False
    assert uc.get_exact("0xabc", 168) is None


def test_ttl_expiry(cache_db: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.wallet_unique_cache.settings.unique_cache_ttl_sec", 10.0)
    now = 1_000_000.0
    uc.put_exact("0xabc", 168, 1, exact=True, now=now)
    assert uc.get_exact("0xabc", 168, now=now + 5) == 1
    assert uc.get_exact("0xabc", 168, now=now + 11) is None


def test_seed_from_followup_fresh_only(
    cache_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("app.wallet_unique_cache.settings.unique_cache_ttl_sec", 100.0)
    fu = tmp_path / "followup.db"
    now = time.time()
    conn = sqlite3.connect(str(fu))
    conn.executescript(
        """
        CREATE TABLE wallets (
            address TEXT PRIMARY KEY,
            tokens_traded_7d INTEGER,
            updated_at REAL NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO wallets VALUES(?,?,?)",
        ("0xfresh000000000000000000000000000000001", 5, now - 10),
    )
    conn.execute(
        "INSERT INTO wallets VALUES(?,?,?)",
        ("0xone00000000000000000000000000000000001", 1, now - 10),
    )
    conn.execute(
        "INSERT INTO wallets VALUES(?,?,?)",
        ("0xstale000000000000000000000000000000001", 5, now - 10_000),
    )
    conn.execute(
        "INSERT INTO wallets VALUES(?,?,?)",
        ("0xnull0000000000000000000000000000000001", None, now - 10),
    )
    conn.commit()
    conn.close()

    n = uc.seed_from_followup(followup_db=fu, period_hours=168, now=now, force=True)
    assert n == 1
    assert uc.get_exact("0xfresh000000000000000000000000000000001", 168, now=now) == 5
    # count=1 must not seed — stale one-deal poison
    assert uc.get_exact("0xone00000000000000000000000000000000001", 168, now=now) is None
    assert uc.get_exact("0xstale000000000000000000000000000000001", 168, now=now) is None


def test_invalidate(cache_db: Path) -> None:
    uc.put_exact("0xabc", 168, 1, exact=True)
    uc.put_exact("0xabc", 720, 2, exact=True)
    assert uc.invalidate("0xAbc") == 2
    assert uc.get_exact("0xabc", 168) is None
    assert uc.get_exact("0xabc", 720) is None
