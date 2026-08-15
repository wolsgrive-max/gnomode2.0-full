"""Guards against unit-test fixtures leaking into production Telegram / ingest."""

from __future__ import annotations

import os
import re

# Classic pytest placeholders: 0xaaa…0001, 0xbbb…00bb — long zero runs in the body.
_ZERO_RUN = re.compile(r"0{20,}", re.IGNORECASE)
# Explicit well-known fixture prefixes used across backend/tests.
_FIXTURE_PREFIXES = (
    "0xaaa000",
    "0xbbb000",
    "0xccc000",
    "0xddd000",
    "0xeee000",
    "0xfff000",
    "0xseed0",
    "0xreal0",
)


def is_synthetic_evm_address(value: str | None) -> bool:
    """True for clearly synthetic / unit-test EVM addresses.

    Real vanity contracts may have short leading-zero runs (e.g. Permit2 has
    ~12); we require a long interior/leading zero run (≥20) or a known fixture
    prefix from our test suite.
    """
    raw = (value or "").strip().lower()
    if not raw:
        return False
    if not raw.startswith("0x"):
        raw = f"0x{raw}"
    body = raw[2:]
    if len(body) != 40 or any(c not in "0123456789abcdef" for c in body):
        return False
    if raw == "0x" + ("0" * 40):
        return True
    if any(raw.startswith(p) for p in _FIXTURE_PREFIXES):
        return True
    return _ZERO_RUN.search(body) is not None


def pytest_telegram_forbidden() -> bool:
    """True when running under pytest (env set by pytest itself)."""
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def refuse_telegram_if_unsafe(*, wallet: str = "", token: str = "") -> None:
    """Raise if this process must not hit live Telegram for this payload."""
    if pytest_telegram_forbidden():
        raise RuntimeError(
            "refusing Telegram send under pytest "
            "(set telegram_chat_id to a mock and patch send_* in tests)"
        )
    if is_synthetic_evm_address(wallet) or is_synthetic_evm_address(token):
        raise RuntimeError(
            f"refusing Telegram send for synthetic address "
            f"wallet={wallet[:14]!r} token={token[:14]!r}"
        )
