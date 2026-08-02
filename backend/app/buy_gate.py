"""Shared gate: real DEX buys vs airdrops / gifts / third-party multicalls."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .constants import QUOTE_TOKENS

logger = logging.getLogger(__name__)

# Outer tx methods that deliver tokens without the wallet swapping.
_NON_BUY_METHODS = frozenset(
    {
        "dispersetoken",
        "disperseether",
        "disperse",
        "airdrop",
        "batchairdrop",
        "batchtransfer",
        "multisend",
        "multiSend",
        "transfer",
        "transferfrom",
        "mint",
        "claim",
        "claimairdrop",
        "claimrewards",
        "distribute",
        "distributetoken",
        "droptokens",
    }
)

_SWAP_LIKE_METHODS = frozenset(
    {
        "execute",
        "multicall",
        "exactinput",
        "exactinputsingle",
        "exactoutput",
        "exactoutputsingle",
        "swap",
        "swapexacttokensfortokens",
        "swapexactethfortokens",
        "swaptokensforexacttokens",
    }
)

# tx_hash -> (fetched_at, sender_lower | None). None sender = miss, short TTL.
_TX_SENDER_CACHE: dict[str, tuple[float, str | None]] = {}
_TX_SENDER_OK_TTL = 3600.0
_TX_SENDER_MISS_TTL = 45.0
_TX_SENDER_CACHE_MAX = 4000

# tx_hash|wallet -> (fetched_at, spent_quote | None)
_QUOTE_SPEND_CACHE: dict[str, tuple[float, bool | None]] = {}


def _addr_hash(node: object) -> str:
    if isinstance(node, dict):
        return str(node.get("hash") or node.get("address_hash") or "").lower()
    return str(node or "").lower()


def _is_contract(node: object) -> bool:
    return isinstance(node, dict) and bool(node.get("is_contract"))


def method_is_non_buy(method: object) -> bool:
    """True for disperse/airdrop/plain transfer — not a DEX swap."""
    m = str(method or "").strip().lower()
    if not m:
        return False
    if m.startswith("disperse") or m.startswith("airdrop") or m.startswith("distribute"):
        return True
    return m in {x.lower() for x in _NON_BUY_METHODS}


def _method_looks_like_swap(method: object) -> bool:
    m = str(method or "").strip().lower()
    if not m:
        return False
    if m.startswith("0x"):
        return True  # selector — often router/pool swap
    if m in _SWAP_LIKE_METHODS:
        return True
    return any(k in m for k in ("swap", "exactinput", "exactoutput", "execute"))


def _from_looks_like_pool(frm: object) -> bool:
    if not isinstance(frm, dict):
        return False
    name = str(frm.get("name") or "").lower()
    return "uniswap" in name or "pool" in name or "pair" in name


def is_dex_buy_transfer(item: dict[str, Any], wallet: str) -> bool:
    """Fast sync pre-filter: inbound from contract, not an airdrop method."""
    wallet_l = wallet.lower()
    if _addr_hash(item.get("to")) != wallet_l:
        return False
    if not _is_contract(item.get("from")):
        return False
    if method_is_non_buy(item.get("method")):
        return False
    return True


async def transaction_sender(tx_hash: str) -> str | None:
    """Blockscout ``tx.from`` (lowercase), cached. ``None`` on failure (short TTL)."""
    key = str(tx_hash or "").strip().lower()
    if not key:
        return None
    now = time.time()
    hit = _TX_SENDER_CACHE.get(key)
    if hit:
        ts, sender = hit
        ttl = _TX_SENDER_OK_TTL if sender else _TX_SENDER_MISS_TTL
        if now - ts < ttl:
            return sender

    from .blockscout import _get_json

    sender: str | None = None
    delay = 0.4
    for attempt in range(4):
        got = await _get_json(f"/transactions/{key}")
        if got and got[0] == 200 and isinstance(got[1], dict):
            sender = _addr_hash(got[1].get("from")) or None
            if sender == "":
                sender = None
            break
        status = got[0] if got else None
        if status in (429, 502, 503) or got is None:
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 6.0)
            continue
        # 404 / hard error — no point retrying
        break

    _TX_SENDER_CACHE[key] = (time.time(), sender)
    if len(_TX_SENDER_CACHE) > _TX_SENDER_CACHE_MAX:
        oldest = sorted(_TX_SENDER_CACHE.items(), key=lambda kv: kv[1][0])
        for k, _ in oldest[: len(oldest) // 2]:
            _TX_SENDER_CACHE.pop(k, None)
    return sender


async def wallet_sent_quote_in_tx(wallet: str, tx_hash: str) -> bool | None:
    """Whether ``wallet`` sent WETH/USDG in ``tx``. ``None`` = lookup failed."""
    tx = str(tx_hash or "").strip().lower()
    wl = wallet.lower()
    if not tx:
        return None
    key = f"{tx}|{wl}"
    now = time.time()
    hit = _QUOTE_SPEND_CACHE.get(key)
    if hit and now - hit[0] < (3600.0 if hit[1] is not None else 45.0):
        return hit[1]

    from .blockscout import _get_json

    spent: bool | None = None
    delay = 0.4
    for _attempt in range(3):
        got = await _get_json(
            f"/transactions/{tx}/token-transfers",
            params={"type": "ERC-20"},
        )
        if got and got[0] == 200:
            data = got[1]
            items = data.get("items") if isinstance(data, dict) else data
            if not isinstance(items, list):
                items = []
            spent = False
            for tr in items:
                tok = tr.get("token") or {}
                addr = str(tok.get("address") or tok.get("address_hash") or "").lower()
                if addr not in QUOTE_TOKENS:
                    continue
                if _addr_hash(tr.get("from")) == wl:
                    spent = True
                    break
            break
        status = got[0] if got else None
        if status in (429, 502, 503) or got is None:
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 5.0)
            continue
        break

    _QUOTE_SPEND_CACHE[key] = (time.time(), spent)
    return spent


async def is_wallet_initiated_buy(item: dict[str, Any], wallet: str) -> bool:
    """True when this wallet (or its spend) initiated the swap that delivered the token."""
    if not is_dex_buy_transfer(item, wallet):
        return False
    tx = str(item.get("transaction_hash") or item.get("tx_hash") or "")
    if not tx:
        return False
    wallet_l = wallet.lower()
    sender = await transaction_sender(tx)
    if sender == wallet_l:
        return True
    if sender:
        # Someone else submitted the tx — only count if this wallet spent quote
        # in the same tx (smart-wallet / router-on-behalf patterns).
        spent = await wallet_sent_quote_in_tx(wallet_l, tx)
        return spent is True
    # Blockscout could not resolve sender: optimistic keep when it looks like a pool swap.
    if _from_looks_like_pool(item.get("from")) and _method_looks_like_swap(
        item.get("method")
    ):
        logger.debug(
            "buy-gate: sender unknown, accepting pool-like transfer %s", tx[:14]
        )
        return True
    return False
