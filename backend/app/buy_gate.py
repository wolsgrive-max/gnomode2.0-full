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
        "claimdividend",
        "claimdividends",
        "claimreward",
        "distribute",
        "distributetoken",
        "droptokens",
    }
)

# 4-byte selectors for plain ERC-20 transfer / transferFrom (not DEX buys).
_NON_BUY_SELECTORS = frozenset(
    {
        "0xa9059cbb",  # transfer(address,uint256)
        "0x23b872dd",  # transferFrom(address,address,uint256)
    }
)
# Do NOT include bare ``launch`` — that is token create + creator firstBuy
# (selector 0x75154d70); GMGN does not list it as a buy (Хвать false positive).
_LAUNCH_BUY_METHODS = frozenset(
    {
        "launchtoken",
        "createlaunch",
        "createandlaunch",
        "launchandbuy",
        "buylaunch",
    }
)
# 4-byte selectors seen on Robinhood launch pads (Blockscout often returns hex).
_LAUNCH_BUY_SELECTORS = frozenset(
    {
        "0x686399cb",  # Pons launchToken((…),uint256,uint256,bytes32)
    }
)
# Creator pad ``launch(params, configId, firstBuyIn, …)`` — create, not a buy.
# Also ``newTokenV6`` factories: wallet is the token creator; GMGN may still
# label the embedded firstBuy as «Покупка» (Хвать must not ingest creators).
_CREATOR_LAUNCH_METHODS = frozenset({"launch", "newtokenv6"})
_CREATOR_LAUNCH_SELECTORS = frozenset(
    {
        "0x75154d70",  # GUH-style launch((…),uint256,uint256,uint256,bytes32)
        "0xbf388406",  # MemeLaunchV2 launch((string,string,…)); MemeCreatorInitialBuyV2
        "0x8cb5772c",  # newTokenV6((string,string,…)) — Prism-style create+firstBuy
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
        *_LAUNCH_BUY_METHODS,
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


def _method_norm(method: object) -> str:
    return str(method or "").strip().lower()


def _selector4(method: str) -> str | None:
    m = method.strip().lower()
    if m.startswith("0x") and len(m) >= 10:
        return m[:10]
    if len(m) == 8 and all(c in "0123456789abcdef" for c in m):
        return f"0x{m}"
    return None


def method_is_creator_launch(method: object) -> bool:
    """True for token-create ``launch`` (creator bag / embedded firstBuy).

    Example: GUH ``0xbead62e9…`` — wallet is TokenLaunched.creator; GMGN has
    no buy activity for that wallet.
    """
    m = _method_norm(method)
    if not m:
        return False
    if m in _CREATOR_LAUNCH_METHODS or m in _CREATOR_LAUNCH_SELECTORS:
        return True
    sel = _selector4(m)
    return sel is not None and sel in _CREATOR_LAUNCH_SELECTORS


def method_is_non_buy(method: object) -> bool:
    """True for disperse/airdrop/plain transfer — not a DEX swap."""
    m = _method_norm(method)
    if not m:
        return False
    # Creator create+launch is not a market buy (and not GMGN «Покупка»).
    if method_is_creator_launch(m):
        return True
    # Launch buys must never be treated as airdrops / mint noise.
    if method_is_launch_buy(m):
        return False
    sel = _selector4(m)
    if sel is not None and sel in _NON_BUY_SELECTORS:
        return True
    if m.startswith("disperse") or m.startswith("airdrop") or m.startswith("distribute"):
        return True
    # Reflection / staking claims (claimDividend, claimRewards, …) are not buys.
    # Bare prefix — avoids counting inbound claim credits toward unique-tokens.
    if m.startswith("claim"):
        return True
    return m in {x.lower() for x in _NON_BUY_METHODS}


def method_is_launch_buy(method: object) -> bool:
    """True for launch-pad first acquisition (e.g. Pons ``launchToken``)."""
    m = _method_norm(method)
    if not m:
        return False
    # Bare ``launch`` / creator selector — create, not a sniper buy.
    if method_is_creator_launch(m):
        return False
    if m in _LAUNCH_BUY_METHODS or m in _LAUNCH_BUY_SELECTORS:
        return True
    sel = _selector4(m)
    if sel is not None and sel in _LAUNCH_BUY_SELECTORS:
        return True
    # ``launchToken``, ``launchAndBuy``, … — not bare ``launch`` / ``launcher``.
    if m.startswith("launch") and m != "launch" and "claim" not in m:
        # Avoid ``launcher`` / ``launchpad`` noise: require buy-ish suffix or
        # a known compound already listed above.
        if m in _LAUNCH_BUY_METHODS:
            return True
        return "token" in m or "buy" in m or m.startswith("launchand")
    return False


def _method_looks_like_swap(method: object) -> bool:
    m = str(method or "").strip().lower()
    if not m:
        return False
    if m.startswith("0x"):
        return True  # selector — often router/pool swap
    if method_is_launch_buy(m):
        return True
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
    if hit:
        age = now - hit[0]
        # True: long TTL. False: short (indexer may still be catching up).
        # None: short negative cache for transport blips.
        ttl = 3600.0 if hit[1] is True else 45.0
        if age < ttl:
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
            if not isinstance(items, list) or not items:
                # Empty/odd 200 is often "not indexed yet", not confident False.
                spent = None
                break
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


def wallet_sent_quote_in_receipt(
    wallet: str,
    receipt: dict[str, Any] | None,
) -> bool | None:
    """On-chain quote spend from a tx receipt (no Blockscout).

    ``True`` / ``False`` when receipt logs are present; ``None`` if receipt
    missing/unusable. Prefer this over indexer for tip/pending classification.
    """
    if not isinstance(receipt, dict):
        return None
    logs = receipt.get("logs")
    if not isinstance(logs, list) or not logs:
        return None
    from .constants import TRANSFER_TOPIC

    wl = (wallet or "").strip().lower()
    if not wl.startswith("0x"):
        wl = "0x" + wl
    wl_topic = "0x" + ("0" * 24) + wl[2:].zfill(40)[-40:]
    xfer = TRANSFER_TOPIC.lower()
    saw_any_transfer = False
    for lg in logs:
        if not isinstance(lg, dict):
            continue
        topics = lg.get("topics") or []
        if not topics:
            continue
        t0 = str(topics[0] or "").lower()
        if t0 != xfer or len(topics) < 3:
            continue
        saw_any_transfer = True
        token = str(lg.get("address") or "").lower()
        if token not in QUOTE_TOKENS:
            continue
        frm = str(topics[1] or "").lower()
        if frm == wl_topic:
            return True
    # Receipt present with Transfer logs but wallet never sent quote.
    return False if saw_any_transfer else None


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
    # Blockscout could not resolve sender: optimistic keep when it looks like a
    # pool swap or a launch-pad allocation (GMGN «Покупка»).
    if method_is_launch_buy(item.get("method")):
        logger.debug(
            "buy-gate: sender unknown, accepting launch-buy transfer %s", tx[:14]
        )
        return True
    if _from_looks_like_pool(item.get("from")) and _method_looks_like_swap(
        item.get("method")
    ):
        logger.debug(
            "buy-gate: sender unknown, accepting pool-like transfer %s", tx[:14]
        )
        return True
    return False
