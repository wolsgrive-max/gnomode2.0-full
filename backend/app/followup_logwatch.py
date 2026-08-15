"""Event-driven follow-up deal discovery via eth_getLogs.

Instead of scanning 200+ wallets one-by-one through GMGN/Blockscout, poll
``Transfer(address,address,uint256)`` where ``to`` is any watching wallet.
One (or a few chunked) RPC calls cover the whole watchlist.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from .chain import RpcClient
from .constants import QUOTE_TOKENS, TRANSFER_TOPIC

logger = logging.getLogger(__name__)

# Soft cap on addresses OR'd into topics[2] per request.
# Alchemy and public RH RPC often 400 on ≥100 OR topics under load; 50 keeps
# each eth_getLogs small enough for tip + progressive catch-up.
TOPIC_WALLET_CHUNK = 50
_TOPIC_WALLET_CHUNK = TOPIC_WALLET_CHUNK  # backwards-compat alias


def topic_batch_count(n_wallets: int, *, chunk: int | None = None) -> int:
    """How many OR'd topic batches ``fetch_inbound_transfers`` will issue."""
    n = max(0, int(n_wallets or 0))
    if n <= 0:
        return 1
    size = max(1, int(chunk if chunk is not None else TOPIC_WALLET_CHUNK))
    return max(1, (n + size - 1) // size)


def topic_address(addr: str) -> str:
    """Left-pad a 20-byte address to a 32-byte topic."""
    a = (addr or "").strip().lower()
    if a.startswith("0x"):
        a = a[2:]
    return "0x" + ("0" * 24) + a.zfill(40)[-40:]


def parse_transfer_log(log: Any) -> tuple[str, str, str, str, int] | None:
    """Return (token, from, to, tx_hash, block) or None if not a std ERC-20 Transfer."""
    try:
        topics = list(log["topics"] if not isinstance(log, dict) else log.get("topics") or [])
    except Exception:  # noqa: BLE001
        return None
    if len(topics) < 3:
        return None

    def _hex(x: Any) -> str:
        if isinstance(x, (bytes, bytearray)):
            return "0x" + bytes(x).hex()
        s = str(x or "")
        return s if s.startswith("0x") else "0x" + s

    t0 = _hex(topics[0]).lower()
    if t0 != TRANSFER_TOPIC.lower():
        return None
    frm = "0x" + _hex(topics[1])[-40:].lower()
    to = "0x" + _hex(topics[2])[-40:].lower()
    try:
        token = str(
            log["address"] if not isinstance(log, dict) else log.get("address") or ""
        ).lower()
    except Exception:  # noqa: BLE001
        return None
    if not token.startswith("0x"):
        token = "0x" + token
    try:
        tx = _hex(
            log["transactionHash"]
            if not isinstance(log, dict)
            else log.get("transactionHash") or log.get("transaction_hash")
        ).lower()
    except Exception:  # noqa: BLE001
        tx = ""
    try:
        raw_block = (
            log["blockNumber"]
            if not isinstance(log, dict)
            else log.get("blockNumber") or log.get("block_number")
        )
        if isinstance(raw_block, int):
            block = int(raw_block)
        else:
            block = int(str(raw_block), 0)
    except Exception:  # noqa: BLE001
        block = 0
    if not token or not to or block <= 0:
        return None
    return token, frm, to, tx, block


@dataclass(frozen=True)
class InboundTransfer:
    wallet: str
    token: str
    sender: str
    tx_hash: str
    block_number: int
    bought_at: float


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


async def fetch_inbound_transfers(
    rpc: RpcClient,
    wallets: list[str],
    *,
    from_block: int,
    to_block: int,
    chunk_size: int = 50_000,
    soft_partial: bool = False,
    batch_timeout_sec: float | None = None,
    batch_parallel: int | None = None,
    deadline_mono: float | None = None,
    topic_wallet_chunk: int | None = None,
) -> list[InboundTransfer]:
    """Fetch ERC-20 Transfer logs where ``to`` ∈ ``wallets`` in ``[from, to]``.

    ``soft_partial``: under large watchlists tip getLogs often times out on one
    of N topic batches — merging successful batches still lets purchase alerts
    fire. Callers that need the incomplete flag should use
    ``fetch_inbound_transfers_result``.
    """
    transfers, _incomplete = await fetch_inbound_transfers_result(
        rpc,
        wallets,
        from_block=from_block,
        to_block=to_block,
        chunk_size=chunk_size,
        soft_partial=soft_partial,
        batch_timeout_sec=batch_timeout_sec,
        batch_parallel=batch_parallel,
        deadline_mono=deadline_mono,
        topic_wallet_chunk=topic_wallet_chunk,
    )
    return transfers


async def fetch_inbound_transfers_result(
    rpc: RpcClient,
    wallets: list[str],
    *,
    from_block: int,
    to_block: int,
    chunk_size: int = 50_000,
    soft_partial: bool = False,
    batch_timeout_sec: float | None = None,
    batch_parallel: int | None = None,
    deadline_mono: float | None = None,
    topic_wallet_chunk: int | None = None,
) -> tuple[list[InboundTransfer], bool]:
    """Like ``fetch_inbound_transfers`` but also returns incomplete flag.

    ``deadline_mono`` (``time.monotonic()``): under soft_partial, cancel pending
    topic batches at the deadline and return whatever succeeded — never discard
    early waves via an outer ``wait_for`` cancel.
    """
    import time as _time

    addrs = sorted({(w or "").strip().lower() for w in wallets if w and w.strip()})
    if not addrs or to_block < from_block:
        return [], False

    # Block timestamp cache for this pass (usually a handful of tip blocks).
    ts_cache: dict[int, float] = {}

    async def _ts(block: int) -> float:
        hit = ts_cache.get(block)
        if hit is not None:
            return hit
        try:
            header = await rpc._call(
                lambda b=block: rpc.w3.eth.get_block(b)
            )
            raw = header.get("timestamp") if isinstance(header, dict) else header["timestamp"]
            val = float(int(raw, 0) if isinstance(raw, str) else int(raw))
        except Exception as exc:  # noqa: BLE001
            logger.debug("block ts %s: %s", block, exc)
            val = 0.0
        ts_cache[block] = val
        return val

    # Topic OR batches in parallel. Tip soft_partial raises parallel so one
    # slow Alchemy batch cannot zero the whole tip window.
    wallet_chunk = max(
        20,
        int(
            topic_wallet_chunk
            if topic_wallet_chunk is not None
            else TOPIC_WALLET_CHUNK
        ),
    )
    batches = list(_chunked(addrs, wallet_chunk))
    parallel = max(1, int(batch_parallel or (4 if soft_partial else 2)))
    sem = asyncio.Semaphore(parallel)
    per_batch = batch_timeout_sec
    if per_batch is None and soft_partial:
        per_batch = 4.0

    async def _one_batch(batch: list[str]) -> list[Any]:
        topics: list[Any] = [
            TRANSFER_TOPIC,
            None,
            [topic_address(a) for a in batch],
        ]
        async with sem:
            try:
                coro = rpc.get_logs_chunked(
                    address=None,
                    topics=topics,
                    from_block=from_block,
                    to_block=to_block,
                    chunk_size=chunk_size,
                    parallel=2 if not soft_partial else 1,
                )
                if per_batch is not None and per_batch > 0:
                    return await asyncio.wait_for(coro, timeout=float(per_batch))
                return await coro
            except Exception as exc:  # noqa: BLE001
                from .chain import _redact_exc

                logger.warning(
                    "logwatch get_logs [%s,%s] wallets=%d soft=%s: %s",
                    from_block,
                    to_block,
                    len(batch),
                    soft_partial,
                    _redact_exc(exc),
                )
                raise

    tasks = [asyncio.create_task(_one_batch(b)) for b in batches]
    raw_logs: list[Any] = []
    failed = 0
    first_exc: BaseException | None = None
    pending: set[asyncio.Task[list[Any]]] = set(tasks)
    while pending:
        timeout: float | None = None
        if deadline_mono is not None:
            timeout = max(0.0, float(deadline_mono) - _time.monotonic())
            if timeout <= 0.0:
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                failed += len(pending)
                pending.clear()
                break
        done, pending = await asyncio.wait(
            pending,
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done and timeout is not None:
            # Deadline hit while waiting — keep completed logs, drop rest.
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            failed += len(pending)
            pending.clear()
            break
        for t in done:
            try:
                part = t.result()
            except BaseException as exc:  # noqa: BLE001
                failed += 1
                if first_exc is None:
                    first_exc = exc
                continue
            raw_logs.extend(part)

    if failed and not soft_partial:
        # Cancel siblings so their TimeoutError is not "never retrieved" and
        # zombie getLogs do not keep holding scoped RPC semaphores.
        if pending:
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            pending.clear()
        assert first_exc is not None
        raise first_exc
    incomplete = failed > 0

    watched = set(addrs)
    # First inbound per (wallet, token) in block order wins.
    best: dict[tuple[str, str], InboundTransfer] = {}
    # Under soft_partial / deadline, skip per-log getBlock (20s wall each) —
    # freshness uses block_number; bought_at backfills later if needed.
    skip_ts = soft_partial or deadline_mono is not None
    for lg in raw_logs:
        parsed = parse_transfer_log(lg)
        if parsed is None:
            continue
        token, frm, to, tx, block = parsed
        if to not in watched:
            continue
        if token in QUOTE_TOKENS:
            continue
        if not tx:
            continue
        key = (to, token)
        prev = best.get(key)
        if prev is not None and prev.block_number <= block:
            continue
        bought_at = 0.0 if skip_ts else await _ts(block)
        best[key] = InboundTransfer(
            wallet=to,
            token=token,
            sender=frm,
            tx_hash=tx,
            block_number=block,
            bought_at=bought_at,
        )

    return (
        sorted(best.values(), key=lambda x: (x.block_number, x.wallet, x.token)),
        incomplete,
    )


async def tx_senders(rpc: RpcClient, tx_hashes: list[str]) -> dict[str, str | None]:
    """Batch ``eth_getTransactionByHash`` → tx_hash → from (lower) or None."""
    meta = await tx_from_and_input(rpc, tx_hashes)
    return {k: v[0] for k, v in meta.items()}


async def tx_from_and_input(
    rpc: RpcClient, tx_hashes: list[str]
) -> dict[str, tuple[str | None, str | None]]:
    """Batch tx → (from_lower | None, input_hex | None)."""
    keys = []
    seen: set[str] = set()
    for h in tx_hashes:
        k = (h or "").strip().lower()
        if not k:
            continue
        if not k.startswith("0x"):
            k = "0x" + k
        if k in seen:
            continue
        seen.add(k)
        keys.append(k)
    out: dict[str, tuple[str | None, str | None]] = {k: (None, None) for k in keys}
    if not keys:
        return out

    for i in range(0, len(keys), 40):
        chunk = keys[i : i + 40]
        try:
            results = await rpc._jsonrpc_batch(
                [("eth_getTransactionByHash", [h]) for h in chunk]
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("tx_from_and_input batch failed: %s", exc)
            continue
        for h, res in zip(chunk, results):
            if not isinstance(res, dict):
                continue
            frm = str(res.get("from") or "").lower() or None
            raw_in = res.get("input") or res.get("data") or ""
            inp = str(raw_in).lower() if raw_in else None
            if inp and not inp.startswith("0x"):
                inp = "0x" + inp
            out[h] = (frm, inp)
    return out


async def backfill_deal_chain_times(
    rpc: RpcClient,
    rows: list[dict[str, Any]],
) -> list[tuple[str, str, int, float]]:
    """Resolve (wallet, token, block, bought_at) for deals missing chain time.

    Uses ``eth_getTransactionByHash`` then block headers. Normalises bare
    64-hex tx hashes (GMGN sometimes omits ``0x``).
    """
    if not rows:
        return []

    def _norm_tx(raw: str) -> str:
        t = (raw or "").strip().lower()
        if not t:
            return ""
        if not t.startswith("0x"):
            t = "0x" + t
        return t

    need_tx: list[str] = []
    plan: list[tuple[str, str, str, int]] = []  # wallet, token, tx, known_block
    for r in rows:
        wallet = str(r.get("wallet") or "").lower()
        token = str(r.get("token") or "").lower()
        tx = _norm_tx(str(r.get("tx_hash") or ""))
        block = int(r.get("block_number") or 0)
        if not wallet or not token or not tx:
            continue
        plan.append((wallet, token, tx, block))
        if block <= 0:
            need_tx.append(tx)

    tx_meta: dict[str, tuple[int, float]] = {}
    # hash → (block, none_ts_yet)
    for i in range(0, len(need_tx), 40):
        chunk = need_tx[i : i + 40]
        try:
            results = await rpc._jsonrpc_batch(
                [("eth_getTransactionByHash", [h]) for h in chunk]
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("backfill tx batch: %s", exc)
            continue
        for h, res in zip(chunk, results):
            if not isinstance(res, dict):
                continue
            raw_b = res.get("blockNumber")
            try:
                block = int(raw_b, 0) if isinstance(raw_b, str) else int(raw_b or 0)
            except (TypeError, ValueError):
                block = 0
            if block > 0:
                tx_meta[h] = (block, 0.0)

    blocks_needed: set[int] = set()
    for wallet, token, tx, known_block in plan:
        block = known_block if known_block > 0 else tx_meta.get(tx, (0, 0.0))[0]
        if block > 0:
            blocks_needed.add(block)

    ts_by_block: dict[int, float] = {}
    block_list = sorted(blocks_needed)
    for i in range(0, len(block_list), 40):
        chunk = block_list[i : i + 40]
        try:
            results = await rpc._jsonrpc_batch(
                [("eth_getBlockByNumber", [hex(b), False]) for b in chunk]
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("backfill block batch: %s", exc)
            continue
        for b, res in zip(chunk, results):
            if not isinstance(res, dict):
                continue
            raw_ts = res.get("timestamp")
            try:
                ts_by_block[b] = float(
                    int(raw_ts, 0) if isinstance(raw_ts, str) else int(raw_ts)
                )
            except (TypeError, ValueError):
                continue

    out: list[tuple[str, str, int, float]] = []
    for wallet, token, tx, known_block in plan:
        block = known_block if known_block > 0 else tx_meta.get(tx, (0, 0.0))[0]
        bought_at = ts_by_block.get(block, 0.0)
        if block <= 0 and bought_at <= 0:
            continue
        out.append((wallet, token, block, bought_at))
    return out
