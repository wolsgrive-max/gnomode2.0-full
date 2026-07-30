"""Follow tracked snipers for Trade #2 / #3 on NEW tokens (RayBot-style).

Monitors Uniswap V4 PoolManager Swap logs. When a tracked wallet buys a token
it has never traded before:
  - persist wallet_trades (1 token = 1 trade_count)
  - if trade_count ∈ [2, sniper_alert_max_trade] and filters pass → Telegram
  - high-mcap buys are still stored but alerts are suppressed
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .chain import RpcClient, checksum, topic_address
from .config import settings
from .constants import UNI_V4_POOL_MANAGER, V4_SWAP_TOPIC
from .database import get_db
from .goplus import check_token_security
from .mcap_checker import _fetch_mcap_batch
from .sniper_score import record_sniper_trade
from .telegram import resolve_chat_id, resolve_topic_id, send_message, telegram_configured

logger = logging.getLogger(__name__)


def _norm_topic(t: Any) -> str:
    if isinstance(t, (bytes, bytearray)):
        h = t.hex()
    else:
        h = str(t)
    if not h.startswith("0x"):
        h = "0x" + h
    return h.lower()


class SniperFollowRunner:
    def __init__(self) -> None:
        self._stop = False
        self._running = False
        self._last_block = 0
        self._wallet_cache: set[str] = set()
        self._wallet_cache_ts = 0.0
        self._last_message = ""
        self._alerts_sent = 0
        self._trades_seen = 0

    @property
    def status(self) -> dict[str, Any]:
        return {
            "enabled": settings.sniper_follow_enabled,
            "running": self._running,
            "last_block": self._last_block,
            "tracked_cached": len(self._wallet_cache),
            "trades_seen": self._trades_seen,
            "alerts_sent": self._alerts_sent,
            "last_message": self._last_message,
        }

    def stop(self) -> None:
        self._stop = True

    async def _refresh_wallets(self) -> set[str]:
        now = time.time()
        if self._wallet_cache and now - self._wallet_cache_ts < 30:
            return self._wallet_cache
        rows = await get_db().aget_active_wallets(
            max_trade_count=settings.sniper_alert_max_trade
        )
        # Also keep wallets past alert max for stats-only recording? Spec says
        # trade #2/#3 alerts; still record higher trades without alerting.
        all_active = await get_db().aget_active_wallets()
        self._wallet_cache = {r["address"].lower() for r in all_active}
        self._wallet_cache_ts = now
        del rows
        return self._wallet_cache

    async def _resolve_filters(self, chat_id: str) -> dict[str, Any]:
        db = get_db()
        row = await db.aget_user_filters(chat_id)
        if not row:
            row = await db.aupsert_user_filters(chat_id)
        return row

    async def _passes_filters(
        self,
        *,
        chat_id: str,
        token: str,
        mcap: float | None,
        amount_usd: float | None,
        liq: float | None,
        honeypot: bool,
    ) -> tuple[bool, str]:
        f = await self._resolve_filters(chat_id)
        min_buy = float(f.get("min_buy_usd") or 0)
        max_mcap = float(f.get("max_mcap_usd") or 0)
        min_liq = float(f.get("min_liq_usd") or 0)
        max_liq = float(f.get("max_liq_usd") or 0)
        exclude_hp = bool(f.get("exclude_honeypots", 1))

        if exclude_hp and honeypot:
            return False, "honeypot"
        if min_buy > 0 and amount_usd is not None and amount_usd > 0 and amount_usd < min_buy:
            return False, "min_buy"
        if max_mcap > 0 and mcap is not None and mcap > max_mcap:
            return False, "max_mcap"
        if min_liq > 0 and liq is not None and liq < min_liq:
            return False, "min_liq"
        if max_liq > 0 and liq is not None and liq > max_liq:
            return False, "max_liq"
        return True, "ok"

    async def _alert(
        self,
        *,
        wallet: str,
        token: str,
        trade_number: int,
        mcap: float | None,
        amount_usd: float | None,
        tx: str,
        symbol: str,
    ) -> None:
        chat = resolve_chat_id()
        if not telegram_configured(chat):
            return
        topic = resolve_topic_id()
        sym = symbol or "TOKEN"
        lines = [
            f"<b>🎯 Sniper Trade #{trade_number}</b>",
            f"<b>{sym}</b>",
            f"Token: <code>{token}</code>",
            f"Wallet: <code>{wallet}</code>",
            f"Mcap: ${_fmt(mcap)} · Buy: ${_fmt(amount_usd)}",
            f"Tx: <code>{tx[:18]}…</code>" if tx else "",
            f'<a href="https://gmgn.ai/robinhood/token/{token}">GMGN</a> · '
            f'<a href="https://gmgn.ai/robinhood/address/{wallet}">Wallet</a>',
        ]
        text = "\n".join(x for x in lines if x)
        await send_message(chat, text, topic_id=topic)
        self._alerts_sent += 1

    async def _handle_swap(
        self,
        *,
        sender: str,
        pool_id: str,
        tx: str,
        block: int,
        wallets: set[str],
    ) -> None:
        if sender not in wallets:
            return
        db = get_db()
        tok_row = await db.aget_token_by_pool_id(pool_id)
        if not tok_row:
            return
        token = str(tok_row["address"])
        if await db.ahas_wallet_token_trade(sender, token):
            return  # 1 token = 1 trade — DCA ignored

        mcap_map = await _fetch_mcap_batch([token])
        info = mcap_map.get(token.lower()) or {}
        mcap = float(info.get("mcap") or 0) or None
        liq = info.get("liquidity_usd")
        amount_usd = None  # V4 amount decode deferred; filter skips when unknown

        honeypot = bool(tok_row.get("honeypot"))
        if not honeypot:
            try:
                sec = await check_token_security(token)
                honeypot = bool(sec.blocked)
            except Exception:  # noqa: BLE001
                pass

        from .launchpads.types import SniperHit

        hit = SniperHit(
            wallet=checksum(sender),
            block=block,
            tx=tx,
            amount_usd=float(amount_usd or 0),
            mcap_at_trade=float(mcap or 0),
        )
        new_pair = await record_sniper_trade(
            sender, token, hit=hit, min_buy_usd=0.0
        )
        if not new_pair:
            return
        self._trades_seen += 1
        wallet_row = await db.aget_wallet(sender)
        trade_number = int((wallet_row or {}).get("trade_count") or 1)
        symbol = str(tok_row.get("symbol") or "")

        # Alert only for trade #2 .. max
        if trade_number < 2 or trade_number > settings.sniper_alert_max_trade:
            self._last_message = (
                f"Trade #{trade_number} stored (no alert) {sender[:10]}→{token[:10]}"
            )
            return

        chat = resolve_chat_id()
        ok, reason = await self._passes_filters(
            chat_id=chat,
            token=token,
            mcap=mcap,
            amount_usd=amount_usd,
            liq=float(liq) if liq is not None else None,
            honeypot=honeypot,
        )
        if not ok:
            self._last_message = (
                f"Trade #{trade_number} suppressed ({reason}) {token[:10]}"
            )
            return
        await self._alert(
            wallet=checksum(sender),
            token=checksum(token),
            trade_number=trade_number,
            mcap=mcap,
            amount_usd=amount_usd,
            tx=tx,
            symbol=symbol,
        )
        self._last_message = f"Alerted trade #{trade_number} on {symbol or token[:10]}"

    async def _poll_once(self, rpc: RpcClient) -> None:
        # Yield RPC to manual parse / watch autoparse.
        try:
            from .jobs import jobs
            from .watch import watch_runner

            if jobs.has_active() or bool(getattr(watch_runner, "running", False)):
                self._last_message = "Paused — watch/parse active"
                return
        except Exception:  # noqa: BLE001
            pass

        tip = await rpc.block_number()
        if self._last_block <= 0:
            self._last_block = max(1, tip - 200)
        if self._last_block >= tip:
            return
        wallets = await self._refresh_wallets()
        if not wallets:
            self._last_block = tip
            self._last_message = "No tracked wallets yet"
            return

        from_block = self._last_block + 1
        # Cap window to avoid huge catch-up spikes
        to_block = min(tip, from_block + 5_000)
        try:
            logs = await rpc.get_logs_chunked(
                address=UNI_V4_POOL_MANAGER,
                topics=[V4_SWAP_TOPIC],
                from_block=from_block,
                to_block=to_block,
                parallel=2,
                chunk_size=min(settings.log_chunk_size, 20_000),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("sniper follow getLogs failed: %s", exc)
            return

        for lg in logs:
            topics = lg.get("topics") or []
            if len(topics) < 3:
                continue
            pool_id = _norm_topic(topics[1])
            sender = topic_address(topics[2]).lower()
            tx = lg.get("transactionHash")
            tx_hex = tx.hex() if isinstance(tx, (bytes, bytearray)) else str(tx or "")
            if tx_hex and not tx_hex.startswith("0x"):
                tx_hex = "0x" + tx_hex
            try:
                await self._handle_swap(
                    sender=sender,
                    pool_id=pool_id,
                    tx=tx_hex,
                    block=int(lg["blockNumber"]),
                    wallets=wallets,
                )
            except Exception:  # noqa: BLE001
                logger.exception("sniper follow handle_swap failed")

        self._last_block = to_block

    async def run_loop(self) -> None:
        if not settings.sniper_follow_enabled:
            logger.info("Sniper follow disabled")
            return
        self._running = True
        self._stop = False
        rpc = RpcClient(concurrency=2)
        logger.info("Sniper follow loop started")
        while not self._stop:
            try:
                await self._poll_once(rpc)
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                logger.exception("Sniper follow poll error")
            await asyncio.sleep(max(3, settings.sniper_follow_interval_sec))
        self._running = False


def _fmt(n: float | None) -> str:
    if n is None:
        return "—"
    try:
        v = float(n)
        text = f"{v:,.2f}"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text
    except (TypeError, ValueError):
        return "—"


sniper_follow = SniperFollowRunner()
