"""RayBot-style Telegram bot: /filters inline keyboard + sniper settings."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .config import settings
from .database import get_db
from .telegram import (
    _TG_CLIENT_KW,
    _bot_api_url,
    _raise_for_telegram_response,
    bot_token,
    resolve_chat_id,
    send_message,
    telegram_configured,
)

logger = logging.getLogger(__name__)

_MIN_BUY_STEPS = (0, 25, 50, 100, 250, 500)
_MAX_MCAP_STEPS = (50_000, 100_000, 150_000, 250_000, 500_000, 1_000_000)
_LIQ_STEPS = (0, 1_000, 5_000, 10_000, 25_000, 50_000)


def _fmt_usd(n: float) -> str:
    if n >= 1_000_000:
        return f"${n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n/1_000:.0f}k"
    return f"${n:.0f}"


def filters_keyboard(f: dict[str, Any]) -> dict[str, Any]:
    min_buy = float(f.get("min_buy_usd") or 0)
    max_mcap = float(f.get("max_mcap_usd") or 0)
    hp = bool(f.get("exclude_honeypots", 1))
    min_liq = float(f.get("min_liq_usd") or 0)
    max_liq = float(f.get("max_liq_usd") or 0)
    return {
        "inline_keyboard": [
            [{"text": f"Min buy: {_fmt_usd(min_buy)}", "callback_data": "sf:noop"}],
            [
                {"text": "−", "callback_data": "sf:min_buy:-"},
                {"text": "+", "callback_data": "sf:min_buy:+"},
            ],
            [{"text": f"Max mcap: {_fmt_usd(max_mcap)}", "callback_data": "sf:noop"}],
            [
                {"text": "−", "callback_data": "sf:max_mcap:-"},
                {"text": "+", "callback_data": "sf:max_mcap:+"},
            ],
            [
                {
                    "text": f"Honeypot shield: {'ON' if hp else 'OFF'}",
                    "callback_data": "sf:hp:toggle",
                }
            ],
            [{"text": f"Min liq: {_fmt_usd(min_liq)}", "callback_data": "sf:noop"}],
            [
                {"text": "−", "callback_data": "sf:min_liq:-"},
                {"text": "+", "callback_data": "sf:min_liq:+"},
            ],
            [
                {
                    "text": f"Max liq: {'off' if max_liq <= 0 else _fmt_usd(max_liq)}",
                    "callback_data": "sf:noop",
                }
            ],
            [
                {"text": "−", "callback_data": "sf:max_liq:-"},
                {"text": "+", "callback_data": "sf:max_liq:+"},
            ],
            [{"text": "↻ Refresh", "callback_data": "sf:refresh"}],
        ]
    }


def _step(values: tuple[float, ...], current: float, direction: str) -> float:
    ordered = list(values)
    # Snap to nearest then move
    nearest = min(ordered, key=lambda v: abs(v - current))
    idx = ordered.index(nearest)
    if direction == "+":
        idx = min(idx + 1, len(ordered) - 1)
    else:
        idx = max(idx - 1, 0)
    return float(ordered[idx])


async def _api(method: str, payload: dict[str, Any] | None = None) -> dict:
    url = _bot_api_url(method)
    async with httpx.AsyncClient(**_TG_CLIENT_KW) as client:
        if payload is None:
            resp = await client.get(url)
        else:
            resp = await client.post(url, json=payload)
        return _raise_for_telegram_response(resp)


async def send_filters_menu(chat_id: str) -> None:
    db = get_db()
    f = await db.aget_user_filters(chat_id) or await db.aupsert_user_filters(chat_id)
    text = (
        "<b>Sniper alert filters</b>\n"
        f"Min buy: <code>{_fmt_usd(float(f.get('min_buy_usd') or 0))}</code>\n"
        f"Max mcap: <code>{_fmt_usd(float(f.get('max_mcap_usd') or 0))}</code>\n"
        f"Honeypot: <code>{'ON' if f.get('exclude_honeypots', 1) else 'OFF'}</code>\n"
        f"Liq: <code>{_fmt_usd(float(f.get('min_liq_usd') or 0))}</code>"
        f"–<code>{'∞' if float(f.get('max_liq_usd') or 0) <= 0 else _fmt_usd(float(f.get('max_liq_usd') or 0))}</code>"
    )
    url = _bot_api_url("sendMessage")
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": filters_keyboard(f),
    }
    async with httpx.AsyncClient(**_TG_CLIENT_KW) as client:
        resp = await client.post(url, json=payload)
        _raise_for_telegram_response(resp)


async def _handle_callback(cb: dict[str, Any]) -> None:
    data = str(cb.get("data") or "")
    msg = cb.get("message") or {}
    chat = str((msg.get("chat") or {}).get("id") or "")
    cb_id = cb.get("id")
    if not chat or not data.startswith("sf:"):
        return
    parts = data.split(":")
    db = get_db()
    f = await db.aget_user_filters(chat) or await db.aupsert_user_filters(chat)

    if parts[1] == "noop":
        await _api("answerCallbackQuery", {"callback_query_id": cb_id})
        return
    if parts[1] == "refresh":
        pass
    elif parts[1] == "hp" and parts[2] == "toggle":
        f = await db.aupsert_user_filters(
            chat, exclude_honeypots=not bool(f.get("exclude_honeypots", 1))
        )
    elif parts[1] == "min_buy":
        f = await db.aupsert_user_filters(
            chat,
            min_buy_usd=_step(_MIN_BUY_STEPS, float(f.get("min_buy_usd") or 0), parts[2]),
        )
    elif parts[1] == "max_mcap":
        f = await db.aupsert_user_filters(
            chat,
            max_mcap_usd=_step(
                _MAX_MCAP_STEPS, float(f.get("max_mcap_usd") or 0), parts[2]
            ),
        )
    elif parts[1] == "min_liq":
        f = await db.aupsert_user_filters(
            chat,
            min_liq_usd=_step(_LIQ_STEPS, float(f.get("min_liq_usd") or 0), parts[2]),
        )
    elif parts[1] == "max_liq":
        f = await db.aupsert_user_filters(
            chat,
            max_liq_usd=_step(_LIQ_STEPS, float(f.get("max_liq_usd") or 0), parts[2]),
        )

    text = (
        "<b>Sniper alert filters</b>\n"
        f"Min buy: <code>{_fmt_usd(float(f.get('min_buy_usd') or 0))}</code>\n"
        f"Max mcap: <code>{_fmt_usd(float(f.get('max_mcap_usd') or 0))}</code>\n"
        f"Honeypot: <code>{'ON' if f.get('exclude_honeypots', 1) else 'OFF'}</code>\n"
        f"Liq: <code>{_fmt_usd(float(f.get('min_liq_usd') or 0))}</code>"
        f"–<code>{'∞' if float(f.get('max_liq_usd') or 0) <= 0 else _fmt_usd(float(f.get('max_liq_usd') or 0))}</code>"
    )
    try:
        await _api(
            "editMessageText",
            {
                "chat_id": chat,
                "message_id": msg.get("message_id"),
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": filters_keyboard(f),
            },
        )
    except Exception:  # noqa: BLE001
        logger.debug("editMessageText failed", exc_info=True)
    if cb_id:
        await _api("answerCallbackQuery", {"callback_query_id": cb_id, "text": "Saved"})


async def _handle_message(msg: dict[str, Any]) -> None:
    chat = str((msg.get("chat") or {}).get("id") or "")
    text = str(msg.get("text") or "").strip()
    if not chat or not text:
        return
    low = text.lower()
    if low in ("/filters", "/settings", "/sniper"):
        await send_filters_menu(chat)
    elif low in ("/start", "/help"):
        await send_message(
            chat,
            "<b>gnomode sniper bot</b>\n"
            "/filters — alert filters (min buy, max mcap, honeypot, liquidity)\n"
            "Alerts fire on sniper Trade #2 and #3 for new tokens.",
        )


class TelegramBotPoller:
    def __init__(self) -> None:
        self._stop = False
        self._running = False
        self._offset = 0

    def stop(self) -> None:
        self._stop = True

    @property
    def running(self) -> bool:
        return self._running

    async def run_loop(self) -> None:
        if not settings.telegram_bot_polling:
            return
        if not bot_token():
            logger.info("Telegram bot polling skipped — no token")
            return
        self._running = True
        self._stop = False
        # Ensure default chat has a filters row
        chat = resolve_chat_id()
        if chat:
            try:
                await get_db().aupsert_user_filters(chat)
            except Exception:  # noqa: BLE001
                logger.debug("seed user_filters failed", exc_info=True)

        logger.info("Telegram bot polling started")
        while not self._stop:
            try:
                if not telegram_configured() and not bot_token():
                    await asyncio.sleep(10)
                    continue
                data = await _api(
                    "getUpdates",
                    {
                        "offset": self._offset,
                        "timeout": 25,
                        "allowed_updates": ["message", "callback_query"],
                    },
                )
                for upd in data.get("result") or []:
                    self._offset = max(self._offset, int(upd.get("update_id", 0)) + 1)
                    if "callback_query" in upd:
                        await _handle_callback(upd["callback_query"])
                    elif "message" in upd:
                        await _handle_message(upd["message"])
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001
                logger.exception("Telegram bot poll error")
                await asyncio.sleep(5)
        self._running = False


telegram_bot = TelegramBotPoller()
