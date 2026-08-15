"""Telegram Bot API helpers for watch alerts."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv

from .config import settings
from .models import BuyerRow

logger = logging.getLogger(__name__)

_TG_TIMEOUT = httpx.Timeout(20.0, connect=10.0)
_MSG_LIMIT = 3500
# Ignore HTTP(S)_PROXY — corporate/system proxies often 403 Telegram API.
_TG_CLIENT_KW = {"timeout": _TG_TIMEOUT, "trust_env": False}
_ENV_PATHS = (
    Path(__file__).resolve().parents[2] / ".env",  # project root
    Path(__file__).resolve().parents[1] / ".env",  # backend/
)
_env_mtime: float | None = None


def _reload_env_files() -> None:
    """Re-read .env into os.environ when the file changes (no full process restart)."""
    global _env_mtime
    mtimes = [p.stat().st_mtime for p in _ENV_PATHS if p.is_file()]
    latest = max(mtimes) if mtimes else 0.0
    if _env_mtime is not None and latest == _env_mtime:
        return
    _env_mtime = latest
    for path in _ENV_PATHS:
        if path.is_file():
            load_dotenv(path, override=True)


def _clean_secret(value: str | None) -> str:
    """Strip whitespace and accidental surrounding quotes from .env values."""
    return (value or "").strip().strip('"').strip("'").strip()


def bot_token() -> str:
    _reload_env_files()
    return _clean_secret(os.getenv("TELEGRAM_BOT_TOKEN")) or _clean_secret(
        settings.telegram_bot_token
    )


def telegram_configured(chat_id: str | None = None) -> bool:
    token = bot_token()
    chat = _clean_secret(chat_id) or resolve_chat_id()
    return bool(token and chat)


def resolve_chat_id(override: str | None = None) -> str:
    _reload_env_files()
    return (
        _clean_secret(override)
        or _clean_secret(os.getenv("TELEGRAM_CHAT_ID"))
        or _clean_secret(settings.telegram_chat_id)
    )


def resolve_topic_id(override: str | None = None) -> int | None:
    """Parse forum topic id (message_thread_id).

    Empty override falls back to TELEGRAM_TOPIC_ID (same pattern as chat id).
    Blank env + blank override → None (no topic / General).
    """
    _reload_env_files()
    raw = (
        _clean_secret(override)
        or _clean_secret(os.getenv("TELEGRAM_TOPIC_ID"))
        or _clean_secret(settings.telegram_topic_id)
    )
    if not raw:
        return None
    try:
        topic = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid TELEGRAM_TOPIC_ID / topic id: {raw!r}") from exc
    if topic <= 0:
        return None
    return topic


def _bot_api_url(method: str) -> str:
    token = bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    return f"https://api.telegram.org/bot{token}/{method}"


def bot_api_url(method: str) -> str:
    """Public wrapper for Telegram Bot API method URLs."""
    return _bot_api_url(method)


TG_CLIENT_KW = _TG_CLIENT_KW


def _fmt_num(n: float | int | None, digits: int = 2) -> str:
    if n is None:
        return "—"
    try:
        v = float(n)
        text = f"{v:,.{digits}f}"
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text
    except (TypeError, ValueError):
        return "—"


def _fmt_hold(minutes: float | None) -> str:
    if minutes is None:
        return "—"
    if minutes < 60:
        return f"{minutes:.1f}m"
    if minutes < 60 * 24:
        return f"{minutes / 60:.1f}h"
    return f"{minutes / (60 * 24):.1f}d"


def format_buyer_block(buyer: BuyerRow) -> str:
    sym = buyer.token_symbol or "TOKEN"
    token = buyer.token
    wallet = buyer.wallet
    lines = [
        f"<b>{sym}</b> early buyer",
        f"Token: <code>{token}</code>",
        f"Wallet: <code>{wallet}</code>",
        f"Bought: ${_fmt_num(buyer.bought_usd)} · mcap@buy ${_fmt_num(buyer.mcap_at_first_buy)}",
        (
            f"Bal {_fmt_num(buyer.wallet_balance_eth)} ETH · "
            f"hold {_fmt_hold(buyer.hold_time_minutes)} · "
            f"30d tokens {buyer.tokens_traded_7d if buyer.tokens_traded_7d is not None else '—'}"
        ),
        f'<a href="https://gmgn.ai/robinhood/token/{token}">GMGN token</a> · '
        f'<a href="https://gmgn.ai/robinhood/address/{wallet}">GMGN wallet</a>',
    ]
    return "\n".join(lines)


def format_followup_deal(
    *,
    wallet: str,
    token: str,
    token_symbol: str,
    deal_index: int,
    mcap_at_buy: float | None,
    bought_usd: float | None = None,
    honeypot_reason: str | None = None,
) -> str:
    """HTML block for 2nd/3rd new-token buy @ low mcap."""
    sym = token_symbol or "TOKEN"
    lines: list[str] = []
    if honeypot_reason:
        # Telegram HTML has no color — red circles + bold math-caps for visibility.
        lines.extend(
            [
                "🔴🔴🔴🔴🔴🔴🔴🔴",
                "<b>‼️ 𝗛𝗢𝗡𝗘𝗬𝗣𝗢𝗧 ‼️</b>",
                "<b>‼️ 𝗛𝗢𝗡𝗘𝗬𝗣𝗢𝗧 ‼️</b>",
                "<b>‼️ 𝗛𝗢𝗡𝗘𝗬𝗣𝗢𝗧 ‼️</b>",
                "🔴🔴🔴🔴🔴🔴🔴🔴",
                f"<b>⚠️ HONEYPOT · {honeypot_reason}</b>",
                "",
            ]
        )
    lines.extend(
        [
            f"<b>Follow-up · сделка #{deal_index}</b>",
            f"<b>{sym}</b> · новый токен @ low mcap",
            f"Token: <code>{token}</code>",
            f"Wallet: <code>{wallet}</code>",
            f"mcap@buy ${_fmt_num(mcap_at_buy)}"
            + (f" · bought ${_fmt_num(bought_usd)}" if bought_usd is not None else ""),
            f'<a href="https://gmgn.ai/robinhood/token/{token}">GMGN token</a> · '
            f'<a href="https://gmgn.ai/robinhood/address/{wallet}">GMGN wallet</a>',
        ]
    )
    return "\n".join(lines)


async def send_followup_deal(
    chat_id: str,
    *,
    wallet: str,
    token: str,
    token_symbol: str,
    deal_index: int,
    mcap_at_buy: float | None,
    bought_usd: float | None = None,
    topic_id: int | None = None,
    honeypot_reason: str | None = None,
    check_honeypot: bool = True,
) -> str | None:
    """Send deal alert: honeypot check first, then Telegram (banner if flagged).

    Returns honeypot reason if flagged, else None.
    """
    reason = honeypot_reason
    if check_honeypot and reason is None and token:
        try:
            from .security import honeypot_reason_for_token

            reason = await asyncio.wait_for(
                honeypot_reason_for_token(token),
                timeout=8.0,
            )
        except Exception:  # noqa: BLE001
            reason = None
    text = format_followup_deal(
        wallet=wallet,
        token=token,
        token_symbol=token_symbol,
        deal_index=deal_index,
        mcap_at_buy=mcap_at_buy,
        bought_usd=bought_usd,
        honeypot_reason=reason,
    )
    await send_message(chat_id, text, topic_id=topic_id)
    return reason


def chunk_buyers(buyers: list[BuyerRow], *, header: str = "Watch alert") -> list[tuple[list[BuyerRow], str]]:
    """Split buyers into (chunk, html_message) pairs under Telegram size limits."""
    out: list[tuple[list[BuyerRow], str]] = []
    current: list[BuyerRow] = []
    current_text = f"<b>{header}</b>\n"
    for buyer in buyers:
        block = format_buyer_block(buyer)
        piece = ("\n\n" if current else "\n") + block
        if current and len(current_text) + len(piece) > _MSG_LIMIT:
            out.append((current, current_text))
            current = [buyer]
            current_text = f"<b>{header}</b> (cont.)\n\n{block}"
        else:
            current.append(buyer)
            current_text += piece
    if current:
        out.append((current, current_text))
    return out


def _message_payload(
    chat_id: str,
    text: str,
    *,
    topic_id: int | None = None,
) -> tuple[str, dict]:
    chat = (chat_id or "").strip()
    if not chat:
        raise RuntimeError("Telegram chat id is empty")
    url = _bot_api_url("sendMessage")
    payload: dict = {
        "chat_id": chat,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if topic_id is not None:
        payload["message_thread_id"] = int(topic_id)
    return url, payload


def _raise_for_telegram_response(resp: httpx.Response) -> dict:
    if resp.status_code == 401:
        raise RuntimeError(
            "Telegram 401 Unauthorized — проверьте TELEGRAM_BOT_TOKEN "
            "(BotFather → /token). После смены токена перезапустите API."
        )
    if resp.status_code == 400:
        try:
            desc = resp.json().get("description") or resp.text[:200]
        except Exception:  # noqa: BLE001
            desc = resp.text[:200]
        raise RuntimeError(f"Telegram 400: {desc}")
    if resp.status_code != 200:
        body = resp.text[:300]
        raise RuntimeError(f"Telegram API {resp.status_code}: {body}")
    data = resp.json()
    if not data.get("ok"):
        desc = data.get("description") or data
        raise RuntimeError(f"Telegram API error: {desc}")
    return data


async def send_message(
    chat_id: str,
    text: str,
    *,
    topic_id: int | None = None,
) -> None:
    url, payload = _message_payload(chat_id, text, topic_id=topic_id)
    async with httpx.AsyncClient(**_TG_CLIENT_KW) as client:
        resp = await client.post(url, json=payload)
        _raise_for_telegram_response(resp)


def send_message_sync(
    chat_id: str,
    text: str,
    *,
    topic_id: int | None = None,
) -> None:
    """Blocking send for shutdown / signal / excepthook paths."""
    url, payload = _message_payload(chat_id, text, topic_id=topic_id)
    with httpx.Client(timeout=httpx.Timeout(8.0, connect=4.0), trust_env=False) as client:
        resp = client.post(url, json=payload)
        _raise_for_telegram_response(resp)


async def get_me() -> dict:
    """Call Telegram getMe — verifies bot token reaches the API."""
    url = _bot_api_url("getMe")
    async with httpx.AsyncClient(**_TG_CLIENT_KW) as client:
        resp = await client.get(url)
        data = _raise_for_telegram_response(resp)
    return data.get("result") or {}


async def test_telegram_connection(
    *,
    chat_id: str = "",
    topic_id: str = "",
) -> dict:
    """Verify token (getMe) and send a short ping into chat/topic."""
    chat = resolve_chat_id(chat_id)
    if not telegram_configured(chat):
        raise RuntimeError("Telegram не настроен (TELEGRAM_BOT_TOKEN / chat id)")
    topic = resolve_topic_id(topic_id)

    me = await get_me()
    username = me.get("username") or ""
    bot_name = me.get("first_name") or "bot"
    who = f"@{username}" if username else bot_name
    ping = f"<b>gnomode</b> · проверка Telegram\nБот: {who}"
    if topic is not None:
        ping += f"\nТопик: <code>{topic}</code>"
    await send_message(chat, ping, topic_id=topic)
    return {
        "ok": True,
        "bot_username": username,
        "bot_id": me.get("id"),
        "chat_id": chat,
        "topic_id": topic,
        "message": "Пинг отправлен в Telegram",
    }


async def send_buyers(
    chat_id: str,
    buyers: list[BuyerRow],
    *,
    header: str = "Watch alert",
    topic_id: int | None = None,
) -> tuple[int, list[BuyerRow]]:
    """Send batched buyer alerts.

    Returns (messages_sent, buyers covered by successfully sent messages).
    If a later batch fails, earlier successful buyers are still returned and the
    error is re-raised so the runner can surface last_error.
    """
    if not buyers:
        return 0, []

    sent_buyers: list[BuyerRow] = []
    messages_sent = 0
    for chunk, text in chunk_buyers(buyers, header=header):
        try:
            await send_message(chat_id, text, topic_id=topic_id)
        except Exception:
            logger.exception("Failed sending Telegram batch (%s buyers)", len(chunk))
            if sent_buyers:
                return messages_sent, sent_buyers
            raise
        sent_buyers.extend(chunk)
        messages_sent += 1
    return messages_sent, sent_buyers
