"""Хвать: one-trade early buyers → follow-up alerts at low mcap."""

from __future__ import annotations

from typing import Any

from .followup import followup_runner
from .followup_store import followup_store
from .models import (
    FollowupConfig,
    TokensUniquePeriod,
    WatchConfig,
    WatchScreenFilters,
    WatchWalletFilters,
)
from .watch import watch_runner
from .watch_store import watch_store

# First buy and subsequent-alert mcap caps (USD).
HVAT_MCAP = 20_000.0
# Dedicated forum topic for deal #2+ alerts (watch discovery may use another topic).
HVAT_FOLLOWUP_TOPIC = "9245"
HVAT_ALERT_DEALS = [2, 3, 4, 5]
HVAT_MAX_DEALS = 5


def _followup_topic(fcfg: FollowupConfig) -> str:
    """Prefer saved follow-up topic; never steal watch discovery topic."""
    return (fcfg.telegram_topic_id or HVAT_FOLLOWUP_TOPIC).strip() or HVAT_FOLLOWUP_TOPIC


def apply_hvat_profile(*, enable: bool = True) -> dict[str, Any]:
    """Enable autoparse + follow-up; keep existing screen/wallet filters."""
    wcfg = watch_store.load_config()
    wallet = wcfg.wallet.model_dump()
    if wallet.get("mcap_threshold") is None:
        wallet["mcap_threshold"] = HVAT_MCAP
    if wallet.get("min_tokens_traded_7d") is None and wallet.get("max_tokens_traded_7d") is None:
        wallet["min_tokens_traded_7d"] = 1.0
        wallet["max_tokens_traded_7d"] = 1.0
    if not wallet.get("tokens_unique_period"):
        wallet["tokens_unique_period"] = TokensUniquePeriod.d7.value
    wallet["exclude_honeypots"] = True if wallet.get("exclude_honeypots") is None else wallet["exclude_honeypots"]

    wcfg = WatchConfig.model_validate(
        {
            **wcfg.model_dump(),
            "enabled": enable,
            "wallet": wallet,
        }
    )
    watch_store.save_config(wcfg)
    watch_runner.notify_config_changed()

    fcfg = followup_store.load_config()
    alert_mcap = float(wallet.get("mcap_threshold") or HVAT_MCAP)
    tg_chat = (fcfg.telegram_chat_id or wcfg.telegram_chat_id or "").strip()
    tg_topic = _followup_topic(fcfg)
    deals = list(fcfg.alert_on_deals or []) or list(HVAT_ALERT_DEALS)
    # Ensure #4/#5 are tracked when enabling Хвать (keep any extras user added).
    for d in HVAT_ALERT_DEALS:
        if d not in deals:
            deals.append(d)
    deals = sorted({int(x) for x in deals if int(x) >= 1})
    max_deals = max(int(fcfg.max_deals or 0), HVAT_MAX_DEALS, max(deals, default=HVAT_MAX_DEALS))
    fcfg = FollowupConfig.model_validate(
        {
            **fcfg.model_dump(),
            "enabled": enable,
            "max_mcap_alert": alert_mcap,
            "alert_on_deals": deals,
            "max_deals": max_deals,
            "buys_only": True,
            "ingest_from_watch": True,
            "telegram_chat_id": tg_chat,
            "telegram_topic_id": tg_topic,
            "bot_commands_enabled": True,
            "prune_enabled": False if fcfg.prune_enabled is None else fcfg.prune_enabled,
            "prune_min_ath_mcap": float(fcfg.prune_min_ath_mcap or 50_000.0),
            "prune_after_hours": float(fcfg.prune_after_hours or 48.0),
        }
    )
    followup_store.save_config(fcfg)
    followup_store.reopen_under_max_deals(max_deals)
    followup_runner.notify_config_changed()

    return {
        "ok": True,
        "mcap_cap": alert_mcap,
        "watch": wcfg,
        "followup": fcfg,
    }


def save_hvat_filters(
    *,
    screen: WatchScreenFilters | dict[str, Any],
    wallet: WatchWalletFilters | dict[str, Any],
    max_tokens_per_cycle: int | None = None,
    interval_sec: int | None = None,
    sync_followup_mcap: bool = True,
    followup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist token/wallet filters + optional #2/#3 alert filters."""
    wcfg = watch_store.load_config()
    screen_model = (
        screen
        if isinstance(screen, WatchScreenFilters)
        else WatchScreenFilters.model_validate(screen)
    )
    wallet_model = (
        wallet
        if isinstance(wallet, WatchWalletFilters)
        else WatchWalletFilters.model_validate(wallet)
    )
    payload: dict[str, Any] = {
        **wcfg.model_dump(),
        "screen": screen_model.model_dump(),
        "wallet": wallet_model.model_dump(),
    }
    if max_tokens_per_cycle is not None:
        payload["max_tokens_per_cycle"] = int(max_tokens_per_cycle)
    if interval_sec is not None:
        payload["interval_sec"] = int(interval_sec)
    saved = watch_store.save_config(WatchConfig.model_validate(payload))
    watch_runner.notify_config_changed()

    fcfg = followup_store.load_config()
    updates: dict[str, Any] = {}
    if followup:
        if "max_mcap_alert" in followup and followup["max_mcap_alert"] is not None:
            updates["max_mcap_alert"] = float(followup["max_mcap_alert"])
        if "min_mcap_alert" in followup:
            raw = followup["min_mcap_alert"]
            updates["min_mcap_alert"] = float(raw) if raw is not None else None
        if "min_bought_usd" in followup:
            raw = followup["min_bought_usd"]
            updates["min_bought_usd"] = float(raw) if raw is not None else None
        if "max_bought_usd" in followup:
            raw = followup["max_bought_usd"]
            updates["max_bought_usd"] = float(raw) if raw is not None else None
        if "telegram_topic_id" in followup:
            updates["telegram_topic_id"] = str(followup["telegram_topic_id"] or "").strip()
        if "telegram_chat_id" in followup:
            updates["telegram_chat_id"] = str(followup["telegram_chat_id"] or "").strip()
        if "alert_on_deals" in followup and followup["alert_on_deals"] is not None:
            deals = [int(x) for x in followup["alert_on_deals"]]
        if "alert_on_deals" in followup and followup["alert_on_deals"] is not None:
            deals = [int(x) for x in followup["alert_on_deals"]]
            updates["alert_on_deals"] = deals or list(HVAT_ALERT_DEALS)
            # Keep tracking at least through the highest alerted deal.
            hi = max(updates["alert_on_deals"], default=HVAT_MAX_DEALS)
            cur_max = int(followup.get("max_deals") or fcfg.max_deals or HVAT_MAX_DEALS)
            updates["max_deals"] = max(cur_max, hi, HVAT_MAX_DEALS)
        elif "max_deals" in followup and followup["max_deals"] is not None:
            updates["max_deals"] = max(1, int(followup["max_deals"]))
        if "prune_enabled" in followup and followup["prune_enabled"] is not None:
            updates["prune_enabled"] = bool(followup["prune_enabled"])
        if "prune_min_ath_mcap" in followup and followup["prune_min_ath_mcap"] is not None:
            updates["prune_min_ath_mcap"] = float(followup["prune_min_ath_mcap"])
        if "prune_after_hours" in followup and followup["prune_after_hours"] is not None:
            updates["prune_after_hours"] = float(followup["prune_after_hours"])

    if "max_mcap_alert" not in updates and sync_followup_mcap and wallet_model.mcap_threshold is not None:
        updates["max_mcap_alert"] = float(wallet_model.mcap_threshold)

    if "telegram_topic_id" not in updates and not (fcfg.telegram_topic_id or "").strip():
        updates["telegram_topic_id"] = HVAT_FOLLOWUP_TOPIC

    if updates:
        fcfg = FollowupConfig.model_validate({**fcfg.model_dump(), **updates})
        followup_store.save_config(fcfg)
        if "max_deals" in updates:
            followup_store.reopen_under_max_deals(int(fcfg.max_deals))
        followup_runner.notify_config_changed()

    return {"ok": True, "watch": saved, "followup": fcfg}


def hvat_status() -> dict[str, Any]:
    from .token_index import token_index

    w = watch_runner.status()
    f = followup_runner.status()
    cfg = watch_store.load_config()
    fcfg = followup_store.load_config()
    return {
        "mcap_cap": float(cfg.wallet.mcap_threshold or HVAT_MCAP),
        "watch": w,
        "followup": f,
        "index": token_index.status(),
        "config": cfg,
        "followup_config": fcfg,
        "profile": {
            "one_trade": True,
            "max_tokens_traded_7d": cfg.wallet.max_tokens_traded_7d,
            "min_tokens_traded_7d": cfg.wallet.min_tokens_traded_7d,
            "tokens_unique_period": cfg.wallet.tokens_unique_period,
            "first_buy_max_mcap": cfg.wallet.mcap_threshold,
            "alert_deals": list(fcfg.alert_on_deals or HVAT_ALERT_DEALS),
            "alert_max_mcap": fcfg.max_mcap_alert,
            "alert_min_mcap": fcfg.min_mcap_alert,
            "alert_min_bought": fcfg.min_bought_usd,
            "alert_max_bought": fcfg.max_bought_usd,
            "telegram_topic_id": fcfg.telegram_topic_id or HVAT_FOLLOWUP_TOPIC,
            "prune_enabled": fcfg.prune_enabled,
            "prune_min_ath_mcap": fcfg.prune_min_ath_mcap,
            "prune_after_hours": fcfg.prune_after_hours,
        },
    }
