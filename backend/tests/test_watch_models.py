"""WatchConfig validation smoke tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models import WatchConfig


def test_watch_config_defaults():
    cfg = WatchConfig()
    assert cfg.enabled is False
    assert cfg.interval_sec == 720
    assert cfg.max_tokens_per_cycle == 40
    assert cfg.screen.exclude_honeypots is True
    assert cfg.screen.min_liq == 500.0
    assert cfg.screen.min_ath_mcap == 40_000.0
    assert cfg.screen.max_pair_age_hours == 24.0
    assert cfg.screen.max_results == 10_000
    assert cfg.screen.sort_by.value == "liquidity"
    assert cfg.screen.sort_order.value == "desc"
    assert cfg.wallet.min_tokens_traded_7d == 1.0
    assert cfg.wallet.max_tokens_traded_7d == 1.0
    assert cfg.wallet.tokens_unique_period.value == "30d"
    assert cfg.parse_priority_min_ath == 50_000.0


def test_watch_config_interval_bounds():
    with pytest.raises(ValidationError):
        WatchConfig(interval_sec=30)
    with pytest.raises(ValidationError):
        WatchConfig(max_tokens_per_cycle=0)
    WatchConfig(interval_sec=60, max_tokens_per_cycle=1)
