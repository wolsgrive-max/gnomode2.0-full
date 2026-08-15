"""Per-wallet follow-up alert filters."""

from __future__ import annotations

from pathlib import Path

from app.followup import alert_kwargs_for_wallet, should_alert_deal
from app.followup_store import FollowupStore
from app.models import BuyerRow, FollowupConfig, WalletAlertFilters


def _buyer(wallet: str, token: str) -> BuyerRow:
    return BuyerRow(
        wallet=wallet,
        token=token,
        token_symbol="TST",
        bought_tokens=1000.0,
        bought_usd=100.0,
        mcap_at_first_buy=5000.0,
        buys_count=1,
        first_tx="0x1",
    )


def test_wallet_alert_filters_persist_and_merge(tmp_path: Path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    w1 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    w2 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    store.ingest_buyers(
        [
            _buyer(w1, "0x1111111111111111111111111111111111111111"),
            _buyer(w2, "0x2222222222222222222222222222222222222222"),
        ],
        max_deals=3,
        max_mcap_alert=20_000,
    )

    updated = store.set_wallet_alert_filters(
        [w1],
        WalletAlertFilters(
            custom=True,
            max_mcap_alert=8_000,
            min_mcap_alert=1_000,
            min_bought_usd=50,
            max_bought_usd=None,
        ),
    )
    assert updated == [w1]

    rows = {r.address: r for r in store.list_wallets(limit=10)}
    assert rows[w1].alert_filters.custom is True
    assert rows[w1].alert_filters.max_mcap_alert == 8_000
    assert rows[w2].alert_filters.custom is False

    cfg = FollowupConfig(max_mcap_alert=20_000, min_mcap_alert=None)
    gate_w1 = alert_kwargs_for_wallet(cfg, rows[w1].alert_filters)
    gate_w2 = alert_kwargs_for_wallet(cfg, rows[w2].alert_filters)

    assert should_alert_deal(2, 7_000, bought_usd=80, **gate_w1)
    assert not should_alert_deal(2, 9_000, bought_usd=80, **gate_w1)  # over wallet max
    assert should_alert_deal(2, 9_000, bought_usd=80, **gate_w2)  # global allows

    cleared = store.set_wallet_alert_filters([w1], WalletAlertFilters(custom=False))
    assert cleared == [w1]
    rows2 = {r.address: r for r in store.list_wallets(limit=10)}
    assert rows2[w1].alert_filters.custom is False


def test_bulk_apply_filters(tmp_path: Path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    addrs = [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "0xcccccccccccccccccccccccccccccccccccccccc",
    ]
    store.ingest_buyers(
        [
            _buyer(addrs[0], "0x1111111111111111111111111111111111111111"),
            _buyer(addrs[1], "0x2222222222222222222222222222222222222222"),
            _buyer(addrs[2], "0x3333333333333333333333333333333333333333"),
        ],
        max_deals=3,
        max_mcap_alert=20_000,
    )
    updated = store.set_wallet_alert_filters(
        addrs[:2],
        WalletAlertFilters(custom=True, max_mcap_alert=12_000, min_bought_usd=10),
    )
    assert set(updated) == set(addrs[:2])
    fmap = store.get_alert_filters_map(addrs)
    assert fmap[addrs[0]].max_mcap_alert == 12_000
    assert fmap[addrs[1]].custom is True
    assert fmap[addrs[2]].custom is False
