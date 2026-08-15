"""Follow-up wallet delete."""

from __future__ import annotations

from pathlib import Path

from app.followup_store import FollowupStore
from app.models import BuyerRow


def test_delete_wallet(tmp_path: Path):
    store = FollowupStore(
        db_path=str(tmp_path / "followup.db"),
        config_path=str(tmp_path / "followup.json"),
    )
    buyers = [
        BuyerRow(
            wallet="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            token="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            token_symbol="TST",
            bought_tokens=1000.0,
            bought_usd=100.0,
            mcap_at_first_buy=5000.0,
            buys_count=1,
            first_tx="0x1",
        )
    ]
    inserted = store.ingest_buyers(buyers, max_deals=3, max_mcap_alert=20_000)
    assert len(inserted) == 1
    assert store.counts() == (1, 0)

    assert store.delete_wallet("0xAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAa") is True
    assert store.counts() == (0, 0)
    assert store.list_wallets(limit=10) == []
    assert store.delete_wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") is False
