"""On-chain quote-spend detection from tx receipts."""

from __future__ import annotations

from app.buy_gate import wallet_sent_quote_in_receipt
from app.constants import TRANSFER_TOPIC, WETH


def _topic_addr(addr: str) -> str:
    a = addr.lower().replace("0x", "")
    return "0x" + ("0" * 24) + a.zfill(40)[-40:]


def test_receipt_quote_true_when_wallet_sends_weth():
    wallet = "0xaaa0000000000000000000000000000000000001"
    receipt = {
        "logs": [
            {
                "address": WETH,
                "topics": [
                    TRANSFER_TOPIC,
                    _topic_addr(wallet),
                    _topic_addr("0xbbb0000000000000000000000000000000000002"),
                ],
            }
        ]
    }
    assert wallet_sent_quote_in_receipt(wallet, receipt) is True


def test_receipt_quote_false_when_no_wallet_quote_out():
    wallet = "0xaaa0000000000000000000000000000000000001"
    other = "0xccc0000000000000000000000000000000000003"
    receipt = {
        "logs": [
            {
                "address": WETH,
                "topics": [
                    TRANSFER_TOPIC,
                    _topic_addr(other),
                    _topic_addr(wallet),
                ],
            }
        ]
    }
    assert wallet_sent_quote_in_receipt(wallet, receipt) is False


def test_receipt_quote_none_without_receipt():
    assert wallet_sent_quote_in_receipt("0xaaa", None) is None
    assert wallet_sent_quote_in_receipt("0xaaa", {"logs": []}) is None
