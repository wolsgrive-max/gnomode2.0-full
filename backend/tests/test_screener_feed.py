"""Tests for truegnomode screener feed adapter."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from app import screener_feed
from app.models import ScreenRequest, ScreenedToken


ADDRESS = "0x00000000000000000000000000000000000000a1"


def test_map_remote_results_uses_observed_peak_for_ath() -> None:
    rows = [
        {
            "address": ADDRESS,
            "symbol": "OK",
            "market_cap": 12_000,
            "ath_mcap": 0,
            "dexscreener_observed_peak_mcap_usd": 88_000,
            "liquidity_usd": 5_000,
            "traders_24h": 10,
            "pair_age_hours": 2.5,
            "extra_donor_field": "ignored",
        }
    ]
    tokens = screener_feed.map_remote_results(rows)
    assert len(tokens) == 1
    assert tokens[0].address == ADDRESS
    assert tokens[0].ath_mcap == 88_000.0
    assert tokens[0].symbol == "OK"


def test_map_remote_results_skips_bad_rows() -> None:
    assert screener_feed.map_remote_results([{"symbol": "no-addr"}]) == []
    assert screener_feed.map_remote_results([]) == []


def test_remote_screen_payload_forces_donor_mode() -> None:
    req = ScreenRequest(min_liq=100, max_results=10)
    payload = screener_feed._remote_screen_payload(
        req, force_enrich_addresses=[" 0xAbc ", ""]
    )
    assert payload["screen_pipeline_mode"] == "donor"
    assert payload["min_liq"] == 100
    assert payload["force_enrich_addresses"] == [" 0xAbc "]


@pytest.mark.asyncio
async def test_fetch_truegnomode_screen_polls_until_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        screener_feed.settings, "truegnomode_screener_url", "http://tg.test"
    )
    monkeypatch.setattr(screener_feed.settings, "truegnomode_poll_interval_sec", 0.01)
    monkeypatch.setattr(screener_feed.settings, "truegnomode_screen_timeout_sec", 5.0)

    calls: list[str] = []
    poll_n = {"n": 0}

    class FakeResponse:
        def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
            self.status_code = status_code
            self._payload = payload
            self.text = str(payload)

        def json(self) -> dict[str, Any]:
            return self._payload

    class FakeClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def __aenter__(self) -> FakeClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            del args

        async def post(self, url: str, json: dict[str, Any] | None = None) -> FakeResponse:
            calls.append(f"POST {url}")
            assert json is not None
            assert json["screen_pipeline_mode"] == "donor"
            return FakeResponse(200, {"job_id": "abc123", "status": "queued"})

        async def get(self, url: str) -> FakeResponse:
            calls.append(f"GET {url}")
            poll_n["n"] += 1
            if poll_n["n"] < 2:
                return FakeResponse(
                    200,
                    {
                        "job_id": "abc123",
                        "status": "running",
                        "progress": {
                            "stage": "filter",
                            "message": "Filtering…",
                            "percent": 50,
                        },
                        "results": [],
                    },
                )
            return FakeResponse(
                200,
                {
                    "job_id": "abc123",
                    "status": "done",
                    "progress": {
                        "stage": "done",
                        "message": "Done",
                        "percent": 100,
                    },
                    "results": [
                        {
                            "address": ADDRESS,
                            "symbol": "TG",
                            "market_cap": 20_000,
                            "ath_mcap": 55_000,
                            "liquidity_usd": 8_000,
                        }
                    ],
                },
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)

    progress: list[tuple[str, str, float]] = []

    async def on_progress(stage: str, message: str, percent: float) -> None:
        progress.append((stage, message, percent))

    tokens = await screener_feed.fetch_truegnomode_screen(
        ScreenRequest(max_results=5),
        on_progress=on_progress,
    )
    assert len(tokens) == 1
    assert tokens[0].symbol == "TG"
    assert tokens[0].ath_mcap == 55_000.0
    assert any(c.startswith("POST ") for c in calls)
    assert sum(1 for c in calls if c.startswith("GET ")) >= 2
    assert any(p[0] == "done" for p in progress)


@pytest.mark.asyncio
async def test_fetch_screened_tokens_falls_back_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(screener_feed.settings, "truegnomode_screener_url", "")

    async def fake_local(
        req: ScreenRequest, on_progress=None
    ) -> list[ScreenedToken]:
        del req, on_progress
        return [ScreenedToken(address=ADDRESS, symbol="LOC")]

    monkeypatch.setattr(screener_feed, "screen_tokens_local", fake_local)
    tokens = await screener_feed.fetch_screened_tokens(ScreenRequest())
    assert tokens[0].symbol == "LOC"


@pytest.mark.asyncio
async def test_fetch_screened_tokens_uses_remote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        screener_feed.settings, "truegnomode_screener_url", "http://tg.test"
    )

    async def boom(*args: Any, **kwargs: Any) -> list[ScreenedToken]:
        del args, kwargs
        raise AssertionError("local screener must not run")

    async def fake_remote(
        req: ScreenRequest, **kwargs: Any
    ) -> list[ScreenedToken]:
        del req, kwargs
        return [ScreenedToken(address=ADDRESS, symbol="REM")]

    monkeypatch.setattr(screener_feed, "screen_tokens_local", boom)
    monkeypatch.setattr(screener_feed, "fetch_truegnomode_screen", fake_remote)
    tokens = await screener_feed.fetch_screened_tokens(ScreenRequest())
    assert tokens[0].symbol == "REM"


def test_using_remote_screener(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(screener_feed.settings, "truegnomode_screener_url", "")
    assert screener_feed.using_remote_screener() is False
    monkeypatch.setattr(
        screener_feed.settings, "truegnomode_screener_url", " http://x "
    )
    assert screener_feed.using_remote_screener() is True
    assert screener_feed.truegnomode_base_url() == "http://x"
