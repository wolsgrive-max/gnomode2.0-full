"""In-memory parse job store."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

from .chain import RpcClient
from .config import settings
from .models import (
    JobLogEntry,
    JobProgress,
    JobResponse,
    JobStatus,
    ParseRequest,
    TokenParseResult,
)
from .replay import parse_token
from .wallet_metrics import (
    balance_filter_active,
    hold_time_filter_active,
    tokens_7d_filter_active,
)

logger = logging.getLogger(__name__)

_LOG_MAX = 250


def _wallet_filters_active(req: ParseRequest) -> bool:
    return (
        balance_filter_active(req)
        or hold_time_filter_active(req)
        or tokens_7d_filter_active(req)
    )


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobResponse] = {}
        self._lock = asyncio.Lock()

    def get(self, job_id: str) -> JobResponse | None:
        return self._jobs.get(job_id)

    async def cancel(self, job_id: str) -> bool:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job or job.status not in (JobStatus.queued, JobStatus.running):
                return False
            job.status = JobStatus.error
            job.error = "Cancelled by user"
            job.progress = JobProgress(stage="error", message="Отменено пользователем", percent=100)
        return True

    def _is_cancelled(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        return job is not None and job.status == JobStatus.error

    def has_active(self) -> bool:
        """True if any parse job is queued or running (used to yield resources)."""
        return any(
            j.status in (JobStatus.queued, JobStatus.running)
            for j in self._jobs.values()
        )

    async def create(self, req: ParseRequest) -> JobResponse:
        job_id = uuid.uuid4().hex[:12]
        job = JobResponse(
            job_id=job_id,
            status=JobStatus.queued,
            progress=JobProgress(stage="queued", message="Queued", percent=0),
            log=[
                JobLogEntry(
                    ts=time.time(),
                    stage="queued",
                    message=f"Queued — {len([t for t in req.tokens if t.strip()])} token(s)",
                    percent=0,
                )
            ],
        )
        self._jobs[job_id] = job
        asyncio.create_task(self._run(job_id, req))
        return job

    def _append_log(self, job: JobResponse, progress: JobProgress) -> None:
        entry = JobLogEntry(
            ts=time.time(),
            stage=progress.stage,
            message=progress.message,
            percent=progress.percent,
            token=progress.current_token,
        )
        if job.log:
            last = job.log[-1]
            # Same step text: refresh percent/ts instead of flooding the UI.
            if last.stage == entry.stage and last.message == entry.message:
                job.log[-1] = entry
                return
        job.log.append(entry)
        if len(job.log) > _LOG_MAX:
            job.log = job.log[-_LOG_MAX:]

    async def _update(self, job_id: str, **kwargs: Any) -> None:
        async with self._lock:
            job = self._jobs[job_id]
            for k, v in kwargs.items():
                if k == "progress" and isinstance(v, JobProgress):
                    job.progress = v
                    self._append_log(job, v)
                elif hasattr(job, k):
                    setattr(job, k, v)

    async def _run(self, job_id: str, req: ParseRequest) -> None:
        # If already cancelled before we started, bail.
        if self._is_cancelled(job_id):
            return
        threshold = (
            req.mcap_threshold
            if req.mcap_threshold is not None
            else settings.mcap_threshold
        )
        await self._update(
            job_id,
            status=JobStatus.running,
            progress=JobProgress(stage="running", message="Starting…", percent=0.01),
        )
        rpc = RpcClient()
        results: list[TokenParseResult] = []
        tokens = [t.strip() for t in req.tokens if t.strip()]
        n = max(len(tokens), 1)
        # Parallel tokens + wallet filters (hold getLogs / Blockscout) starve the
        # public RPC and look "frozen". Keep concurrency for bare parses only.
        if _wallet_filters_active(req):
            concurrency = 1
        else:
            concurrency = max(1, min(settings.parse_token_concurrency, len(tokens) or 1))
        sem = asyncio.Semaphore(concurrency)
        token_frac = [0.0] * len(tokens)
        slots: list[TokenParseResult | None] = [None] * len(tokens)

        async def run_one(i: int, token: str) -> None:
            async with sem:
                await self._update(
                    job_id,
                    progress=JobProgress(
                        stage="token",
                        message=f"Token {i + 1}/{len(tokens)}: {token[:10]}…",
                        percent=round((sum(token_frac) / n) * 100, 2),
                        current_token=token,
                    ),
                )

                async def on_progress(
                    stage: str,
                    message: str,
                    percent: float,
                    _i=i,
                    _token=token,
                ):
                    token_frac[_i] = min(max(percent, 0.0), 0.99)
                    await self._update(
                        job_id,
                        progress=JobProgress(
                            stage=stage,
                            message=message,
                            percent=round((sum(token_frac) / n) * 100, 2),
                            current_token=_token,
                        ),
                    )

                try:
                    result = await asyncio.wait_for(
                        parse_token(
                            rpc,
                            token,
                            threshold,
                            on_progress=on_progress,
                            exclude_honeypots=req.exclude_honeypots,
                            wallet_filters=req,
                        ),
                        timeout=600,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Failed parsing %s", token)
                    result = TokenParseResult(token=token, error=str(exc))
                    await on_progress("error", f"Token failed: {exc}", 1.0)

                token_frac[i] = 1.0
                slots[i] = result
                # Preserve token order in the live results list.
                ordered = [r for r in slots if r is not None]
                await self._update(job_id, results=ordered)

        try:
            if not tokens:
                if not self._is_cancelled(job_id):
                    await self._update(
                        job_id,
                        status=JobStatus.done,
                        results=[],
                        progress=JobProgress(
                            stage="done", message="Done — no tokens", percent=100
                        ),
                    )
                return

            if concurrency == 1:
                for i, token in enumerate(tokens):
                    await run_one(i, token)
            else:
                await asyncio.gather(*[run_one(i, t) for i, t in enumerate(tokens)])

            # Don't overwrite cancelled status.
            if not self._is_cancelled(job_id):
                results = [r for r in slots if r is not None]
                total_wallets = sum(len(r.buyers) for r in results)
                await self._update(
                    job_id,
                    status=JobStatus.done,
                    results=results,
                    progress=JobProgress(
                        stage="done",
                        message=f"Done — {total_wallets} wallets across {len(results)} token(s)",
                        percent=100,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Job %s failed", job_id)
            await self._update(
                job_id,
                status=JobStatus.error,
                error=str(exc),
                progress=JobProgress(stage="error", message=str(exc), percent=100),
            )


jobs = JobStore()
