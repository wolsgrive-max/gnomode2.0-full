# Prompt: MCAP Tracker — Track Tokens Below 50k, Auto-Analyze at Target

## Context

This is a Robinhood Chain (chain_id=4663) meme token analyzer called **Gnomode 2.0**. Backend: FastAPI + SQLite WAL. Frontend: React + Vite.

### Existing Architecture (relevant files)

| File | Purpose |
|------|---------|
| `backend/app/database.py` | SQLite WAL store; tables: `migrated_tokens`, `wallet_trades`, `tracked_wallets`, `blacklist` |
| `backend/app/token_index.py` | In-memory 24h token index; `enrich_pending()` fetches DexScreener market_cap for each token |
| `backend/app/config.py` | App settings (`mcap_threshold: float = 15_000.0`, etc.) |
| `backend/app/watch.py` | Periodic pipeline: screen → parse → Telegram |
| `backend/app/screener.py` | Fetches mcap from DexScreener; `_pair_to_screened()` builds `ScreenedToken(market_cap=...)` |
| `backend/app/replay.py` | `parse_token(rpc, token, mcap_threshold)` — computes mcap from on-chain swaps, finds early buyers |
| `backend/app/migration_scan.py` | `store_migration(event)` — stores to `migrated_tokens` via `db.ainsert_token()` |
| `backend/app/models.py` | All Pydantic models: `ScreenedToken`, `MigratedTokenRow`, `WatchConfig`, etc. |
| `backend/app/data/watch.json` | Watch config file with `screen.min_ath_mcap: 50000` (silently ignored — no model field) |
| `backend/app/migration_pipeline.py` | `parse_token_migration()` — detects launchpad, calls `store_migration()` |
| `frontend/src/SnipersPage.tsx` | Main UI page with tabs: Tokens, Snipers, Trades, Blacklist |

### Key Data Flow

```
token_index.scan_new_pools()     — RPC: getLogs V3 Factory + V4 PoolManager
    ↓
token_index.enrich_pending()     — HTTP: DexScreener batch fetch (30 per request)
    ↓                             — Builds ScreenedToken (address, symbol, market_cap, etc.)
    ↓
screener.screen_tokens(req)      — Filters by liq/mcap/traders/age
    ↓
WatchRunner._cycle_body()        — SCREEN → PARSE → Telegram
    ↓
replay.parse_token(rpc, token, mcap_threshold)
```

### Key Signatures

```python
# database.py — insert_token
def insert_token(self, *, address, symbol, name, launchpad_id, dex, pool_id,
                 curve_address, deploy_block, migration_block, migration_tx,
                 honeypot, start_mcap=None) -> None

# token_index.py — enrich_pending
async def enrich_pending(self, *, stale_limit=None, on_progress=None) -> None:
    # Inside: for each entry, either:
    #   entry.screened = _pair_to_screened(addr, {}, best)  # has market_cap
    #   entry.screened = self._minimal(entry)                # placeholder, no mcap

# replay.py — parse_token (returns TokenParseResult with .buyers list)
async def parse_token(rpc, token, mcap_threshold, on_progress=None, *,
                      exclude_honeypots=True, wallet_filters=None) -> TokenParseResult

# migration_scan.py — store_migration
async def store_migration(event, rpc=None) -> dict  # stores to migrated_tokens
```

## Goal

Build an **MCAP Tracker** system that:

1. **Discovers** tokens with mcap > 0 and < 50k (from `token_index.enrich_pending()`)
2. **Tracks** them — checks mcap every 5 minutes via DexScreener
3. **Detects trend** — growing (+20% in 4h), stable (±15%), falling (-30% in 4h), dead (-70% in 24h)
4. **Auto-analyzes** when mcap reaches 50k — runs `parse_token(mcap_threshold=50000)` and saves to `migrated_tokens`
5. **Auto-cleans** dead tokens (>70% drop in 24h or tracked >7 days)
6. **No Telegram notifications** — just adds to migrated_tokens list

---

## Requirements

### 1. Database — New Tables (`database.py`)

Add to `_init_schema()`:

```sql
CREATE TABLE IF NOT EXISTS token_mcap_tracker (
    address TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    launchpad_id TEXT,
    dex TEXT,
    pool_id TEXT,
    first_seen_mcap REAL,
    current_mcap REAL,
    peak_mcap REAL,
    last_checked_at TEXT,
    trend TEXT DEFAULT 'unknown',
    trend_since TEXT,
    added_at TEXT,
    target_reached_at TEXT
);

CREATE TABLE IF NOT EXISTS mcap_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_address TEXT NOT NULL,
    mcap REAL NOT NULL,
    price_usd REAL,
    liquidity_usd REAL,
    checked_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mcap_snapshots_addr_time 
    ON mcap_snapshots(token_address, checked_at);
```

Add these CRUD methods to the `Database` class (following the existing pattern: sync method + async wrapper via `_run()`):

```python
# insert_mcap_tracker — INSERT OR IGNORE when token doesn't exist yet
def insert_mcap_tracker(self, *, address, symbol="", name="", launchpad_id="",
                        dex="", pool_id="", first_seen_mcap=0.0) -> None

# update_mcap_tracker — UPDATE current_mcap, peak_mcap, last_checked_at, trend, trend_since
def update_mcap_tracker(self, address, *, current_mcap, peak_mcap=None,
                        last_checked_at, trend, trend_since) -> None

# get_mcap_tracker_pending — SELECT WHERE target_reached_at IS NULL
def get_mcap_tracker_pending(self) -> list[dict]

# get_mcap_tracker_all — SELECT all rows (for API)
def get_mcap_tracker_all(self) -> list[dict]

# get_mcap_tracker_one — SELECT one by address
def get_mcap_tracker_one(self, address: str) -> dict | None

# insert_mcap_snapshot
def insert_mcap_snapshot(self, *, token_address, mcap, price_usd=None,
                         liquidity_usd=None, checked_at) -> None

# get_mcap_snapshots — SELECT WHERE token_address=? AND checked_at >= ?
def get_mcap_snapshots(self, token_address, since_iso) -> list[dict]

# update_mcap_target_reached — SET target_reached_at WHERE address=?
def update_mcap_target_reached(self, address, target_reached_at) -> None

# delete_mcap_tracker — DELETE WHERE address=?
def delete_mcap_tracker(self, address) -> None

# cleanup_mcap_tracker — DELETE WHERE (trend='dead' AND last_checked_at < 24h ago)
#                        OR (added_at < max_age_days ago)
def cleanup_mcap_tracker(self, *, max_age_days=7, dead_hours=24) -> int
```

Each needs a matching `async def a*` method that calls `await self._run(lambda: self.*(...))`.

### 2. Config (`config.py`)

Add to `Settings`:

```python
mcap_tracker_enabled: bool = True
mcap_tracker_interval_sec: int = 300   # check every 5 minutes
mcap_tracker_target: float = 50_000.0
mcap_tracker_max_age_days: int = 7
mcap_tracker_min_growth_pct: float = 20.0   # +20% in 4h = growing
mcap_tracker_dead_pct: float = 70.0         # -70% in 24h = dead
```

### 3. Models (`models.py`)

```python
class McapTrackerRow(BaseModel):
    address: str
    symbol: str | None = None
    name: str | None = None
    launchpad_id: str | None = None
    dex: str | None = None
    pool_id: str | None = None
    first_seen_mcap: float | None = None
    current_mcap: float | None = None
    peak_mcap: float | None = None
    last_checked_at: str | None = None
    trend: str = "unknown"
    trend_since: str | None = None
    added_at: str | None = None
    target_reached_at: str | None = None

class McapSnapshotRow(BaseModel):
    id: int
    token_address: str
    mcap: float
    price_usd: float | None = None
    liquidity_usd: float | None = None
    checked_at: str

class McapTrackerAddRequest(BaseModel):
    address: str
    symbol: str = ""
    name: str = ""
    launchpad_id: str = ""
    dex: str = ""
    first_seen_mcap: float = 0.0
```

Also add `McapTrackerConfig` to `WatchConfig`:

```python
class McapTrackerConfig(BaseModel):
    enabled: bool = True
    interval_sec: int = 300
    target_mcap: float = 50_000.0
    max_age_days: int = 7

class WatchConfig(BaseModel):
    # ... existing fields ...
    mcap_tracker: McapTrackerConfig = Field(default_factory=McapTrackerConfig)
```

### 4. Core Module: `mcap_checker.py` — NEW FILE

Location: `backend/app/mcap_checker.py`

```python
"""Periodic MCAP tracker: discover pre-50k tokens, trigger analysis at 50k."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from .config import settings
from .database import get_db

logger = logging.getLogger(__name__)

# Trend detection thresholds
GROWTH_PCT = settings.mcap_tracker_min_growth_pct  # 20
DEAD_PCT = settings.mcap_tracker_dead_pct           # 70
STABLE_WINDOW_HOURS = 4
DEAD_WINDOW_HOURS = 24


async def _fetch_mcap_batch(rpc, addresses: list[str]) -> dict[str, float]:
    """
    Fetch current mcap from DexScreener in batches of 30.
    
    Returns dict {address.lower(): mcap}
    Uses same API as token_index: GET /tokens/v1/robinhood/{addrs}
    Picks best pair by liquidity for each token.
    """
    result: dict[str, float] = {}
    batch_size = 30
    async with httpx.AsyncClient(timeout=10) as client:
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i:i + batch_size]
            url = f"https://api.dexscreener.com/tokens/v1/robinhood/{','.join(batch)}"
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    pairs = resp.json()
                    # Group by token address
                    by_token: dict[str, list] = {}
                    for p in pairs:
                        for side in ("baseToken", "quoteToken"):
                            a = (p.get(side) or {}).get("address", "")
                            if a:
                                by_token.setdefault(a.lower(), []).append(p)
                    for addr, addr_pairs in by_token.items():
                        best = max(addr_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0)))
                        mcap = best.get("marketCap") or best.get("fdv") or 0
                        result[addr] = float(mcap)
            except Exception as exc:
                logger.debug("DexScreener batch failed: %s", exc)
    return result


def _detect_trend(snapshots: list[dict]) -> str:
    """
    Determine mcap trend from snapshot history.
    
    snapshots: [{"mcap": float, "checked_at": str}, ...] sorted ASC by time
    Returns: 'growing' | 'stable' | 'falling' | 'dead' | 'unknown'
    
    Logic:
    - Insufficient data (<2 snapshots) → 'unknown'
    - -70% in 24h → 'dead'
    - +20% in 4h → 'growing'
    - -30% in 4h → 'falling'
    - ±15% in 4h → 'stable'
    - Otherwise → 'unknown'
    """
    if len(snapshots) < 2:
        return "unknown"
    
    now = datetime.now(timezone.utc)
    four_hours_ago = now - timedelta(hours=STABLE_WINDOW_HOURS)
    day_ago = now - timedelta(hours=DEAD_WINDOW_HOURS)
    
    def parse_ts(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    
    recent = [s for s in snapshots if parse_ts(s["checked_at"]) >= four_hours_ago]
    day_hist = [s for s in snapshots if parse_ts(s["checked_at"]) >= day_ago]
    
    if not recent:
        return "unknown"
    
    # Dead check: -70% in 24h
    if len(day_hist) >= 2:
        first = day_hist[0]["mcap"]
        last_mcap = day_hist[-1]["mcap"]
        if first > 0 and ((last_mcap - first) / first) * 100 <= -DEAD_PCT:
            return "dead"
    
    # Trend in 4h window
    if len(recent) >= 2:
        first = recent[0]["mcap"]
        last_mcap = recent[-1]["mcap"]
        if first > 0:
            change = ((last_mcap - first) / first) * 100
            if change >= GROWTH_PCT:
                return "growing"
            elif change <= -30:
                return "falling"
            elif abs(change) <= 15:
                return "stable"
    
    return "unknown"


async def check_mcap_tracker() -> None:
    """
    Main check loop.
    
    1. Fetch pending tokens from token_mcap_tracker (target_reached_at IS NULL)
    2. Batch fetch current mcap from DexScreener (30 per request)
    3. Save snapshot to mcap_snapshots
    4. Update current_mcap, peak_mcap, trend
    5. If mcap >= target → run parse_token() → store_migration() → remove from tracker
    6. Cleanup dead/old tokens
    """
    db = get_db()
    target = settings.mcap_tracker_target
    
    pending = await db.aget_mcap_tracker_pending()
    if not pending:
        await db.acleanup_mcap_tracker(
            max_age_days=settings.mcap_tracker_max_age_days
        )
        return
    
    addresses = [t["address"] for t in pending]
    mcap_map = await _fetch_mcap_batch(None, addresses)
    now_iso = datetime.now(timezone.utc).isoformat()
    rpc = None  # lazy import RpcClient when needed for analysis
    
    for token in pending:
        addr = token["address"]
        current_mcap = mcap_map.get(addr.lower(), 0.0)
        if current_mcap <= 0:
            continue
        
        # Save snapshot
        await db.ainsert_mcap_snapshot(
            token_address=addr,
            mcap=current_mcap,
            price_usd=None,
            liquidity_usd=None,
            checked_at=now_iso,
        )
        
        # Update tracker
        peak = max(token.get("peak_mcap") or 0, current_mcap)
        
        snapshots = await db.aget_mcap_snapshots(
            addr,
            since_iso=(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        )
        trend = _detect_trend(snapshots)
        trend_since = token.get("trend_since")
        if trend != token.get("trend"):
            trend_since = now_iso
        
        await db.aupdate_mcap_tracker(
            addr,
            current_mcap=current_mcap,
            peak_mcap=peak,
            last_checked_at=now_iso,
            trend=trend,
            trend_since=trend_since,
        )
        
        # If reached target → analyze
        if current_mcap >= target:
            logger.info("MCAP Tracker: %s reached %.0f — running analysis", addr[:10], current_mcap)
            try:
                from .migration_pipeline import parse_token_migration
                result = await parse_token_migration(addr)
                if result.get("ok"):
                    await db.aupdate_mcap_target_reached(addr, now_iso)
                    await db.adelete_mcap_tracker(addr)
                    logger.info("MCAP Tracker: %s stored (mcap=%.0f)", addr[:10], current_mcap)
                else:
                    logger.debug("MCAP Tracker: %s analysis failed: %s", addr[:10], result.get("message"))
            except Exception as exc:
                logger.exception("MCAP Tracker: analysis failed for %s", addr[:10])
    
    # Cleanup
    deleted = await db.acleanup_mcap_tracker(
        max_age_days=settings.mcap_tracker_max_age_days
    )
    if deleted:
        logger.info("MCAP Tracker: cleaned up %d dead/old tokens", deleted)


async def add_token_to_tracker(
    token_address: str,
    symbol: str = "",
    name: str = "",
    launchpad_id: str = "",
    dex: str = "",
    pool_id: str = "",
    first_seen_mcap: float = 0.0,
) -> bool:
    """
    Add a token to the mcap tracker.
    Returns True if added, False if duplicate or already migrated.
    """
    db = get_db()
    
    # Skip if already migrated
    if await db.aget_token(token_address):
        return False
    # Skip if already tracked
    if await db.aget_mcap_tracker_one(token_address):
        return False
    
    now_iso = datetime.now(timezone.utc).isoformat()
    await db.ainsert_mcap_tracker(
        address=token_address,
        symbol=symbol,
        name=name,
        launchpad_id=launchpad_id,
        dex=dex,
        pool_id=pool_id,
        first_seen_mcap=first_seen_mcap,
    )
    await db.ainsert_mcap_snapshot(
        token_address=token_address,
        mcap=first_seen_mcap,
        price_usd=None,
        liquidity_usd=None,
        checked_at=now_iso,
    )
    logger.info("MCAP Tracker: added %s (%s) mcap=%.0f", token_address[:10], symbol, first_seen_mcap)
    return True
```

### 5. Token Index Hook (`token_index.py`)

In `enrich_pending()`, after each token is enriched (line 333 or 335), add:

```python
# After entry.screened is set (line 333 or 335):
if entry.screened and entry.screened.market_cap > 0:
    from .mcap_checker import add_token_to_tracker
    target = settings.mcap_tracker_target
    
    if entry.screened.market_cap < target:
        # Below target → track it
        asyncio.ensure_future(add_token_to_tracker(
            token_address=entry.address,
            symbol=entry.screened.symbol,
            name=entry.screened.name,
            dex=entry.dex,
            pool_id=entry.pool_id or "",
            first_seen_mcap=entry.screened.market_cap,
        ))
    else:
        # Already at 50k+ → analyze immediately
        asyncio.ensure_future(_analyze_already_above(entry.address, entry.screened.market_cap))
```

Add `_analyze_already_above` helper (or inline it):

```python
async def _analyze_already_above(token: str, mcap: float):
    from .migration_pipeline import parse_token_migration
    from .database import get_db
    db = get_db()
    existing = await db.aget_token(token)
    if existing:
        return
    result = await parse_token_migration(token)
    if result.get("ok"):
        logger.info("MCAP: %s already at %.0f — analyzed", token[:10], mcap)
```

**Important:** Use `asyncio.ensure_future()` (not `await`) inside `enrich_pending()` because it's a hot loop and should not block.

### 6. Watch Integration (`watch.py`)

In `WatchRunner._cycle_body()`, after the SCREEN phase completes and before PARSE phase, add:

```python
# MCAP Tracker phase
if cfg.mcap_tracker and cfg.mcap_tracker.enabled:
    await prog("mcap_tracker", "Checking MCAP tracker…", 0.95)
    try:
        from .mcap_checker import check_mcap_tracker
        await check_mcap_tracker()
    except Exception as exc:
        logger.exception("MCAP tracker check failed")
```

### 7. API Endpoints (`main.py`)

```python
@app.get("/api/mcap-tracker")
async def get_mcap_tracker_list():
    """List all tracked tokens."""
    db = get_db()
    tokens = await db.aget_mcap_tracker_all()
    return [McapTrackerRow(**t).model_dump() for t in tokens]

@app.get("/api/mcap-tracker/{address}")
async def get_mcap_tracker_detail(address: str):
    """Get token detail with mcap history."""
    db = get_db()
    token = await db.aget_mcap_tracker_one(address)
    if not token:
        raise HTTPException(404, "Token not found in tracker")
    snapshots = await db.aget_mcap_snapshots(
        address,
        since_iso=(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    )
    return {
        "token": McapTrackerRow(**token).model_dump(),
        "snapshots": [McapSnapshotRow(**s).model_dump() for s in snapshots],
    }

@app.post("/api/mcap-tracker")
async def add_mcap_tracker(req: McapTrackerAddRequest):
    """Manually add a token to tracker."""
    from .mcap_checker import add_token_to_tracker
    added = await add_token_to_tracker(
        token_address=req.address,
        symbol=req.symbol,
        name=req.name,
        launchpad_id=req.launchpad_id,
        dex=req.dex,
        first_seen_mcap=req.first_seen_mcap,
    )
    if not added:
        raise HTTPException(400, "Token already tracked or migrated")
    return {"ok": True}

@app.delete("/api/mcap-tracker/{address}")
async def delete_mcap_tracker(address: str):
    """Remove token from tracker."""
    db = get_db()
    await db.adelete_mcap_tracker(address)
    return {"ok": True}
```

Add imports at top of `main.py`: `from .models import McapTrackerRow, McapSnapshotRow, McapTrackerAddRequest`

### 8. Frontend — MCAP Tracker Tab (`SnipersPage.tsx`)

1. Add `'mcap-tracker'` to the `Tab` union type
2. Add state: `mcapTokens` (array of McapTrackerRow), `mcapLoading` (bool)
3. Add load logic in the `load()` handler for `tab === 'mcap-tracker'`:
   ```tsx
   const r = await fetch('/api/mcap-tracker')
   setMcapTokens(await r.json())
   ```
4. Add auto-refresh every 30s (same pattern as existing `setInterval` for status)
5. Add tab button in the nav:
   ```tsx
   ['mcap-tracker', 'MCAP Tracker']
   ```
6. Add table rendering for `tab === 'mcap-tracker'`:
   ```
   | Token (symbol + addr, click to copy) | Current MCAP | Peak MCAP | Trend | Time in Tracker |
   ```
   - Trend: colored badge (green='growing', yellow='stable', orange='falling', red='dead', gray='unknown')
   - Current/Peak MCAP: formatted like "$12.3k", "$1.2M"
   - Click address → copy to clipboard (reuse existing copyAddr function)
   - Add "Analyze" button per row: calls POST `/api/migrations/parse` with the token address

### 9. Watch Config (`watch.json`)

Add `mcap_tracker` section:

```json
{
  "mcap_tracker": {
    "enabled": true,
    "interval_sec": 300,
    "target_mcap": 50000.0,
    "max_age_days": 7
  }
}
```

### 10. Background Loop (optional but recommended)

If the watch pipeline is disabled or doesn't run frequently enough, add a standalone background task in `main.py` that runs `check_mcap_tracker()` every 5 minutes:

```python
async def _mcap_tracker_loop():
    while True:
        try:
            await asyncio.sleep(settings.mcap_tracker_interval_sec)
            if settings.mcap_tracker_enabled:
                from .mcap_checker import check_mcap_tracker
                await check_mcap_tracker()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("MCAP tracker loop error")
```

Start it in the lifespan handler alongside the watch runner.

---

## Implementation Order

1. **`config.py`** — add mcap_tracker settings
2. **`database.py`** — add tables, CRUD methods
3. **`models.py`** — add McapTrackerRow, McapSnapshotRow, McapTrackerConfig, McapTrackerAddRequest
4. **`mcap_checker.py`** — implement core logic
5. **`token_index.py`** — hook in enrich_pending
6. **`watch.py`** — integrate check_mcap_tracker
7. **`main.py`** — API endpoints, bg loop
8. **`SnipersPage.tsx`** — MCAP Tracker tab
9. **`watch.json`** — add mcap_tracker section

## Important Notes

1. **DexScreener batch limit**: 30 addresses per request. The `_fetch_mcap_batch` function handles this.
2. **Rate limiting**: DexScreener allows ~300 req/min. Requests are sequential (no concurrency needed).
3. **SQLite WAL**: All DB writes through `_run()` pattern. `ainsert_mcap_snapshot` is called every check — high volume but no issue with WAL mode.
4. **Dedup**: `add_token_to_tracker` checks both `migrated_tokens` and existing tracker entries.
5. **Error handling**: All exceptions in `check_mcap_tracker` are caught and logged — never crash the watch loop.
6. **No Telegram**: Analysis at 50k just stores to `migrated_tokens`. No notifications.
7. **Existing migration system stays unchanged**: bags/hoodfun WS subscriptions and gap-fill continue working as before.
