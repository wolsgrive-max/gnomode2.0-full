import { useCallback, useEffect, useState } from 'react'

type Tab = 'tokens' | 'mcap-tracker' | 'snipers' | 'trades' | 'blacklist'

type BusStatus = {
  enabled: boolean
  running: boolean
  last_seen_block: number
  queue_size: number
  wss_url: string
}

type SniperRow = {
  address: string
  first_seen: string | null
  trade_count: number
  winrate: number | null
  first_token?: string | null
  first_mcap?: number | null
  is_active?: boolean
}

type UserFilters = {
  chat_id: string
  min_buy_usd: number
  max_mcap_usd: number
  exclude_honeypots: boolean
  min_liq_usd: number
  max_liq_usd: number
}

type FollowStatus = {
  enabled: boolean
  running: boolean
  last_block: number
  tracked_cached: number
  trades_seen: number
  alerts_sent: number
  last_message: string
}


type MigratedToken = {
  address: string
  symbol: string | null
  name: string | null
  launchpad_id: string | null
  dex: string | null
  migration_block: number | null
  migration_tx: string | null
  honeypot: boolean
  created_at: string | null
}

type TradeRow = {
  id: number
  wallet: string
  token: string
  mcap_at_trade: number | null
  amount_usd: number | null
  tx_hash: string | null
  block: number | null
}

type BlacklistRow = {
  address: string
  reason: string | null
  source: string | null
  created_at: string | null
}

type McapTrackerRow = {
  address: string
  symbol: string | null
  name: string | null
  launchpad_id: string | null
  dex: string | null
  pool_id: string | null
  first_seen_mcap: number | null
  current_mcap: number | null
  peak_mcap: number | null
  last_checked_at: string | null
  trend: string
  trend_since: string | null
  added_at: string | null
  target_reached_at: string | null
}

type ParseResult = {
  ok: boolean
  token: string
  launchpad_id: string
  dex: string
  honeypot: boolean
  snipers: number
  new_pairs: number
  message: string
}

const GMGN = (a: string) => `https://gmgn.ai/robinhood/address/${a}`
const GMGN_TOKEN = (a: string) => `https://gmgn.ai/robinhood/token/${a}`
const BS = (a: string) => `https://robinhoodchain.blockscout.com/address/${a}`
const BS_TX = (h: string) => `https://robinhoodchain.blockscout.com/tx/${h}`

const LAUNCHPADS = [
  { id: '', label: 'Авто' },
  { id: 'bags', label: 'Bags' },
  { id: 'hoodfun', label: 'hood.fun' },
  { id: 'flap', label: 'Flap' },
  { id: 'clanker', label: 'Clanker' },
  { id: 'unknown_v3', label: 'Unknown V3' },
  { id: 'unknown_v4', label: 'Unknown V4' },
]

function fmtMcap(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n) || n <= 0) return '—'
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}k`
  return `$${n.toFixed(0)}`
}

function trendColor(trend: string): string {
  switch (trend) {
    case 'growing':
      return '#2f9e44'
    case 'stable':
      return '#e6a817'
    case 'falling':
      return '#e8590c'
    case 'dead':
      return '#c92a2a'
    default:
      return 'var(--muted)'
  }
}

function timeInTracker(addedAt: string | null): string {
  if (!addedAt) return '—'
  const t = Date.parse(addedAt)
  if (!Number.isFinite(t)) return '—'
  const hours = Math.max(0, (Date.now() - t) / 3_600_000)
  if (hours < 1) return `${Math.round(hours * 60)}м`
  if (hours < 48) return `${hours.toFixed(1)}ч`
  return `${(hours / 24).toFixed(1)}д`
}

export default function SnipersPage() {
  const [tab, setTab] = useState<Tab>('tokens')
  const [status, setStatus] = useState<BusStatus | null>(null)
  const [snipers, setSnipers] = useState<SniperRow[]>([])
  const [filters, setFilters] = useState<UserFilters | null>(null)
  const [follow, setFollow] = useState<FollowStatus | null>(null)
  const [tokens, setTokens] = useState<MigratedToken[]>([])
  const [mcapTokens, setMcapTokens] = useState<McapTrackerRow[]>([])
  const [trades, setTrades] = useState<TradeRow[]>([])
  const [blacklist, setBlacklist] = useState<BlacklistRow[]>([])
  const [walletFilter, setWalletFilter] = useState('')
  const [tokenFilter, setTokenFilter] = useState('')
  const [blAddr, setBlAddr] = useState('')
  const [blReason, setBlReason] = useState('')
  const [parseToken, setParseToken] = useState('')
  const [parseLp, setParseLp] = useState('')
  const [parseBusy, setParseBusy] = useState(false)
  const [parseResult, setParseResult] = useState<ParseResult | null>(null)
  const [scanHours, setScanHours] = useState('168')
  const [scanBusy, setScanBusy] = useState(false)
  const [scanMsg, setScanMsg] = useState<string | null>(null)
  const [scanPct, setScanPct] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [analyzeBusy, setAnalyzeBusy] = useState('')
  const [copiedAddr, setCopiedAddr] = useState('')

  const copyAddr = useCallback(async (addr: string) => {
    try {
      await navigator.clipboard.writeText(addr)
      setCopiedAddr(addr)
      setTimeout(() => setCopiedAddr((c) => (c === addr ? '' : c)), 1200)
    } catch { /* ignore */ }
  }, [])

  const loadStatus = useCallback(async () => {
    try {
      const r = await fetch('/api/migrations/status')
      if (r.ok) setStatus(await r.json())
    } catch {
      /* ignore */
    }
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      await loadStatus()
      if (tab === 'snipers') {
        const [r, rf, rfo] = await Promise.all([
          fetch('/api/snipers?limit=200'),
          fetch('/api/snipers/filters'),
          fetch('/api/snipers/follow/status'),
        ])
        if (!r.ok) throw new Error(await r.text())
        setSnipers(await r.json())
        if (rf.ok) setFilters(await rf.json())
        if (rfo.ok) setFollow(await rfo.json())
      } else if (tab === 'tokens') {
        const r = await fetch('/api/migrations?limit=200')
        if (!r.ok) throw new Error(await r.text())
        setTokens(await r.json())
      } else if (tab === 'mcap-tracker') {
        const r = await fetch('/api/mcap-tracker')
        if (!r.ok) throw new Error(await r.text())
        setMcapTokens(await r.json())
      } else if (tab === 'trades') {
        const q = new URLSearchParams()
        if (walletFilter.trim()) q.set('wallet', walletFilter.trim())
        if (tokenFilter.trim()) q.set('token', tokenFilter.trim())
        q.set('limit', '200')
        const r = await fetch(`/api/trades?${q}`)
        if (!r.ok) throw new Error(await r.text())
        setTrades(await r.json())
      } else {
        const r = await fetch('/api/blacklist')
        if (!r.ok) throw new Error(await r.text())
        setBlacklist(await r.json())
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [tab, walletFilter, tokenFilter, loadStatus])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    const id = window.setInterval(() => void loadStatus(), 8_000)
    return () => window.clearInterval(id)
  }, [loadStatus])

  useEffect(() => {
    if (tab !== 'mcap-tracker') return
    const id = window.setInterval(() => void load(), 30_000)
    return () => window.clearInterval(id)
  }, [tab, load])

  async function runScan() {
    setScanBusy(true)
    setError(null)
    setScanMsg('Запуск скана…')
    setScanPct(0)
    setParseResult(null)
    try {
      const hours = Number(scanHours) || 168
      const start = await fetch('/api/migrations/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hours, max_tokens: 500 }),
      })
      if (!start.ok) throw new Error(await start.text())
      const job = await start.json()
      const jobId = job.job_id as string

      for (;;) {
        await new Promise((r) => setTimeout(r, 1200))
        const r = await fetch(`/api/migrations/scan/${jobId}`)
        if (!r.ok) throw new Error(await r.text())
        const st = await r.json()
        setScanMsg(st.progress?.message || st.status)
        setScanPct(Number(st.progress?.percent || 0))
        if (st.status === 'done') {
          const res = st.result || {}
          const saved = Number(res.processed ?? res.found ?? 0)
          const found = Number(res.candidates ?? 0)
          const skipped = Number(res.skipped ?? 0)
          setParseResult({
            ok: saved > 0,
            token: '',
            launchpad_id: '',
            dex: '',
            honeypot: false,
            snipers: Number(res.snipers || 0),
            new_pairs: 0,
            message:
              st.progress?.message ||
              `Сохранено ${saved} из ${found} (пропущено ${skipped})`,
          })
          setTab('tokens')
          await load()
          break
        }
        if (st.status === 'error') {
          throw new Error(st.error || st.progress?.message || 'Ошибка скана')
        }
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setScanBusy(false)
    }
  }

  async function runParse() {
    const token = parseToken.trim()
    if (!token.startsWith('0x') || token.length < 42) {
      setError('Нужен адрес токена 0x…')
      return
    }
    setParseBusy(true)
    setError(null)
    setParseResult(null)
    try {
      const body: { token: string; launchpad_id?: string } = { token }
      if (parseLp) body.launchpad_id = parseLp
      const r = await fetch('/api/migrations/parse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!r.ok) throw new Error(await r.text())
      const res = (await r.json()) as ParseResult
      setParseResult(res)
      setTab('tokens')
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setParseBusy(false)
    }
  }

  async function addBlacklist() {
    const address = blAddr.trim()
    if (!address) return
    const r = await fetch('/api/blacklist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ address, reason: blReason, source: 'manual' }),
    })
    if (!r.ok) {
      setError(await r.text())
      return
    }
    setBlAddr('')
    setBlReason('')
    await load()
  }

  async function removeBlacklist(address: string) {
    await fetch(`/api/blacklist/${address}`, { method: 'DELETE' })
    await load()
  }

  async function analyzeTracked(address: string) {
    setAnalyzeBusy(address)
    setError(null)
    try {
      const r = await fetch('/api/migrations/parse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: address }),
      })
      if (!r.ok) throw new Error(await r.text())
      const res = (await r.json()) as ParseResult
      setParseResult(res)
      if (res.ok) {
        await fetch(`/api/mcap-tracker/${address}`, { method: 'DELETE' })
        setTab('tokens')
      }
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setAnalyzeBusy('')
    }
  }

  async function removeTracked(address: string) {
    await fetch(`/api/mcap-tracker/${address}`, { method: 'DELETE' })
    await load()
  }

  async function syncRhj() {
    setLoading(true)
    try {
      const r = await fetch('/api/blacklist/sync-rhj', { method: 'POST' })
      if (!r.ok) throw new Error(await r.text())
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  const busLabel = !status
    ? '…'
    : !status.enabled
      ? 'выкл'
      : status.running
        ? 'live'
        : 'стоп'

  return (
    <section className="panel">
      <header className="hero" style={{ marginBottom: '1.25rem' }}>
        <h1>Миграции</h1>
        <p className="lede">
          «Найти миграции» ищет выпускников bonding-curve лаунчпадов: Bags, hood.fun, Flap.sh.
          Instant-launch (Clanker и т.п.) и сырые Uniswap пулы не сохраняются.
        </p>
      </header>

      <div
        className="meta-panel"
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '0.85rem 1.25rem',
          marginBottom: '1rem',
          fontSize: '0.92rem',
        }}
      >
        <span>
          WS: <strong>{busLabel}</strong>
        </span>
        <span>
          Block: <strong>{status?.last_seen_block || '—'}</strong>
        </span>
        <span>
          Queue: <strong>{status?.queue_size ?? '—'}</strong>
          {status && status.queue_size > 0 && status.running ? (
            <span style={{ color: 'var(--accent)', marginLeft: '0.5rem' }}>обрабатывается…</span>
          ) : null}
        </span>
        {status?.wss_url ? (
          <span style={{ color: 'var(--muted)', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {status.wss_url}
          </span>
        ) : null}
      </div>

      <div className="input-panel" style={{ marginBottom: '1.25rem' }}>
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.65rem',
            alignItems: 'flex-end',
            marginBottom: '0.85rem',
          }}
        >
          <label className="field compact" style={{ margin: 0 }}>
            <span>Окно поиска</span>
            <select value={scanHours} onChange={(e) => setScanHours(e.target.value)}>
              <option value="24">24 часа</option>
              <option value="72">3 дня</option>
              <option value="168">7 дней</option>
              <option value="336">14 дней</option>
              <option value="720">30 дней</option>
            </select>
          </label>
          <button
            type="button"
            className={scanBusy ? 'primary busy' : 'primary'}
            disabled={scanBusy}
            onClick={() => void runScan()}
          >
            {scanBusy ? 'Ищем миграции…' : 'Найти миграции'}
          </button>
          {scanBusy && (
            <span style={{ color: 'var(--muted)', fontSize: '0.9rem' }}>
              {Math.round(scanPct)}% · {scanMsg}
            </span>
          )}
        </div>
        {!scanBusy && scanMsg && !parseResult && (
          <p style={{ margin: '0 0 0.75rem', color: 'var(--muted)' }}>{scanMsg}</p>
        )}

        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.5rem',
            alignItems: 'flex-end',
          }}
        >
          <label className="field" style={{ flex: '1 1 18rem', margin: 0 }}>
            <span>Или парс одного токена</span>
            <input
              value={parseToken}
              onChange={(e) => setParseToken(e.target.value)}
              placeholder="0x… token address"
              spellCheck={false}
            />
          </label>
          <label className="field compact" style={{ margin: 0 }}>
            <span>Launchpad</span>
            <select value={parseLp} onChange={(e) => setParseLp(e.target.value)}>
              {LAUNCHPADS.map((lp) => (
                <option key={lp.id || 'auto'} value={lp.id}>
                  {lp.label}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className={parseBusy ? 'primary busy' : 'primary'}
            disabled={parseBusy || scanBusy}
            onClick={() => void runParse()}
          >
            {parseBusy ? 'Парсим…' : 'Парсить'}
          </button>
          <button
            type="button"
            className="ghost"
            disabled={scanBusy}
            onClick={async () => {
              try {
                const r = await fetch('/api/migrations/gap-fill', { method: 'POST', body: '{}', headers: {'Content-Type': 'application/json'} })
                setError(r.ok ? 'Gap-fill запущен' : await r.text())
                await load()
              } catch (e) { setError(String(e)) }
            }}
          >
            Gap-fill
          </button>
        </div>
        {parseResult && (
          <p style={{ margin: '0.65rem 0 0', color: 'var(--muted)' }}>
            {parseResult.message}
            {parseResult.honeypot ? ' · honeypot' : ''}
            {parseResult.snipers
              ? ` · snipers ${parseResult.snipers}`
              : ''}
          </p>
        )}
      </div>

      <div className="page-nav" style={{ marginBottom: '1rem' }}>
        {(
          [
            ['tokens', 'Токены'],
            ['mcap-tracker', 'MCAP Tracker'],
            ['snipers', 'Снайперы'],
            ['trades', 'Сделки'],
            ['blacklist', 'Blacklist'],
          ] as const
        ).map(([id, label]) => (
          <button
            key={id}
            type="button"
            className={tab === id ? 'nav-link active' : 'nav-link'}
            onClick={() => setTab(id)}
          >
            {label}
          </button>
        ))}
        <button type="button" className="nav-link" onClick={() => void load()} disabled={loading}>
          Обновить
        </button>
      </div>

      {error && (
        <p style={{ color: 'crimson', marginTop: 0 }} role="alert">
          {error}
        </p>
      )}

      {tab === 'tokens' && (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Токен</th>
                <th>Launchpad</th>
                <th>DEX</th>
                <th>Block</th>
                <th>HP</th>
                <th>Tx</th>
              </tr>
            </thead>
            <tbody>
              {tokens.map((t) => (
                <tr key={t.address}>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                      <a href={GMGN_TOKEN(t.address)} target="_blank" rel="noreferrer">
                        {t.symbol || t.address.slice(0, 10)}
                      </a>
                      <a href={BS(t.address)} target="_blank" rel="noreferrer">
                        bs
                      </a>
                    </div>
                    <div
                      className="mono muted"
                      onClick={() => copyAddr(t.address)}
                      style={{
                        fontSize: '0.75rem',
                        wordBreak: 'break-all',
                        cursor: 'pointer',
                      }}
                      title="Копировать адрес"
                    >
                      {copiedAddr === t.address ? 'Скопировано ✓' : t.address}
                    </div>
                  </td>
                  <td>{t.launchpad_id || '—'}</td>
                  <td>{t.dex || '—'}</td>
                  <td>{t.migration_block ?? '—'}</td>
                  <td>{t.honeypot ? 'yes' : 'no'}</td>
                  <td>
                    {t.migration_tx && !t.migration_tx.endsWith('0'.repeat(64)) ? (
                      <a href={BS_TX(t.migration_tx)} target="_blank" rel="noreferrer">
                        tx
                      </a>
                    ) : (
                      '—'
                    )}
                  </td>
                </tr>
              ))}
              {!tokens.length && !loading && (
                <tr>
                  <td colSpan={6}>Нет миграций — вставьте адрес выше или дождитесь WS</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'mcap-tracker' && (
        <div className="table-scroll">
          <p className="muted" style={{ marginTop: 0 }}>
            Токены с mcap &lt; $50k. Проверка каждые 5 мин; при достижении цели — авто-анализ
            в «Токены». Обновление вкладки каждые 30 сек.
          </p>
          <table>
            <thead>
              <tr>
                <th>Токен</th>
                <th>Current</th>
                <th>Peak</th>
                <th>Trend</th>
                <th>В трекере</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {mcapTokens.map((t) => (
                <tr key={t.address}>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                      <a href={GMGN_TOKEN(t.address)} target="_blank" rel="noreferrer">
                        {t.symbol || t.address.slice(0, 10)}
                      </a>
                      <a href={BS(t.address)} target="_blank" rel="noreferrer">
                        bs
                      </a>
                    </div>
                    <div
                      className="mono muted"
                      onClick={() => copyAddr(t.address)}
                      style={{
                        fontSize: '0.75rem',
                        wordBreak: 'break-all',
                        cursor: 'pointer',
                      }}
                      title="Копировать адрес"
                    >
                      {copiedAddr === t.address ? 'Скопировано ✓' : t.address}
                    </div>
                  </td>
                  <td>{fmtMcap(t.current_mcap)}</td>
                  <td>{fmtMcap(t.peak_mcap)}</td>
                  <td>
                    <span
                      style={{
                        color: trendColor(t.trend),
                        fontWeight: 600,
                        textTransform: 'uppercase',
                        fontSize: '0.8rem',
                      }}
                    >
                      {t.trend || 'unknown'}
                    </span>
                  </td>
                  <td>{timeInTracker(t.added_at)}</td>
                  <td style={{ whiteSpace: 'nowrap' }}>
                    <button
                      type="button"
                      className="primary"
                      style={{ marginRight: '0.35rem', padding: '0.25rem 0.55rem' }}
                      disabled={analyzeBusy === t.address || parseBusy}
                      onClick={() => void analyzeTracked(t.address)}
                    >
                      {analyzeBusy === t.address ? '…' : 'Analyze'}
                    </button>
                    <button
                      type="button"
                      style={{ padding: '0.25rem 0.55rem' }}
                      onClick={() => void removeTracked(t.address)}
                    >
                      ✕
                    </button>
                  </td>
                </tr>
              ))}
              {!mcapTokens.length && !loading && (
                <tr>
                  <td colSpan={6}>
                    Пусто — токены &lt;$50k подтянутся из token index после enrich
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'snipers' && (
        <>
          <div className="input-panel" style={{ marginBottom: '1rem' }}>
            <p style={{ margin: '0 0 0.5rem', color: 'var(--muted)' }}>
              Follow:{' '}
              <strong>
                {follow?.running ? 'online' : follow?.enabled ? 'starting…' : 'off'}
              </strong>
              {follow?.last_message ? ` · ${follow.last_message}` : ''}
              {follow ? ` · alerts ${follow.alerts_sent} · trades ${follow.trades_seen}` : ''}
            </p>
            {filters && (
              <div
                style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  gap: '0.75rem',
                  alignItems: 'flex-end',
                }}
              >
                <label className="field compact" style={{ margin: 0 }}>
                  <span>Min buy $</span>
                  <input
                    type="number"
                    value={filters.min_buy_usd}
                    onChange={(e) =>
                      setFilters({ ...filters, min_buy_usd: Number(e.target.value) || 0 })
                    }
                  />
                </label>
                <label className="field compact" style={{ margin: 0 }}>
                  <span>Max mcap $</span>
                  <input
                    type="number"
                    value={filters.max_mcap_usd}
                    onChange={(e) =>
                      setFilters({ ...filters, max_mcap_usd: Number(e.target.value) || 0 })
                    }
                  />
                </label>
                <label className="field check-field" style={{ margin: 0 }}>
                  <span>Honeypot</span>
                  <label className="check-inline">
                    <input
                      type="checkbox"
                      checked={filters.exclude_honeypots}
                      onChange={(e) =>
                        setFilters({ ...filters, exclude_honeypots: e.target.checked })
                      }
                    />
                    shield
                  </label>
                </label>
                <button
                  type="button"
                  className="primary"
                  onClick={() => {
                    void (async () => {
                      const r = await fetch('/api/snipers/filters', {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          min_buy_usd: filters.min_buy_usd,
                          max_mcap_usd: filters.max_mcap_usd,
                          exclude_honeypots: filters.exclude_honeypots,
                          min_liq_usd: filters.min_liq_usd,
                          max_liq_usd: filters.max_liq_usd,
                        }),
                      })
                      if (r.ok) setFilters(await r.json())
                    })()
                  }}
                >
                  Save filters
                </button>
                <span style={{ color: 'var(--muted)', fontSize: '0.85rem' }}>
                  Telegram: /filters
                </span>
              </div>
            )}
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Кошелёк</th>
                  <th>Trades</th>
                  <th>First token</th>
                  <th>First mcap</th>
                  <th>First seen</th>
                </tr>
              </thead>
              <tbody>
                {snipers.map((s) => (
                  <tr key={s.address}>
                    <td>
                      <a href={GMGN(s.address)} target="_blank" rel="noreferrer">
                        {s.address.slice(0, 10)}…
                      </a>{' '}
                      <a href={BS(s.address)} target="_blank" rel="noreferrer">
                        bs
                      </a>
                    </td>
                    <td>{s.trade_count}</td>
                    <td>
                      {s.first_token ? (
                        <a href={GMGN(s.first_token)} target="_blank" rel="noreferrer">
                          {s.first_token.slice(0, 8)}…
                        </a>
                      ) : (
                        '—'
                      )}
                    </td>
                    <td>
                      {s.first_mcap != null && Number.isFinite(s.first_mcap)
                        ? `$${Math.round(s.first_mcap).toLocaleString()}`
                        : '—'}
                    </td>
                    <td>
                      {s.first_seen ? new Date(s.first_seen).toLocaleString('ru-RU') : '—'}
                    </td>
                  </tr>
                ))}
                {!snipers.length && !loading && (
                  <tr>
                    <td colSpan={5}>Пока пусто — спарсите миграцию (snipers пишутся автоматически)</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}

      {tab === 'trades' && (
        <>
          <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
            <input
              placeholder="wallet"
              value={walletFilter}
              onChange={(e) => setWalletFilter(e.target.value)}
            />
            <input
              placeholder="token"
              value={tokenFilter}
              onChange={(e) => setTokenFilter(e.target.value)}
            />
            <button type="button" onClick={() => void load()}>
              Фильтр
            </button>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Wallet</th>
                  <th>Token</th>
                  <th>USD</th>
                  <th>Mcap</th>
                  <th>Block</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((t) => (
                  <tr key={t.id}>
                    <td>
                      <a href={GMGN(t.wallet)} target="_blank" rel="noreferrer">
                        {t.wallet.slice(0, 10)}…
                      </a>
                    </td>
                    <td>
                      <a href={GMGN_TOKEN(t.token)} target="_blank" rel="noreferrer">
                        {t.token.slice(0, 10)}…
                      </a>
                    </td>
                    <td>{t.amount_usd != null ? `$${t.amount_usd.toFixed(2)}` : '—'}</td>
                    <td>{t.mcap_at_trade != null ? `$${Math.round(t.mcap_at_trade)}` : '—'}</td>
                    <td>{t.block ?? '—'}</td>
                  </tr>
                ))}
                {!trades.length && !loading && (
                  <tr>
                    <td colSpan={5}>Нет сделок</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}

      {tab === 'blacklist' && (
        <>
          <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '0.75rem' }}>
            <input
              placeholder="0x…"
              value={blAddr}
              onChange={(e) => setBlAddr(e.target.value)}
              style={{ minWidth: '18rem' }}
            />
            <input
              placeholder="reason"
              value={blReason}
              onChange={(e) => setBlReason(e.target.value)}
            />
            <button type="button" onClick={() => void addBlacklist()}>
              Добавить
            </button>
            <button type="button" onClick={() => void syncRhj()}>
              Sync RHJ
            </button>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Address</th>
                  <th>Reason</th>
                  <th>Source</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {blacklist.map((b) => (
                  <tr key={b.address}>
                    <td>{b.address.slice(0, 12)}…</td>
                    <td>{b.reason || '—'}</td>
                    <td>{b.source || '—'}</td>
                    <td>
                      <button type="button" onClick={() => void removeBlacklist(b.address)}>
                        Удалить
                      </button>
                    </td>
                  </tr>
                ))}
                {!blacklist.length && !loading && (
                  <tr>
                    <td colSpan={4}>Blacklist пуст</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  )
}
