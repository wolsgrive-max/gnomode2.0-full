import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import './App.css'
import ScreenerPage from './ScreenerPage'
import WatchPage from './WatchPage'
import FollowupPage from './FollowupPage'
import HvatPage from './HvatPage'
import SettingsPage from './SettingsPage'
import MigrationsPage from './MigrationsPage'
import { FilterPresets } from './FilterPresets'
import { loadJson, saveJson } from './session'
import { useVisitedGmgnWallets } from './useVisitedGmgnWallets'

const BUYERS_SESSION_KEY = 'gnomode.session.buyers'
const APP_SESSION_KEY = 'gnomode.session.app'

type BuyerRow = {
  wallet: string
  token: string
  token_symbol: string
  bought_tokens: number
  bought_usd: number
  mcap_at_first_buy: number
  buys_count: number
  first_tx: string
  first_block: number
  wallet_balance_eth?: number | null
  hold_time_minutes?: number | null
  tokens_traded_7d?: number | null
}

type TokenResult = {
  token: string
  symbol: string
  name: string
  decimals: number
  total_supply: number
  pool: {
    address: string
    dex: string
    quote_symbol: string
    liquidity_usd: number
  } | null
  buyers: BuyerRow[]
  error: string | null
  stats: Record<string, unknown>
}

type JobProgress = {
  stage: string
  message: string
  percent: number
  current_token: string | null
}

type JobLogEntry = {
  ts: number
  stage: string
  message: string
  percent: number
  token: string | null
}

type JobResponse = {
  job_id: string
  status: 'queued' | 'running' | 'done' | 'error'
  progress: JobProgress
  log?: JobLogEntry[]
  results: TokenResult[]
  error: string | null
}

function formatLogTime(ts: number) {
  const d = new Date(ts * 1000)
  return d.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

type SortKey =
  | 'mcap_at_first_buy'
  | 'bought_usd'
  | 'bought_tokens'
  | 'buys_count'
  | 'wallet'
  | 'wallet_balance_eth'
  | 'hold_time_minutes'
  | 'tokens_traded_7d'
type AppPage = 'buyers' | 'migrations' | 'screener' | 'hvat' | 'watch' | 'followup' | 'settings'

type WalletFilters = {
  min_wallet_balance_eth: string
  max_wallet_balance_eth: string
  min_hold_time_minutes: string
  max_hold_time_minutes: string
  min_tokens_traded_7d: string
  max_tokens_traded_7d: string
}

type BuyersSession = {
  v: 1
  threshold: number
  excludeHoneypots: boolean
  walletFilters: WalletFilters
  query: string
  sortKey: SortKey
  sortAsc: boolean
  job: JobResponse | null
}

type AppSession = {
  v: 1
  page: AppPage
  buyerInput: string
}

function loadBuyersSession(): BuyersSession | null {
  const raw = loadJson<BuyersSession>(BUYERS_SESSION_KEY)
  if (!raw || raw.v !== 1) return null
  return raw
}

function slimJobForStorage(job: JobResponse | null): JobResponse | null {
  if (!job) return null
  const log = job.log ?? []
  return {
    ...job,
    log: log.length > 80 ? log.slice(-80) : log,
  }
}

const DEFAULT_WALLET_FILTERS: WalletFilters = {
  min_wallet_balance_eth: '',
  max_wallet_balance_eth: '',
  min_hold_time_minutes: '',
  max_hold_time_minutes: '',
  min_tokens_traded_7d: '',
  max_tokens_traded_7d: '',
}

/** Everything worth saving in a wallet-parsing preset. */
type WalletPreset = WalletFilters & {
  threshold: number
  exclude_honeypots: boolean
}

function parseOpt(raw: string): number | null {
  const t = raw.trim()
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

function shortAddr(a: string) {
  if (!a || a.length < 12) return a
  return `${a.slice(0, 6)}…${a.slice(-4)}`
}

function gmgnToken(addr: string) {
  return `https://gmgn.ai/robinhood/token/${addr}`
}

function gmgnWallet(addr: string) {
  return `https://gmgn.ai/robinhood/address/${addr}`
}

function fmtNum(n: number, digits = 2) {
  if (!Number.isFinite(n)) return '—'
  return n.toLocaleString(undefined, { maximumFractionDigits: digits })
}

function fmtHold(minutes: number | null | undefined) {
  if (minutes == null || !Number.isFinite(minutes)) return '—'
  if (minutes < 60) return `${fmtNum(minutes, 1)}m`
  if (minutes < 60 * 24) return `${fmtNum(minutes / 60, 1)}h`
  return `${fmtNum(minutes / (60 * 24), 1)}d`
}

function exportCsv(rows: BuyerRow[]) {
  const header = [
    'wallet',
    'token',
    'symbol',
    'bought_tokens',
    'bought_usd',
    'mcap_at_first_buy',
    'buys_count',
    'first_tx',
    'first_block',
    'wallet_balance_eth',
    'hold_time_minutes',
    'tokens_traded_7d',
  ]
  const lines = [header.join(',')]
  for (const r of rows) {
    lines.push(
      [
        r.wallet,
        r.token,
        r.token_symbol,
        r.bought_tokens,
        r.bought_usd,
        r.mcap_at_first_buy,
        r.buys_count,
        r.first_tx,
        r.first_block,
        r.wallet_balance_eth ?? '',
        r.hold_time_minutes ?? '',
        r.tokens_traded_7d ?? '',
      ].join(','),
    )
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `early-buyers-${Date.now()}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function EarlyBuyersPage({
  input,
  setInput,
}: {
  input: string
  setInput: (v: string) => void
}) {
  const restored = useMemo(() => loadBuyersSession(), [])
  const [threshold, setThreshold] = useState(restored?.threshold ?? 15000)
  const [excludeHoneypots, setExcludeHoneypots] = useState(
    restored?.excludeHoneypots ?? true,
  )
  const [walletFilters, setWalletFilters] = useState<WalletFilters>(
    restored?.walletFilters
      ? { ...DEFAULT_WALLET_FILTERS, ...restored.walletFilters }
      : DEFAULT_WALLET_FILTERS,
  )
  const [job, setJob] = useState<JobResponse | null>(restored?.job ?? null)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState(restored?.query ?? '')
  const [sortKey, setSortKey] = useState<SortKey>(
    restored?.sortKey ?? 'mcap_at_first_buy',
  )
  const [sortAsc, setSortAsc] = useState(restored?.sortAsc ?? true)
  const { markVisited, clearVisited, isVisited, visitedCount } = useVisitedGmgnWallets()
  const [busy, setBusy] = useState(
    () => restored?.job?.status === 'queued' || restored?.job?.status === 'running',
  )

  useEffect(() => {
    saveJson(BUYERS_SESSION_KEY, {
      v: 1,
      threshold,
      excludeHoneypots,
      walletFilters,
      query,
      sortKey,
      sortAsc,
      job: slimJobForStorage(job),
    } satisfies BuyersSession)
  }, [threshold, excludeHoneypots, walletFilters, query, sortKey, sortAsc, job])

  const setWalletFilter = <K extends keyof WalletFilters>(key: K, value: WalletFilters[K]) => {
    setWalletFilters((f) => ({ ...f, [key]: value }))
  }

  const walletPreset: WalletPreset = {
    ...walletFilters,
    threshold,
    exclude_honeypots: excludeHoneypots,
  }

  const applyWalletPreset = useCallback((values: WalletPreset) => {
    const { threshold: t, exclude_honeypots: hp, ...rest } = values
    setWalletFilters({ ...DEFAULT_WALLET_FILTERS, ...rest })
    if (typeof t === 'number' && Number.isFinite(t)) setThreshold(t)
    if (typeof hp === 'boolean') setExcludeHoneypots(hp)
  }, [])

  const allBuyers = useMemo(() => {
    if (!job?.results) return []
    return job.results.flatMap((r) =>
      r.buyers.map((b) => ({ ...b, token_symbol: b.token_symbol || r.symbol })),
    )
  }, [job])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    let rows = allBuyers
    if (q) {
      rows = rows.filter(
        (r) =>
          r.wallet.toLowerCase().includes(q) ||
          r.token.toLowerCase().includes(q) ||
          r.token_symbol.toLowerCase().includes(q),
      )
    }
    const mul = sortAsc ? 1 : -1
    return [...rows].sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (typeof av === 'string' && typeof bv === 'string') {
        return av.localeCompare(bv) * mul
      }
      const an = av == null ? -Infinity : (av as number)
      const bn = bv == null ? -Infinity : (bv as number)
      return (an - bn) * mul
    })
  }, [allBuyers, query, sortKey, sortAsc])

  useEffect(() => {
    if (!job || (job.status !== 'queued' && job.status !== 'running')) return
    const id = job.job_id
    const t = setInterval(async () => {
      try {
        const res = await fetch(`/api/parse/${id}`)
        if (res.status === 404) {
          // Server restarted — keep last snapshot from localStorage/state.
          setJob((prev) =>
            prev
              ? {
                  ...prev,
                  status: 'error',
                  error:
                    'Задача потеряна после перезапуска сервера. Показан последний сохранённый снимок.',
                  progress: {
                    ...prev.progress,
                    stage: 'error',
                    message: 'Задача потеряна — восстановлено из сессии',
                  },
                }
              : prev,
          )
          setBusy(false)
          return
        }
        if (!res.ok) throw new Error(await res.text())
        const data = (await res.json()) as JobResponse
        setJob(data)
        if (data.status === 'done' || data.status === 'error') {
          setBusy(false)
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e))
        setBusy(false)
      }
    }, 1200)
    return () => clearInterval(t)
  }, [job?.job_id, job?.status])

  const startParse = useCallback(async () => {
    setError(null)
    setJob(null)
    const tokens = input
      .split(/[\s,;]+/)
      .map((t) => t.trim())
      .filter(Boolean)
    if (!tokens.length) {
      setError('Вставьте хотя бы один адрес токена')
      return
    }
    setBusy(true)
    try {
      const res = await fetch('/api/parse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tokens,
          mcap_threshold: threshold,
          exclude_honeypots: excludeHoneypots,
          min_wallet_balance_eth: parseOpt(walletFilters.min_wallet_balance_eth),
          max_wallet_balance_eth: parseOpt(walletFilters.max_wallet_balance_eth),
          min_hold_time_minutes: parseOpt(walletFilters.min_hold_time_minutes),
          max_hold_time_minutes: parseOpt(walletFilters.max_hold_time_minutes),
          min_tokens_traded_7d: parseOpt(walletFilters.min_tokens_traded_7d),
          max_tokens_traded_7d: parseOpt(walletFilters.max_tokens_traded_7d),
        }),
      })
      if (!res.ok) throw new Error(await res.text())
      const data = (await res.json()) as JobResponse
      setJob(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setBusy(false)
    }
  }, [input, threshold, excludeHoneypots, walletFilters])

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortAsc((v) => !v)
    else {
      setSortKey(key)
      setSortAsc(key === 'mcap_at_first_buy')
    }
  }

  const progress = job?.progress.percent ?? 0
  const sortDir = sortAsc ? '↑' : '↓'
  const jobLog = job?.log ?? []
  const logEndRef = useRef<HTMLLIElement | null>(null)

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
  }, [jobLog.length, job?.progress.message])

  return (
    <>
      <header className="hero">
        <p className="brand">gnomode</p>
        <h1>Ранние покупатели на Robinhood Chain</h1>
        <p className="lede">
          Находит кошельки, которые купили токен, пока рыночная капитализация была ниже вашего порога.
        </p>
      </header>

      <section className="panel input-panel">
        <label className="field">
          <span>Адрес(а) токена</span>
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="0x… по одному в строке или через запятую"
            rows={4}
            spellCheck={false}
          />
        </label>
        <div className="row">
          <label className="field compact">
            <span>Порог mcap (USD)</span>
            <input
              type="number"
              min={0}
              step={500}
              value={threshold}
              onChange={(e) => setThreshold(Number(e.target.value) || 0)}
            />
          </label>
          <label className="field check-field">
            <span>Безопасность</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={excludeHoneypots}
                onChange={(e) => setExcludeHoneypots(e.target.checked)}
              />
                Пропускать honeypot (GMGN)
            </label>
          </label>
          <button
            className={`primary${busy ? ' busy' : ''}`}
            disabled={busy}
            onClick={startParse}
          >
            {busy ? 'Парсинг…' : 'Парсить'}
          </button>
        </div>
        <FilterPresets
          storageKey="gnomode.presets.wallets"
          current={walletPreset}
          onApply={applyWalletPreset}
          disabled={busy}
        />
        <div className="filter-grid">
          <label className="field">
            <span>Мин. баланс (ETH)</span>
            <input
              type="number"
              min={0}
              step={0.001}
              value={walletFilters.min_wallet_balance_eth}
              onChange={(e) => setWalletFilter('min_wallet_balance_eth', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. баланс (ETH)</span>
            <input
              type="number"
              min={0}
              step={0.001}
              value={walletFilters.max_wallet_balance_eth}
              onChange={(e) => setWalletFilter('max_wallet_balance_eth', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Мин. холд (мин)</span>
            <input
              type="number"
              min={0}
              value={walletFilters.min_hold_time_minutes}
              onChange={(e) => setWalletFilter('min_hold_time_minutes', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. холд (мин)</span>
            <input
              type="number"
              min={0}
              value={walletFilters.max_hold_time_minutes}
              onChange={(e) => setWalletFilter('max_hold_time_minutes', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Мин. токенов за 7д</span>
            <input
              type="number"
              min={0}
              value={walletFilters.min_tokens_traded_7d}
              onChange={(e) => setWalletFilter('min_tokens_traded_7d', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. токенов за 7д</span>
            <input
              type="number"
              min={0}
              value={walletFilters.max_tokens_traded_7d}
              onChange={(e) => setWalletFilter('max_tokens_traded_7d', e.target.value)}
              placeholder="любой"
            />
          </label>
        </div>
        {error && <p className="err">{error}</p>}
        {job && (job.status === 'queued' || job.status === 'running') && (
          <div className="progress-wrap">
            <div className="progress-meta">
              <span>{job.progress.message || job.progress.stage}</span>
              <span className="pct">{fmtNum(progress, 1)}%</span>
            </div>
            <div className="bar">
              <div className="bar-fill" style={{ width: `${Math.min(progress, 100)}%` }} />
            </div>
          </div>
        )}
        {job && jobLog.length > 0 && (
          <div className="job-log" aria-live="polite">
            <div className="job-log-head">
              <span>Лог парсинга</span>
              <span className="muted">{jobLog.length} шагов</span>
            </div>
            <ol className="job-log-list">
              {jobLog.map((entry, i) => (
                <li
                  key={`${entry.ts}-${i}`}
                  className="job-log-row"
                  ref={i === jobLog.length - 1 ? logEndRef : undefined}
                >
                  <time dateTime={new Date(entry.ts * 1000).toISOString()}>
                    {formatLogTime(entry.ts)}
                  </time>
                  <span className={`job-log-stage stage-${entry.stage}`}>{entry.stage}</span>
                  <span className="job-log-msg" title={entry.message}>
                    {entry.message}
                  </span>
                  <span className="job-log-pct">{fmtNum(entry.percent, 0)}%</span>
                </li>
              ))}
            </ol>
          </div>
        )}
        {job?.status === 'error' && <p className="err">{job.error || 'Ошибка задачи'}</p>}
      </section>

      {job?.results?.some((r) => r.error || r.pool) && (
        <section className="panel meta-panel">
          {job.results.map((r) => (
            <div key={r.token} className="token-meta">
              <div>
                <a
                  href={gmgnToken(r.token)}
                  target="_blank"
                  rel="noreferrer"
                  title={r.token}
                >
                  <strong>{r.symbol || shortAddr(r.token)}</strong>
                </a>
                {!r.error && (
                  <span className="badge">{r.buyers.length} кошельков</span>
                )}
                {r.error?.toLowerCase().includes('honeypot') && (
                  <span className="badge badge-warn">honeypot</span>
                )}
                <a
                  className="mono muted"
                  href={gmgnToken(r.token)}
                  target="_blank"
                  rel="noreferrer"
                  title={r.token}
                >
                  {' '}
                  {shortAddr(r.token)}
                </a>
              </div>
              {r.error ? (
                <p className="err">{r.error}</p>
              ) : (
                <p className="muted">
                  {r.pool?.dex} · {r.pool?.quote_symbol}
                  {r.pool && (
                    <>
                      {' · '}
                      <a
                        href={`https://robinhoodchain.blockscout.com/address/${r.pool.address}`}
                        target="_blank"
                        rel="noreferrer"
                      >
                        пул
                      </a>
                    </>
                  )}
                </p>
              )}
            </div>
          ))}
        </section>
      )}

      {allBuyers.length > 0 && (
        <section className="panel table-panel">
          <div className="table-toolbar">
            <input
              className="search"
              placeholder="Фильтр по кошельку / токену…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              spellCheck={false}
            />
            <div className="toolbar-right">
              <span className="muted">{filtered.length} кошельков</span>
              {visitedCount > 0 && (
                <>
                  <span className="muted visited-count" title="Открыто на GMGN в этом браузере">
                    {visitedCount} просмотрено
                  </span>
                  <button type="button" className="ghost" onClick={clearVisited}>
                    Сбросить просмотры
                  </button>
                </>
              )}
              <button type="button" className="ghost" onClick={() => exportCsv(filtered)}>
                Экспорт CSV
              </button>
            </div>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th
                    className={sortKey === 'wallet' ? 'active' : undefined}
                    data-dir={sortKey === 'wallet' ? sortDir : undefined}
                    onClick={() => toggleSort('wallet')}
                  >
                    Кошелёк
                  </th>
                  <th>Токен</th>
                  <th
                    className={sortKey === 'bought_tokens' ? 'active' : undefined}
                    data-dir={sortKey === 'bought_tokens' ? sortDir : undefined}
                    onClick={() => toggleSort('bought_tokens')}
                  >
                    Куплено
                  </th>
                  <th
                    className={sortKey === 'bought_usd' ? 'active' : undefined}
                    data-dir={sortKey === 'bought_usd' ? sortDir : undefined}
                    onClick={() => toggleSort('bought_usd')}
                  >
                    USD ≈
                  </th>
                  <th
                    className={sortKey === 'mcap_at_first_buy' ? 'active' : undefined}
                    data-dir={sortKey === 'mcap_at_first_buy' ? sortDir : undefined}
                    onClick={() => toggleSort('mcap_at_first_buy')}
                  >
                    Mcap при покупке
                  </th>
                  <th
                    className={sortKey === 'buys_count' ? 'active' : undefined}
                    data-dir={sortKey === 'buys_count' ? sortDir : undefined}
                    onClick={() => toggleSort('buys_count')}
                  >
                    Покупок
                  </th>
                  <th
                    className={sortKey === 'wallet_balance_eth' ? 'active' : undefined}
                    data-dir={sortKey === 'wallet_balance_eth' ? sortDir : undefined}
                    onClick={() => toggleSort('wallet_balance_eth')}
                  >
                    Баланс
                  </th>
                  <th
                    className={sortKey === 'hold_time_minutes' ? 'active' : undefined}
                    data-dir={sortKey === 'hold_time_minutes' ? sortDir : undefined}
                    onClick={() => toggleSort('hold_time_minutes')}
                  >
                    Холд
                  </th>
                  <th
                    className={sortKey === 'tokens_traded_7d' ? 'active' : undefined}
                    data-dir={sortKey === 'tokens_traded_7d' ? sortDir : undefined}
                    onClick={() => toggleSort('tokens_traded_7d')}
                  >
                    Токены 7д
                  </th>
                  <th>Tx</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => {
                  const viewed = isVisited(r.wallet)
                  return (
                  <tr
                    key={`${r.token}-${r.wallet}-${r.first_tx}`}
                    className={viewed ? 'row-visited' : undefined}
                  >
                    <td className="mono">
                      <a
                        href={gmgnWallet(r.wallet)}
                        target="_blank"
                        rel="noreferrer"
                        title={viewed ? `${r.wallet} (просмотрено на GMGN)` : r.wallet}
                        className={viewed ? 'wallet-link visited' : 'wallet-link'}
                        onClick={() => markVisited(r.wallet)}
                        onAuxClick={(e) => {
                          if (e.button === 1) markVisited(r.wallet)
                        }}
                      >
                        {shortAddr(r.wallet)}
                      </a>
                    </td>
                    <td>
                      <a
                        href={gmgnToken(r.token)}
                        target="_blank"
                        rel="noreferrer"
                        title={r.token}
                      >
                        {r.token_symbol || shortAddr(r.token)}
                      </a>
                    </td>
                    <td>{fmtNum(r.bought_tokens, 4)}</td>
                    <td>${fmtNum(r.bought_usd, 2)}</td>
                    <td>${fmtNum(r.mcap_at_first_buy, 0)}</td>
                    <td>{r.buys_count}</td>
                    <td>
                      {r.wallet_balance_eth != null
                        ? `${fmtNum(r.wallet_balance_eth, 4)} ETH`
                        : '—'}
                    </td>
                    <td>{fmtHold(r.hold_time_minutes)}</td>
                    <td>{r.tokens_traded_7d ?? '—'}</td>
                    <td className="mono">
                      {r.first_tx ? (
                        <a
                          href={`https://robinhoodchain.blockscout.com/tx/${r.first_tx}`}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {shortAddr(r.first_tx)}
                        </a>
                      ) : (
                        '—'
                      )}
                    </td>
                  </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {job?.status === 'done' && allBuyers.length === 0 && !job.results.some((r) => r.error) && (
        <p className="empty">
          Ранних покупателей ниже mcap ${fmtNum(threshold, 0)} не найдено.
        </p>
      )}
    </>
  )
}

export default function App() {
  const appRestored = useMemo(() => loadJson<AppSession>(APP_SESSION_KEY), [])
  const [page, setPage] = useState<AppPage>(appRestored?.page ?? 'buyers')
  const [buyerInput, setBuyerInput] = useState(appRestored?.buyerInput ?? '')

  useEffect(() => {
    saveJson(APP_SESSION_KEY, {
      v: 1,
      page,
      buyerInput,
    } satisfies AppSession)
  }, [page, buyerInput])

  const useInBuyers = useCallback((addresses: string[]) => {
    setBuyerInput(addresses.join('\n'))
    setPage('buyers')
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [])

  return (
    <div className="page">
      <nav className="page-nav" aria-label="Главное меню">
        <button
          type="button"
          className={page === 'migrations' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('migrations')}
        >
          Миграции
        </button>
        <button
          type="button"
          className={page === 'buyers' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('buyers')}
        >
          Кошельки
        </button>
        <button
          type="button"
          className={page === 'screener' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('screener')}
        >
          Скринер
        </button>
        <button
          type="button"
          className={page === 'hvat' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('hvat')}
        >
          Хвать
        </button>
        <button
          type="button"
          className={page === 'watch' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('watch')}
        >
          Автопарс
        </button>
        <button
          type="button"
          className={page === 'followup' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('followup')}
        >
          Follow-up
        </button>
        <button
          type="button"
          className={page === 'settings' ? 'nav-link active' : 'nav-link'}
          onClick={() => setPage('settings')}
        >
          Настройки
        </button>
      </nav>

      {/* Keep pages mounted so results survive tab switches */}
      <div hidden={page !== 'buyers'}>
        <EarlyBuyersPage input={buyerInput} setInput={setBuyerInput} />
      </div>
      <div hidden={page !== 'screener'}>
        <ScreenerPage onUseInBuyers={useInBuyers} />
      </div>
      <div hidden={page !== 'migrations'}>
        <MigrationsPage onParse={useInBuyers} />
      </div>
      <div hidden={page !== 'hvat'}>
        <HvatPage tabActive={page === 'hvat'} />
      </div>
      <div hidden={page !== 'watch'}>
        <WatchPage />
      </div>
      <div hidden={page !== 'followup'}>
        <FollowupPage tabActive={page === 'followup'} />
      </div>
      <div hidden={page !== 'settings'}>
        <SettingsPage />
      </div>

      <footer className="foot">
        Robinhood Chain · Uniswap V2/V3/V4 · публичный RPC по умолчанию
      </footer>
    </div>
  )
}
