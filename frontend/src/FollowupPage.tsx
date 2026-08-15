import { useCallback, useEffect, useState } from 'react'

type FollowupConfig = {
  enabled: boolean
  interval_sec: number
  max_mcap_alert: number
  min_mcap_alert: number | null
  min_bought_usd: number | null
  max_bought_usd: number | null
  alert_on_deals: number[]
  max_deals: number
  buys_only: boolean
  track_transfers: boolean
  telegram_chat_id: string
  telegram_topic_id: string
  bot_commands_enabled: boolean
  raybot_enabled: boolean
  ingest_from_watch: boolean
}

type JobLogEntry = {
  ts: number
  stage: string
  message: string
  percent: number
}

type FollowupStatus = {
  enabled: boolean
  running: boolean
  telegram_configured: boolean
  bot_commands_enabled: boolean
  bot_polling: boolean
  raybot_configured: boolean
  next_run_ts: number | null
  last_run_ts: number | null
  last_run_duration_sec: number | null
  last_error: string | null
  last_message: string
  wallets_watching: number
  wallets_done: number
  last_checked: number
  last_new_deals: number
  last_alerts_sent: number
  stop_requested: boolean
  log: JobLogEntry[]
}

type FollowupDeal = {
  wallet: string
  token: string
  token_symbol: string
  deal_index: number
  mcap_at_buy: number | null
  bought_usd: number | null
  notified: boolean
}

type FollowupWallet = {
  address: string
  status: string
  deal_count: number
  wallet_balance_eth: number | null
  tokens_traded_7d: number | null
  raybot_synced: boolean
  first_mcap: number | null
  deals: FollowupDeal[]
}

const DEFAULT_CFG: FollowupConfig = {
  enabled: false,
  interval_sec: 5,
  max_mcap_alert: 20000,
  min_mcap_alert: null,
  min_bought_usd: null,
  max_bought_usd: null,
  alert_on_deals: [2, 3, 4, 5],
  max_deals: 5,
  buys_only: true,
  track_transfers: false,
  telegram_chat_id: '',
  telegram_topic_id: '',
  bot_commands_enabled: true,
  raybot_enabled: false,
  ingest_from_watch: true,
}

function fmtTs(ts: number | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString()
}

function fmtNum(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return '—'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function numOrEmpty(v: number | null | undefined): string {
  return v == null || Number.isNaN(v) ? '' : String(v)
}

function parseOptional(raw: string): number | null {
  const t = raw.trim()
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

export default function FollowupPage({ tabActive = true }: { tabActive?: boolean }) {
  const [cfg, setCfg] = useState<FollowupConfig>(DEFAULT_CFG)
  const [status, setStatus] = useState<FollowupStatus | null>(null)
  const [wallets, setWallets] = useState<FollowupWallet[]>([])
  const [saving, setSaving] = useState(false)
  const [flash, setFlash] = useState('')

  const refresh = useCallback(async () => {
    const [c, s, w] = await Promise.all([
      fetch('/api/followup').then((r) => r.json()),
      fetch('/api/followup/status').then((r) => r.json()),
      fetch('/api/followup/wallets?limit=200').then((r) => r.json()),
    ])
    setCfg({ ...DEFAULT_CFG, ...c })
    setStatus(s)
    setWallets(Array.isArray(w) ? w : [])
  }, [])

  useEffect(() => {
    if (!tabActive) return
    void refresh().catch((e) => setFlash(String(e)))
    const tick = () => {
      if (document.visibilityState === 'hidden') return
      // Status + wallets only — avoid clobbering in-progress config edits.
      void (async () => {
        try {
          const [s, w] = await Promise.all([
            fetch('/api/followup/status').then((r) => r.json()),
            fetch('/api/followup/wallets?limit=200').then((r) => r.json()),
          ])
          setStatus(s)
          setWallets(Array.isArray(w) ? w : [])
        } catch {
          /* ignore */
        }
      })()
    }
    const id = window.setInterval(tick, 8000)
    const onVis = () => {
      if (document.visibilityState === 'visible') tick()
    }
    document.addEventListener('visibilitychange', onVis)
    return () => {
      window.clearInterval(id)
      document.removeEventListener('visibilitychange', onVis)
    }
  }, [tabActive, refresh])

  const save = async () => {
    setSaving(true)
    setFlash('')
    try {
      const res = await fetch('/api/followup', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfg),
      })
      if (!res.ok) throw new Error(await res.text())
      setCfg(await res.json())
      setFlash('Конфиг сохранён')
      await refresh()
    } catch (e) {
      setFlash(String(e))
    } finally {
      setSaving(false)
    }
  }

  const post = async (path: string) => {
    setFlash('')
    try {
      const res = await fetch(path, { method: 'POST' })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || res.statusText)
      setFlash(typeof data.message === 'string' ? data.message : 'OK')
      await refresh()
    } catch (e) {
      setFlash(String(e))
    }
  }

  return (
    <>
      <header className="hero">
        <p className="brand">gnomode</p>
        <h1>Follow-up кошельков</h1>
        <p className="lede">
          Свой Telegram-бот (без RayBot): первая сделка на низком mcap → таблица →
          алерт на 2-й/3-й новый токен только при низком mcap. Команды:{' '}
          <code>/status</code> <code>/filters</code> <code>/on</code> <code>/off</code>.
        </p>
      </header>

      <section className="panel meta-panel watch-status">
        <div className="watch-status-grid">
          <div>
            <span className="muted">Статус</span>
            <div>{status?.running ? 'цикл' : cfg.enabled ? 'ожидание' : 'выкл'}</div>
          </div>
          <div>
            <span className="muted">Watching / done</span>
            <div>
              {status?.wallets_watching ?? 0} / {status?.wallets_done ?? 0}
            </div>
          </div>
          <div>
            <span className="muted">Telegram-бот</span>
            <div>
              {status?.telegram_configured ? 'token ok' : 'нет токена'} ·{' '}
              {status?.bot_polling ? 'polling' : 'idle'}
            </div>
          </div>
          <div>
            <span className="muted">Последний / следующий</span>
            <div className="muted tiny">
              {fmtTs(status?.last_run_ts)} → {fmtTs(status?.next_run_ts)}
            </div>
          </div>
        </div>
        {status?.last_message ? <p className="watch-msg">{status.last_message}</p> : null}
        {status?.last_error ? <p className="watch-error">{status.last_error}</p> : null}
        {flash ? <p className="muted">{flash}</p> : null}
      </section>

      <section className="panel input-panel">
        <h2 className="section-title">Расписание и фильтры (нативные)</h2>
        <div className="row">
          <label className="field check-field">
            <span className="muted">Follow-up</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={cfg.enabled}
                onChange={(e) => setCfg({ ...cfg, enabled: e.target.checked })}
              />
              Вкл
            </label>
          </label>
          <label className="field check-field">
            <span className="muted">Автопарс → таблица</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={cfg.ingest_from_watch}
                onChange={(e) =>
                  setCfg({ ...cfg, ingest_from_watch: e.target.checked })
                }
              />
              Ingest
            </label>
          </label>
          <label className="field check-field">
            <span className="muted">Команды бота</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={cfg.bot_commands_enabled}
                onChange={(e) =>
                  setCfg({ ...cfg, bot_commands_enabled: e.target.checked })
                }
              />
              /status /filters…
            </label>
          </label>
          <label className="field check-field">
            <span className="muted">Только buys</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={cfg.buys_only}
                onChange={(e) => setCfg({ ...cfg, buys_only: e.target.checked })}
              />
              buys_only
            </label>
          </label>
          <label className="field check-field">
            <span className="muted">Transfers (EOA)</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={cfg.track_transfers}
                onChange={(e) =>
                  setCfg({ ...cfg, track_transfers: e.target.checked })
                }
              />
              track_transfers
            </label>
          </label>
          <label className="field compact">
            <span className="muted">Интервал, сек</span>
            <input
              type="number"
              value={cfg.interval_sec}
              min={5}
              onChange={(e) =>
                setCfg({ ...cfg, interval_sec: Number(e.target.value) || 5 })
              }
            />
          </label>
          <label className="field compact">
            <span className="muted">Max mcap алерта, $</span>
            <input
              type="number"
              value={cfg.max_mcap_alert}
              min={0}
              onChange={(e) =>
                setCfg({
                  ...cfg,
                  max_mcap_alert: Number(e.target.value) || 0,
                })
              }
            />
          </label>
          <label className="field compact">
            <span className="muted">Min mcap, $</span>
            <input
              type="number"
              value={numOrEmpty(cfg.min_mcap_alert)}
              min={0}
              placeholder="пусто = off"
              onChange={(e) =>
                setCfg({ ...cfg, min_mcap_alert: parseOptional(e.target.value) })
              }
            />
          </label>
          <label className="field compact">
            <span className="muted">Min buy USD</span>
            <input
              type="number"
              value={numOrEmpty(cfg.min_bought_usd)}
              min={0}
              placeholder="off"
              onChange={(e) =>
                setCfg({ ...cfg, min_bought_usd: parseOptional(e.target.value) })
              }
            />
          </label>
          <label className="field compact">
            <span className="muted">Max buy USD</span>
            <input
              type="number"
              value={numOrEmpty(cfg.max_bought_usd)}
              min={0}
              placeholder="off"
              onChange={(e) =>
                setCfg({ ...cfg, max_bought_usd: parseOptional(e.target.value) })
              }
            />
          </label>
          <label className="field compact">
            <span className="muted">Max сделок</span>
            <input
              type="number"
              value={cfg.max_deals}
              min={1}
              max={20}
              onChange={(e) =>
                setCfg({ ...cfg, max_deals: Number(e.target.value) || 5 })
              }
            />
          </label>
          <label className="field">
            <span className="muted">Telegram chat id</span>
            <input
              value={cfg.telegram_chat_id}
              onChange={(e) =>
                setCfg({ ...cfg, telegram_chat_id: e.target.value })
              }
              placeholder="пусто = .env"
            />
          </label>
          <label className="field compact">
            <span className="muted">Topic id</span>
            <input
              value={cfg.telegram_topic_id}
              onChange={(e) =>
                setCfg({ ...cfg, telegram_topic_id: e.target.value })
              }
            />
          </label>
        </div>
        <p className="muted tiny">
          Алерт на deal #2–#5 при mcap ≤ max (и ≥ min, если задан). Высокий
          mcap — запись без уведомления. buys_only=on — только входящие с контракта
          (DEX). track_transfers учитывается при buys_only=off. В Telegram: /help
        </p>
        <div className="row">
          <button
            type="button"
            className={`primary${saving ? ' busy' : ''}`}
            disabled={saving}
            onClick={() => void save()}
          >
            Сохранить
          </button>
          <button
            type="button"
            className="ghost"
            onClick={() => void post('/api/followup/run')}
            disabled={!!status?.running}
          >
            Запустить сейчас
          </button>
          <button
            type="button"
            className="ghost danger"
            onClick={() => void post('/api/followup/stop')}
          >
            Стоп
          </button>
          <button
            type="button"
            className="ghost"
            onClick={() => void post('/api/followup/test-telegram')}
          >
            Проверить Telegram
          </button>
          <button
            type="button"
            className="ghost"
            onClick={() => void post('/api/followup/reset-counters')}
          >
            Сброс счётчиков
          </button>
        </div>
      </section>

      <section className="panel">
        <h2 className="section-title">Лог</h2>
        <div className="log-box">
          {(status?.log ?? []).slice(-50).map((e, i) => (
            <div key={`${e.ts}-${i}`} className="log-line">
              <span className="muted">{fmtTs(e.ts)}</span> [{e.stage}] {e.message}
            </div>
          ))}
          {!status?.log?.length ? <p className="muted">Пока пусто</p> : null}
        </div>
      </section>

      <section className="panel">
        <h2 className="section-title">Таблица ({wallets.length})</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Адрес</th>
                <th>Статус</th>
                <th>Сделок</th>
                <th>Баланс ETH</th>
                <th>Токенов 7д</th>
                <th>1-й mcap</th>
                <th>История</th>
              </tr>
            </thead>
            <tbody>
              {wallets.map((w) => (
                <tr key={w.address}>
                  <td>
                    <a
                      href={`https://gmgn.ai/robinhood/address/${w.address}`}
                      target="_blank"
                      rel="noreferrer"
                    >
                      <code>
                        {w.address.slice(0, 6)}…{w.address.slice(-4)}
                      </code>
                    </a>
                  </td>
                  <td>{w.status}</td>
                  <td>{w.deal_count}</td>
                  <td>{fmtNum(w.wallet_balance_eth)}</td>
                  <td>{w.tokens_traded_7d ?? '—'}</td>
                  <td>{fmtNum(w.first_mcap)}</td>
                  <td className="muted tiny">
                    {w.deals
                      .map((d) => {
                        const buy =
                          d.bought_usd != null ? ` $${fmtNum(d.bought_usd)}` : ''
                        return `#${d.deal_index} ${d.token_symbol || d.token.slice(0, 6)} @${fmtNum(d.mcap_at_buy)}${buy}${d.notified ? ' ✓' : ''}`
                      })
                      .join(' · ') || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {!wallets.length ? (
            <p className="muted">Нет кошельков — включите ingest из автопарса.</p>
          ) : null}
        </div>
      </section>
    </>
  )
}
