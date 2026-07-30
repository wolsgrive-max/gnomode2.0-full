import { useCallback, useEffect, useMemo, useState } from 'react'
import { FilterPresets } from './FilterPresets'

type ScreenSortBy = 'liquidity' | 'market_cap' | 'traders' | 'pair_age'
type ScreenSortOrder = 'asc' | 'desc'

type ScreenFiltersForm = {
  min_liq: string
  max_liq: string
  min_mcap: string
  max_mcap: string
  min_traders: string
  max_traders: string
  min_pair_age_hours: string
  max_pair_age_hours: string
  sort_by: ScreenSortBy
  sort_order: ScreenSortOrder
  max_results: string
  exclude_honeypots: boolean
}

type WalletFiltersForm = {
  mcap_threshold: string
  exclude_honeypots: boolean
  min_wallet_balance_eth: string
  max_wallet_balance_eth: string
  min_hold_time_minutes: string
  max_hold_time_minutes: string
  min_tokens_traded_7d: string
  max_tokens_traded_7d: string
  min_buy_usd: string
}

type WatchConfig = {
  enabled: boolean
  interval_sec: number
  max_tokens_per_cycle: number
  telegram_chat_id: string
  telegram_topic_id?: string
  gnome_banter_enabled?: boolean
  screen: {
    min_liq: number | null
    max_liq: number | null
    min_mcap: number | null
    max_mcap: number | null
    min_traders: number | null
    max_traders: number | null
    min_pair_age_hours: number | null
    max_pair_age_hours: number | null
    exclude_honeypots: boolean
    sort_by: ScreenSortBy
    sort_order: ScreenSortOrder
    max_results: number
  }
  wallet: {
    mcap_threshold: number | null
    exclude_honeypots: boolean
    min_wallet_balance_eth: number | null
    max_wallet_balance_eth: number | null
    min_hold_time_minutes: number | null
    max_hold_time_minutes: number | null
    min_tokens_traded_7d: number | null
    max_tokens_traded_7d: number | null
    min_buy_usd?: number | null
  }
}

type JobLogEntry = {
  ts: number
  stage: string
  message: string
  percent: number
  token?: string | null
}

type WatchStatus = {
  enabled: boolean
  running: boolean
  telegram_configured: boolean
  next_run_ts: number | null
  last_run_ts: number | null
  last_run_duration_sec: number | null
  last_error: string | null
  last_message: string
  last_tokens_screened: number
  last_tokens_parsed: number
  last_buyers_found: number
  last_buyers_new: number
  last_buyers_sent: number
  last_buyers_skipped: number
  seen_count: number
  needs_catchup?: boolean
  catchup_lookback_hours?: number | null
  is_catchup_run?: boolean
  gnome_banter_enabled?: boolean
  gnome_banter_next_ts?: number | null
  stop_requested?: boolean
  log?: JobLogEntry[]
}

function fmtLogTime(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

async function readApiError(res: Response, fallback: string) {
  try {
    const data = (await res.json()) as { detail?: unknown }
    if (typeof data.detail === 'string') return data.detail
    if (Array.isArray(data.detail)) {
      return data.detail
        .map((d) => (typeof d === 'object' && d && 'msg' in d ? String((d as { msg: string }).msg) : String(d)))
        .join('; ')
    }
  } catch {
    /* ignore */
  }
  return `${fallback} (${res.status})`
}

function fmtLookback(hours: number | null | undefined) {
  if (hours == null || !Number.isFinite(hours)) return '—'
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))} мин`
  if (Math.abs(hours - 24) < 0.05) return '24 ч'
  return `${hours.toFixed(1)} ч`
}

const DEFAULT_SCREEN: ScreenFiltersForm = {
  min_liq: '',
  max_liq: '',
  min_mcap: '',
  max_mcap: '',
  min_traders: '',
  max_traders: '',
  min_pair_age_hours: '',
  max_pair_age_hours: '',
  sort_by: 'liquidity',
  sort_order: 'desc',
  max_results: '500',
  exclude_honeypots: true,
}

const DEFAULT_WALLET: WalletFiltersForm = {
  mcap_threshold: '20000',
  exclude_honeypots: true,
  min_wallet_balance_eth: '0.001',
  max_wallet_balance_eth: '',
  min_hold_time_minutes: '',
  max_hold_time_minutes: '',
  min_tokens_traded_7d: '1',
  max_tokens_traded_7d: '1',
  min_buy_usd: '',
}

function parseOpt(raw: string): number | null {
  const t = raw.trim()
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

function numToStr(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return ''
  return String(v)
}

function fmtTs(ts: number | null) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('ru-RU')
}

function fmtAgo(ts: number | null) {
  if (!ts) return 'никогда'
  const secs = Math.max(0, Date.now() / 1000 - ts)
  if (secs < 60) return `${Math.round(secs)}с назад`
  if (secs < 3600) return `${Math.round(secs / 60)}м назад`
  return `${Math.round(secs / 3600)}ч назад`
}

function fmtIn(ts: number | null) {
  if (!ts) return '—'
  const secs = Math.max(0, ts - Date.now() / 1000)
  if (secs < 60) return `через ${Math.round(secs)}с`
  if (secs < 3600) return `через ${Math.round(secs / 60)}м`
  return `через ${Math.round(secs / 3600)}ч`
}

function configToForms(cfg: WatchConfig): {
  screen: ScreenFiltersForm
  wallet: WalletFiltersForm
  enabled: boolean
  intervalMin: string
  maxTokens: string
  chatId: string
  topicId: string
  gnomeBanter: boolean
} {
  return {
    enabled: cfg.enabled,
    intervalMin: String(Math.max(1, Math.round(cfg.interval_sec / 60))),
    maxTokens: String(cfg.max_tokens_per_cycle),
    chatId: cfg.telegram_chat_id || '',
    topicId: cfg.telegram_topic_id || '',
    gnomeBanter: cfg.gnome_banter_enabled !== false,
    screen: {
      min_liq: numToStr(cfg.screen.min_liq),
      max_liq: numToStr(cfg.screen.max_liq),
      min_mcap: numToStr(cfg.screen.min_mcap),
      max_mcap: numToStr(cfg.screen.max_mcap),
      min_traders: numToStr(cfg.screen.min_traders),
      max_traders: numToStr(cfg.screen.max_traders),
      min_pair_age_hours: numToStr(cfg.screen.min_pair_age_hours),
      max_pair_age_hours: numToStr(cfg.screen.max_pair_age_hours),
      sort_by: cfg.screen.sort_by,
      sort_order: cfg.screen.sort_order,
      max_results: String(cfg.screen.max_results || 500),
      exclude_honeypots: cfg.screen.exclude_honeypots,
    },
    wallet: {
      mcap_threshold: numToStr(cfg.wallet.mcap_threshold) || '20000',
      exclude_honeypots: cfg.wallet.exclude_honeypots,
      min_wallet_balance_eth: numToStr(cfg.wallet.min_wallet_balance_eth),
      max_wallet_balance_eth: numToStr(cfg.wallet.max_wallet_balance_eth),
      min_hold_time_minutes: numToStr(cfg.wallet.min_hold_time_minutes),
      max_hold_time_minutes: numToStr(cfg.wallet.max_hold_time_minutes),
      min_tokens_traded_7d: numToStr(cfg.wallet.min_tokens_traded_7d),
      max_tokens_traded_7d: numToStr(cfg.wallet.max_tokens_traded_7d),
      min_buy_usd: numToStr(cfg.wallet.min_buy_usd ?? null),
    },
  }
}

export default function WatchPage() {
  const [enabled, setEnabled] = useState(false)
  const [intervalMin, setIntervalMin] = useState('15')
  const [maxTokens, setMaxTokens] = useState('20')
  const [chatId, setChatId] = useState('')
  const [topicId, setTopicId] = useState('')
  const [gnomeBanter, setGnomeBanter] = useState(true)
  const [screen, setScreen] = useState<ScreenFiltersForm>(DEFAULT_SCREEN)
  const [wallet, setWallet] = useState<WalletFiltersForm>(DEFAULT_WALLET)
  const [status, setStatus] = useState<WatchStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [actionMsg, setActionMsg] = useState('')
  const [error, setError] = useState('')

  const load = useCallback(async () => {
    try {
      const [cfgRes, stRes] = await Promise.all([
        fetch('/api/watch'),
        fetch('/api/watch/status'),
      ])
      if (!cfgRes.ok) throw new Error(`Конфиг: ошибка ${cfgRes.status}`)
      if (!stRes.ok) throw new Error(`Статус: ошибка ${stRes.status}`)
      const cfg = (await cfgRes.json()) as WatchConfig
      const st = (await stRes.json()) as WatchStatus
      const forms = configToForms(cfg)
      setEnabled(forms.enabled)
      setIntervalMin(forms.intervalMin)
      setMaxTokens(forms.maxTokens)
      setChatId(forms.chatId)
      setTopicId(forms.topicId)
      setGnomeBanter(forms.gnomeBanter)
      setScreen(forms.screen)
      setWallet(forms.wallet)
      setStatus(st)
      setError('')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    const id = window.setInterval(() => {
      void fetch('/api/watch/status')
        .then((r) => (r.ok ? r.json() : null))
        .then((st) => {
          if (st) setStatus(st as WatchStatus)
        })
        .catch(() => undefined)
    }, 2000)
    return () => window.clearInterval(id)
  }, [])

  const buildConfig = useCallback((): WatchConfig => {
    const mins = Math.max(1, Number(intervalMin) || 15)
    const maxTok = Math.min(2000, Math.max(1, Number(maxTokens) || 20))
    const maxResults = Math.min(2000, Math.max(1, Number(screen.max_results) || 500))
    return {
      enabled,
      interval_sec: mins * 60,
      max_tokens_per_cycle: maxTok,
      telegram_chat_id: chatId.trim(),
      telegram_topic_id: topicId.trim(),
      gnome_banter_enabled: gnomeBanter,
      screen: {
        min_liq: parseOpt(screen.min_liq),
        max_liq: parseOpt(screen.max_liq),
        min_mcap: parseOpt(screen.min_mcap),
        max_mcap: parseOpt(screen.max_mcap),
        min_traders: parseOpt(screen.min_traders),
        max_traders: parseOpt(screen.max_traders),
        min_pair_age_hours: parseOpt(screen.min_pair_age_hours),
        max_pair_age_hours: parseOpt(screen.max_pair_age_hours),
        exclude_honeypots: screen.exclude_honeypots,
        sort_by: screen.sort_by,
        sort_order: screen.sort_order,
        max_results: maxResults,
      },
      wallet: {
        mcap_threshold: parseOpt(wallet.mcap_threshold),
        exclude_honeypots: wallet.exclude_honeypots,
        min_wallet_balance_eth: parseOpt(wallet.min_wallet_balance_eth),
        max_wallet_balance_eth: parseOpt(wallet.max_wallet_balance_eth),
        min_hold_time_minutes: parseOpt(wallet.min_hold_time_minutes),
        max_hold_time_minutes: parseOpt(wallet.max_hold_time_minutes),
        min_tokens_traded_7d: parseOpt(wallet.min_tokens_traded_7d),
        max_tokens_traded_7d: parseOpt(wallet.max_tokens_traded_7d),
        min_buy_usd: parseOpt(wallet.min_buy_usd),
      },
    }
  }, [enabled, intervalMin, maxTokens, chatId, topicId, gnomeBanter, screen, wallet])

  const save = useCallback(async () => {
    setSaving(true)
    setActionMsg('')
    setError('')
    try {
      const res = await fetch('/api/watch', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildConfig()),
      })
      if (!res.ok) throw new Error(await readApiError(res, 'Не удалось сохранить'))
      const cfg = (await res.json()) as WatchConfig
      const forms = configToForms(cfg)
      setEnabled(forms.enabled)
      setIntervalMin(forms.intervalMin)
      setMaxTokens(forms.maxTokens)
      setChatId(forms.chatId)
      setTopicId(forms.topicId)
      setGnomeBanter(forms.gnomeBanter)
      setScreen(forms.screen)
      setWallet(forms.wallet)
      setActionMsg('Сохранено')
      const st = await fetch('/api/watch/status')
      if (st.ok) setStatus((await st.json()) as WatchStatus)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }, [buildConfig])

  const runNow = useCallback(async () => {
    setActionMsg('')
    setError('')
    try {
      // Persist current form first so the cycle uses what you see.
      const saveRes = await fetch('/api/watch', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildConfig()),
      })
      if (!saveRes.ok) throw new Error(await readApiError(saveRes, 'Не удалось сохранить'))
      const res = await fetch('/api/watch/run', { method: 'POST' })
      if (!res.ok) throw new Error(await readApiError(res, 'Не удалось запустить'))
      setStatus((await res.json()) as WatchStatus)
      setActionMsg('Цикл запущен')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [buildConfig])

  const stopNow = useCallback(async () => {
    setActionMsg('')
    setError('')
    try {
      const res = await fetch('/api/watch/stop', { method: 'POST' })
      if (!res.ok) throw new Error(await readApiError(res, 'Не удалось остановить'))
      setStatus((await res.json()) as WatchStatus)
      setActionMsg('Остановка запрошена')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  const clearSeen = useCallback(async () => {
    if (
      !window.confirm(
        'Очистить историю дедупа? Ранее отправленные пары кошелёк+токен снова могут попасть в алерты.',
      )
    ) {
      return
    }
    try {
      const res = await fetch('/api/watch/clear-seen', { method: 'POST' })
      if (!res.ok) throw new Error(await readApiError(res, 'Не удалось очистить'))
      setActionMsg('История дедупа очищена')
      const st = await fetch('/api/watch/status')
      if (st.ok) setStatus((await st.json()) as WatchStatus)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  const resetCounters = useCallback(async () => {
    setActionMsg('')
    setError('')
    try {
      const res = await fetch('/api/watch/reset-counters', { method: 'POST' })
      if (!res.ok) throw new Error(await readApiError(res, 'Не удалось сбросить'))
      setStatus((await res.json()) as WatchStatus)
      setActionMsg('Счётчики сброшены')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  const testTelegram = useCallback(async () => {
    setActionMsg('')
    setError('')
    try {
      // Persist form first so chat/topic fields are what we test.
      const saveRes = await fetch('/api/watch', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildConfig()),
      })
      if (!saveRes.ok) throw new Error(await readApiError(saveRes, 'Не удалось сохранить'))
      const res = await fetch('/api/watch/test-telegram', { method: 'POST' })
      if (!res.ok) throw new Error(await readApiError(res, 'Telegram недоступен'))
      const data = (await res.json()) as {
        message?: string
        bot_username?: string
        topic_id?: number | null
      }
      const who = data.bot_username ? `@${data.bot_username}` : 'бот'
      const topic = data.topic_id != null ? ` · топик ${data.topic_id}` : ''
      setActionMsg(data.message || `OK: ${who}${topic}`)
      const st = await fetch('/api/watch/status')
      if (st.ok) setStatus((await st.json()) as WatchStatus)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [buildConfig])

  const setScreenField = <K extends keyof ScreenFiltersForm>(key: K, value: ScreenFiltersForm[K]) => {
    setScreen((prev) => ({ ...prev, [key]: value }))
  }

  const setWalletField = <K extends keyof WalletFiltersForm>(key: K, value: WalletFiltersForm[K]) => {
    setWallet((prev) => ({ ...prev, [key]: value }))
  }

  const screenPreset = useMemo(() => screen, [screen])
  const walletPreset = useMemo(() => wallet, [wallet])

  if (loading) {
    return (
      <>
        <header className="hero">
          <p className="brand">gnomode</p>
          <h1>Автопарс</h1>
          <p className="lede">Загрузка…</p>
        </header>
      </>
    )
  }

  return (
    <>
      <header className="hero">
        <p className="brand">gnomode</p>
        <h1>Автопарс и алерты в Telegram</h1>
        <p className="lede">
          По расписанию скринит токены, парсит ранних покупателей по фильтрам кошельков и
          отправляет новые пары кошелёк+токен в Telegram.
        </p>
      </header>

      <section className="panel meta-panel watch-status">
        <div className="watch-status-grid">
          <div>
            <span className="muted">Статус</span>
            <strong>
              {status?.running ? 'Выполняется' : status?.enabled ? 'Включён' : 'Выключен'}
            </strong>
          </div>
          <div>
            <span className="muted">Telegram</span>
            <strong>{status?.telegram_configured ? 'Настроен' : 'Не настроен'}</strong>
          </div>
          <div>
            <span className="muted">Последний запуск</span>
            <strong>{fmtAgo(status?.last_run_ts ?? null)}</strong>
            <div className="muted tiny">{fmtTs(status?.last_run_ts ?? null)}</div>
          </div>
          <div>
            <span className="muted">Следующий запуск</span>
            <strong>{status?.enabled ? fmtIn(status?.next_run_ts ?? null) : '—'}</strong>
          </div>
          <div>
            <span className="muted">Последний цикл</span>
            <strong>
              {status
                ? `${status.last_tokens_parsed} ток · ${status.last_buyers_sent} отпр. · ${status.last_buyers_skipped} проп.`
                : '—'}
            </strong>
          </div>
          <div>
            <span className="muted">Уже отправлено</span>
            <strong>{status?.seen_count ?? 0}</strong>
          </div>
          <div>
            <span className="muted">Догон</span>
            <strong>
              {status?.is_catchup_run
                ? `сейчас · ${fmtLookback(status.catchup_lookback_hours)}`
                : status?.needs_catchup
                  ? `ожидает · ${fmtLookback(status.catchup_lookback_hours)}`
                  : 'не нужен'}
            </strong>
          </div>
          <div>
            <span className="muted">Гном в чате</span>
            <strong>
              {status?.gnome_banter_enabled === false
                ? 'выкл'
                : status?.enabled
                  ? status?.gnome_banter_next_ts
                    ? fmtIn(status.gnome_banter_next_ts)
                    : 'ждёт'
                  : 'когда автопарс вкл'}
            </strong>
          </div>
        </div>
        {status?.last_message ? (
          <p className="watch-msg">{status.last_message}</p>
        ) : null}
        {status?.last_error ? <p className="watch-error">{status.last_error}</p> : null}
        {!status?.telegram_configured ? (
          <p className="muted">
            Укажите <code>TELEGRAM_BOT_TOKEN</code> в <code>.env</code> и chat id ниже (или{' '}
            <code>TELEGRAM_CHAT_ID</code>).
          </p>
        ) : null}
      </section>

      <section className="panel input-panel">
        <h2 className="section-title">Расписание</h2>
        <div className="row">
          <label className="field check-field">
            <span>Включено</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={enabled}
                onChange={(e) => setEnabled(e.target.checked)}
              />
              Запускать по интервалу
            </label>
          </label>
          <label className="field compact">
            <span>Интервал (минуты)</span>
            <input
              type="number"
              min={1}
              max={1440}
              value={intervalMin}
              onChange={(e) => setIntervalMin(e.target.value)}
            />
          </label>
          <label className="field compact">
            <span>Макс. токенов / цикл</span>
            <input
              type="number"
              min={1}
              max={2000}
              value={maxTokens}
              onChange={(e) => setMaxTokens(e.target.value)}
            />
          </label>
          <label className="field">
            <span>Telegram chat id</span>
            <input
              type="text"
              value={chatId}
              onChange={(e) => setChatId(e.target.value)}
              placeholder="из .env, если пусто"
              spellCheck={false}
            />
          </label>
          <label className="field compact">
            <span>Topic id (топик)</span>
            <input
              type="text"
              value={topicId}
              onChange={(e) => setTopicId(e.target.value)}
              placeholder="из .env / пусто"
              spellCheck={false}
            />
          </label>
          <label className="field check-field">
            <span>Гном</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={gnomeBanter}
                onChange={(e) => setGnomeBanter(e.target.checked)}
              />
              Жалобы в TG каждые 10–15 мин
            </label>
          </label>
        </div>
        <div className="row">
          <button type="button" className={`primary${saving ? ' busy' : ''}`} disabled={saving} onClick={save}>
            {saving ? 'Сохранение…' : 'Сохранить'}
          </button>
          <button type="button" className="ghost" onClick={runNow} disabled={!!status?.running}>
            {status?.running ? 'Выполняется…' : 'Запустить сейчас'}
          </button>
          <button
            type="button"
            className="ghost danger"
            onClick={stopNow}
            disabled={!status?.running && !status?.stop_requested}
          >
            {status?.stop_requested ? 'Останавливается…' : 'Принудительный стоп'}
          </button>
          <button type="button" className="ghost" onClick={resetCounters}>
            Сброс счётчиков
          </button>
          <button type="button" className="ghost" onClick={testTelegram}>
            Проверить Telegram
          </button>
          <button type="button" className="ghost" onClick={clearSeen}>
            Очистить дедуп
          </button>
          {actionMsg ? <span className="muted">{actionMsg}</span> : null}
        </div>
        {error ? <p className="watch-error">{error}</p> : null}
      </section>

      <section className="panel meta-panel">
        <div className="job-log" aria-live="polite">
          <div className="job-log-head">
            <h2 className="section-title">Лог автопарса</h2>
            <span className="muted">
              {status?.log?.length ? `${status.log.length} записей` : 'пока пусто'}
            </span>
          </div>
          <ol className="job-log-list">
            {(status?.log ?? []).slice().reverse().map((entry, i) => (
              <li key={`${entry.ts}-${entry.stage}-${i}`} className="job-log-row">
                <time dateTime={new Date(entry.ts * 1000).toISOString()}>
                  {fmtLogTime(entry.ts)}
                </time>
                <span className={`job-log-stage stage-${entry.stage}`}>{entry.stage}</span>
                <span className="job-log-msg" title={entry.message}>
                  {entry.message}
                </span>
                <span className="job-log-pct">{Number.isFinite(entry.percent) ? `${Math.round(entry.percent)}%` : ''}</span>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="panel input-panel">
        <h2 className="section-title">Фильтры токенов (скринер)</h2>
        <FilterPresets
          storageKey="gnomode.presets.watch.tokens"
          current={screenPreset}
          onApply={(v) => setScreen({ ...DEFAULT_SCREEN, ...v })}
        />
        <div className="filter-grid">
          <label className="field">
            <span>Мин. ликвидность ($)</span>
            <input type="number" min={0} value={screen.min_liq} onChange={(e) => setScreenField('min_liq', e.target.value)} placeholder="любая" />
          </label>
          <label className="field">
            <span>Макс. ликвидность ($)</span>
            <input type="number" min={0} value={screen.max_liq} onChange={(e) => setScreenField('max_liq', e.target.value)} placeholder="любая" />
          </label>
          <label className="field">
            <span>Мин. mcap ($)</span>
            <input type="number" min={0} value={screen.min_mcap} onChange={(e) => setScreenField('min_mcap', e.target.value)} placeholder="любая" />
          </label>
          <label className="field">
            <span>Макс. mcap ($)</span>
            <input type="number" min={0} value={screen.max_mcap} onChange={(e) => setScreenField('max_mcap', e.target.value)} placeholder="любая" />
          </label>
          <label className="field">
            <span>Мин. трейдеров (24ч)</span>
            <input type="number" min={0} value={screen.min_traders} onChange={(e) => setScreenField('min_traders', e.target.value)} placeholder="любое" />
          </label>
          <label className="field">
            <span>Макс. трейдеров (24ч)</span>
            <input type="number" min={0} value={screen.max_traders} onChange={(e) => setScreenField('max_traders', e.target.value)} placeholder="любое" />
          </label>
          <label className="field">
            <span>Мин. возраст пары (ч)</span>
            <input type="number" min={0} value={screen.min_pair_age_hours} onChange={(e) => setScreenField('min_pair_age_hours', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Макс. возраст пары (ч)</span>
            <input type="number" min={0} value={screen.max_pair_age_hours} onChange={(e) => setScreenField('max_pair_age_hours', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Сортировка</span>
            <select value={screen.sort_by} onChange={(e) => setScreenField('sort_by', e.target.value as ScreenSortBy)}>
              <option value="liquidity">Ликвидность</option>
              <option value="market_cap">Капитализация</option>
              <option value="traders">Трейдеры</option>
              <option value="pair_age">Возраст пары</option>
            </select>
          </label>
          <label className="field">
            <span>Порядок</span>
            <select value={screen.sort_order} onChange={(e) => setScreenField('sort_order', e.target.value as ScreenSortOrder)}>
              <option value="desc">По убыванию</option>
              <option value="asc">По возрастанию</option>
            </select>
          </label>
          <label className="field">
            <span>Макс. результатов</span>
            <input type="number" min={1} max={2000} value={screen.max_results} onChange={(e) => setScreenField('max_results', e.target.value)} />
          </label>
          <label className="field check-field">
            <span>Безопасность</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={screen.exclude_honeypots}
                onChange={(e) => setScreenField('exclude_honeypots', e.target.checked)}
              />
              Пропускать honeypot
            </label>
          </label>
        </div>
      </section>

      <section className="panel input-panel">
        <h2 className="section-title">Фильтры кошельков</h2>
        <FilterPresets
          storageKey="gnomode.presets.watch.wallets"
          current={walletPreset}
          onApply={(v) => setWallet({ ...DEFAULT_WALLET, ...v })}
        />
        <div className="filter-grid">
          <label className="field">
            <span>Порог mcap (USD)</span>
            <input type="number" min={0} step={500} value={wallet.mcap_threshold} onChange={(e) => setWalletField('mcap_threshold', e.target.value)} />
          </label>
          <label className="field">
            <span>Мин. баланс (ETH)</span>
            <input type="number" min={0} step={0.001} value={wallet.min_wallet_balance_eth} onChange={(e) => setWalletField('min_wallet_balance_eth', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Макс. баланс (ETH)</span>
            <input type="number" min={0} step={0.001} value={wallet.max_wallet_balance_eth} onChange={(e) => setWalletField('max_wallet_balance_eth', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Мин. холд (мин)</span>
            <input type="number" min={0} value={wallet.min_hold_time_minutes} onChange={(e) => setWalletField('min_hold_time_minutes', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Макс. холд (мин)</span>
            <input type="number" min={0} value={wallet.max_hold_time_minutes} onChange={(e) => setWalletField('max_hold_time_minutes', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Мин. токенов за 7д</span>
            <input type="number" min={0} value={wallet.min_tokens_traded_7d} onChange={(e) => setWalletField('min_tokens_traded_7d', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Макс. токенов за 7д</span>
            <input type="number" min={0} value={wallet.max_tokens_traded_7d} onChange={(e) => setWalletField('max_tokens_traded_7d', e.target.value)} placeholder="любой" />
          </label>
          <label className="field">
            <span>Мин. buy USD</span>
            <input type="number" min={0} step={1} value={wallet.min_buy_usd} onChange={(e) => setWalletField('min_buy_usd', e.target.value)} placeholder="0" />
          </label>
          <label className="field check-field">
            <span>Безопасность</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={wallet.exclude_honeypots}
                onChange={(e) => setWalletField('exclude_honeypots', e.target.checked)}
              />
              Пропускать honeypot
            </label>
          </label>
        </div>
      </section>
    </>
  )
}
