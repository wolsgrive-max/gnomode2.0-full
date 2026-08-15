import { useCallback, useEffect, useMemo, useState } from 'react'
import { FilterPresets } from './FilterPresets'

type ScreenSortBy = 'liquidity' | 'market_cap' | 'traders' | 'pair_age'
type ScreenSortOrder = 'asc' | 'desc'

type ScreenFiltersForm = {
  min_liq: string
  max_liq: string
  min_mcap: string
  max_mcap: string
  min_ath_mcap: string
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
    min_ath_mcap?: number | null
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
  }
}

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
  telegram_chat_id: string
  telegram_topic_id: string
  bot_commands_enabled: boolean
  raybot_enabled: boolean
  ingest_from_watch: boolean
}

const DEFAULT_SCREEN: ScreenFiltersForm = {
  min_liq: '',
  max_liq: '',
  min_mcap: '',
  max_mcap: '',
  min_ath_mcap: '50000',
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
  min_wallet_balance_eth: '',
  max_wallet_balance_eth: '',
  min_hold_time_minutes: '',
  max_hold_time_minutes: '',
  min_tokens_traded_7d: '1',
  max_tokens_traded_7d: '1',
}

function parseOpt(raw: string): number | null {
  const t = raw.trim()
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

function numToStr(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return ''
  return String(n)
}

async function readApiError(res: Response, fallback: string) {
  try {
    const data = (await res.json()) as { detail?: unknown }
    if (typeof data.detail === 'string') return data.detail
  } catch {
    /* ignore */
  }
  return `${fallback} (${res.status})`
}

export default function SettingsPage() {
  const [baseWatch, setBaseWatch] = useState<WatchConfig | null>(null)
  const [baseFollowup, setBaseFollowup] = useState<FollowupConfig | null>(null)
  const [screen, setScreen] = useState<ScreenFiltersForm>(DEFAULT_SCREEN)
  const [wallet, setWallet] = useState<WalletFiltersForm>(DEFAULT_WALLET)
  const [syncFollowupMcap, setSyncFollowupMcap] = useState(true)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState('')
  const [error, setError] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const [wRes, fRes] = await Promise.all([
        fetch('/api/watch'),
        fetch('/api/followup'),
      ])
      if (!wRes.ok) throw new Error(await readApiError(wRes, 'Конфиг автопарса'))
      if (!fRes.ok) throw new Error(await readApiError(fRes, 'Конфиг follow-up'))
      const cfg = (await wRes.json()) as WatchConfig
      const fcfg = (await fRes.json()) as FollowupConfig
      setBaseWatch(cfg)
      setBaseFollowup(fcfg)
      setScreen({
        ...DEFAULT_SCREEN,
        min_liq: numToStr(cfg.screen.min_liq),
        max_liq: numToStr(cfg.screen.max_liq),
        min_mcap: numToStr(cfg.screen.min_mcap),
        max_mcap: numToStr(cfg.screen.max_mcap),
        min_ath_mcap: numToStr(cfg.screen.min_ath_mcap) || '50000',
        min_traders: numToStr(cfg.screen.min_traders),
        max_traders: numToStr(cfg.screen.max_traders),
        min_pair_age_hours: numToStr(cfg.screen.min_pair_age_hours),
        max_pair_age_hours: numToStr(cfg.screen.max_pair_age_hours),
        sort_by: cfg.screen.sort_by,
        sort_order: cfg.screen.sort_order,
        max_results: String(cfg.screen.max_results || 500),
        exclude_honeypots: cfg.screen.exclude_honeypots,
      })
      setWallet({
        ...DEFAULT_WALLET,
        mcap_threshold: numToStr(cfg.wallet.mcap_threshold) || '20000',
        exclude_honeypots: cfg.wallet.exclude_honeypots,
        min_wallet_balance_eth: numToStr(cfg.wallet.min_wallet_balance_eth),
        max_wallet_balance_eth: numToStr(cfg.wallet.max_wallet_balance_eth),
        min_hold_time_minutes: numToStr(cfg.wallet.min_hold_time_minutes),
        max_hold_time_minutes: numToStr(cfg.wallet.max_hold_time_minutes),
        min_tokens_traded_7d: numToStr(cfg.wallet.min_tokens_traded_7d),
        max_tokens_traded_7d: numToStr(cfg.wallet.max_tokens_traded_7d),
      })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const setScreenField = <K extends keyof ScreenFiltersForm>(
    key: K,
    value: ScreenFiltersForm[K],
  ) => setScreen((prev) => ({ ...prev, [key]: value }))

  const setWalletField = <K extends keyof WalletFiltersForm>(
    key: K,
    value: WalletFiltersForm[K],
  ) => setWallet((prev) => ({ ...prev, [key]: value }))

  const screenPreset = useMemo(
    () => ({
      min_liq: screen.min_liq,
      max_liq: screen.max_liq,
      min_mcap: screen.min_mcap,
      max_mcap: screen.max_mcap,
      min_ath_mcap: screen.min_ath_mcap,
      min_traders: screen.min_traders,
      max_traders: screen.max_traders,
      min_pair_age_hours: screen.min_pair_age_hours,
      max_pair_age_hours: screen.max_pair_age_hours,
      sort_by: screen.sort_by,
      sort_order: screen.sort_order,
      max_results: screen.max_results,
      exclude_honeypots: screen.exclude_honeypots,
    }),
    [screen],
  )

  const walletPreset = useMemo(
    () => ({
      mcap_threshold: wallet.mcap_threshold,
      exclude_honeypots: wallet.exclude_honeypots,
      min_wallet_balance_eth: wallet.min_wallet_balance_eth,
      max_wallet_balance_eth: wallet.max_wallet_balance_eth,
      min_hold_time_minutes: wallet.min_hold_time_minutes,
      max_hold_time_minutes: wallet.max_hold_time_minutes,
      min_tokens_traded_7d: wallet.min_tokens_traded_7d,
      max_tokens_traded_7d: wallet.max_tokens_traded_7d,
    }),
    [wallet],
  )

  const save = async () => {
    if (!baseWatch) return
    setSaving(true)
    setMsg('')
    setError('')
    try {
      const maxResults = Math.min(2000, Math.max(1, Number(screen.max_results) || 500))
      const payload: WatchConfig = {
        ...baseWatch,
        screen: {
          min_liq: parseOpt(screen.min_liq),
          max_liq: parseOpt(screen.max_liq),
          min_mcap: parseOpt(screen.min_mcap),
          max_mcap: parseOpt(screen.max_mcap),
          min_ath_mcap: parseOpt(screen.min_ath_mcap),
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
        },
      }
      const wRes = await fetch('/api/watch', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!wRes.ok) throw new Error(await readApiError(wRes, 'Сохранение автопарса'))
      const savedWatch = (await wRes.json()) as WatchConfig
      setBaseWatch(savedWatch)

      if (syncFollowupMcap && baseFollowup) {
        const mcap =
          parseOpt(wallet.mcap_threshold) ?? baseFollowup.max_mcap_alert ?? 20000
        const fPayload = { ...baseFollowup, max_mcap_alert: mcap }
        const fRes = await fetch('/api/followup', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(fPayload),
        })
        if (!fRes.ok) throw new Error(await readApiError(fRes, 'Сохранение follow-up'))
        setBaseFollowup((await fRes.json()) as FollowupConfig)
      }

      setMsg(
        syncFollowupMcap
          ? 'Сохранено: фильтры токена + 1-й сделки; max mcap Follow-up синхронизирован'
          : 'Сохранено: фильтры токена + 1-й сделки (автопарс)',
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  if (loading) {
    return (
      <>
        <header className="hero">
          <p className="brand">gnomode</p>
          <h1>Настройки</h1>
          <p className="lede">Загрузка…</p>
        </header>
      </>
    )
  }

  return (
    <>
      <header className="hero">
        <p className="brand">gnomode</p>
        <h1>Настройки фильтров</h1>
        <p className="lede">
          Здесь задаются фильтры отбора <b>токенов</b> и условия <b>первой сделки</b>{' '}
          кошелька. Их использует автопарс (и early buyers при пороге mcap). Расписание
          и Telegram — во вкладке Автопарс / Follow-up.
        </p>
      </header>

      <section className="panel input-panel">
        <h2 className="section-title">1. Фильтры токена</h2>
        <p className="muted tiny">
          Какие токены попадают в скринер/автопарс (ликвидность, mcap, ATH, возраст,
          активность).
        </p>
        <FilterPresets
          storageKey="gnomode.presets.settings.tokens"
          current={screenPreset}
          onApply={(v) => setScreen({ ...DEFAULT_SCREEN, ...v })}
        />
        <div className="filter-grid">
          <label className="field">
            <span>Мин. ликвидность ($)</span>
            <input
              type="number"
              min={0}
              value={screen.min_liq}
              onChange={(e) => setScreenField('min_liq', e.target.value)}
              placeholder="любая"
            />
          </label>
          <label className="field">
            <span>Макс. ликвидность ($)</span>
            <input
              type="number"
              min={0}
              value={screen.max_liq}
              onChange={(e) => setScreenField('max_liq', e.target.value)}
              placeholder="любая"
            />
          </label>
          <label className="field">
            <span>Мин. mcap ($)</span>
            <input
              type="number"
              min={0}
              value={screen.min_mcap}
              onChange={(e) => setScreenField('min_mcap', e.target.value)}
              placeholder="любая"
            />
          </label>
          <label className="field">
            <span>Макс. mcap ($)</span>
            <input
              type="number"
              min={0}
              value={screen.max_mcap}
              onChange={(e) => setScreenField('max_mcap', e.target.value)}
              placeholder="любая"
            />
          </label>
          <label className="field">
            <span>Мин. ATH mcap ($)</span>
            <input
              type="number"
              min={0}
              value={screen.min_ath_mcap}
              onChange={(e) => setScreenField('min_ath_mcap', e.target.value)}
              placeholder="выкл"
              title="Парсить кошельки только после ATH ≥ порога"
            />
          </label>
          <label className="field">
            <span>Мин. трейдеров (24ч)</span>
            <input
              type="number"
              min={0}
              value={screen.min_traders}
              onChange={(e) => setScreenField('min_traders', e.target.value)}
              placeholder="любое"
            />
          </label>
          <label className="field">
            <span>Макс. трейдеров (24ч)</span>
            <input
              type="number"
              min={0}
              value={screen.max_traders}
              onChange={(e) => setScreenField('max_traders', e.target.value)}
              placeholder="любое"
            />
          </label>
          <label className="field">
            <span>Мин. возраст пары (ч)</span>
            <input
              type="number"
              min={0}
              value={screen.min_pair_age_hours}
              onChange={(e) => setScreenField('min_pair_age_hours', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. возраст пары (ч)</span>
            <input
              type="number"
              min={0}
              value={screen.max_pair_age_hours}
              onChange={(e) => setScreenField('max_pair_age_hours', e.target.value)}
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Сортировка</span>
            <select
              value={screen.sort_by}
              onChange={(e) =>
                setScreenField('sort_by', e.target.value as ScreenSortBy)
              }
            >
              <option value="liquidity">Ликвидность</option>
              <option value="market_cap">Капитализация</option>
              <option value="traders">Трейдеры</option>
              <option value="pair_age">Возраст пары</option>
            </select>
          </label>
          <label className="field">
            <span>Порядок</span>
            <select
              value={screen.sort_order}
              onChange={(e) =>
                setScreenField('sort_order', e.target.value as ScreenSortOrder)
              }
            >
              <option value="desc">По убыванию</option>
              <option value="asc">По возрастанию</option>
            </select>
          </label>
          <label className="field">
            <span>Макс. результатов</span>
            <input
              type="number"
              min={1}
              max={2000}
              value={screen.max_results}
              onChange={(e) => setScreenField('max_results', e.target.value)}
            />
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
        <h2 className="section-title">2. Фильтры первой сделки кошелька</h2>
        <p className="muted tiny">
          Условия early buyer: вход до порога mcap, баланс, холд, активность за 7д.
          Такие кошельки уходят в Telegram и в Follow-up как deal #1.
        </p>
        <FilterPresets
          storageKey="gnomode.presets.settings.wallets"
          current={walletPreset}
          onApply={(v) => setWallet({ ...DEFAULT_WALLET, ...v })}
        />
        <div className="filter-grid">
          <label className="field">
            <span>Порог mcap 1-й сделки ($)</span>
            <input
              type="number"
              min={0}
              step={500}
              value={wallet.mcap_threshold}
              onChange={(e) => setWalletField('mcap_threshold', e.target.value)}
            />
          </label>
          <label className="field">
            <span>Мин. баланс (ETH)</span>
            <input
              type="number"
              min={0}
              step={0.001}
              value={wallet.min_wallet_balance_eth}
              onChange={(e) =>
                setWalletField('min_wallet_balance_eth', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. баланс (ETH)</span>
            <input
              type="number"
              min={0}
              step={0.001}
              value={wallet.max_wallet_balance_eth}
              onChange={(e) =>
                setWalletField('max_wallet_balance_eth', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Мин. холд (мин)</span>
            <input
              type="number"
              min={0}
              value={wallet.min_hold_time_minutes}
              onChange={(e) =>
                setWalletField('min_hold_time_minutes', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. холд (мин)</span>
            <input
              type="number"
              min={0}
              value={wallet.max_hold_time_minutes}
              onChange={(e) =>
                setWalletField('max_hold_time_minutes', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Мин. токенов за 7д</span>
            <input
              type="number"
              min={0}
              value={wallet.min_tokens_traded_7d}
              onChange={(e) =>
                setWalletField('min_tokens_traded_7d', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field">
            <span>Макс. токенов за 7д</span>
            <input
              type="number"
              min={0}
              value={wallet.max_tokens_traded_7d}
              onChange={(e) =>
                setWalletField('max_tokens_traded_7d', e.target.value)
              }
              placeholder="любой"
            />
          </label>
          <label className="field check-field">
            <span>Безопасность</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={wallet.exclude_honeypots}
                onChange={(e) =>
                  setWalletField('exclude_honeypots', e.target.checked)
                }
              />
              Пропускать honeypot
            </label>
          </label>
          <label className="field check-field">
            <span>Follow-up</span>
            <label className="check-inline">
              <input
                type="checkbox"
                checked={syncFollowupMcap}
                onChange={(e) => setSyncFollowupMcap(e.target.checked)}
              />
              Синхр. max mcap алерта Follow-up с порогом 1-й сделки
            </label>
          </label>
        </div>
      </section>

      <section className="panel input-panel">
        <div className="row">
          <button
            type="button"
            className={`primary${saving ? ' busy' : ''}`}
            disabled={saving}
            onClick={() => void save()}
          >
            Сохранить настройки
          </button>
          <button type="button" className="ghost" onClick={() => void load()}>
            Сбросить с сервера
          </button>
        </div>
        {msg ? <p className="watch-msg">{msg}</p> : null}
        {error ? <p className="watch-error">{error}</p> : null}
      </section>
    </>
  )
}
