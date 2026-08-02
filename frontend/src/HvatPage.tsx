import { useCallback, useEffect, useState } from 'react'

type ScreenSortBy = 'liquidity' | 'market_cap' | 'traders' | 'pair_age'
type ScreenSortOrder = 'asc' | 'desc'
type TokensUniquePeriod = '12h' | '24h' | '1d' | '3d' | '7d' | '30d'

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
  last_message: string
  last_error: string | null
  last_tokens_screened: number
  last_tokens_parsed: number
  last_buyers_found: number
  last_buyers_sent: number
  last_buyers_skipped: number
  seen_count: number
  log?: JobLogEntry[]
}

type FollowupStatus = {
  enabled: boolean
  running: boolean
  telegram_configured: boolean
  next_run_ts: number | null
  last_run_ts: number | null
  last_message: string
  last_error: string | null
  wallets_watching: number
  wallets_done: number
  last_checked: number
  last_new_deals: number
  last_alerts_sent: number
  log?: JobLogEntry[]
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
  first_mcap: number | null
  tokens_traded_7d: number | null
  alert_filters?: WalletAlertFilters
  deals: FollowupDeal[]
}

type WalletAlertFilters = {
  custom: boolean
  max_mcap_alert: number | null
  min_mcap_alert: number | null
  min_bought_usd: number | null
  max_bought_usd: number | null
  prune_enabled?: boolean | null
  prune_min_ath_mcap?: number | null
  prune_after_hours?: number | null
}

type ScreenForm = {
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

type WalletForm = {
  mcap_threshold: string
  exclude_honeypots: boolean
  min_wallet_balance_eth: string
  max_wallet_balance_eth: string
  min_hold_time_minutes: string
  max_hold_time_minutes: string
  min_tokens_traded_7d: string
  max_tokens_traded_7d: string
  tokens_unique_period: TokensUniquePeriod
}

type WatchConfig = {
  enabled: boolean
  interval_sec: number
  max_tokens_per_cycle: number
  screen: {
    min_liq: number | null
    max_liq: number | null
    min_mcap: number | null
    max_mcap: number | null
    min_ath_mcap: number | null
    min_traders: number | null
    max_traders: number | null
    min_pair_age_hours: number | null
    max_pair_age_hours: number | null
    sort_by: ScreenSortBy
    sort_order: ScreenSortOrder
    max_results: number
    exclude_honeypots: boolean
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
    tokens_unique_period?: TokensUniquePeriod
  }
}

type FollowupConfig = {
  max_mcap_alert: number
  min_mcap_alert: number | null
  min_bought_usd: number | null
  max_bought_usd: number | null
  alert_on_deals: number[]
  telegram_chat_id: string
  telegram_topic_id: string
  prune_enabled?: boolean
  prune_min_ath_mcap?: number
  prune_after_hours?: number
}

type AlertForm = {
  max_mcap_alert: string
  min_mcap_alert: string
  min_bought_usd: string
  max_bought_usd: string
  telegram_topic_id: string
  prune_enabled: boolean
  prune_min_ath_mcap: string
  prune_after_hours: string
}

type IndexStatus = {
  tokens_24h: number
  enriched: number
  building: boolean
  cold_started: boolean
  refreshing: boolean
  cold_tail?: boolean
}

type HvatStatus = {
  mcap_cap: number
  watch: WatchStatus
  followup: FollowupStatus
  index?: IndexStatus
  config?: WatchConfig
  followup_config?: FollowupConfig
  profile: {
    one_trade: boolean
    max_tokens_traded_7d: number | null
    min_tokens_traded_7d?: number | null
    tokens_unique_period?: TokensUniquePeriod
    first_buy_max_mcap: number | null
    alert_deals: number[]
    alert_max_mcap: number
    alert_min_mcap?: number | null
    alert_min_bought?: number | null
    alert_max_bought?: number | null
    telegram_topic_id?: string
    prune_enabled?: boolean
    prune_min_ath_mcap?: number
    prune_after_hours?: number
  }
}

const PERIODS: TokensUniquePeriod[] = ['12h', '24h', '1d', '3d', '7d', '30d']

const DEFAULT_SCREEN: ScreenForm = {
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

const DEFAULT_WALLET: WalletForm = {
  mcap_threshold: '20000',
  exclude_honeypots: true,
  min_wallet_balance_eth: '',
  max_wallet_balance_eth: '',
  min_hold_time_minutes: '',
  max_hold_time_minutes: '',
  min_tokens_traded_7d: '1',
  max_tokens_traded_7d: '1',
  tokens_unique_period: '7d',
}

const DEFAULT_ALERT: AlertForm = {
  max_mcap_alert: '20000',
  min_mcap_alert: '',
  min_bought_usd: '',
  max_bought_usd: '',
  telegram_topic_id: '9245',
  prune_enabled: false,
  prune_min_ath_mcap: '50000',
  prune_after_hours: '48',
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

function fmtLogTime(ts: number) {
  return new Date(ts * 1000).toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function gmgnWallet(addr: string) {
  return `https://gmgn.ai/robinhood/address/${addr}`
}

function shortAddr(addr: string) {
  if (addr.length < 12) return addr
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`
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

function cfgToScreen(cfg: WatchConfig): ScreenForm {
  return {
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
    sort_by: cfg.screen.sort_by || 'liquidity',
    sort_order: cfg.screen.sort_order || 'desc',
    max_results: String(cfg.screen.max_results || 500),
    exclude_honeypots: cfg.screen.exclude_honeypots !== false,
  }
}

function cfgToWallet(cfg: WatchConfig): WalletForm {
  const period = cfg.wallet.tokens_unique_period
  return {
    ...DEFAULT_WALLET,
    mcap_threshold: numToStr(cfg.wallet.mcap_threshold) || '20000',
    exclude_honeypots: cfg.wallet.exclude_honeypots !== false,
    min_wallet_balance_eth: numToStr(cfg.wallet.min_wallet_balance_eth),
    max_wallet_balance_eth: numToStr(cfg.wallet.max_wallet_balance_eth),
    min_hold_time_minutes: numToStr(cfg.wallet.min_hold_time_minutes),
    max_hold_time_minutes: numToStr(cfg.wallet.max_hold_time_minutes),
    min_tokens_traded_7d: numToStr(cfg.wallet.min_tokens_traded_7d) || '1',
    max_tokens_traded_7d: numToStr(cfg.wallet.max_tokens_traded_7d) || '1',
    tokens_unique_period: PERIODS.includes(period as TokensUniquePeriod)
      ? (period as TokensUniquePeriod)
      : '7d',
  }
}

function cfgToAlert(fcfg?: FollowupConfig, profile?: HvatStatus['profile']): AlertForm {
  return {
    ...DEFAULT_ALERT,
    max_mcap_alert:
      numToStr(fcfg?.max_mcap_alert ?? profile?.alert_max_mcap) || DEFAULT_ALERT.max_mcap_alert,
    min_mcap_alert: numToStr(fcfg?.min_mcap_alert ?? profile?.alert_min_mcap),
    min_bought_usd: numToStr(fcfg?.min_bought_usd ?? profile?.alert_min_bought),
    max_bought_usd: numToStr(fcfg?.max_bought_usd ?? profile?.alert_max_bought),
    telegram_topic_id:
      (fcfg?.telegram_topic_id || profile?.telegram_topic_id || DEFAULT_ALERT.telegram_topic_id).trim() ||
      DEFAULT_ALERT.telegram_topic_id,
    prune_enabled: fcfg?.prune_enabled ?? profile?.prune_enabled ?? DEFAULT_ALERT.prune_enabled,
    prune_min_ath_mcap:
      numToStr(fcfg?.prune_min_ath_mcap ?? profile?.prune_min_ath_mcap) ||
      DEFAULT_ALERT.prune_min_ath_mcap,
    prune_after_hours:
      numToStr(fcfg?.prune_after_hours ?? profile?.prune_after_hours) ||
      DEFAULT_ALERT.prune_after_hours,
  }
}

function walletFiltersToAlert(
  wf: WalletAlertFilters | undefined,
  global: AlertForm,
): { form: AlertForm; custom: boolean } {
  if (!wf?.custom) {
    return { form: { ...global }, custom: false }
  }
  return {
    custom: true,
    form: {
      ...global,
      max_mcap_alert: numToStr(wf.max_mcap_alert) || global.max_mcap_alert,
      min_mcap_alert: numToStr(wf.min_mcap_alert),
      min_bought_usd: numToStr(wf.min_bought_usd),
      max_bought_usd: numToStr(wf.max_bought_usd),
      prune_enabled: wf.prune_enabled ?? global.prune_enabled,
      prune_min_ath_mcap: numToStr(wf.prune_min_ath_mcap) || global.prune_min_ath_mcap,
      prune_after_hours: numToStr(wf.prune_after_hours) || global.prune_after_hours,
    },
  }
}

function fmtWalletFilters(wf?: WalletAlertFilters, global?: FollowupConfig | null): string {
  if (!wf?.custom) return 'общие'
  const maxM = wf.max_mcap_alert ?? global?.max_mcap_alert
  const minM = wf.min_mcap_alert
  const minB = wf.min_bought_usd
  const maxB = wf.max_bought_usd
  const pruneOn = wf.prune_enabled ?? global?.prune_enabled ?? false
  const ath = wf.prune_min_ath_mcap ?? global?.prune_min_ath_mcap ?? 50_000
  const hrs = wf.prune_after_hours ?? global?.prune_after_hours ?? 48
  const prune = pruneOn ? `ATH≥${fmtNum(ath)}/${hrs}ч` : 'prune off'
  return `mcap ${minM ?? 0}…${maxM ?? '—'} · buy ${minB ?? '—'}…${maxB ?? '—'} · ${prune}`
}

export default function HvatPage({ tabActive = true }: { tabActive?: boolean }) {
  const [st, setSt] = useState<HvatStatus | null>(null)
  const [wallets, setWallets] = useState<FollowupWallet[]>([])
  const [screen, setScreen] = useState<ScreenForm>(DEFAULT_SCREEN)
  const [wallet, setWallet] = useState<WalletForm>(DEFAULT_WALLET)
  const [alert, setAlert] = useState<AlertForm>(DEFAULT_ALERT)
  const [alertCustom, setAlertCustom] = useState(false)
  const [globalAlert, setGlobalAlert] = useState<AlertForm>(DEFAULT_ALERT)
  const [selected, setSelected] = useState<string[]>([])
  const [focusAddr, setFocusAddr] = useState<string | null>(null)
  const [maxTokensCycle, setMaxTokensCycle] = useState('20')
  const [intervalMin, setIntervalMin] = useState('20')
  const [busy, setBusy] = useState(false)
  const [flash, setFlash] = useState('')

  const refresh = useCallback(async () => {
    const [h, w] = await Promise.all([
      fetch('/api/hvat/status').then((r) => r.json() as Promise<HvatStatus>),
      fetch('/api/followup/wallets?limit=200').then((r) => r.json()),
    ])
    setSt(h)
    const list = Array.isArray(w) ? (w as FollowupWallet[]) : []
    setWallets(list)
    if (h.config) {
      setScreen(cfgToScreen(h.config))
      setWallet(cfgToWallet(h.config))
      setMaxTokensCycle(String(h.config.max_tokens_per_cycle || 20))
      setIntervalMin(String(Math.max(1, Math.round((h.config.interval_sec || 1200) / 60))))
    }
    const gAlert = cfgToAlert(h.followup_config, h.profile)
    setGlobalAlert(gAlert)
    setSelected((prev) => prev.filter((a) => list.some((x) => x.address === a)))
    setFocusAddr((prev) => {
      const next = prev && list.some((x) => x.address === prev) ? prev : null
      if (!next) {
        setAlert(gAlert)
        setAlertCustom(false)
      } else {
        const row = list.find((x) => x.address === next)
        const loaded = walletFiltersToAlert(row?.alert_filters, gAlert)
        setAlert(loaded.form)
        setAlertCustom(loaded.custom)
      }
      return next
    })
  }, [])

  useEffect(() => {
    if (!tabActive) return
    void refresh()
    const tick = () => {
      if (document.visibilityState === 'hidden') return
      void (async () => {
        try {
          const [h, w] = await Promise.all([
            fetch('/api/hvat/status').then((r) => r.json() as Promise<HvatStatus>),
            fetch('/api/followup/wallets?limit=200').then((r) => r.json()),
          ])
          setSt(h)
          setWallets(Array.isArray(w) ? (w as FollowupWallet[]) : [])
          if (h.followup_config || h.profile) {
            setGlobalAlert(cfgToAlert(h.followup_config, h.profile))
          }
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

  const setScreenField = <K extends keyof ScreenForm>(key: K, value: ScreenForm[K]) =>
    setScreen((prev) => ({ ...prev, [key]: value }))
  const setWalletField = <K extends keyof WalletForm>(key: K, value: WalletForm[K]) =>
    setWallet((prev) => ({ ...prev, [key]: value }))
  const setAlertField = <K extends keyof AlertForm>(key: K, value: AlertForm[K]) =>
    setAlert((prev) => ({ ...prev, [key]: value }))

  function loadWalletIntoAlert(addr: string) {
    const row = wallets.find((x) => x.address === addr)
    const loaded = walletFiltersToAlert(row?.alert_filters, globalAlert)
    setFocusAddr(addr)
    setAlert(loaded.form)
    setAlertCustom(loaded.custom)
    setSelected((prev) => (prev.includes(addr) ? prev : [...prev, addr]))
  }

  function toggleSelect(addr: string) {
    setSelected((prev) =>
      prev.includes(addr) ? prev.filter((a) => a !== addr) : [...prev, addr],
    )
  }

  function toggleSelectAll() {
    if (selected.length === wallets.length) {
      setSelected([])
      return
    }
    setSelected(wallets.map((w) => w.address))
  }

  function showGlobalAlerts() {
    setFocusAddr(null)
    setAlert(globalAlert)
    setAlertCustom(false)
  }

  async function post(path: string, okMsg: string) {
    setBusy(true)
    setFlash('')
    try {
      const res = await fetch(path, { method: 'POST' })
      if (!res.ok) throw new Error(`${res.status}`)
      setFlash(okMsg)
      await refresh()
    } catch (e) {
      setFlash(e instanceof Error ? e.message : 'Ошибка')
    } finally {
      setBusy(false)
    }
  }

  async function saveFilters() {
    setBusy(true)
    setFlash('')
    try {
      const mins = Math.max(1, Math.min(1440, Math.round(Number(intervalMin) || 20)))
      const body = {
        max_tokens_per_cycle: parseOpt(maxTokensCycle) ?? 20,
        interval_sec: mins * 60,
        sync_followup_mcap: false,
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
          sort_by: screen.sort_by,
          sort_order: screen.sort_order,
          max_results: parseOpt(screen.max_results) ?? 500,
          exclude_honeypots: screen.exclude_honeypots,
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
          tokens_unique_period: wallet.tokens_unique_period,
        },
        followup: {
          max_mcap_alert:
            parseOpt((focusAddr ? globalAlert : alert).max_mcap_alert) ?? 20000,
          min_mcap_alert: parseOpt((focusAddr ? globalAlert : alert).min_mcap_alert),
          min_bought_usd: parseOpt((focusAddr ? globalAlert : alert).min_bought_usd),
          max_bought_usd: parseOpt((focusAddr ? globalAlert : alert).max_bought_usd),
          telegram_topic_id: (focusAddr ? globalAlert : alert).telegram_topic_id.trim() || '9245',
          alert_on_deals: [2, 3, 4, 5],
          max_deals: 5,
          prune_enabled: (focusAddr ? globalAlert : alert).prune_enabled,
          prune_min_ath_mcap:
            parseOpt((focusAddr ? globalAlert : alert).prune_min_ath_mcap) ?? 50000,
          prune_after_hours:
            parseOpt((focusAddr ? globalAlert : alert).prune_after_hours) ?? 48,
        },
      }
      // Saving globals: when a wallet is focused, keep global mcap/bought from globalAlert.
      const res = await fetch('/api/hvat/filters', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      if (!res.ok) throw new Error(`save ${res.status}`)
      setFlash(
        focusAddr
          ? 'Общие фильтры сохранены (личные правки — через «Применить к выбранным»)'
          : 'Общие фильтры сохранены',
      )
      if (!focusAddr) {
        setGlobalAlert({ ...alert })
      }
      await refresh()
    } catch (e) {
      setFlash(e instanceof Error ? e.message : 'Ошибка сохранения')
    } finally {
      setBusy(false)
    }
  }

  async function applyAlertToSelected(custom: boolean) {
    if (selected.length === 0) {
      setFlash('Выбери кошельки в таблице')
      return
    }
    setBusy(true)
    setFlash('')
    try {
      const gPruneOn = globalAlert.prune_enabled
      const gPruneAth = parseOpt(globalAlert.prune_min_ath_mcap)
      const gPruneHrs = parseOpt(globalAlert.prune_after_hours)
      const pruneOn = alert.prune_enabled
      const pruneAth = parseOpt(alert.prune_min_ath_mcap)
      const pruneHrs = parseOpt(alert.prune_after_hours)
      // Don't pin prune overrides when they match globals — keeps global edits working.
      const pruneSame =
        pruneOn === gPruneOn &&
        pruneAth === gPruneAth &&
        pruneHrs === gPruneHrs
      const filters: WalletAlertFilters = custom
        ? {
            custom: true,
            max_mcap_alert: parseOpt(alert.max_mcap_alert),
            min_mcap_alert: parseOpt(alert.min_mcap_alert),
            min_bought_usd: parseOpt(alert.min_bought_usd),
            max_bought_usd: parseOpt(alert.max_bought_usd),
            prune_enabled: pruneSame ? null : pruneOn,
            prune_min_ath_mcap: pruneSame ? null : pruneAth,
            prune_after_hours: pruneSame ? null : pruneHrs,
          }
        : {
            custom: false,
            max_mcap_alert: null,
            min_mcap_alert: null,
            min_bought_usd: null,
            max_bought_usd: null,
            prune_enabled: null,
            prune_min_ath_mcap: null,
            prune_after_hours: null,
          }
      const res = await fetch('/api/followup/wallets/filters', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ addresses: selected, filters }),
      })
      if (!res.ok) throw new Error(`filters ${res.status}`)
      const data = (await res.json()) as { count: number }
      setFlash(
        custom
          ? `Личные фильтры применены к ${data.count} кош.`
          : `Сброшены к общим: ${data.count} кош.`,
      )
      setAlertCustom(custom)
      await refresh()
    } catch (e) {
      setFlash(e instanceof Error ? e.message : 'Ошибка фильтров')
    } finally {
      setBusy(false)
    }
  }

  async function deleteWallet(address: string) {
    if (!window.confirm(`Удалить ${shortAddr(address)} из слежки?`)) return
    setBusy(true)
    setFlash('')
    try {
      const res = await fetch(`/api/followup/wallets/${encodeURIComponent(address)}`, {
        method: 'DELETE',
      })
      if (!res.ok) throw new Error(`delete ${res.status}`)
      setFlash('Кошелёк удалён')
      setWallets((prev) => prev.filter((w) => w.address.toLowerCase() !== address.toLowerCase()))
      await refresh()
    } catch (e) {
      setFlash(e instanceof Error ? e.message : 'Ошибка удаления')
    } finally {
      setBusy(false)
    }
  }

  const watch = st?.watch
  const follow = st?.followup
  const index = st?.index
  const active = Boolean(watch?.enabled && follow?.enabled)
  const indexLabel = !index
    ? '—'
    : !index.cold_started
      ? `прогрев ${index.enriched}/${index.tokens_24h}`
      : index.cold_tail
        ? `готов ${index.enriched}/${index.tokens_24h} (хвост)`
        : `${index.enriched}/${index.tokens_24h}`
  const logs = [
    ...(watch?.log ?? []).slice(-8).map((x) => ({ ...x, src: 'парс' })),
    ...(follow?.log ?? []).slice(-8).map((x) => ({ ...x, src: 'след' })),
  ]
    .sort((a, b) => a.ts - b.ts)
    .slice(-14)

  return (
    <section className="panel hvat-panel">
      <header className="hvat-hero">
        <h1 className="hvat-title">Хвать</h1>
        <p className="lede">
          Токены по фильтрам → кошельки с одной сделкой → алерты на #2–#5 на низкой mcap.
        </p>
      </header>

      <div className="row gap actions">
        <button
          type="button"
          className="primary"
          disabled={busy}
          onClick={() => void post('/api/hvat/enable', 'Хвать включён')}
        >
          Включить
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void post('/api/hvat/run', 'Цикл запущен')}
        >
          Запустить сейчас
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void post('/api/hvat/disable', 'Хвать выключен')}
        >
          Выключить
        </button>
        <button type="button" className="primary" disabled={busy} onClick={() => void saveFilters()}>
          Сохранить фильтры
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => void post('/api/followup/test-telegram', 'Пинг в топик алертов отправлен')}
        >
          Тест TG #2–#5
        </button>
        {flash ? <span className="muted">{flash}</span> : null}
      </div>

      <div className="stat-grid hvat-stats">
        <div>
          <div className="muted">Статус</div>
          <strong>{active ? 'активен' : 'выкл'}</strong>
        </div>
        <div>
          <div className="muted">Парс</div>
          <strong>
            {watch?.running ? 'идёт' : watch?.enabled ? 'ожидание' : 'выкл'}
          </strong>
        </div>
        <div>
          <div className="muted">След. сделки</div>
          <strong>
            {follow?.running ? 'идёт' : follow?.enabled ? 'ожидание' : 'выкл'}
          </strong>
        </div>
        <div>
          <div className="muted">В слежке</div>
          <strong>{follow?.wallets_watching ?? 0}</strong>
        </div>
        <div>
          <div className="muted">Индекс 24h</div>
          <strong>{indexLabel}</strong>
        </div>
        <div>
          <div className="muted">Найдено / отпр.</div>
          <strong>
            {watch?.last_buyers_found ?? 0} / {watch?.last_buyers_sent ?? 0}
          </strong>
        </div>
        <div>
          <div className="muted">Алерты #2–#5</div>
          <strong>{follow?.last_alerts_sent ?? 0}</strong>
        </div>
      </div>

      <p className="muted hvat-meta">
        Период уникальных токенов: {wallet.tokens_unique_period} · порог 1-й сделки ≤{' '}
        {wallet.mcap_threshold || '—'} · интервал парса {intervalMin} мин
        <br />
        Алерты #2–#5: mcap {alert.min_mcap_alert || '0'}…{alert.max_mcap_alert || '—'} · buy{' '}
        {alert.min_bought_usd || '—'}…{alert.max_bought_usd || '—'} · топик {alert.telegram_topic_id}
        <br />
        Парс: {watch?.last_message || '—'} · след. {fmtTs(watch?.next_run_ts)}
        <br />
        Follow-up: {follow?.last_message || '—'} · след. {fmtTs(follow?.next_run_ts)}
      </p>
      {(watch?.last_error || follow?.last_error) && (
        <p className="error">{watch?.last_error || follow?.last_error}</p>
      )}

      <div className="hvat-filters">
        <div className="hvat-filter-card">
          <h2 className="section-title">Фильтры токенов</h2>
          <div className="filter-grid hvat-filter-grid">
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
              <input type="number" min={0} value={screen.min_mcap} onChange={(e) => setScreenField('min_mcap', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Макс. mcap ($)</span>
              <input type="number" min={0} value={screen.max_mcap} onChange={(e) => setScreenField('max_mcap', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Мин. ATH mcap ($)</span>
              <input type="number" min={0} value={screen.min_ath_mcap} onChange={(e) => setScreenField('min_ath_mcap', e.target.value)} placeholder="выкл" />
            </label>
            <label className="field">
              <span>Мин. трейдеров (24ч)</span>
              <input type="number" min={0} value={screen.min_traders} onChange={(e) => setScreenField('min_traders', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Макс. трейдеров (24ч)</span>
              <input type="number" min={0} value={screen.max_traders} onChange={(e) => setScreenField('max_traders', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Мин. возраст пары (ч)</span>
              <input type="number" min={0} step={0.1} value={screen.min_pair_age_hours} onChange={(e) => setScreenField('min_pair_age_hours', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Макс. возраст пары (ч)</span>
              <input type="number" min={0} step={0.1} value={screen.max_pair_age_hours} onChange={(e) => setScreenField('max_pair_age_hours', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Сортировка</span>
              <select value={screen.sort_by} onChange={(e) => setScreenField('sort_by', e.target.value as ScreenSortBy)}>
                <option value="liquidity">Ликвидность</option>
                <option value="market_cap">Mcap</option>
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
            <label className="field">
              <span>Токенов за цикл</span>
              <input type="number" min={1} max={2000} value={maxTokensCycle} onChange={(e) => setMaxTokensCycle(e.target.value)} />
            </label>
            <label className="field">
              <span>Интервал парса (мин)</span>
              <input
                type="number"
                min={1}
                max={1440}
                value={intervalMin}
                onChange={(e) => setIntervalMin(e.target.value)}
              />
            </label>
            <label className="field checkbox-field">
              <span>Honeypot</span>
              <label className="check">
                <input
                  type="checkbox"
                  checked={screen.exclude_honeypots}
                  onChange={(e) => setScreenField('exclude_honeypots', e.target.checked)}
                />
                Пропускать honeypot
              </label>
            </label>
          </div>
        </div>

        <div className="hvat-filter-card">
          <h2 className="section-title">Фильтры кошельков</h2>
          <div className="filter-grid hvat-filter-grid">
            <label className="field">
              <span>Порог mcap 1-й сделки ($)</span>
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
              <span>Мин. уникальных токенов</span>
              <input type="number" min={0} value={wallet.min_tokens_traded_7d} onChange={(e) => setWalletField('min_tokens_traded_7d', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Макс. уникальных токенов</span>
              <input type="number" min={0} value={wallet.max_tokens_traded_7d} onChange={(e) => setWalletField('max_tokens_traded_7d', e.target.value)} placeholder="любой" />
            </label>
            <label className="field">
              <span>Период уникальных токенов</span>
              <select
                value={wallet.tokens_unique_period}
                onChange={(e) =>
                  setWalletField('tokens_unique_period', e.target.value as TokensUniquePeriod)
                }
              >
                {PERIODS.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </label>
            <label className="field checkbox-field">
              <span>Honeypot</span>
              <label className="check">
                <input
                  type="checkbox"
                  checked={wallet.exclude_honeypots}
                  onChange={(e) => setWalletField('exclude_honeypots', e.target.checked)}
                />
                Пропускать honeypot токена
              </label>
            </label>
          </div>
        </div>

        <div className="hvat-filter-card">
          <h2 className="section-title">Алерты сделок #2 – #5</h2>
          <p className="muted hvat-card-note">
            {focusAddr
              ? `Фильтры для ${shortAddr(focusAddr)}${alertCustom ? ' (личные)' : ' (сейчас общие)'}.`
              : selected.length > 1
                ? `Выбрано ${selected.length} кош. — правь значения и примени.`
                : 'Общие фильтры по умолчанию. Кликни кошелёк в таблице, чтобы посмотреть/задать личные.'}
          </p>
          <div className="row gap hvat-alert-actions">
            <button type="button" className="ghost" disabled={busy} onClick={showGlobalAlerts}>
              Показать общие
            </button>
            <button
              type="button"
              className="primary"
              disabled={busy || selected.length === 0}
              onClick={() => void applyAlertToSelected(true)}
            >
              Применить к выбранным ({selected.length})
            </button>
            <button
              type="button"
              disabled={busy || selected.length === 0}
              onClick={() => void applyAlertToSelected(false)}
            >
              Сбросить к общим
            </button>
          </div>
          <label className="check hvat-custom-check">
            <input
              type="checkbox"
              checked={alertCustom}
              onChange={(e) => setAlertCustom(e.target.checked)}
            />
            Личные фильтры (не общие)
          </label>
          <div className="filter-grid hvat-filter-grid">
            <label className="field">
              <span>Мин. mcap покупки ($)</span>
              <input
                type="number"
                min={0}
                value={alert.min_mcap_alert}
                onChange={(e) => setAlertField('min_mcap_alert', e.target.value)}
                placeholder="выкл"
              />
            </label>
            <label className="field">
              <span>Макс. mcap покупки ($)</span>
              <input
                type="number"
                min={0}
                step={500}
                value={alert.max_mcap_alert}
                onChange={(e) => setAlertField('max_mcap_alert', e.target.value)}
              />
            </label>
            <label className="field">
              <span>Мин. сумма покупки ($)</span>
              <input
                type="number"
                min={0}
                value={alert.min_bought_usd}
                onChange={(e) => setAlertField('min_bought_usd', e.target.value)}
                placeholder="выкл"
              />
            </label>
            <label className="field">
              <span>Макс. сумма покупки ($)</span>
              <input
                type="number"
                min={0}
                value={alert.max_bought_usd}
                onChange={(e) => setAlertField('max_bought_usd', e.target.value)}
                placeholder="выкл"
              />
            </label>
            <label className="field">
              <span>Telegram topic id</span>
              <input
                type="text"
                value={alert.telegram_topic_id}
                onChange={(e) => setAlertField('telegram_topic_id', e.target.value)}
                placeholder="9245"
              />
            </label>
            <label className="check field">
              <span>Автоудаление (#1–#5 не до ATH)</span>
              <input
                type="checkbox"
                checked={alert.prune_enabled}
                onChange={(e) => setAlertField('prune_enabled', e.target.checked)}
              />
            </label>
            <label className="field">
              <span>Мин. ATH токена ($)</span>
              <input
                type="number"
                min={0}
                step={1000}
                value={alert.prune_min_ath_mcap}
                onChange={(e) => setAlertField('prune_min_ath_mcap', e.target.value)}
                disabled={!alert.prune_enabled}
              />
            </label>
            <label className="field">
              <span>Срок ожидания (часы)</span>
              <input
                type="number"
                min={1}
                step={1}
                value={alert.prune_after_hours}
                onChange={(e) => setAlertField('prune_after_hours', e.target.value)}
                disabled={!alert.prune_enabled}
                placeholder="48 = 2 дня"
              />
            </label>
          </div>
          <p className="muted hvat-card-note">
            Если токен сделки #1–#5 не достиг ATH за срок — кошелёк снимается со слежки
            (после последней сделки тоже, в т.ч. статус done). Сохрани общие фильтры или примени личные.
          </p>
        </div>
      </div>

      <h2 className="section-title">Кошельки ({wallets.length})</h2>
      {wallets.length === 0 ? (
        <p className="empty">Пока пусто — сохрани фильтры, включи Хвать и дождись автопарса.</p>
      ) : (
        <>
          <div className="row gap hvat-wallet-toolbar">
            <button type="button" className="ghost" disabled={busy} onClick={toggleSelectAll}>
              {selected.length === wallets.length ? 'Снять все' : 'Выбрать все'}
            </button>
            <span className="muted">выбрано {selected.length}</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>
                    <input
                      type="checkbox"
                      checked={selected.length > 0 && selected.length === wallets.length}
                      onChange={toggleSelectAll}
                      aria-label="Выбрать все"
                    />
                  </th>
                  <th>Кошелёк</th>
                  <th>Статус</th>
                  <th>Сделок</th>
                  <th>1-я mcap</th>
                  <th>Фильтры #2–#5</th>
                  <th>Последние deals</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {wallets.map((w) => {
                  const isSel = selected.includes(w.address)
                  const isFocus = focusAddr === w.address
                  return (
                    <tr
                      key={w.address}
                      className={isFocus ? 'hvat-row-focus' : isSel ? 'hvat-row-sel' : undefined}
                    >
                      <td>
                        <input
                          type="checkbox"
                          checked={isSel}
                          onChange={() => toggleSelect(w.address)}
                          aria-label={`Выбрать ${shortAddr(w.address)}`}
                        />
                      </td>
                      <td>
                        <button
                          type="button"
                          className="linkish mono"
                          onClick={() => loadWalletIntoAlert(w.address)}
                          title="Показать фильтры алертов"
                        >
                          {shortAddr(w.address)}
                        </button>{' '}
                        <a
                          className="muted"
                          href={gmgnWallet(w.address)}
                          target="_blank"
                          rel="noreferrer"
                          title="GMGN"
                        >
                          ↗
                        </a>
                      </td>
                      <td>{w.status}</td>
                      <td>{w.deal_count}</td>
                      <td>{fmtNum(w.first_mcap)}</td>
                      <td className="muted">
                        {fmtWalletFilters(w.alert_filters, st?.followup_config)}
                      </td>
                      <td>
                        {(w.deals ?? [])
                          .slice(-3)
                          .map(
                            (d) =>
                              `#${d.deal_index} ${d.token_symbol || d.token.slice(0, 6)} @${fmtNum(d.mcap_at_buy)}${d.notified ? '✓' : ''}`,
                          )
                          .join(' · ') || '—'}
                      </td>
                      <td>
                        <button
                          type="button"
                          className="ghost"
                          disabled={busy}
                          onClick={() => void deleteWallet(w.address)}
                        >
                          Удалить
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      <h2 className="section-title">Лог</h2>
      <ul className="job-log">
        {logs.length === 0 ? (
          <li className="muted">Нет записей</li>
        ) : (
          logs.map((x, i) => (
            <li key={`${x.ts}-${i}`}>
              <span className="muted">{fmtLogTime(x.ts)}</span>{' '}
              <span className="muted">[{x.src}]</span> {x.message}
            </li>
          ))
        )}
      </ul>
    </section>
  )
}
