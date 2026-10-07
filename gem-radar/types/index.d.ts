export type GemRadarTab = 'scan' | 'watchlist' | 'history'

export type ScanSummary = {
  id: number
  contract: string
  chain: string
  ts: number
  verdict: string
  score: number
  confidence: number
  mode: string
  model: string | null
  is_mock: boolean
  critical: string[]
  freshness: Record<string, number>
  change?: number
  changes?: string[]
  flags_added?: string[]
  flags_removed?: string[]
}

export type WatchEntry = {
  contract: string
  chain: string
  added_at: number
  current_score: number | null
  previous_score: number | null
  change: number | null
  verdict: string | null
  confidence: number | null
  scanned_at: number | null
  movement?: string
  changes: string[]
}

export type PanelData = {
  latest: ScanSummary | null
  watchlist: WatchEntry[]
  history: ScanSummary[]
}

declare module 'claude-code' {
  interface PluginState {
    'gem-radar': {
      tab: GemRadarTab
      panel: PanelData | null
      busy: string | null
      lastText: string | null
    }
  }
}
