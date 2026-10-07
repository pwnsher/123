// Gem Radar mod: slash commands, a display-only side panel, and the DEEP-scan
// interpretation call. All data collection and scoring is deterministic Python
// (../src/gem_radar), run with $.process.run. This module never trades, never
// touches wallets, and makes a model call only for a DEEP scan the person ran.
import { atom, read, update } from 'claude-code'
import type { EngineInterface, Register } from 'claude-code'

import type { GemRadarTab, PanelData, ScanSummary, WatchEntry } from '../types'
import { cliFlags, isoTime, modelCandidates, modelLabel, parseArgs, policyCheck, shortAddr } from './lib'

type Engine = EngineInterface

const PANE = 'gem-radar'
const tab = atom({ plugin: 'gem-radar', key: 'tab' } as const, 'scan' as GemRadarTab)
const panel = atom({ plugin: 'gem-radar', key: 'panel' } as const, null as PanelData | null)
const busy = atom({ plugin: 'gem-radar', key: 'busy' } as const, null as string | null)
const lastText = atom({ plugin: 'gem-radar', key: 'lastText' } as const, null as string | null)


type Llm = { status: string; system: string; prompt: string; note?: string }
type CliOut = { result: Record<string, unknown> & { kind?: string; scan_id?: number }; text: string }

async function py($: Engine, args: string[], opts: { stdin?: string; offline?: boolean; timeoutMs?: number } = {}) {
  const python = (await $.env.get('GEM_RADAR_PYTHON')) || 'python3'
  const env: Record<string, string> = { PYTHONPATH: `${$.plugin.root}/src`, PYTHONIOENCODING: 'utf-8' }
  if (opts.offline) env.GEM_RADAR_OFFLINE = '1'
  return $.process.run([python, '-m', 'gem_radar', ...args], {
    env, stdin: opts.stdin, timeoutMs: opts.timeoutMs ?? 30_000,
  })
}

function parseJson<T>(stdout: string): T | undefined {
  const line = stdout.trim().split('\n').pop()
  if (!line) return undefined
  try {
    return JSON.parse(line) as T
  } catch {
    return undefined
  }
}

// Local reads only (the CLI turns the network off): never a provider or model call.
async function refreshPanel($: Engine): Promise<void> {
  const r = await py($, ['panel-data'], { offline: true })
  const data = parseJson<PanelData>(r.stdout)
  if (r.exitCode === 0 && data) await update($, panel, () => data)
}

async function interpret($: Engine, llm: Llm): Promise<{ model?: string; text?: string; note: string }> {
  let sessionModel: string | undefined
  try {
    sessionModel = await $.session.model()
  } catch {
    sessionModel = undefined
  }
  const tried: string[] = []
  for (const model of modelCandidates(sessionModel)) {
    let r
    try {
      r = await $.model.complete({ model, system: llm.system, prompt: llm.prompt, maxTokens: 900, timeoutMs: 120_000 })
    } catch {
      tried.push(`${model}: not allowed`)
      continue
    }
    if (r.isAnswered) return { model, text: policyCheck(r.text), note: '' }
    if (r.reason === 'api-error' && (r.status === 400 || r.status === 403 || r.status === 404)) {
      tried.push(`${model}: HTTP ${r.status}`)
      continue
    }
    return { note: `${model}: ${r.reason}${r.reason === 'api-error' ? ` ${r.status ?? ''} ${r.error}` : ''}` }
  }
  return { note: tried.join('; ') || 'no model available' }
}

async function runScan($: Engine, kind: 'scan' | 'recheck', rawArgs: string): Promise<{ text: string }> {
  const a = parseArgs(rawArgs)
  if (!a.ok) return { text: `Gem Radar: ${a.error}` }
  await update($, busy, () => `${kind === 'scan' ? 'Scanning' : 'Rechecking'} ${shortAddr(a.contract)}…`)
  try {
    const flags = kind === 'scan' ? cliFlags(a) : cliFlags({ chain: a.chain })
    const r = await py($, [kind, a.contract, ...flags, '--json', '--for-mod'], { timeoutMs: 300_000 })
    const out = parseJson<CliOut>(r.stdout)
    if (!out) return { text: `Gem Radar failed (exit ${r.exitCode}): ${r.stderr.slice(-800) || 'no output'}` }
    let text = out.text
    const res = out.result
    const analysis = res.analysis as { llm?: Llm | null; interpretation?: string | null } | undefined
    if (res.kind === 'verdict' && analysis?.llm?.status === 'PENDING' && res.scan_id) {
      const got = await interpret($, analysis.llm)
      analysis.interpretation = got.text ?? null
      analysis.llm = { ...analysis.llm, status: got.text ? 'DONE' : 'UNAVAILABLE', note: got.note }
      const model = got.model ? modelLabel(got.model) : `none (no model answered: ${got.note})`
      const f = await py($, ['finalize', String(res.scan_id), '--model', model, '--json'], {
        stdin: JSON.stringify(res), offline: true,
      })
      const fin = parseJson<CliOut>(f.stdout)
      if (f.exitCode === 0 && fin) text = fin.text
    }
    await update($, lastText, () => text)
    await refreshPanel($)
    return { text }
  } finally {
    await update($, busy, () => null)
  }
}

async function runLocal($: Engine, args: string[]): Promise<{ text: string }> {
  const r = await py($, [...args, '--json'], { offline: true })
  const out = parseJson<CliOut>(r.stdout)
  await refreshPanel($)
  return { text: out?.text ?? `Gem Radar failed (exit ${r.exitCode}): ${r.stderr.slice(-500)}` }
}

// A command that fails answers with the reason instead of being skipped silently.
const failed = (_$: Engine, e: { command: string }, next: { error?: { message?: string } }) => ({
  text: `Gem Radar /${e.command} failed: ${next.error?.message?.slice(0, 300) ?? 'unknown error'}`,
})

function watchArgs(raw: string): { contract?: string; flags: string[]; error?: string } {
  if (!raw.trim()) return { flags: [] }
  const a = parseArgs(raw)
  if (!a.ok) return { flags: [], error: a.error }
  return { contract: a.contract, flags: cliFlags({ chain: a.chain }) }
}

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await $.command.register({
      name: 'gem', description: 'Gem Radar: score a token contract (GEM / WATCH / AVOID)',
      argumentHint: '<contract> [--chain <name>] [--deep auto|force|off]',
    })
    await $.command.register({
      name: 'gem-recheck', description: 'Gem Radar: rescan bypassing the cache and show what changed',
      argumentHint: '<contract> [--chain <name>]',
    })
    await $.command.register({ name: 'radar', description: 'Gem Radar: open the panel (scan, watchlist, history)' })
    await $.command.register({
      name: 'gem-watch', description: 'Gem Radar: add a contract to the watchlist (no args: list it)',
      argumentHint: '[<contract>] [--chain <name>]',
    })
    await $.command.register({
      name: 'gem-unwatch', description: 'Gem Radar: remove a contract from the watchlist', argumentHint: '<contract>',
    })
    await $.command.register({
      name: 'gem-history', description: 'Gem Radar: stored scans for a contract and what changed',
      argumentHint: '<contract> [--chain <name>]',
    })
    return next(e)
  })

  on('command.run', { command: 'gem' }, async ($, e) => runScan($, 'scan', e.args)).catch(failed)
  on('command.run', { command: 'gem-recheck' }, async ($, e) => runScan($, 'recheck', e.args)).catch(failed)

  on('command.run', { command: 'gem-watch' }, async ($, e) => {
    const w = watchArgs(e.args)
    if (w.error) return { text: `Gem Radar: ${w.error}` }
    return runLocal($, w.contract ? ['watch', 'add', w.contract, ...w.flags] : ['watch', 'list'])
  }).catch(failed)

  on('command.run', { command: 'gem-unwatch' }, async ($, e) => {
    const w = watchArgs(e.args)
    if (w.error || !w.contract) return { text: `Gem Radar: ${w.error ?? 'usage: /gem-unwatch <contract>'}` }
    return runLocal($, ['watch', 'remove', w.contract])
  }).catch(failed)

  on('command.run', { command: 'gem-history' }, async ($, e) => {
    const w = watchArgs(e.args)
    if (w.error || !w.contract) return { text: `Gem Radar: ${w.error ?? 'usage: /gem-history <contract>'}` }
    return runLocal($, ['history', w.contract, ...w.flags])
  }).catch(failed)

  on('command.run', { command: 'radar' }, async $ => {
    await refreshPanel($)
    const data = await read($, panel)
    try {
      await $.ui.open({ id: PANE, title: 'Gem Radar', focus: true })
    } catch {
      return { text: 'Gem Radar: this surface cannot show panels. Use /gem, /gem-watch and /gem-history.' }
    }
    const n = data?.history.length ?? 0
    return { text: `Gem Radar panel opened (display only). ${n} recent scan(s), ${data?.watchlist.length ?? 0} watched.` }
  }).catch(failed)

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const table = $.ui.resolve(e)
    const { Box, Text, Button } = table
    const Input = 'Input' in table ? table.Input : undefined // mobile draws no Input
    const current = await read($, tab)
    const data = await read($, panel)
    const working = await read($, busy)
    const text = await read($, lastText)

    const tabButton = (id: GemRadarTab, label: string, hotkey: string) => (
      <Button key={`tab-${id}`} label={label} hotkey={hotkey} variant={current === id ? 'primary' : 'secondary'}
        onPress={() => update($, tab, () => id)} />
    )

    const verdictColor = (v: string | null | undefined) =>
      v === 'GEM' ? 'success' : v === 'AVOID' ? 'error' : v === 'WATCH' ? 'warning' : 'inactive'

    const scanTab = () => {
      const s: ScanSummary | null | undefined = data?.latest
      return (
        <Box flexDirection="column">
          {Input && (
            <Input key="contract" label="Contract" placeholder="paste an address, Enter to scan"
              onSubmit={(value: string) => { void runScan($, 'scan', value).then(r => $.ui.toast(r.text.split('\n')[0] ?? '')) }} />
          )}
          {working && <Text color="warning">{working}</Text>}
          {!s && <Text dimColor>No scans yet. Run /gem {'<contract>'} or submit one above.</Text>}
          {s && (
            <Box flexDirection="column">
              <Text bold color={verdictColor(s.verdict)}>
                {s.verdict} — {s.score}/100 — Confidence {s.confidence}/100{s.is_mock ? '  [TEST/MOCK]' : ''}
              </Text>
              <Text>Chain: {s.chain} · {s.contract}</Text>
              <Text dimColor>Last updated: {isoTime(s.ts)} · {s.mode}{s.model && s.mode === 'DEEP' ? ` · ${s.model}` : ''}</Text>
              <Text dimColor>
                Freshness: {Object.entries(s.freshness).map(([k, v]) => `${k} ${v}`).join(', ') || 'no data'}
              </Text>
              {s.critical.map(c => <Text color="error">CRITICAL: {c}</Text>)}
            </Box>
          )}
          {text && <Text dimColor>{'\n'}{text}</Text>}
        </Box>
      )
    }

    const watchTab = () => {
      const list: WatchEntry[] = data?.watchlist ?? []
      if (list.length === 0) return <Text dimColor>Watchlist is empty. /gem-watch {'<contract>'} adds one.</Text>
      return (
        <Box flexDirection="column">
          <Text dimColor>No background monitoring: /gem-recheck {'<contract>'} refreshes a row.</Text>
          {list.map(w => (
            <Box flexDirection="column">
              <Text color={verdictColor(w.verdict)}>
                {w.chain} {shortAddr(w.contract)} · {w.previous_score ?? '—'} → {w.current_score ?? '—'}
                {w.change !== null ? ` (${w.change >= 0 ? '+' : ''}${w.change})` : ''} · {w.verdict ?? 'not scanned'} · {isoTime(w.scanned_at)}
              </Text>
              {w.previous_score !== null && w.changes.slice(0, 4).map(c => <Text dimColor>  · {c}</Text>)}
            </Box>
          ))}
        </Box>
      )
    }

    const historyTab = () => {
      const list: ScanSummary[] = data?.history ?? []
      if (list.length === 0) return <Text dimColor>No stored scans.</Text>
      return (
        <Box flexDirection="column">
          {list.map(h => (
            <Box flexDirection="column">
              <Text color={verdictColor(h.verdict)}>
                #{h.id} {isoTime(h.ts)} {h.chain} {shortAddr(h.contract)} {h.verdict} {h.score} (conf {h.confidence}) {h.mode}
                {h.change !== undefined ? ` ${h.change >= 0 ? '+' : ''}${h.change}` : ''}{h.is_mock ? ' [TEST/MOCK]' : ''}
              </Text>
              {(h.flags_added ?? []).length > 0 && <Text color="error">  + flags: {(h.flags_added ?? []).join(', ')}</Text>}
              {(h.flags_removed ?? []).length > 0 && <Text color="success">  − flags: {(h.flags_removed ?? []).join(', ')}</Text>}
              {(h.changes ?? []).slice(0, 3).map(c => <Text dimColor>  · {c}</Text>)}
            </Box>
          ))}
        </Box>
      )
    }

    return (
      <Box flexDirection="column">
        <Box flexDirection="row" gap={1}>
          {tabButton('scan', 'Scan', '1')}
          {tabButton('watchlist', 'Watchlist', '2')}
          {tabButton('history', 'History', '3')}
        </Box>
        {current === 'scan' ? scanTab() : current === 'watchlist' ? watchTab() : historyTab()}
        <Text dimColor>Display only · heuristic risk analysis, not financial advice.</Text>
      </Box>
    )
  })
}
