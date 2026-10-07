// Mod tests: run with `claude plugin test <gem-radar folder>`.
// The test's `on` hooks stand in for the engine beneath the plugin: they answer
// $.process.run with TEST/MOCK CLI output and count every $.model.complete.
import { expect, mock, test } from 'claude-code/testing'

import { modelCandidates, parseArgs, policyCheck } from '../hooks/lib'

const PANEL = {
  latest: {
    id: 7, contract: '0x1111111111111111111111111111111111111111', chain: 'ethereum', ts: 1_790_000_000,
    verdict: 'WATCH', score: 63, confidence: 76, mode: 'DEEP', model: 'opus', is_mock: true,
    critical: [], freshness: { LIVE: 12 },
  },
  watchlist: [{
    contract: '0x1111111111111111111111111111111111111111', chain: 'ethereum', added_at: 1, current_score: 61,
    previous_score: 74, change: -13, verdict: 'WATCH', confidence: 70, scanned_at: 1_790_000_000,
    movement: '74 → 61 (-13)', changes: ['Holders: 18 → 9'],
  }],
  history: [],
}

function scanOut(mode: 'FAST' | 'DEEP') {
  return JSON.stringify({
    text: `WATCH — 63/100 — Confidence 76/100\n(TEST/MOCK ${mode})`,
    result: {
      kind: 'verdict', scan_id: 7,
      analysis: {
        mode, model: 'none', reasons: mode === 'DEEP' ? ['score 63 is in the 40-80 band'] : [],
        interpretation: null,
        llm: mode === 'DEEP'
          ? { status: 'PENDING', system: 'use ONLY facts in the pack', prompt: 'Evidence pack (JSON): {}' }
          : null,
      },
    },
  })
}

type Calls = { argv: string[]; env: Record<string, string>; stdin?: string }[]

function stand(on: any, mode: 'FAST' | 'DEEP', model: 'answers' | 'missing') {
  const proc: Calls = []
  const models: { model: string; system?: string }[] = []
  const opened: string[] = []
  mock.env(on, {})
  on('session.start', async (_$: unknown, e: any) => ({ cwd: e.cwd }))
  on('command.register', async (_$: unknown, e: any) => ({ value: { command: e.name } }))
  on('session.model', async () => ({ value: 'claude-opus-5-5' }))
  on('ui.open', async (_$: unknown, e: any) => {
    opened.push(e.id)
    return { value: { isPlaced: true } }
  })
  on('process.run', async (_$: unknown, e: any) => {
    const argv = [...e.argv] as string[]
    proc.push({ argv, env: e.init?.env ?? {}, stdin: e.init?.stdin })
    const sub = argv[3]
    const stdout =
      sub === 'panel-data' ? JSON.stringify(PANEL)
        : sub === 'scan' || sub === 'recheck' ? scanOut(mode)
          : sub === 'finalize' ? JSON.stringify({ text: 'FINALIZED (TEST/MOCK)', result: {} })
            : JSON.stringify({ text: `ran ${sub}`, result: [] })
    return { value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('model.complete', async (_$: unknown, e: any) => {
    models.push({ model: e.model, system: e.system })
    const usage = { input_tokens: 1, output_tokens: 1, cache_creation_input_tokens: 0, cache_read_input_tokens: 0 }
    if (model === 'missing') {
      return { value: { isAnswered: false, reason: 'api-error', status: 404, error: 'invalid_request', usage } }
    }
    return { value: { isAnswered: true, text: 'Conflict on liquidity most plausibly reflects pool coverage.', usage } }
  })
  return { proc, models, opened }
}

async function start($: any) {
  await $.session.start({ cwd: '/tmp', surface: 'terminal', isInteractive: true })
}

const run = ($: any, command: string, args = '') =>
  $.command.run({ command, args, origin: { kind: 'composer' }, presentation: { isFullscreen: true, columns: 160 } })

test('opening the panel and switching tabs makes no model call and no provider request', async ($: any, on: any) => {
  const { proc, models, opened } = stand(on, 'FAST', 'answers')
  await start($)
  const answer = await run($, 'radar')
  expect(answer.text).toContain('Gem Radar panel opened')
  expect(opened).toEqual(['gem-radar'])
  // /radar read local data only, with the network switched off
  expect(proc.map(p => p.argv[3])).toEqual(['panel-data'])
  expect(proc[0]?.env.GEM_RADAR_OFFLINE).toBe('1')
  const before = proc.length
  for (const surface of ['terminal', 'desktop'] as const) {
    const ui = await $.ui.mount({
      plugin: 'gem-radar', surface, component: 'Pane', requestId: 'gem-radar',
      props: { title: 'Gem Radar', isFocused: true, bodyColumns: 100, placement: 'dock',
        scroll: { offset: 0, bodyRows: 40 }, view: {} },
    })
    expect(await ui.find({ type: 'Text', text: /WATCH — 63\/100 — Confidence 76\/100/ })).toBeDefined()
    await ui.press({ key: 'tab-watchlist' })
    expect(await ui.find({ type: 'Text', text: /74 → 61/ })).toBeDefined()
    await ui.press({ key: 'tab-history' })
    expect(await ui.find({ type: 'Text', text: /No stored scans/ })).toBeDefined()
    await ui.press({ key: 'tab-scan' })
    await ui.unmount()
  }
  expect(models.length).toBe(0)
  expect(proc.length).toBe(before)
})

test('/gem on a DEEP scan asks one model, from the evidence pack only, and stores it', async ($: any, on: any) => {
  const { proc, models } = stand(on, 'DEEP', 'answers')
  await start($)
  const out = await run($, 'gem', '0x1111111111111111111111111111111111111111 --chain ethereum')
  expect(models.length).toBe(1)
  expect(models[0]?.system).toContain('use ONLY facts')
  const fin = proc.find(p => p.argv[3] === 'finalize')
  expect(fin).toBeDefined()
  expect(fin?.argv).toContain('7')
  expect(JSON.parse(fin?.stdin ?? '{}').analysis.interpretation).toContain('pool coverage')
  expect(fin?.env.GEM_RADAR_OFFLINE).toBe('1')
  expect(out.text).toBe('FINALIZED (TEST/MOCK)')
  const scan = proc.find(p => p.argv[3] === 'scan')
  expect(scan?.argv.slice(4)).toEqual(['0x1111111111111111111111111111111111111111', '--chain', 'ethereum', '--json', '--for-mod'])
})

test('/gem on a FAST scan makes no model call', async ($: any, on: any) => {
  const { models } = stand(on, 'FAST', 'answers')
  await start($)
  const out = await run($, 'gem', '0x1111111111111111111111111111111111111111')
  expect(models.length).toBe(0)
  expect(out.text).toContain('WATCH — 63/100')
})

test('no available model is recorded, not invented', async ($: any, on: any) => {
  const { proc, models } = stand(on, 'DEEP', 'missing')
  await start($)
  await run($, 'gem', '0x1111111111111111111111111111111111111111')
  expect(models.length).toBeGreaterThan(0)
  const fin = proc.find(p => p.argv[3] === 'finalize')
  const i = fin?.argv.indexOf('--model') ?? -1
  expect(fin?.argv[i + 1]).toContain('none (no model answered')
  expect(JSON.parse(fin?.stdin ?? '{}').analysis.interpretation).toBe(null)
})

test('malformed arguments never reach the CLI', async ($: any, on: any) => {
  const { proc } = stand(on, 'FAST', 'answers')
  await start($)
  const out = await run($, 'gem', '0xabc;rm')
  expect(out.text).toContain('characters no address format uses')
  expect(proc.length).toBe(0)
})

test('helpers: arguments, model choice, output policy', async () => {
  expect(parseArgs('abc --chain base --deep off')).toEqual({ ok: true, contract: 'abc', chain: 'base', deep: 'off' })
  expect(parseArgs('').ok).toBe(false)
  expect(parseArgs('a b').ok).toBe(false)
  expect(parseArgs('a --deep maybe').ok).toBe(false)
  expect(modelCandidates('claude-opus-5-5')).toEqual(['claude-opus-5-5', 'opus'])
  expect(modelCandidates('claude-sonnet-5-5')).toEqual(['opus', 'claude-sonnet-5-5'])
  expect(policyCheck('This token is safe to hold.')).toContain('withheld')
  expect(policyCheck('Liquidity readings disagree.')).toBe('Liquidity readings disagree.')
})
