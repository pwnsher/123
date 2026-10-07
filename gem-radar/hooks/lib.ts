// Pure helpers for the Gem Radar mod (no `$`): argument parsing, model choice,
// output policy. Kept separate so they are easy to read and test.

export type ParsedArgs =
  | { ok: true; contract: string; chain?: string; deep?: 'auto' | 'force' | 'off' }
  | { ok: false; error: string }

const ADDRESS = /^[0-9A-Za-z:_-]{1,128}$/
const CHAIN = /^[a-z0-9-]{2,32}$/

export function parseArgs(args: string): ParsedArgs {
  const parts = args.trim().split(/\s+/).filter(Boolean)
  let contract: string | undefined
  let chain: string | undefined
  let deep: 'auto' | 'force' | 'off' | undefined
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i] ?? ''
    if (p === '--chain' || p.startsWith('--chain=')) {
      const v = p.includes('=') ? p.split('=')[1] : parts[++i]
      if (!v || !CHAIN.test(v.toLowerCase())) return { ok: false, error: '--chain needs a chain name' }
      chain = v.toLowerCase()
    } else if (p === '--deep' || p.startsWith('--deep=')) {
      const v = p.includes('=') ? p.split('=')[1] : parts[++i]
      if (v !== 'auto' && v !== 'force' && v !== 'off') {
        return { ok: false, error: '--deep takes auto, force or off' }
      }
      deep = v
    } else if (contract === undefined) {
      contract = p
    } else {
      return { ok: false, error: `unexpected argument: ${p.slice(0, 64)}` }
    }
  }
  if (!contract) return { ok: false, error: 'usage: /gem <contract> [--chain <name>] [--deep auto|force|off]' }
  if (!ADDRESS.test(contract)) return { ok: false, error: 'contract contains characters no address format uses' }
  return { ok: true, contract, chain, deep }
}

export function cliFlags(a: { chain?: string; deep?: string }): string[] {
  const out: string[] = []
  if (a.chain) out.push('--chain', a.chain)
  if (a.deep) out.push('--deep', a.deep)
  return out
}

// The strongest model this session can use, detected at run time: the session's own
// model first when it is a top-tier family, then the `opus` alias (Claude Code resolves
// it to the newest Opus the account may use), then the session's model whatever it is.
export function modelCandidates(sessionModel: string | undefined): string[] {
  const out: string[] = []
  const s = (sessionModel ?? '').trim()
  if (s && /opus|fable/i.test(s)) out.push(s)
  out.push('opus')
  if (s && !out.includes(s)) out.push(s)
  return out
}

export function modelLabel(m: string): string {
  return m === 'opus' ? 'opus (Claude Code alias for the newest Opus this session may use)' : m
}

// The interpretation may not call a token safe, guaranteed or risk-free.
const FORBIDDEN =
  /\b(is|looks|appears|seems|totally|completely)\s+(safe|risk[- ]free|guaranteed)\b|\bguaranteed\s+(returns?|gains?|profit)|\bcertain\s+to\s+(rise|pump|moon|go up)/i

export function policyCheck(text: string): string {
  if (FORBIDDEN.test(text)) {
    return '[interpretation withheld: the model output broke the no-"safe"/no-guarantee policy]'
  }
  return text.trim().slice(0, 4000)
}

export function shortAddr(a: string): string {
  return a.length > 14 ? `${a.slice(0, 6)}…${a.slice(-4)}` : a
}

export function isoTime(ts: number | null | undefined): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z')
}
