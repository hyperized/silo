import { describe, it, expect, vi, afterEach, beforeEach } from 'vitest'
import fixture from './__fixtures__/status.json'
import { fetchMockStatus, mockWire, resetMockSession } from './mock'
import { MOCK_SCENARIOS, mockScenario, type MockScenario } from './mockMode'
import { StatusUnreachableError, parseStatus, type WireStatus } from './status'
import { deriveHealth } from '../lib/health'

describe('mockScenario', () => {
  it.each(MOCK_SCENARIOS)('picks %s from ?mock=%s', (name) => {
    expect(mockScenario(`?mock=${name}`, undefined)).toBe(name)
  })

  it.each<[string, string, string | undefined, MockScenario | null]>([
    ['?mock=1 alone', '?mock=1', undefined, 'attention'],
    ['the env flag alone', '', '1', 'attention'],
    ['a named scenario over the env flag', '?mock=degraded', '1', 'degraded'],
    ['an unknown value', '?mock=bogus', undefined, null],
    ['an unknown value with the env flag', '?mock=bogus', '1', 'attention'],
    ['an empty value', '?mock=', undefined, null],
    ['nothing', '', undefined, null],
    ['an env flag other than 1', '', '0', null],
    ['unrelated params', '?foo=bar', undefined, null],
  ])('handles %s', (_n, search, env, want) => {
    expect(mockScenario(search, env)).toBe(want)
  })
})

describe('mockWire', () => {
  const base = fixture as unknown as WireStatus
  const nowMs = 1_800_000_000_000
  const delta = nowMs / 1000 - 1761641000

  it('shifts every set timestamp by the distance from the fixture clock', () => {
    const w = mockWire('healthy', nowMs)
    base.nodes.forEach((n, i) => {
      expect(Number(w.nodes[i]?.lastChangeUnix)).toBe(Number(n.lastChangeUnix) + delta)
    })
    // The backup run is placed relative to the session instead, see the backup tests below.
    expect(Number(w.health?.startedUnix)).toBe(Number(base.health?.startedUnix) + delta)
    expect(Number(w.gc?.lastRunUnix)).toBe(Number(base.gc?.lastRunUnix) + delta)
  })

  it('tolerates a fixture from a silod without health or gc', async () => {
    const old = structuredClone(base)
    delete old.health
    delete old.gc
    for (const v of old.volumes) delete v.replicas
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: old }))
    const fresh = await import('./mock')
    for (const sc of ['healthy', 'attention', 'degraded', 'incident'] as const) {
      const w = fresh.mockWire(sc, nowMs)
      expect(w.health).toBeUndefined()
      expect(w.gc).toBeUndefined()
      expect(w.volumes.slice(0, 2).every((v) => !('replicas' in v))).toBe(true)
    }
    vi.doUnmock('./__fixtures__/status.json')
  })

  it('leaves zero health and gc timestamps untouched', async () => {
    const zeroed = structuredClone(base)
    if (zeroed.health) zeroed.health.startedUnix = '0'
    if (zeroed.gc) delete zeroed.gc.lastRunUnix
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: zeroed }))
    const fresh = await import('./mock')
    const w = fresh.mockWire('healthy', nowMs)
    vi.doUnmock('./__fixtures__/status.json')
    expect(w.health?.startedUnix).toBe('0')
    expect(w.gc?.lastRunUnix).toBeUndefined()
  })

  it('leaves zero timestamps untouched', async () => {
    const zeroed = structuredClone(base)
    for (const n of zeroed.nodes) n.lastChangeUnix = '0'
    if (zeroed.backup) {
      zeroed.backup.nextRunUnix = '0'
      if (zeroed.backup.lastRun) {
        zeroed.backup.lastRun.startedUnix = '0'
        zeroed.backup.lastRun.finishedUnix = '0'
      }
    }
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: zeroed }))
    const fresh = await import('./mock')
    const w = fresh.mockWire('healthy', nowMs)
    vi.doUnmock('./__fixtures__/status.json')
    expect(w.nodes.map((n) => n.lastChangeUnix)).toEqual(['0', '0', '0'])
    expect(w.backup?.nextRunUnix).toBe(String(nowMs / 1000 + 120))
  })

  it('degraded tolerates a fixture without a backup', async () => {
    const bare = structuredClone(base)
    bare.backup = null
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: bare }))
    const fresh = await import('./mock')
    expect(fresh.mockWire('degraded', nowMs).backup).toBeNull()
    vi.doUnmock('./__fixtures__/status.json')
  })

  it('tolerates a fixture without backup or last run', async () => {
    const bare = structuredClone(base)
    bare.backup = null
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: bare }))
    const fresh = await import('./mock')
    const w = fresh.mockWire('attention', nowMs)
    vi.doUnmock('./__fixtures__/status.json')
    expect(w.backup).toBeNull()
    const noRun = structuredClone(base)
    if (noRun.backup) noRun.backup.lastRun = null
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: noRun }))
    const fresh2 = await import('./mock')
    expect(fresh2.mockWire('attention', nowMs).backup?.lastRun).toBeNull()
    vi.doUnmock('./__fixtures__/status.json')
  })

  it('tolerates a fixture with fewer than three nodes', async () => {
    const small = structuredClone(base)
    small.nodes = small.nodes.slice(0, 1)
    vi.resetModules()
    vi.doMock('./__fixtures__/status.json', () => ({ default: small }))
    const fresh = await import('./mock')
    expect(fresh.mockWire('attention', nowMs).nodes).toHaveLength(1)
    expect(fresh.mockWire('degraded', nowMs).nodes).toHaveLength(1)
    expect(fresh.mockWire('incident', nowMs).nodes).toHaveLength(1)
    vi.doUnmock('./__fixtures__/status.json')
  })

  it('does not mutate the imported fixture', () => {
    mockWire('degraded', nowMs)
    mockWire('empty', nowMs)
    expect(base.nodes).toHaveLength(3)
    expect(base.nodes[2]?.state).toBe('NODE_STATE_ALIVE')
    expect(base.backup?.running).toBe(false)
  })

  it('healthy keeps the fixture contents', () => {
    const w = mockWire('healthy', nowMs)
    expect(w.nodes.every((n) => n.state === 'NODE_STATE_ALIVE')).toBe(true)
    expect(w.backup?.lastRun?.ok).toBe(true)
    expect(w.volumes).toHaveLength(12)
  })

  it('error uses the healthy payload', () => {
    expect(mockWire('error', nowMs)).toEqual(mockWire('healthy', nowMs))
  })

  it('attention makes one node suspect, one disk 84% full and the last backup fail', () => {
    const w = mockWire('attention', nowMs)
    expect(w.nodes[1]?.state).toBe('NODE_STATE_SUSPECT')
    expect(w.nodes[1]?.lastChangeUnix).toBe(String(nowMs / 1000 - 40))
    const c = w.nodes[2]
    expect(Number(c?.usedBytes) / Number(c?.capacityBytes)).toBeCloseTo(0.84, 2)
    expect(w.backup?.lastRun?.ok).toBe(false)
    expect(w.backup?.lastRun?.error).toContain('denied')
  })

  it('attention adds a pressured node, clock skew and a GC error but keeps the replication factor covered', () => {
    const w = mockWire('attention', nowMs)
    expect(w.nodes[1]?.pressured).toBe(true)
    expect(Math.abs(Number(w.nodes[2]?.clockSkewMs) + 750)).toBeLessThanOrEqual(10)
    expect(w.health).toMatchObject({ clockSkewAlerts: '2', replicationFactor: 2, aliveNodes: 2 })
    expect(w.volumes.slice(0, 2).map((v) => v.replicas)).toEqual([['node-a', 'node-b'], ['node-b', 'node-a']])
    expect(w.volumes.every((v) => !v.replicas?.includes('node-c'))).toBe(true)
    expect(w.gc?.lastError).toContain('input/output error')
  })

  it('attention stays a warning overall', async () => {
    const { deriveHealth } = await import('../lib/health')
    const { parseStatus } = await import('./status')
    const h = deriveHealth(parseStatus(mockWire('attention', nowMs)), nowMs)
    expect(h.level).toBe('attention')
    expect(h.findings.map((f) => f.text)).toEqual([
      'node-b suspect for 40 s',
      'node-b pressured',
      expect.stringMatching(/^Clock skew 7[4-5]\d ms$/),
      'node-c disk 84% full',
      '12 volumes degraded',
      expect.stringMatching(/^Backup failed /),
      expect.stringMatching(/^GC failed /),
    ])
  })

  it('degraded drops alive nodes below the replication factor and damages a map', () => {
    const w = mockWire('degraded', nowMs)
    expect(w.health?.aliveNodes).toBe(2)
    expect(w.volumes.find((v) => v.path === '/db')?.mapDamaged).toBe(true)
    expect(w.volumes.find((v) => v.path === '/cache')?.mapDamaged).toBe(false)
  })

  it('degraded makes one node dead and a backup run', () => {
    const w = mockWire('degraded', nowMs)
    expect(w.nodes[2]?.state).toBe('NODE_STATE_DEAD')
    expect(w.backup?.running).toBe(true)
  })

  it('empty removes nodes, volumes, storage and backup', () => {
    const w = mockWire('empty', nowMs)
    expect(w.nodes).toEqual([])
    expect(w.volumes).toEqual([])
    expect(w.storage).toBeNull()
    expect(w.backup).toBeNull()
  })

  it('unconfigured disables backups and removes volumes', () => {
    const w = mockWire('unconfigured', nowMs)
    expect(w.backup).toEqual({
      configured: false,
      target: '',
      intervalSeconds: '0',
      nextRunUnix: '0',
      running: false,
      lastRun: null,
    })
    expect(w.volumes).toEqual([])
    expect(w.nodes).toHaveLength(3)
  })
})

describe('fetchMockStatus', () => {
  beforeEach(() => {
    resetMockSession()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('resolves a parsed status after the delay', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_800_000_000_000)
    let settled = false
    const p = fetchMockStatus('healthy', 300).then((s) => {
      settled = true
      return s
    })
    await vi.advanceTimersByTimeAsync(299)
    expect(settled).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    const s = await p
    expect(s.nodes).toHaveLength(3)
    expect(s.nodes[0]?.lastChange).toBeInstanceOf(Date)
  })

  it('uses a 300 ms default delay', async () => {
    vi.useFakeTimers()
    const p = fetchMockStatus('empty')
    await vi.advanceTimersByTimeAsync(300)
    expect((await p).nodes).toEqual([])
  })

  it('rejects with StatusUnreachableError for the error scenario', async () => {
    vi.useFakeTimers()
    const p = fetchMockStatus('error', 10).catch((e: unknown) => e)
    await vi.advanceTimersByTimeAsync(10)
    expect(await p).toBeInstanceOf(StatusUnreachableError)
  })

  it('measures elapsed time from the first call of the session', async () => {
    vi.useFakeTimers()
    const t0 = 1_800_000_000_000
    vi.setSystemTime(t0)
    const first = fetchMockStatus('healthy', 10)
    await vi.advanceTimersByTimeAsync(10)
    await first
    vi.setSystemTime(t0 + 130_000)
    const later = fetchMockStatus('healthy', 10)
    await vi.advanceTimersByTimeAsync(10)
    const s = await later
    expect(s.gc?.lastRunAt?.getTime()).toBe((t0 / 1000 + 120) * 1000)
  })

  it('starts a new timeline after resetMockSession', async () => {
    vi.useFakeTimers()
    const t0 = 1_800_000_000_000
    vi.setSystemTime(t0)
    const first = fetchMockStatus('healthy', 10)
    await vi.advanceTimersByTimeAsync(10)
    await first
    resetMockSession()
    vi.setSystemTime(t0 + 500_000)
    const again = fetchMockStatus('healthy', 10)
    await vi.advanceTimersByTimeAsync(10)
    const s = await again
    // Without the reset the 500 s gap would have produced run 8; a fresh timeline stays on the shifted fixture run.
    expect(s.gc?.lastRunAt?.getTime()).toBe((Math.floor((t0 + 500_010) / 1000) - 4000) * 1000)
  })
})

describe('mockWire over time', () => {
  const since = 1_800_000_000_000
  const at = (sc: MockScenario, elapsedS: number): WireStatus => mockWire(sc, since + elapsedS * 1000, since)
  const num = (v: string | undefined): number => Number(v)

  it('adds ten generated volumes with consistent leases and replicas', () => {
    const w = at('healthy', 0)
    const gen = w.volumes.slice(2)
    expect(gen).toHaveLength(10)
    for (const v of gen) {
      expect(v.path).toMatch(/^\/csi\/volumes\/pvc-[0-9a-f]{4}$/)
      expect(num(v.sizeBytes)).toBeGreaterThanOrEqual(5 * 1024 ** 3)
      expect(num(v.sizeBytes)).toBeLessThanOrEqual(40 * 1024 ** 3)
      expect(v.extentSizeBytes).toBe(String(4 * 1024 ** 2))
      expect(v.replicas).toEqual(['node-a', 'node-b', 'node-c'])
      expect(v.attachedHere).toBe(v.leaseHolder === 'node-a')
    }
    expect(new Set(gen.map((v) => v.leaseHolder))).toEqual(new Set(['node-a', 'node-b', 'node-c', '']))
  })

  it('grows node usage over an hour and never past capacity', () => {
    const t0 = at('healthy', 0)
    const t1 = at('healthy', 3600)
    t0.nodes.forEach((n, i) => {
      const grown = num(t1.nodes[i]?.usedBytes) - num(n.usedBytes)
      expect(grown).toBeGreaterThan(1.2 * 1024 ** 3)
      expect(grown).toBeLessThan(3 * 1024 ** 3)
    })
    for (const n of at('healthy', 1e9).nodes) expect(n.usedBytes).toBe(n.capacityBytes)
  })

  it('never lets node usage decrease', () => {
    let prev = [0, 0, 0]
    for (let t = 0; t <= 7200; t += 17) {
      const used = at('healthy', t).nodes.map((n) => num(n.usedBytes))
      used.forEach((u, i) => {
        expect(u).toBeGreaterThanOrEqual(prev[i] ?? 0)
      })
      prev = used
    }
  })

  it('mirrors node-a in storage', () => {
    const w = at('healthy', 1800)
    const a = w.nodes[0]
    expect(w.storage?.usedBytes).toBe(a?.usedBytes)
    expect(num(w.storage?.availableBytes)).toBe(num(w.storage?.capacityBytes) - num(a?.usedBytes))
    expect(num(w.storage?.chunkCount)).toBe(Math.round(num(a?.usedBytes) / 262144))
    expect(num(at('healthy', 1e9).storage?.availableBytes)).toBe(0)
  })

  it('grows written extents monotonically and leaves idle volumes at their start', () => {
    let prev: number[] = []
    for (let t = 0; t <= 600; t += 5) {
      const ext = at('healthy', t).volumes.map((v) => num(v.extentsWritten))
      ext.forEach((e, i) => {
        expect(e).toBeGreaterThanOrEqual(prev[i] ?? 0)
      })
      prev = ext
    }
    const start = at('healthy', 0).volumes
    const end = at('healthy', 600).volumes
    expect(end[1]?.extentsWritten).toBe('0')
    expect(num(end[0]?.extentsWritten)).toBeGreaterThan(num(start[0]?.extentsWritten))
  })

  it('clamps written extents to the volume size', () => {
    const w = at('healthy', 1e7)
    for (const v of w.volumes) {
      expect(num(v.extentsWritten) * num(v.extentSizeBytes)).toBeLessThanOrEqual(num(v.sizeBytes))
    }
    const small = w.volumes.find((v) => v.path === '/csi/volumes/pvc-67d5')
    expect(num(small?.extentsWritten) * num(small?.extentSizeBytes)).toBe(num(small?.sizeBytes))
  })

  it('keeps node-a skew at zero and the others within 10 ms of their base', () => {
    for (let t = 0; t <= 600; t += 7) {
      const w = at('healthy', t)
      expect(w.nodes[0]?.clockSkewMs).toBe('0')
      expect(Math.abs(num(w.nodes[1]?.clockSkewMs) - 42)).toBeLessThanOrEqual(10)
      expect(Math.abs(num(w.nodes[2]?.clockSkewMs) + 17)).toBeLessThanOrEqual(10)
      const max = Math.max(...w.nodes.map((n) => Math.abs(num(n.clockSkewMs))))
      expect(w.health?.clockSkewMs).toBe(String(max))
    }
  })

  it('keeps the attention skew over the warning line', () => {
    for (let t = 0; t <= 600; t += 7) expect(num(at('attention', t).health?.clockSkewMs)).toBeGreaterThan(500)
  })

  it('runs GC every 60 s and varies the figures per run', () => {
    const t0 = since / 1000
    // Before the first run the fixture's run stays where it was, so the page sees one run, not one per poll.
    expect(at('healthy', 59).gc?.lastRunUnix).toBe(at('healthy', 0).gc?.lastRunUnix)
    expect(at('healthy', 60).gc?.lastRunUnix).toBe(String(t0 + 60))
    expect(at('healthy', 119).gc?.lastRunUnix).toBe(String(t0 + 60))
    expect(at('healthy', 120).gc?.lastRunUnix).toBe(String(t0 + 120))
    const freed = new Set<string | undefined>()
    for (let k = 1; k <= 20; k++) {
      const gc = at('healthy', k * 60).gc
      freed.add(gc?.lastBytesFreed)
      expect(num(gc?.lastBytesFreed)).toBeGreaterThanOrEqual(50 * 1024 ** 2)
      expect(num(gc?.lastBytesFreed)).toBeLessThanOrEqual(2 * 1024 ** 3)
      expect(gc?.lastError).toBe('')
    }
    expect(freed.size).toBeGreaterThan(10)
  })

  it('attention keeps its GC error on every run', () => {
    for (const t of [0, 60, 600]) expect(at('attention', t).gc?.lastError).toContain('input/output error')
  })

  it('runs a backup every 120 s with a matching schedule', () => {
    const t0 = since / 1000
    const first = at('healthy', 0).backup
    expect(first?.intervalSeconds).toBe('120')
    expect(first?.nextRunUnix).toBe(String(t0 + 120))
    // The first run starts 100 s before the session, so it is never overdue on the first poll.
    expect(first?.lastRun?.startedUnix).toBe(String(t0 - 100))
    expect(first?.lastRun?.finishedUnix).toBe(String(t0 - 80))
    expect(at('healthy', 119).backup?.lastRun?.startedUnix).toBe(String(t0 - 100))
    const b = at('healthy', 130).backup
    expect(b?.lastRun?.startedUnix).toBe(String(t0 + 120))
    expect(b?.lastRun?.finishedUnix).toBe(String(t0 + 140))
    expect(b?.nextRunUnix).toBe(String(t0 + 240))
    expect(b?.lastRun?.manifest).toBe(`manifests/node-a/${new Date((t0 + 120) * 1000).toISOString().slice(0, 19)}Z.json`)
    expect(b?.lastRun?.ok).toBe(true)
    expect(at('healthy', 370).backup?.lastRun?.bytesUploaded).not.toBe(b?.lastRun?.bytesUploaded)
  })

  it('keeps the backup scenario rules as runs advance', () => {
    expect(at('attention', 500).backup?.lastRun?.ok).toBe(false)
    expect(at('degraded', 500).backup?.running).toBe(true)
    expect(at('unconfigured', 500).backup?.configured).toBe(false)
  })

  it('treats a since after now as no elapsed time', () => {
    const early = mockWire('healthy', since, since + 5000)
    const base0 = mockWire('healthy', since, since)
    expect(early.nodes).toEqual(base0.nodes)
    expect(early.volumes).toEqual(base0.volumes)
  })

  describe('incident', () => {
    it('is an attention case led by the node-c disk', () => {
      const h = deriveHealth(parseStatus(at('incident', 190)), since + 190_000)
      expect(h.level).toBe('attention')
      expect(h.findings[0]?.text).toBe('node-c disk 84% full')
      expect(h.nodes).toMatchObject({ total: 3, up: 3 })
    })

    it('has exactly one degraded volume with a short replica set', () => {
      const w = at('incident', 190)
      const short = w.volumes.filter((v) => (v.replicas?.length ?? 0) < 3)
      expect(short.map((v) => v.path)).toEqual(['/csi/volumes/pvc-3b0d'])
      expect(short[0]?.replicas).toEqual(['node-a', 'node-b'])
      const h = deriveHealth(parseStatus(w), since + 190_000)
      expect(h.findings.map((f) => f.text)).toContain('1 volume degraded')
    })

    it('fails GC on every seventh run only', () => {
      expect(at('incident', 3 * 60).gc?.lastError).toBe('lease scan timed out after 30 s')
      expect(at('incident', 4 * 60).gc?.lastError).toBe('')
      expect(at('incident', 10 * 60).gc?.lastError).toBe('lease scan timed out after 30 s')
      expect(at('incident', 0).gc?.lastError).toBe('')
    })

    it('keeps node-c under 90% for hours and backups green', () => {
      const w = at('incident', 6 * 3600)
      const c = w.nodes[2]
      const ratio = num(c?.usedBytes) / num(c?.capacityBytes)
      expect(ratio).toBeGreaterThan(0.84)
      expect(ratio).toBeLessThan(0.9)
      expect(w.backup?.lastRun?.ok).toBe(true)
    })
  })
})
