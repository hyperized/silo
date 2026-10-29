import { describe, it, expect, vi, afterEach } from 'vitest'
import fixture from './__fixtures__/status.json'
import { fetchMockStatus, mockWire } from './mock'
import { MOCK_SCENARIOS, mockScenario, type MockScenario } from './mockMode'
import { StatusUnreachableError, type WireStatus } from './status'

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
    expect(Number(w.backup?.nextRunUnix)).toBe(Number(base.backup?.nextRunUnix) + delta)
    expect(Number(w.backup?.lastRun?.startedUnix)).toBe(Number(base.backup?.lastRun?.startedUnix) + delta)
    expect(Number(w.backup?.lastRun?.finishedUnix)).toBe(Number(base.backup?.lastRun?.finishedUnix) + delta)
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
    for (const sc of ['healthy', 'attention', 'degraded'] as const) {
      const w = fresh.mockWire(sc, nowMs)
      expect(w.health).toBeUndefined()
      expect(w.gc).toBeUndefined()
      expect(w.volumes.every((v) => !('replicas' in v))).toBe(true)
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
    expect(w.backup?.nextRunUnix).toBe('0')
    expect(w.backup?.lastRun?.startedUnix).toBe('0')
    expect(w.backup?.lastRun?.finishedUnix).toBe('0')
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
    expect(w.volumes).toHaveLength(2)
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
    expect(w.nodes[2]?.clockSkewMs).toBe('-750')
    expect(w.health).toMatchObject({ clockSkewMs: '750', clockSkewAlerts: '2', replicationFactor: 2, aliveNodes: 2 })
    expect(w.volumes.map((v) => v.replicas)).toEqual([['node-a', 'node-b'], ['node-b', 'node-a']])
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
      'Clock skew 750 ms',
      'node-c disk 84% full',
      '2 volumes degraded',
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
})
