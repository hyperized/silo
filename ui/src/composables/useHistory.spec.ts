import { describe, it, expect, afterEach } from 'vitest'
import { effectScope, nextTick, ref, shallowRef, type EffectScope } from 'vue'
import type { Status } from '../api/status'
import { backup, backupRun, gc, GIB, node, status, volume } from '../test/factories'
import { CHART_BUCKETS, HISTORY_CAPACITY, MAX_RUNS, SAMPLE_EVERY_MS, SPAN_MS, SPARK_POINTS, useHistory, type HistoryOptions } from './useHistory'

const T0 = 1_800_000_000_000
const MIN = 60_000

let scope: EffectScope | undefined

afterEach(() => {
  scope?.stop()
  scope = undefined
})

function setup(initial: Status | null = null, opts?: HistoryOptions) {
  scope = effectScope()
  const st = shallowRef<Status | null>(initial)
  const now = ref(T0)
  const h = scope.run(() => useHistory(st, now, opts))
  if (!h) throw new Error('scope is not active')
  async function feed(s: Status | null, t: number): Promise<void> {
    now.value = t
    st.value = s
    await nextTick()
  }
  return { h, st, now, feed }
}

/** A status whose nodes report the given used bytes, in that order. */
function usage(used: Record<string, number>, over: Partial<Status> = {}): Status {
  return status({ nodes: Object.entries(used).map(([id, u]) => node({ id, usedBytes: u })), ...over })
}

describe('useHistory: sampling', () => {
  it('exposes its defaults', () => {
    expect(HISTORY_CAPACITY * SAMPLE_EVERY_MS).toBe(SPAN_MS['1h'])
    expect(CHART_BUCKETS).toBe(60)
  })

  it('is empty and null-safe before any status', () => {
    const { h } = setup()
    expect(h.sampleCount.value).toBe(0)
    expect(h.usedByNode.value).toEqual({ times: [], series: [] })
    expect(h.clusterUsedRatePerHour.value).toBeNull()
    expect(h.secondsUntilFull.value).toBeNull()
    expect(h.writeRateBytesPerSec.value).toBeNull()
    expect(h.gcRuns.value).toEqual([])
    expect(h.backupRuns.value).toEqual([])
    expect(h.volumeWriteSparkline('/db')).toEqual([])
    expect(h.nodeWriteSparkline('node-a')).toEqual([])
    expect(h.nodeSkewSparkline('node-a')).toEqual([])
    expect(h.clusterWriteSparkline.value).toEqual([])
  })

  it('samples a status that is already there', () => {
    const { h } = setup(status())
    expect(h.sampleCount.value).toBe(1)
    expect(h.samples.value[0]?.t).toBe(T0)
  })

  it('ignores null statuses', async () => {
    const { h, feed } = setup(status())
    await feed(null, T0 + 10_000)
    expect(h.sampleCount.value).toBe(1)
  })

  it.each([
    { name: 'default interval', opts: undefined, gaps: [1000, 2999, 3000, 4000, 6000], want: 3 },
    { name: 'custom interval', opts: { sampleEveryMs: 500 }, gaps: [400, 500, 900, 1000], want: 3 },
  ])('skips statuses that arrive too soon ($name)', async ({ opts, gaps, want }) => {
    const { h, feed } = setup(null, opts)
    await feed(status(), T0)
    for (const g of gaps) await feed(status(), T0 + g)
    expect(h.sampleCount.value).toBe(want)
  })

  it('wraps around at capacity, keeping the newest samples', async () => {
    const { h, feed } = setup(null, { capacity: 3 })
    for (let i = 0; i < 5; i++) await feed(status(), T0 + i * SAMPLE_EVERY_MS)
    expect(h.sampleCount.value).toBe(3)
    expect(h.samples.value.map((s) => s.t)).toEqual([2, 3, 4].map((i) => T0 + i * SAMPLE_EVERY_MS))
  })

  it('records node, volume and cluster figures', async () => {
    const { h, feed } = setup()
    await feed(
      status({
        nodes: [node({ id: 'a', usedBytes: 1, capacityBytes: 10, clockSkewMs: -4, state: 'suspect' })],
        volumes: [volume({ path: '/v', allocatedBytes: 5, sizeBytes: 50, leaseHolder: 'a' })],
        gc: gc({ lastRunAt: new Date(T0 - 1000) }),
        backup: backup({ lastRun: backupRun({ startedAt: new Date(T0 - 2000) }) }),
      }),
      T0,
    )
    const s = h.samples.value[0]
    expect(s?.nodes.get('a')).toEqual({ usedBytes: 1, capacityBytes: 10, clockSkewMs: -4, state: 'suspect' })
    expect(s?.volumes.get('/v')).toEqual({ allocatedBytes: 5, sizeBytes: 50, leaseHolder: 'a' })
    expect(s?.usedBytes).toBe(1)
    expect(s?.capacityBytes).toBe(10)
    expect(s?.gcLastRunAt).toBe(T0 - 1000)
    expect(s?.backupLastRunAt).toBe(T0 - 2000)
  })

  it('records zero totals and null runs when nothing is reported', async () => {
    const { h, feed } = setup()
    await feed(status({ nodes: [node({ capacityBytes: 0 })], storage: null, backup: null, gc: null }), T0)
    const s = h.samples.value[0]
    expect(s?.capacityBytes).toBe(0)
    expect(s?.usedBytes).toBe(0)
    expect(s?.gcLastRunAt).toBeNull()
    expect(s?.backupLastRunAt).toBeNull()
  })
})

describe('useHistory: usedByNode', () => {
  it('resamples to 60 buckets ending now, in the latest node order', async () => {
    const { h, feed, now } = setup(null, { sampleEveryMs: MIN })
    for (let i = 0; i < 60; i++) await feed(usage({ b: i, a: 100 + i }), T0 + i * MIN)
    // Half a bucket on, so each sample sits in the middle of its own bucket.
    now.value = T0 + 59 * MIN + 30_000
    const got = h.usedByNode.value
    expect(got.times).toHaveLength(60)
    expect(got.times.at(-1)).toBe(now.value)
    expect(got.series[0]?.values.slice(0, 3)).toEqual([0, 1, 2])
    expect(got.series.map((s) => s.name)).toEqual(['b', 'a'])
    expect(got.series[0]?.values).toHaveLength(60)
    expect(got.series[0]?.values.at(-1)).toBe(59)
    expect(got.series[1]?.values.at(-1)).toBe(159)
  })

  it('pads a new node with 0 and drops a node that left the latest status', async () => {
    const { h, feed } = setup(null, { sampleEveryMs: MIN })
    for (let i = 0; i < 30; i++) await feed(usage({ a: 10, gone: 7 }), T0 + i * MIN)
    for (let i = 30; i < 60; i++) await feed(usage({ a: 10, b: 5 }), T0 + i * MIN)
    const got = h.usedByNode.value
    expect(got.series.map((s) => s.name)).toEqual(['a', 'b'])
    const b = got.series[1]?.values ?? []
    expect(b.slice(0, 29).every((v) => v === 0)).toBe(true)
    expect(b.slice(31).every((v) => v === 5)).toBe(true)
    // The vanished node's samples stay in the buffer until they age out.
    expect(h.samples.value[0]?.nodes.has('gone')).toBe(true)
  })

  it('repeats neighbouring values where nothing was sampled', async () => {
    const { h, feed } = setup(null, { sampleEveryMs: MIN })
    await feed(usage({ a: 4 }), T0)
    await feed(usage({ a: 8 }), T0 + 30 * MIN)
    await feed(usage({ a: 8 }), T0 + 50 * MIN)
    const values = h.usedByNode.value.series[0]?.values ?? []
    // The hour starts 10 min before T0, so the chart skips the first 10 empty buckets.
    expect(values).toHaveLength(50)
    expect(h.usedByNode.value.times).toHaveLength(50)
    expect(values[0]).toBe(4)
    expect(new Set(values)).toEqual(new Set([4, 8]))
    expect(values.at(-1)).toBe(8)
  })

  it('follows the span', async () => {
    const { h, feed } = setup(null, { sampleEveryMs: MIN })
    await feed(usage({ a: 1 }), T0)
    await feed(usage({ a: 9 }), T0 + 3 * 3600_000)
    expect(h.spanMs.value).toBe(SPAN_MS['1h'])
    expect(new Set(h.usedByNode.value.series[0]?.values)).toEqual(new Set([9]))
    h.span.value = '6h'
    expect(h.spanMs.value).toBe(SPAN_MS['6h'])
    expect(new Set(h.usedByNode.value.series[0]?.values)).toEqual(new Set([1, 9]))
    h.span.value = '24h'
    // The first bucket is the one holding T0, not the start of the 24 h span.
    const width = SPAN_MS['24h'] / 60
    const start = T0 + 3 * 3600_000 - SPAN_MS['24h']
    expect(h.usedByNode.value.times[0]).toBe(start + (Math.floor((T0 - start) / width) + 1) * width)
  })

  it('is empty once every sample has aged out of the span', async () => {
    const { h, feed, now } = setup()
    await feed(usage({ a: 1 }), T0)
    now.value = T0 + 2 * 3600_000
    expect(h.usedByNode.value).toEqual({ times: [], series: [] })
    expect(h.clusterUsedRatePerHour.value).toBeNull()
  })
})

describe('useHistory: coarse tier', () => {
  const HOUR = 3600_000

  it('fills long spans from the coarse tier once fine samples wrap', async () => {
    const { h, feed } = setup(null, { capacity: 2, sampleEveryMs: 1000, coarseEveryMs: HOUR })
    await feed(usage({ a: 1 }), T0)
    await feed(usage({ a: 2 }), T0 + 30 * MIN)
    await feed(usage({ a: 3 }), T0 + 2 * HOUR)
    await feed(usage({ a: 4 }), T0 + 4 * HOUR)
    // Fine keeps the last two; coarse kept T0 and T0 + 2 h but not the half-hour sample.
    expect(h.samples.value.map((r) => r.t)).toEqual([T0 + 2 * HOUR, T0 + 4 * HOUR])
    h.span.value = '6h'
    expect(new Set(h.usedByNode.value.series[0]?.values)).toEqual(new Set([1, 3, 4]))
    expect(h.clusterUsedRatePerHour.value).not.toBeNull()
  })

  it('bounds the coarse tier', async () => {
    const { h, feed } = setup(null, { capacity: 1, sampleEveryMs: 0, coarseEveryMs: 0, coarseCapacity: 2 })
    for (let i = 0; i < 4; i++) await feed(usage({ a: i + 1 }), T0 + i * HOUR)
    h.span.value = '24h'
    // Coarse holds 3 and 4, fine holds 4: the first two samples are gone.
    expect(new Set(h.usedByNode.value.series[0]?.values)).toEqual(new Set([3, 4]))
  })
})

describe('useHistory: trends', () => {
  it('projects growth and time until full', async () => {
    const { h, feed } = setup()
    // 1 GiB every 3 s on a 100 GiB node, starting at 30 GiB.
    for (let i = 0; i < 5; i++) await feed(usage({ a: 30 * GIB + i * GIB }), T0 + i * 3000)
    expect(h.clusterUsedRatePerHour.value).toBeCloseTo(1200 * GIB)
    expect(h.secondsUntilFull.value).toBeCloseTo((66 * GIB) / (GIB / 3))
  })

  it('has no projection with one sample, shrinking usage or no capacity', async () => {
    const one = setup(usage({ a: 1 }))
    expect(one.h.clusterUsedRatePerHour.value).toBeNull()
    expect(one.h.secondsUntilFull.value).toBeNull()
    one.st.value = null
    scope?.stop()

    const shrink = setup()
    await shrink.feed(usage({ a: 50 * GIB }), T0)
    await shrink.feed(usage({ a: 40 * GIB }), T0 + 3000)
    expect(shrink.h.clusterUsedRatePerHour.value).toBeLessThan(0)
    expect(shrink.h.secondsUntilFull.value).toBeNull()
    scope?.stop()

    const nocap = setup()
    const blind = (u: number): Status =>
      status({ nodes: [node({ capacityBytes: 0, usedBytes: u })], storage: null })
    await nocap.feed(blind(1), T0)
    await nocap.feed(blind(2), T0 + 3000)
    expect(nocap.h.secondsUntilFull.value).toBeNull()
  })
})

describe('useHistory: write rates and sparklines', () => {
  const vols = (a: number, b: number, extra = false): Status =>
    status({
      nodes: [node({ id: 'n1', clockSkewMs: a }), node({ id: 'n2' })],
      volumes: [
        volume({ path: '/a', allocatedBytes: a, leaseHolder: 'n1' }),
        volume({ path: '/b', allocatedBytes: b, leaseHolder: 'n2' }),
        ...(extra ? [volume({ path: '/new', allocatedBytes: 999, leaseHolder: 'n1' })] : []),
      ],
    })

  it('sums allocation growth per second, ignoring shrinkage and new volumes', async () => {
    const { h, feed } = setup()
    await feed(vols(100, 1000), T0)
    await feed(vols(400, 400, true), T0 + 3000)
    expect(h.writeRateBytesPerSec.value).toBe(100)
  })

  it('is null when two samples share an instant', async () => {
    const { h, feed } = setup(null, { sampleEveryMs: 0 })
    await feed(vols(1, 1), T0)
    await feed(vols(2, 2), T0)
    expect(h.writeRateBytesPerSec.value).toBeNull()
    expect(h.clusterWriteSparkline.value).toEqual([0])
  })

  it('builds per-volume, per-node and skew sparklines', async () => {
    const { h, feed } = setup()
    await feed(vols(0, 0), T0)
    await feed(vols(10, 5), T0 + 3000)
    await feed(vols(10, 2), T0 + 6000)
    await feed(vols(30, 3, true), T0 + 9000)
    // Samples are 3 s apart, so every growth is divided by 3.
    expect(h.volumeWriteSparkline('/a')).toEqual([10 / 3, 0, 20 / 3])
    expect(h.volumeWriteSparkline('/b')).toEqual([5 / 3, 0, 1 / 3])
    expect(h.volumeWriteSparkline('/missing')).toEqual([0, 0, 0])
    expect(h.nodeWriteSparkline('n1')).toEqual([10 / 3, 0, 20 / 3])
    expect(h.nodeWriteSparkline('n2')).toEqual([5 / 3, 0, 1 / 3])
    expect(h.clusterWriteSparkline.value).toEqual([5, 0, 7])
    expect(h.nodeSkewSparkline('n1')).toEqual([0, 10, 10, 30])
    expect(h.nodeSkewSparkline('missing')).toEqual([])
  })

  it('caps sparklines at the last 20 points', async () => {
    const { h, feed } = setup()
    for (let i = 0; i < 30; i++) await feed(vols(i, i), T0 + i * 3000)
    expect(h.volumeWriteSparkline('/a')).toHaveLength(SPARK_POINTS)
    expect(h.nodeSkewSparkline('n1')).toEqual(Array.from({ length: SPARK_POINTS }, (_, i) => i + 10))
  })
})

describe('useHistory: runs', () => {
  const withGc = (at: number | null, over: Parameters<typeof gc>[0] = {}): Status =>
    status({ gc: gc({ lastRunAt: at === null ? null : new Date(at), ...over }) })

  it('seeds from the first status and records each new GC run', async () => {
    const { h, feed } = setup(withGc(T0 - 5000))
    await feed(withGc(T0 - 5000), T0 + 3000)
    await feed(withGc(null), T0 + 6000)
    // Recorded even though this status arrives too soon to be sampled.
    await feed(withGc(T0 + 6500, { lastError: 'disk gone', lastBytesFreed: 7, lastOrphansDeleted: 3, lastDurationMs: 9 }), T0 + 7000)
    await feed(status({ gc: null }), T0 + 9000)
    expect(h.gcRuns.value).toEqual([
      { at: T0 - 5000, ok: true, bytesFreed: 1.2 * GIB, orphans: 312, durationMs: 1840 },
      { at: T0 + 6500, ok: false, bytesFreed: 7, orphans: 3, durationMs: 9 },
    ])
  })

  it('keeps at most 24 GC runs', async () => {
    const { h, feed } = setup()
    for (let i = 0; i < MAX_RUNS + 6; i++) await feed(withGc(T0 + i), T0 + i * 3000)
    expect(h.gcRuns.value).toHaveLength(MAX_RUNS)
    expect(h.gcRuns.value[0]?.at).toBe(T0 + 6)
  })

  it('records each new backup run, keyed on its start', async () => {
    const run = (at: number | null, ok = true): Status =>
      status({ backup: backup({ lastRun: backupRun({ startedAt: at === null ? null : new Date(at), ok, bytesUploaded: 42 }) }) })
    const { h, feed } = setup(run(T0 - 100))
    await feed(run(T0 - 100), T0 + 3000)
    await feed(run(null), T0 + 6000)
    await feed(status({ backup: backup() }), T0 + 9000)
    await feed(status({ backup: null }), T0 + 12_000)
    await feed(run(T0 + 13_000, false), T0 + 15_000)
    expect(h.backupRuns.value).toEqual([
      { at: T0 - 100, ok: true, bytesUploaded: 42 },
      { at: T0 + 13_000, ok: false, bytesUploaded: 42 },
    ])
  })
})
