import { computed, ref, shallowRef, watch, type ComputedRef, type Ref } from 'vue'
import type { NodeState, Status } from '../api/status'
import { deriveHealth, gcOutcome, type GcOutcome } from '../lib/health'
import { linearTrend, bucketMeans, fillGaps, RingBuffer, window, type Sample } from '../lib/series'

/** One hour of history at the default sampling interval. */
export const HISTORY_CAPACITY = 1200
export const SAMPLE_EVERY_MS = 3000
/**
 * A second, coarser tier keeps one sample a minute for a day, so the 6 h and
 * 24 h spans fill in while the page stays open without holding 24 h of
 * fine samples in memory.
 */
export const COARSE_CAPACITY = 1440
export const COARSE_EVERY_MS = 60_000
/** Most points per chart line, whatever the span. */
export const CHART_BUCKETS = 60
/** Bars in a per-row sparkline. */
export const SPARK_POINTS = 20
/** GC and backup runs kept for the run strips. */
export const MAX_RUNS = 24

export type HistorySpan = '1h' | '6h' | '24h'

export const SPAN_MS: Readonly<Record<HistorySpan, number>> = {
  '1h': 3600_000,
  '6h': 6 * 3600_000,
  '24h': 24 * 3600_000,
}

export interface NodeSample {
  usedBytes: number
  capacityBytes: number
  clockSkewMs: number
  state: NodeState
}

export interface VolumeSample {
  /**
   * Written extents times extent size, as parsed (clamped to the volume size).
   * Stands in for extentsWritten, which the domain model folds into this.
   */
  writtenBytes: number
  sizeBytes: number
  leaseHolder: string
}

/** Everything recorded from one status, keyed by node id and volume path. */
export interface HistorySample {
  t: number
  nodes: ReadonlyMap<string, NodeSample>
  volumes: ReadonlyMap<string, VolumeSample>
  usedBytes: number
  capacityBytes: number
  /** Epoch ms of the last finished GC run, or null. */
  gcLastRunAt: number | null
  /** Epoch ms the last backup run started, or null. */
  backupLastRunAt: number | null
}

export interface GcRun {
  at: number
  outcome: GcOutcome
  bytesFreed: number
  orphans: number
  durationMs: number
}

export interface BackupRunPoint {
  at: number
  ok: boolean
  bytesUploaded: number
}

export interface UsedByNode {
  times: number[]
  series: { name: string; values: number[] }[]
}

export interface HistoryOptions {
  capacity?: number
  sampleEveryMs?: number
  coarseCapacity?: number
  coarseEveryMs?: number
}

export interface History {
  span: Ref<HistorySpan>
  spanMs: ComputedRef<number>
  samples: Readonly<Ref<readonly HistorySample[]>>
  sampleCount: ComputedRef<number>
  usedByNode: ComputedRef<UsedByNode>
  clusterUsedRatePerHour: ComputedRef<number | null>
  secondsUntilFull: ComputedRef<number | null>
  writeRateBytesPerSec: ComputedRef<number | null>
  /** Cluster-wide growth of written bytes, in bytes per second, one point per recent sample interval. */
  clusterWriteSparkline: ComputedRef<number[]>
  gcRuns: ComputedRef<GcRun[]>
  backupRuns: ComputedRef<BackupRunPoint[]>
  /** Bytes per second written to one volume, per recent sample interval. */
  volumeWriteSparkline(path: string): number[]
  /** Bytes per second written to the volumes whose lease the node holds, per recent sample interval. */
  nodeWriteSparkline(id: string): number[]
  nodeSkewSparkline(id: string): number[]
}

function record(s: Status, t: number): HistorySample {
  // Same totals as the capacity card, so the trend and the bar agree.
  const cap = deriveHealth(s, t).capacity
  return {
    t,
    nodes: new Map(
      s.nodes.map((n) => [
        n.id,
        { usedBytes: n.usedBytes, capacityBytes: n.capacityBytes, clockSkewMs: n.clockSkewMs, state: n.state },
      ]),
    ),
    volumes: new Map(
      s.volumes.map((v) => [v.path, { writtenBytes: v.writtenBytes, sizeBytes: v.sizeBytes, leaseHolder: v.leaseHolder }]),
    ),
    usedBytes: cap ? cap.usedBytes : 0,
    capacityBytes: cap ? cap.capacityBytes : 0,
    gcLastRunAt: s.gc?.lastRunAt?.getTime() ?? null,
    backupLastRunAt: s.backup?.lastRun?.startedAt?.getTime() ?? null,
  }
}

/**
 * Sum of written-bytes growth between two samples, over volumes `pick` accepts.
 * A discard frees extents and lowers the count; that delta counts as zero, since
 * a trim is not a negative write.
 */
function grownBytes(a: HistorySample, b: HistorySample, pick: (path: string, v: VolumeSample) => boolean): number {
  let sum = 0
  for (const [path, v] of b.volumes) {
    const before = a.volumes.get(path)
    if (before && pick(path, v)) sum += Math.max(v.writtenBytes - before.writtenBytes, 0)
  }
  return sum
}

/**
 * useHistory turns the snapshot status into time series by sampling it into
 * a ring buffer as it arrives.
 *
 * It owns no timers: it samples when `status` changes and stamps each sample
 * with `now`. A status arriving sooner than `sampleEveryMs` after the previous
 * sample is skipped for the series, though GC and backup runs are still
 * picked up from it. Memory is bounded by `capacity` samples of
 * (nodes + volumes) entries each; the run lists hold at most MAX_RUNS.
 *
 * Nodes and volumes that vanish keep their samples until those age out of the
 * buffer. In `usedByNode`, a bucket where other nodes were sampled but this
 * one was absent reads 0 (it did not exist yet, or had gone); buckets where
 * nothing was sampled repeat the neighbouring value, and the buckets before
 * the first sample are left out, so the chart covers only time it has data for.
 *
 * Spans longer than the fine buffer reaches read from a coarse tier of one
 * sample per `coarseEveryMs`, bounded by `coarseCapacity`. Sparklines and the
 * current write rate always use the fine samples.
 *
 * All derived values are empty or null until there is enough data: two
 * samples for rates and trends, one for the chart.
 */
export function useHistory(
  status: Readonly<Ref<Status | null>>,
  now: Readonly<Ref<number>>,
  opts: HistoryOptions = {},
): History {
  const sampleEveryMs = opts.sampleEveryMs ?? SAMPLE_EVERY_MS
  const coarseEveryMs = opts.coarseEveryMs ?? COARSE_EVERY_MS
  const buffer = new RingBuffer<HistorySample>(opts.capacity ?? HISTORY_CAPACITY)
  const coarseBuffer = new RingBuffer<HistorySample>(opts.coarseCapacity ?? COARSE_CAPACITY)
  const gcBuffer = new RingBuffer<GcRun>(MAX_RUNS)
  const backupBuffer = new RingBuffer<BackupRunPoint>(MAX_RUNS)
  // Snapshots of the buffers, replaced on every push: the buffers themselves
  // are not reactive, and a fresh array per push is cheap at these sizes.
  const samples = shallowRef<readonly HistorySample[]>([])
  const coarse = shallowRef<readonly HistorySample[]>([])
  const gcList = shallowRef<GcRun[]>([])
  const backupList = shallowRef<BackupRunPoint[]>([])
  const span = ref<HistorySpan>('1h')

  function noteRuns(s: Status): void {
    const g = s.gc
    const gcAt = g?.lastRunAt?.getTime()
    if (g && gcAt !== undefined && gcAt !== gcBuffer.last()?.at) {
      gcBuffer.push({
        at: gcAt,
        outcome: gcOutcome(g.lastError),
        bytesFreed: g.lastBytesFreed,
        orphans: g.lastOrphansDeleted,
        durationMs: g.lastDurationMs,
      })
      gcList.value = gcBuffer.toArray()
    }
    const run = s.backup?.lastRun
    const runAt = run?.startedAt?.getTime()
    if (run && runAt !== undefined && runAt !== backupBuffer.last()?.at) {
      backupBuffer.push({ at: runAt, ok: run.ok, bytesUploaded: run.bytesUploaded })
      backupList.value = backupBuffer.toArray()
    }
  }

  watch(
    status,
    (s) => {
      if (!s) return
      noteRuns(s)
      const t = now.value
      const prev = buffer.last()
      if (prev && t - prev.t < sampleEveryMs) return
      const r = record(s, t)
      buffer.push(r)
      samples.value = buffer.toArray()
      const lastCoarse = coarseBuffer.last()
      if (!lastCoarse || t - lastCoarse.t >= coarseEveryMs) {
        coarseBuffer.push(r)
        coarse.value = coarseBuffer.toArray()
      }
    },
    { immediate: true },
  )

  const spanMs = computed(() => SPAN_MS[span.value])
  // Coarse samples only fill the time before the oldest fine one, so no instant is counted twice.
  const merged = computed<readonly HistorySample[]>(() => {
    const fine = samples.value
    const from = fine[0]?.t ?? Number.POSITIVE_INFINITY
    const older = coarse.value.filter((r) => r.t < from)
    return older.length === 0 ? fine : [...older, ...fine]
  })
  const inSpan = computed(() => window(merged.value, now.value, spanMs.value))

  const usedByNode = computed<UsedByNode>(() => {
    const all = merged.value
    const latest = all.at(-1)
    if (!latest) return { times: [], series: [] }
    const nowMs = now.value
    const sampled = bucketMeans(
      all.map((r) => ({ t: r.t, v: 1 })),
      nowMs,
      spanMs.value,
      CHART_BUCKETS,
    )
    // The chart starts at the first sampled bucket: drawing the empty part of
    // the span flat would claim history this page never saw.
    const first = sampled.means.findIndex((m) => m !== null)
    if (first < 0) return { times: [], series: [] }
    const series = [...latest.nodes.keys()].map((name) => {
      const pts: Sample[] = []
      for (const r of all) {
        const n = r.nodes.get(name)
        if (n) pts.push({ t: r.t, v: n.usedBytes })
      }
      const own = bucketMeans(pts, nowMs, spanMs.value, CHART_BUCKETS).means
      return { name, values: fillGaps(own.map((m, i) => m ?? (sampled.means[i] === null ? null : 0)).slice(first)) }
    })
    return { times: sampled.times.slice(first), series }
  })

  const usedTrend = computed(() => linearTrend(inSpan.value.map((r) => ({ t: r.t, v: r.usedBytes }))))

  const clusterUsedRatePerHour = computed(() => (inSpan.value.length < 2 ? null : usedTrend.value.perSecond * 3600))

  const secondsUntilFull = computed(() => {
    const latest = inSpan.value.at(-1)
    if (!latest || inSpan.value.length < 2 || latest.capacityBytes <= 0) return null
    return usedTrend.value.secondsUntil(latest.capacityBytes)
  })

  const writeRateBytesPerSec = computed(() => {
    const a = samples.value.at(-2)
    const b = samples.value.at(-1)
    if (!a || !b) return null
    const dt = (b.t - a.t) / 1000
    if (dt <= 0) return null
    return grownBytes(a, b, () => true) / dt
  })

  /**
   * Per-second values over the last SPARK_POINTS + 1 samples. Dividing by the
   * interval keeps a pause or a slow poll from showing up as a spike.
   */
  function perSecond(fn: (a: HistorySample, b: HistorySample) => number): number[] {
    const out: number[] = []
    let prev: HistorySample | undefined
    for (const r of samples.value.slice(-(SPARK_POINTS + 1))) {
      if (prev) {
        const dt = (r.t - prev.t) / 1000
        out.push(dt > 0 ? fn(prev, r) / dt : 0)
      }
      prev = r
    }
    return out
  }

  return {
    span,
    spanMs,
    samples,
    sampleCount: computed(() => samples.value.length),
    usedByNode,
    clusterUsedRatePerHour,
    secondsUntilFull,
    writeRateBytesPerSec,
    clusterWriteSparkline: computed(() => perSecond((a, b) => grownBytes(a, b, () => true))),
    gcRuns: computed(() => gcList.value),
    backupRuns: computed(() => backupList.value),
    volumeWriteSparkline: (path) => perSecond((a, b) => grownBytes(a, b, (p) => p === path)),
    nodeWriteSparkline: (id) => perSecond((a, b) => grownBytes(a, b, (_, v) => v.leaseHolder === id)),
    nodeSkewSparkline: (id) =>
      samples.value
        .slice(-SPARK_POINTS)
        .flatMap((r) => {
          const n = r.nodes.get(id)
          return n ? [n.clockSkewMs] : []
        }),
  }
}
