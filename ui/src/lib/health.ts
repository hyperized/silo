import type { Backup, ClusterHealth, ClusterNode, Gc, NodeState, Status, Volume } from '../api/status'
import { formatAgo, formatCount, formatDuration, formatPercent } from './format'

export const FULL_WARN = 0.8
export const FULL_CRIT = 0.9
/** Grace before a missed scheduled run counts as overdue; a busy node may start a little late. */
export const OVERDUE_GRACE_MS = 15 * 60 * 1000

/**
 * Clock skew thresholds in milliseconds. silod's leases and HLC timestamps
 * tolerate small offsets; half a second is worth a look, two seconds is enough
 * to make lease expiry and ordering decisions disagree between nodes.
 */
export const SKEW_WARN_MS = 500
export const SKEW_CRIT_MS = 2000
/** A node that has been up this long with GC enabled should have finished a run. */
export const GC_NEVER_AFTER_MS = 24 * 3600 * 1000

export type Level = 'healthy' | 'attention' | 'degraded'

export interface Finding {
  level: Exclude<Level, 'healthy'>
  text: string
  /** Raw text from silod shown verbatim under the finding, e.g. a backup error. */
  detail?: string
}

export type GcKind = 'off' | 'running' | 'never' | 'ok' | 'skipped' | 'failed'

/** How the last sweep ended, read from gc.lastError. */
export type GcOutcome = 'ok' | 'skipped' | 'failed'

/**
 * silod prefixes lastError with this when it chose not to sweep, for example
 * while this node's extent maps disagree with their replicas. Deleting chunks
 * then could free live data, so the GC waits; the next sweep or two after the
 * maps settle clears it. Anything else in lastError is a real failure.
 */
export const GC_SKIP_PREFIX = 'skipped:'

/** healthy: alive nodes cover the replication factor; degraded: they do not. */
export type ReplicationKind = 'healthy' | 'degraded'

export type BackupKind = 'unconfigured' | 'running' | 'never' | 'ok' | 'failed' | 'overdue'

export interface NodeCounts {
  /** Nodes that have not left. */
  total: number
  up: number
  suspect: number
  down: number
}

export interface Capacity {
  usedBytes: number
  capacityBytes: number
}

export interface Health {
  level: Level
  findings: Finding[]
  backup: BackupKind | null
  nodes: NodeCounts
  /** Summed over the nodes that report a disk, or null when none does. */
  capacity: Capacity | null
}

const RANK: Readonly<Record<Level, number>> = { healthy: 0, attention: 1, degraded: 2 }

export function worst(a: Level, b: Level): Level {
  return RANK[a] >= RANK[b] ? a : b
}

/** used/capacity, or null when the capacity has not been reported. */
export function usageRatio(used: number, capacity: number): number | null {
  if (capacity <= 0) return null
  return Math.min(Math.max(used / capacity, 0), 1)
}

export function fillLevel(ratio: number | null): Level {
  if (ratio === null) return 'healthy'
  if (ratio >= FULL_CRIT) return 'degraded'
  if (ratio >= FULL_WARN) return 'attention'
  return 'healthy'
}

export function nodeLevel(state: NodeState): Level {
  if (state === 'dead') return 'degraded'
  if (state === 'suspect' || state === 'unknown') return 'attention'
  return 'healthy'
}

export function skewLevel(ms: number): Level {
  const a = Math.abs(ms)
  if (a >= SKEW_CRIT_MS) return 'degraded'
  if (a >= SKEW_WARN_MS) return 'attention'
  return 'healthy'
}

/** null when silod did not report a replication factor (older silod), or nothing is loaded yet. */
export function replicationKind(h: ClusterHealth | null | undefined): ReplicationKind | null {
  if (!h || h.replicationFactor <= 0) return null
  return h.aliveNodes >= h.replicationFactor ? 'healthy' : 'degraded'
}

/** ok for an empty lastError, skipped for silod's skip sentinel, failed for anything else. */
export function gcOutcome(lastError: string): GcOutcome {
  if (lastError === '') return 'ok'
  return lastError.startsWith(GC_SKIP_PREFIX) ? 'skipped' : 'failed'
}

/** The reason after the skip prefix, or lastError unchanged when it is not a skip. */
export function gcSkipReason(lastError: string): string {
  return lastError.startsWith(GC_SKIP_PREFIX) ? lastError.slice(GC_SKIP_PREFIX.length).trimStart() : lastError
}

export function gcKind(gc: Gc): GcKind {
  if (!gc.enabled) return 'off'
  if (gc.running) return 'running'
  if (gc.lastError) return gcOutcome(gc.lastError)
  return gc.lastRunAt ? 'ok' : 'never'
}

export function stateById(nodes: readonly ClusterNode[]): ReadonlyMap<string, NodeState> {
  return new Map(nodes.map((n) => [n.id, n.state]))
}

/**
 * A volume is degraded when a replica sits on a node that is not alive (or is
 * missing from the node table), or when its set is shorter than the
 * replication factor. An older silod reports neither, so nothing is degraded.
 */
export function volumeDegraded(v: Volume, states: ReadonlyMap<string, NodeState>, replicationFactor: number): boolean {
  return v.replicas.length < replicationFactor || v.replicas.some((id) => states.get(id) !== 'alive')
}

/** The largest absolute skew known: silod's own summary or any per-node measurement. */
export function maxSkewMs(s: Status): number {
  return s.nodes.reduce((m, n) => Math.max(m, Math.abs(n.clockSkewMs)), s.health?.clockSkewMs ?? 0)
}

const plural = (n: number, one: string, many: string): string => `${formatCount(n)} ${n === 1 ? one : many}`

export function backupKind(b: Backup | null, nowMs: number): BackupKind {
  if (!b?.configured) return 'unconfigured'
  if (b.running) return 'running'
  const run = b.lastRun
  if (run && !run.ok) return 'failed'
  const missedSchedule = b.nextRunAt !== null && nowMs - b.nextRunAt.getTime() > OVERDUE_GRACE_MS
  const finished = run?.finishedAt ?? null
  const tooOld =
    finished !== null && b.intervalSeconds > 0 && nowMs - finished.getTime() > 2 * b.intervalSeconds * 1000
  if (missedSchedule || tooOld) return 'overdue'
  return run ? 'ok' : 'never'
}

/** Derives the overall level and the issues list, worst first. */
export function deriveHealth(s: Status, nowMs: number): Health {
  const findings: Finding[] = []
  const add = (level: Finding['level'], text: string, detail?: string): void => {
    findings.push(detail ? { level, text, detail } : { level, text })
  }
  const lasted = (n: ClusterNode): string =>
    n.lastChange ? ` for ${formatDuration((nowMs - n.lastChange.getTime()) / 1000)}` : ''

  const h = s.health
  if (h && replicationKind(h) === 'degraded') {
    add('degraded', `Cluster degraded: ${plural(h.aliveNodes, 'node', 'nodes')} alive, replication factor ${String(h.replicationFactor)}`)
  }
  if (h?.isolated) {
    const tries = h.rejoinAttempts > 0 ? `, ${plural(h.rejoinAttempts, 'rejoin attempt', 'rejoin attempts')}` : ''
    add('degraded', `${s.respondingNodeId} isolated from its peers${tries}`)
  }

  const active = s.nodes.filter((n) => n.state !== 'left')
  if (active.length === 0) add('attention', 'No nodes')
  // Most recent state change first: that is the one the operator is likely chasing.
  const byChange = [...active].sort((a, b) => (b.lastChange?.getTime() ?? 0) - (a.lastChange?.getTime() ?? 0))
  for (const n of byChange) {
    if (n.state === 'dead') add('degraded', `${n.id} down${lasted(n)}`)
    else if (n.state === 'suspect') add('attention', `${n.id} suspect${lasted(n)}`)
    else if (n.state === 'unknown') add('attention', `${n.id} state unknown`)
  }
  for (const n of active) if (n.pressured) add('attention', `${n.id} pressured`)

  const skew = maxSkewMs(s)
  const skewAt = skewLevel(skew)
  if (skewAt !== 'healthy') add(skewAt, `Clock skew ${formatCount(skew)} ms`)

  const reported = active.filter((n) => n.capacityBytes > 0)
  let capacityBytes = reported.reduce((a, n) => a + n.capacityBytes, 0)
  let usedBytes = reported.reduce((a, n) => a + n.usedBytes, 0)
  if (capacityBytes === 0 && s.storage) {
    capacityBytes = s.storage.capacityBytes
    usedBytes = s.storage.usedBytes
  }

  const fullest: { id: string; ratio: number }[] = []
  for (const n of reported) fullest.push({ id: n.id, ratio: usageRatio(n.usedBytes, n.capacityBytes) as number })
  if (s.storage && !reported.some((n) => n.id === s.respondingNodeId)) {
    const r = usageRatio(s.storage.usedBytes, s.storage.capacityBytes)
    if (r !== null) fullest.push({ id: s.respondingNodeId, ratio: r })
  }
  const full = fullest.filter((f) => fillLevel(f.ratio) !== 'healthy').sort((a, b) => b.ratio - a.ratio)
  for (const f of full) add(fillLevel(f.ratio) as Finding['level'], `${f.id} disk ${formatPercent(f.ratio)} full`)

  const damaged = s.volumes.filter((v) => v.mapDamaged).length
  if (damaged) add('degraded', `${plural(damaged, 'volume map', 'volume maps')} damaged`)
  const states = stateById(s.nodes)
  const rf = h?.replicationFactor ?? 0
  const degradedVolumes = s.volumes.filter((v) => volumeDegraded(v, states, rf)).length
  if (degradedVolumes) add('attention', `${plural(degradedVolumes, 'volume', 'volumes')} degraded`)

  // An empty cluster has nothing to back up yet; flagging it would only add noise.
  const kind = active.length > 0 ? backupKind(s.backup, nowMs) : null
  const run = s.backup?.lastRun ?? null
  const when = run?.finishedAt ?? run?.startedAt ?? null
  const ago = when ? ` ${formatAgo(when, nowMs)}` : ''
  if (kind === 'failed') add('attention', `Backup failed${ago}`, run?.error)
  if (kind === 'overdue') add('attention', when ? `Backup overdue, last run${ago}` : 'Backup overdue')

  if (s.gc) {
    const gc = gcKind(s.gc)
    const at = s.gc.lastRunAt
    // A skipped sweep is not an issue: silod abstains on purpose and clears it itself.
    if (gc === 'failed') add('attention', at ? `GC failed ${formatAgo(at, nowMs)}` : 'GC failed', s.gc.lastError)
    const started = h?.startedAt ?? null
    if (gc === 'never' && started && nowMs - started.getTime() > GC_NEVER_AFTER_MS) {
      add('attention', `GC has not run since silod started ${formatAgo(started, nowMs)}`)
    }
  }

  // Stable sort: within a level the order above (cluster, nodes, skew, disks, volumes, backups, GC) holds.
  findings.sort((a, b) => RANK[b.level] - RANK[a.level])
  const level = findings.reduce<Level>((acc, f) => worst(acc, f.level), 'healthy')
  return {
    level,
    findings,
    backup: kind,
    nodes: {
      total: active.length,
      up: active.filter((n) => n.state === 'alive').length,
      suspect: active.filter((n) => n.state === 'suspect').length,
      down: active.filter((n) => n.state === 'dead').length,
    },
    capacity: capacityBytes > 0 ? { usedBytes, capacityBytes } : null,
  }
}
