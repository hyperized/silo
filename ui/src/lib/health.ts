import type { Backup, ClusterNode, NodeState, Status } from '../api/status'
import { formatAgo, formatDuration, formatPercent } from './format'

export const FULL_WARN = 0.8
export const FULL_CRIT = 0.9
/** Grace before a missed scheduled run counts as overdue; a busy node may start a little late. */
export const OVERDUE_GRACE_MS = 15 * 60 * 1000

export type Level = 'healthy' | 'attention' | 'degraded'

export interface Finding {
  level: Exclude<Level, 'healthy'>
  text: string
  /** Raw text from silod shown verbatim under the finding, e.g. a backup error. */
  detail?: string
}

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

  const active = s.nodes.filter((n) => n.state !== 'left')
  if (active.length === 0) add('attention', 'No nodes')
  // Most recent state change first: that is the one the operator is likely chasing.
  const byChange = [...active].sort((a, b) => (b.lastChange?.getTime() ?? 0) - (a.lastChange?.getTime() ?? 0))
  for (const n of byChange) {
    if (n.state === 'dead') add('degraded', `${n.id} down${lasted(n)}`)
    else if (n.state === 'suspect') add('attention', `${n.id} suspect${lasted(n)}`)
    else if (n.state === 'unknown') add('attention', `${n.id} state unknown`)
  }

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

  // An empty cluster has nothing to back up yet; flagging it would only add noise.
  const kind = active.length > 0 ? backupKind(s.backup, nowMs) : null
  const run = s.backup?.lastRun ?? null
  const when = run?.finishedAt ?? run?.startedAt ?? null
  const ago = when ? ` ${formatAgo(when, nowMs)}` : ''
  if (kind === 'failed') add('attention', `Backup failed${ago}`, run?.error)
  if (kind === 'overdue') add('attention', when ? `Backup overdue, last run${ago}` : 'Backup overdue')

  // Stable sort: within a level the order above (nodes, disks, backups) holds.
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
