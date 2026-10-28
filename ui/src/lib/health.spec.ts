import { describe, it, expect } from 'vitest'
import type { Backup, BackupRun, ClusterNode, NodeState, Status } from '../api/status'
import {
  backupKind,
  deriveHealth,
  fillLevel,
  nodeLevel,
  OVERDUE_GRACE_MS,
  usageRatio,
  worst,
  type Level,
} from './health'

const NOW = 1_800_000_000_000
const MIN = 60_000
const HOUR = 3_600_000
const ago = (ms: number): Date => new Date(NOW - ms)
const inFuture = (ms: number): Date => new Date(NOW + ms)

function node(id: string, state: NodeState = 'alive', over: Partial<ClusterNode> = {}): ClusterNode {
  return {
    id,
    gossipAddress: '',
    dataAddress: '',
    state,
    incarnation: 1,
    lastChange: null,
    capacityBytes: 100,
    usedBytes: 30,
    ...over,
  }
}

function run(over: Partial<BackupRun> = {}): BackupRun {
  return {
    startedAt: ago(2 * HOUR + MIN),
    finishedAt: ago(2 * HOUR),
    ok: true,
    error: '',
    chunksUploaded: 0,
    chunksSkipped: 0,
    bytesUploaded: 0,
    manifest: '',
    ...over,
  }
}

function backup(over: Partial<Backup> = {}): Backup {
  return {
    configured: true,
    target: 's3://b/c',
    intervalSeconds: 6 * 3600,
    nextRunAt: inFuture(4 * HOUR),
    running: false,
    lastRun: run(),
    ...over,
  }
}

function status(over: Partial<Status> = {}): Status {
  return {
    respondingNodeId: 'a',
    version: '1.0.0',
    nodes: [node('a'), node('b'), node('c')],
    storage: null,
    backup: backup(),
    volumes: [],
    ...over,
  }
}

describe('worst', () => {
  it.each<[Level, Level, Level]>([
    ['healthy', 'healthy', 'healthy'],
    ['healthy', 'attention', 'attention'],
    ['attention', 'healthy', 'attention'],
    ['attention', 'degraded', 'degraded'],
    ['degraded', 'attention', 'degraded'],
    ['degraded', 'degraded', 'degraded'],
  ])('worst(%s, %s) is %s', (a, b, want) => {
    expect(worst(a, b)).toBe(want)
  })
})

describe('usageRatio', () => {
  it.each<[number, number, number | null]>([
    [0, 0, null],
    [10, -5, null],
    [50, 100, 0.5],
    [0, 100, 0],
    [150, 100, 1],
    [-10, 100, 0],
  ])('usageRatio(%d, %d) is %s', (used, cap, want) => {
    expect(usageRatio(used, cap)).toBe(want)
  })
})

describe('fillLevel', () => {
  it.each<[number | null, Level]>([
    [null, 'healthy'],
    [0, 'healthy'],
    [0.79, 'healthy'],
    [0.8, 'attention'],
    [0.89, 'attention'],
    [0.9, 'degraded'],
    [1, 'degraded'],
  ])('fillLevel(%s) is %s', (ratio, want) => {
    expect(fillLevel(ratio)).toBe(want)
  })
})

describe('nodeLevel', () => {
  it.each<[NodeState, Level]>([
    ['alive', 'healthy'],
    ['left', 'healthy'],
    ['suspect', 'attention'],
    ['unknown', 'attention'],
    ['dead', 'degraded'],
  ])('nodeLevel(%s) is %s', (state, want) => {
    expect(nodeLevel(state)).toBe(want)
  })
})

describe('backupKind', () => {
  it('is unconfigured without a backup or when not configured', () => {
    expect(backupKind(null, NOW)).toBe('unconfigured')
    expect(backupKind(backup({ configured: false }), NOW)).toBe('unconfigured')
  })

  it('is running while a run is active, even after a failure', () => {
    expect(backupKind(backup({ running: true, lastRun: run({ ok: false }) }), NOW)).toBe('running')
  })

  it('is failed when the last run failed, even if overdue', () => {
    const b = backup({ lastRun: run({ ok: false }), nextRunAt: ago(5 * HOUR) })
    expect(backupKind(b, NOW)).toBe('failed')
  })

  it('is ok for a recent successful run', () => {
    expect(backupKind(backup(), NOW)).toBe('ok')
  })

  it('is never when no run has happened and nothing is missed', () => {
    expect(backupKind(backup({ lastRun: null }), NOW)).toBe('never')
  })

  it('is overdue when the scheduled time was missed beyond the grace', () => {
    const b = backup({ nextRunAt: ago(OVERDUE_GRACE_MS + 1) })
    expect(backupKind(b, NOW)).toBe('overdue')
  })

  it('is not overdue within the grace period', () => {
    expect(backupKind(backup({ nextRunAt: ago(OVERDUE_GRACE_MS) }), NOW)).toBe('ok')
    expect(backupKind(backup({ nextRunAt: ago(10 * MIN) }), NOW)).toBe('ok')
  })

  it('is overdue when the last finish is older than two intervals', () => {
    const b = backup({
      intervalSeconds: 3600,
      nextRunAt: inFuture(HOUR),
      lastRun: run({ finishedAt: ago(2 * HOUR + 1) }),
    })
    expect(backupKind(b, NOW)).toBe('overdue')
  })

  it('is not overdue at exactly two intervals', () => {
    const b = backup({ intervalSeconds: 3600, lastRun: run({ finishedAt: ago(2 * HOUR) }) })
    expect(backupKind(b, NOW)).toBe('ok')
  })

  it('ignores the age check without an interval or finish time', () => {
    const old = run({ finishedAt: ago(100 * HOUR) })
    expect(backupKind(backup({ intervalSeconds: 0, lastRun: old }), NOW)).toBe('ok')
    expect(backupKind(backup({ lastRun: run({ finishedAt: null }) }), NOW)).toBe('ok')
  })

  it('has no missed schedule when nextRunAt is unknown', () => {
    expect(backupKind(backup({ nextRunAt: null, lastRun: null }), NOW)).toBe('never')
  })
})

describe('deriveHealth', () => {
  const stor = (used: number, cap: number) => ({
    dataDir: '/d',
    capacityBytes: cap,
    usedBytes: used,
    availableBytes: cap - used,
    chunkCount: 1,
  })

  it('reports a healthy three node cluster', () => {
    const h = deriveHealth(status(), NOW)
    expect(h.level).toBe('healthy')
    expect(h.findings).toEqual([])
    expect(h.backup).toBe('ok')
    expect(h.nodes).toEqual({ total: 3, up: 3, suspect: 0, down: 0 })
    expect(h.capacity).toEqual({ usedBytes: 90, capacityBytes: 300 })
  })

  it('excludes left nodes from the counts', () => {
    const h = deriveHealth(status({ nodes: [node('a'), node('b', 'left'), node('c', 'left')] }), NOW)
    expect(h.level).toBe('healthy')
    expect(h.nodes).toEqual({ total: 1, up: 1, suspect: 0, down: 0 })
    expect(h.capacity).toEqual({ usedBytes: 30, capacityBytes: 100 })
  })

  it('counts up, suspect and down nodes', () => {
    const nodes = [node('a'), node('b', 'suspect'), node('c', 'dead'), node('d', 'unknown')]
    expect(deriveHealth(status({ nodes }), NOW).nodes).toEqual({ total: 4, up: 1, suspect: 1, down: 1 })
  })

  it('degrades for a dead node and says how long', () => {
    const nodes = [node('a'), node('c', 'dead', { lastChange: ago(3 * MIN) })]
    const h = deriveHealth(status({ nodes }), NOW)
    expect(h.level).toBe('degraded')
    expect(h.findings).toEqual([{ level: 'degraded', text: 'c down for 3 min' }])
  })

  it('needs attention for a suspect node and omits the duration without a state change time', () => {
    const timed = deriveHealth(status({ nodes: [node('a'), node('b', 'suspect', { lastChange: ago(41_000) })] }), NOW)
    expect(timed.level).toBe('attention')
    expect(timed.findings).toEqual([{ level: 'attention', text: 'b suspect for 41 s' }])
    const untimed = deriveHealth(status({ nodes: [node('a'), node('b', 'suspect')] }), NOW)
    expect(untimed.findings).toEqual([{ level: 'attention', text: 'b suspect' }])
  })

  it('needs attention for an unknown node state', () => {
    const h = deriveHealth(status({ nodes: [node('a'), node('x', 'unknown')] }), NOW)
    expect(h.level).toBe('attention')
    expect(h.findings).toEqual([{ level: 'attention', text: 'x state unknown' }])
  })

  it('flags an empty cluster without a backup verdict', () => {
    const h = deriveHealth(status({ nodes: [] }), NOW)
    expect(h.level).toBe('attention')
    expect(h.backup).toBeNull()
    expect(h.findings).toEqual([{ level: 'attention', text: 'No nodes' }])
    expect(h.nodes).toEqual({ total: 0, up: 0, suspect: 0, down: 0 })
    expect(h.capacity).toBeNull()
  })

  it('treats only left nodes like an empty cluster', () => {
    const h = deriveHealth(status({ nodes: [node('a', 'left')] }), NOW)
    expect(h.level).toBe('attention')
    expect(h.backup).toBeNull()
  })

  it('falls back to the responding node storage when no node reports capacity', () => {
    const nodes = [node('a', 'alive', { capacityBytes: 0, usedBytes: 0 }), node('b', 'alive', { capacityBytes: 0, usedBytes: 0 })]
    const h = deriveHealth(status({ nodes, storage: stor(100, 200) }), NOW)
    expect(h.capacity).toEqual({ usedBytes: 100, capacityBytes: 200 })
    expect(h.level).toBe('healthy')
  })

  it('has no capacity when nothing reports it', () => {
    const nodes = [node('a', 'alive', { capacityBytes: 0, usedBytes: 0 })]
    expect(deriveHealth(status({ nodes, storage: null }), NOW).capacity).toBeNull()
  })

  it('needs attention when a node disk is 80% full', () => {
    const h = deriveHealth(status({ nodes: [node('a'), node('b', 'alive', { usedBytes: 80 })] }), NOW)
    expect(h.level).toBe('attention')
    expect(h.findings).toEqual([{ level: 'attention', text: 'b disk 80% full' }])
  })

  it('degrades when a node disk is 90% full', () => {
    const h = deriveHealth(status({ nodes: [node('a'), node('b', 'alive', { usedBytes: 90 })] }), NOW)
    expect(h.level).toBe('degraded')
    expect(h.findings).toEqual([{ level: 'degraded', text: 'b disk 90% full' }])
  })

  it('lists the fullest disk first', () => {
    const nodes = [node('a', 'alive', { usedBytes: 85 }), node('b', 'alive', { usedBytes: 95 })]
    const h = deriveHealth(status({ nodes }), NOW)
    expect(h.findings.map((f) => f.text)).toEqual(['b disk 95% full', 'a disk 85% full'])
  })

  it('checks the responding node storage when it is not among the reporting nodes', () => {
    const h = deriveHealth(status({ nodes: [node('b'), node('c')], storage: stor(95, 100), respondingNodeId: 'a' }), NOW)
    expect(h.findings).toEqual([{ level: 'degraded', text: 'a disk 95% full' }])
  })

  it('ignores responding node storage that reports no capacity', () => {
    const h = deriveHealth(status({ nodes: [node('b')], storage: stor(0, 0), respondingNodeId: 'a' }), NOW)
    expect(h.findings).toEqual([])
  })

  it('does not double count the responding node when it reports capacity', () => {
    expect(deriveHealth(status({ storage: stor(95, 100) }), NOW).findings).toEqual([])
  })

  describe('backup', () => {
    it('unconfigured adds no finding', () => {
      const h = deriveHealth(status({ backup: null }), NOW)
      expect(h.backup).toBe('unconfigured')
      expect(h.level).toBe('healthy')
      expect(h.findings).toEqual([])
    })

    it.each<[string, Backup]>([
      ['running', backup({ running: true })],
      ['never', backup({ lastRun: null, nextRunAt: inFuture(3 * HOUR) })],
      ['ok', backup()],
    ])('%s adds no finding', (kind, b) => {
      const h = deriveHealth(status({ backup: b }), NOW)
      expect(h.backup).toBe(kind)
      expect(h.findings).toEqual([])
    })

    it('failed carries the run error as detail', () => {
      const b = backup({ lastRun: run({ ok: false, error: 'denied (403)', finishedAt: ago(48 * MIN) }) })
      const h = deriveHealth(status({ backup: b }), NOW)
      expect(h.backup).toBe('failed')
      expect(h.level).toBe('attention')
      expect(h.findings).toEqual([{ level: 'attention', text: 'Backup failed 48 min ago', detail: 'denied (403)' }])
    })

    it('failed without an error has no detail key', () => {
      const b = backup({ lastRun: run({ ok: false, error: '' }) })
      const f = deriveHealth(status({ backup: b }), NOW).findings[0]
      expect(f).toEqual({ level: 'attention', text: 'Backup failed 2 h ago' })
      expect(f).not.toHaveProperty('detail')
    })

    it('failed falls back to the start time, then to no age', () => {
      const started = backup({ lastRun: run({ ok: false, finishedAt: null, startedAt: ago(3 * HOUR) }) })
      expect(deriveHealth(status({ backup: started }), NOW).findings[0]?.text).toBe('Backup failed 3 h ago')
      const bare = backup({ lastRun: run({ ok: false, finishedAt: null, startedAt: null }) })
      expect(deriveHealth(status({ backup: bare }), NOW).findings[0]?.text).toBe('Backup failed')
    })

    it('overdue names the last run', () => {
      const b = backup({ nextRunAt: ago(HOUR), lastRun: run({ finishedAt: ago(14 * HOUR) }) })
      const h = deriveHealth(status({ backup: b }), NOW)
      expect(h.backup).toBe('overdue')
      expect(h.level).toBe('attention')
      expect(h.findings).toEqual([{ level: 'attention', text: 'Backup overdue, last run 14 h ago' }])
    })

    it('overdue without any run has no age', () => {
      const b = backup({ lastRun: null, nextRunAt: ago(HOUR) })
      expect(deriveHealth(status({ backup: b }), NOW).findings).toEqual([{ level: 'attention', text: 'Backup overdue' }])
    })
  })

  it('orders degraded first, then nodes, disks and backups within a level', () => {
    const nodes = [
      node('a', 'suspect', { lastChange: ago(10 * MIN) }),
      node('b', 'suspect', { lastChange: ago(MIN), usedBytes: 85 }),
      node('c', 'dead', { lastChange: ago(5 * MIN), usedBytes: 95 }),
    ]
    const h = deriveHealth(status({ nodes, backup: backup({ lastRun: run({ ok: false }) }) }), NOW)
    expect(h.level).toBe('degraded')
    expect(h.findings.map((f) => f.text)).toEqual([
      'c down for 5 min',
      'c disk 95% full',
      'b suspect for 1 min',
      'a suspect for 10 min',
      'b disk 85% full',
      'Backup failed 2 h ago',
    ])
  })
})
