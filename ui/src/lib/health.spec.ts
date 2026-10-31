import { describe, it, expect } from 'vitest'
import type { Backup, BackupRun, ClusterHealth, ClusterNode, Gc, NodeState, Status, Volume } from '../api/status'
import {
  backupKind,
  deriveHealth,
  fillLevel,
  GC_NEVER_AFTER_MS,
  gcKind,
  gcOutcome,
  gcSkipReason,
  maxSkewMs,
  nodeLevel,
  OVERDUE_GRACE_MS,
  replicationKind,
  SKEW_CRIT_MS,
  SKEW_WARN_MS,
  skewLevel,
  stateById,
  usageRatio,
  volumeDegraded,
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
    pressured: false,
    clockSkewMs: 0,
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

function cluster(over: Partial<ClusterHealth> = {}): ClusterHealth {
  return {
    replicationFactor: 3,
    aliveNodes: 3,
    isolated: false,
    rejoinAttempts: 0,
    clockSkewMs: 0,
    clockSkewAlerts: 0,
    startedAt: ago(48 * HOUR),
    ...over,
  }
}

function gc(over: Partial<Gc> = {}): Gc {
  return {
    enabled: true,
    running: false,
    lastRunAt: ago(HOUR),
    lastDurationMs: 1000,
    lastOrphansDeleted: 1,
    lastBytesFreed: 1,
    lastError: '',
    ...over,
  }
}

function vol(path: string, over: Partial<Volume> = {}): Volume {
  return {
    path,
    sizeBytes: 100,
    writtenBytes: 0,
    extentSizeBytes: 1,
    leaseHolder: '',
    replicas: ['a', 'b', 'c'],
    mapDamaged: false,
    attachedHere: false,
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
    health: null,
    gc: null,
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

describe('skewLevel', () => {
  it.each<[number, Level]>([
    [0, 'healthy'],
    [SKEW_WARN_MS - 1, 'healthy'],
    [SKEW_WARN_MS, 'attention'],
    [-SKEW_WARN_MS, 'attention'],
    [SKEW_CRIT_MS - 1, 'attention'],
    [SKEW_CRIT_MS, 'degraded'],
    [-5000, 'degraded'],
  ])('%d ms is %s', (ms, want) => {
    expect(skewLevel(ms)).toBe(want)
  })
})

describe('replicationKind', () => {
  it.each<[string, ClusterHealth | null, ReturnType<typeof replicationKind>]>([
    ['no report', null, null],
    ['no replication factor', cluster({ replicationFactor: 0 }), null],
    ['all alive', cluster(), 'healthy'],
    ['more alive than replicas', cluster({ aliveNodes: 5 }), 'healthy'],
    ['one short', cluster({ aliveNodes: 2 }), 'degraded'],
    ['none alive', cluster({ aliveNodes: 0 }), 'degraded'],
  ])('%s', (_n, h, want) => {
    expect(replicationKind(h)).toBe(want)
  })
})

describe('gcKind', () => {
  it.each<[string, Gc, ReturnType<typeof gcKind>]>([
    ['disabled, even with an error', gc({ enabled: false, lastError: 'x' }), 'off'],
    ['running, even after a failure', gc({ running: true, lastError: 'x' }), 'running'],
    ['failed', gc({ lastError: 'x' }), 'failed'],
    ['skipped', gc({ lastError: 'skipped: maps disagree' }), 'skipped'],
    ['running, even after a skip', gc({ running: true, lastError: 'skipped: maps disagree' }), 'running'],
    ['ok', gc(), 'ok'],
    ['never', gc({ lastRunAt: null }), 'never'],
  ])('%s', (_n, g, want) => {
    expect(gcKind(g)).toBe(want)
  })
})

describe('gcOutcome', () => {
  it.each<[string, string, ReturnType<typeof gcOutcome>]>([
    ['empty is ok', '', 'ok'],
    ['silod diverged sentinel', "skipped: this node's extent maps disagree with their replicas; wait for the extent scrubber to reconcile them", 'skipped'],
    ['silod incomplete sentinel', 'skipped: this node does not hold the extent maps of every live volume', 'skipped'],
    ['prefix without a space', 'skipped:reason', 'skipped'],
    ['a real failure', 'could not list the local chunk store: EIO', 'failed'],
    ['skipped later in the text', 'sweep skipped: disk gone', 'failed'],
    ['capitalised prefix', 'Skipped: no', 'failed'],
  ])('%s', (_n, err, want) => {
    expect(gcOutcome(err)).toBe(want)
  })
})

describe('gcSkipReason', () => {
  it.each([
    ['strips the prefix and its space', 'skipped: maps disagree', 'maps disagree'],
    ['strips a prefix without a space', 'skipped:maps disagree', 'maps disagree'],
    ['leaves a failure alone', 'disk gone', 'disk gone'],
    ['leaves empty alone', '', ''],
  ])('%s', (_n, err, want) => {
    expect(gcSkipReason(err)).toBe(want)
  })
})

describe('volumeDegraded', () => {
  const states = stateById([node('a'), node('b'), node('c', 'suspect')])

  it.each<[string, Volume, number, boolean]>([
    ['all replicas alive', vol('/v', { replicas: ['a', 'b'] }), 2, false],
    ['a replica not alive', vol('/v', { replicas: ['a', 'c'] }), 2, true],
    ['a replica missing from the node table', vol('/v', { replicas: ['a', 'z'] }), 2, true],
    ['a set shorter than the factor', vol('/v', { replicas: ['a', 'b'] }), 3, true],
    ['nothing reported by an older silod', vol('/v', { replicas: [] }), 0, false],
  ])('%s', (_n, v, rf, want) => {
    expect(volumeDegraded(v, states, rf)).toBe(want)
  })
})

describe('maxSkewMs', () => {
  it('takes the largest absolute value of the summary and the nodes', () => {
    const nodes = [node('a'), node('b', 'alive', { clockSkewMs: -900 }), node('c', 'alive', { clockSkewMs: 300 })]
    expect(maxSkewMs(status({ nodes, health: cluster({ clockSkewMs: 400 }) }))).toBe(900)
    expect(maxSkewMs(status({ nodes, health: cluster({ clockSkewMs: 1200 }) }))).toBe(1200)
    expect(maxSkewMs(status({ nodes: [node('a')] }))).toBe(0)
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

  describe('cluster health', () => {
    const texts = (over: Partial<Status>): string[] => deriveHealth(status(over), NOW).findings.map((f) => f.text)

    it('stays healthy with every new field at its healthy value', () => {
      const h = deriveHealth(status({ health: cluster(), gc: gc(), volumes: [vol('/v')] }), NOW)
      expect(h.level).toBe('healthy')
      expect(h.findings).toEqual([])
    })

    it('degrades when fewer nodes are alive than the replication factor', () => {
      const h = deriveHealth(status({ health: cluster({ aliveNodes: 1 }) }), NOW)
      expect(h.level).toBe('degraded')
      expect(h.findings).toEqual([{ level: 'degraded', text: 'Cluster degraded: 1 node alive, replication factor 3' }])
      expect(texts({ health: cluster({ aliveNodes: 2 }) })).toEqual(['Cluster degraded: 2 nodes alive, replication factor 3'])
    })

    it('degrades when the responding node is isolated, with rejoin attempts when there are any', () => {
      expect(deriveHealth(status({ health: cluster({ isolated: true }) }), NOW).findings).toEqual([
        { level: 'degraded', text: 'a isolated from its peers' },
      ])
      expect(texts({ health: cluster({ isolated: true, rejoinAttempts: 1 }) })).toEqual([
        'a isolated from its peers, 1 rejoin attempt',
      ])
      expect(texts({ health: cluster({ isolated: true, rejoinAttempts: 12 }) })).toEqual([
        'a isolated from its peers, 12 rejoin attempts',
      ])
    })

    it.each<[number, Level | null]>([
      [499, null],
      [500, 'attention'],
      [1999, 'attention'],
      [2000, 'degraded'],
    ])('flags a clock skew of %d ms as %s', (ms, want) => {
      const f = deriveHealth(status({ health: cluster({ clockSkewMs: ms }) }), NOW).findings
      expect(f).toEqual(want ? [{ level: want, text: `Clock skew ${ms.toLocaleString('en')} ms` }] : [])
    })

    it('flags node skew even without a health report', () => {
      expect(texts({ nodes: [node('a'), node('b', 'alive', { clockSkewMs: -600 })] })).toEqual(['Clock skew 600 ms'])
    })

    it('needs attention for a pressured node but ignores one that left', () => {
      const nodes = [node('a'), node('b', 'alive', { pressured: true }), node('c', 'left', { pressured: true })]
      expect(deriveHealth(status({ nodes }), NOW).findings).toEqual([{ level: 'attention', text: 'b pressured' }])
    })

    it('counts degraded volumes and damaged maps', () => {
      const nodes = [node('a'), node('b'), node('c', 'dead')]
      const volumes = [
        vol('/one', { replicas: ['a', 'b'] }),
        vol('/two', { replicas: ['a', 'b', 'c'], mapDamaged: true }),
        vol('/three', { replicas: ['a', 'b', 'c'] }),
      ]
      const h = deriveHealth(status({ nodes, volumes, health: cluster({ aliveNodes: 3 }) }), NOW)
      expect(h.findings).toEqual([
        { level: 'degraded', text: 'c down' },
        { level: 'degraded', text: '1 volume map damaged' },
        { level: 'attention', text: '3 volumes degraded' },
      ])
      expect(texts({ volumes: [vol('/a', { mapDamaged: true }), vol('/b', { mapDamaged: true })] })).toEqual([
        '2 volume maps damaged',
      ])
      expect(texts({ volumes: [vol('/a', { replicas: ['a', 'z'] })] })).toEqual(['1 volume degraded'])
    })

    it('does not call a volume degraded for a short set when the factor is unknown', () => {
      expect(texts({ volumes: [vol('/a', { replicas: ['a'] })] })).toEqual([])
    })

    describe('gc', () => {
      it('failed carries the error and the age of the last run', () => {
        expect(deriveHealth(status({ gc: gc({ lastError: 'disk gone' }) }), NOW).findings).toEqual([
          { level: 'attention', text: 'GC failed 1 h ago', detail: 'disk gone' },
        ])
      })

      it('skipped is not an issue', () => {
        expect(texts({ gc: gc({ lastError: 'skipped: maps disagree' }) })).toEqual([])
      })

      it('failed without a finished run has no age', () => {
        expect(texts({ gc: gc({ lastError: 'x', lastRunAt: null }) })).toEqual(['GC failed'])
      })

      it('needs attention when it never ran after a day of uptime', () => {
        expect(deriveHealth(status({ health: cluster(), gc: gc({ lastRunAt: null }) }), NOW).findings).toEqual([
          { level: 'attention', text: 'GC has not run since silod started 2 d ago' },
        ])
      })

      it.each<[string, Partial<Status>]>([
        ['within the first day', { health: cluster({ startedAt: ago(GC_NEVER_AFTER_MS) }), gc: gc({ lastRunAt: null }) }],
        ['without a start time', { health: cluster({ startedAt: null }), gc: gc({ lastRunAt: null }) }],
        ['without a health report', { gc: gc({ lastRunAt: null }) }],
        ['while disabled', { health: cluster(), gc: gc({ enabled: false, lastRunAt: null }) }],
        ['while running', { health: cluster(), gc: gc({ running: true, lastRunAt: null }) }],
      ])('does not flag a missing run %s', (_n, over) => {
        expect(texts(over)).toEqual([])
      })
    })

    it('orders cluster findings before node, volume, backup and GC findings within a level', () => {
      const nodes = [node('a', 'alive', { pressured: true }), node('b', 'suspect'), node('c')]
      const h = deriveHealth(
        status({
          nodes,
          health: cluster({ aliveNodes: 2, isolated: true, clockSkewMs: 700 }),
          volumes: [vol('/v')],
          gc: gc({ lastError: 'x' }),
          backup: backup({ lastRun: run({ ok: false }) }),
        }),
        NOW,
      )
      expect(h.level).toBe('degraded')
      expect(h.findings.map((f) => f.text)).toEqual([
        'Cluster degraded: 2 nodes alive, replication factor 3',
        'a isolated from its peers',
        'b suspect',
        'a pressured',
        'Clock skew 700 ms',
        '1 volume degraded',
        'Backup failed 2 h ago',
        'GC failed 1 h ago',
      ])
    })
  })
})
