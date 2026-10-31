import fixture from './__fixtures__/status.json'
import type { MockScenario } from './mockMode'
import { parseStatus, StatusUnreachableError, type Status, type WireStatus, type WireVolume } from './status'

// The fixture's timestamps are fixed; shifting them so "now" sits just after
// the last backup keeps ages like "2 minutes ago" meaningful during development.
const FIXTURE_NOW = 1761641000

const GIB = 1024 ** 3
const MIB = 1024 ** 2
const GC_PERIOD_S = 60
const GC_INCIDENT_ERRORS: Readonly<Record<number, string>> = {
  3: 'lease scan timed out after 30 s',
  5: "skipped: this node's extent maps disagree with their replicas; wait for the extent scrubber to reconcile them",
}
const BACKUP_PERIOD_S = 120
const NODE_IDS = ['node-a', 'node-b', 'node-c']

function shift(v: string, delta: number): string {
  const n = Number(v)
  return n > 0 ? String(n + delta) : v
}

/** Cheap deterministic stand-in for randomness: 0..999, stable for a given (k, salt). */
function hash(k: number, salt: number): number {
  return (((k * 31 + salt) * 2654435761) >>> 0) % 1000
}

/**
 * Cumulative growth after t seconds at `rate` per second with a slow wobble on
 * top. The wobble's slope is at most half the rate, so the curve never goes
 * down and sparklines still get a shape.
 */
function grow(rate: number, t: number, period: number, phase: number): number {
  return rate * t + 0.5 * rate * period * (Math.cos(phase) - Math.cos(t / period + phase))
}

const GENERATED: readonly { suffix: string; gib: number; holder: string }[] = [
  { suffix: '6d41', gib: 20, holder: 'node-a' },
  { suffix: '3b0d', gib: 40, holder: 'node-b' },
  { suffix: '67d5', gib: 5, holder: 'node-c' },
  { suffix: '1d5c', gib: 10, holder: 'node-b' },
  { suffix: '35c6', gib: 15, holder: '' },
  { suffix: '8a2e', gib: 25, holder: 'node-a' },
  { suffix: 'c07f', gib: 8, holder: 'node-c' },
  { suffix: '4e91', gib: 30, holder: '' },
  { suffix: 'b5d3', gib: 12, holder: 'node-b' },
  { suffix: '92af', gib: 35, holder: 'node-a' },
]

function generatedVolumes(respondingNodeId: string): WireVolume[] {
  return GENERATED.map((g, i) => ({
    path: `/csi/volumes/pvc-${g.suffix}`,
    inode: '',
    sizeBytes: String(g.gib * GIB),
    extentSizeBytes: String(4 * MIB),
    leaseHolder: g.holder,
    extentsWritten: String(100 + ((i * 53) % 300)),
    replicas: [...NODE_IDS],
    mapDamaged: false,
    attachedHere: g.holder === respondingNodeId,
  }))
}

/** Extents per second; every fifth volume sits idle so some sparklines stay flat. */
function volumeRate(i: number): number {
  return i % 5 === 1 ? 0 : 0.2 + ((i * 7919) % 29) / 10
}

function evolveVolumes(volumes: WireVolume[], elapsed: number): void {
  volumes.forEach((v, i) => {
    const maxExtents = Math.floor(Number(v.sizeBytes) / Number(v.extentSizeBytes))
    const written = Number(v.extentsWritten) + grow(volumeRate(i), elapsed, 30, i)
    v.extentsWritten = String(Math.min(maxExtents, Math.floor(written)))
  })
}

function isoSeconds(unix: number): string {
  return `${new Date(unix * 1000).toISOString().slice(0, 19)}Z`
}

export function mockWire(scenario: MockScenario, nowMs: number, sinceMs: number = nowMs): WireStatus {
  const nowS = Math.floor(nowMs / 1000)
  const sinceS = Math.floor(sinceMs / 1000)
  const elapsed = Math.max(0, nowMs - sinceMs) / 1000
  // Anchored on the session start so fixture times age with the clock; a
  // timestamp that moved on every poll would read as a new GC run each time.
  const delta = Math.min(sinceS, nowS) - FIXTURE_NOW
  const w = structuredClone(fixture) as WireStatus
  w.volumes.push(...generatedVolumes(w.respondingNodeId))
  for (const n of w.nodes) n.lastChangeUnix = shift(n.lastChangeUnix, delta)
  if (w.health?.startedUnix) w.health.startedUnix = shift(w.health.startedUnix, delta)

  const gcK = Math.floor(elapsed / GC_PERIOD_S)
  if (w.gc) {
    if (w.gc.lastRunUnix) w.gc.lastRunUnix = shift(w.gc.lastRunUnix, delta)
    if (gcK >= 1) {
      w.gc.lastRunUnix = String(sinceS + gcK * GC_PERIOD_S)
      w.gc.lastDurationMs = String(400 + hash(gcK, 1) * 4)
      w.gc.lastOrphansDeleted = String(20 + hash(gcK, 2))
      w.gc.lastBytesFreed = String(50 * MIB + Math.round((hash(gcK, 3) / 999) * (2 * GIB - 50 * MIB)))
    }
  }

  const backupK = Math.floor(elapsed / BACKUP_PERIOD_S)
  if (w.backup) {
    w.backup.intervalSeconds = String(BACKUP_PERIOD_S)
    w.backup.nextRunUnix = String(sinceS + (backupK + 1) * BACKUP_PERIOD_S)
    const run = w.backup.lastRun
    if (run) {
      // The first run sits one short period back, so the two-interval overdue
      // rule holds from the first poll instead of flagging a fixture-aged run.
      const started = backupK >= 1 ? sinceS + backupK * BACKUP_PERIOD_S : sinceS - 100
      run.startedUnix = String(started)
      run.finishedUnix = String(started + 20)
      if (backupK >= 1) {
        run.bytesUploaded = String(20 * MIB + hash(backupK, 4) * MIB)
        run.chunksUploaded = String(5 + Math.floor(hash(backupK, 5) / 10))
        run.manifest = `manifests/node-a/${isoSeconds(started)}.json`
      }
    }
  }

  const [, b, c] = w.nodes
  switch (scenario) {
    case 'attention':
      if (b) {
        b.state = 'NODE_STATE_SUSPECT'
        b.lastChangeUnix = String(nowS - 40)
      }
      if (b) b.pressured = true
      if (c) {
        c.usedBytes = String(Math.round(Number(c.capacityBytes) * 0.84))
        c.clockSkewMs = '-750'
      }
      // aliveNodes counts NODE_STATE_ALIVE only, so with node-b suspect a
      // factor of 3 would make this scenario Degraded. A factor of 2 keeps it a
      // warning while both volumes, which have a copy on node-b, turn degraded.
      if (w.health) {
        w.health.replicationFactor = 2
        w.health.aliveNodes = 2
        w.health.clockSkewAlerts = '2'
      }
      for (const v of w.volumes) {
        if (v.replicas) v.replicas = v.replicas.filter((id) => id !== 'node-c')
      }
      if (w.gc) {
        w.gc.lastError = 'listing chunks under /var/lib/silo/chunks failed: input/output error'
      }
      if (w.backup?.lastRun) {
        w.backup.lastRun.ok = false
        w.backup.lastRun.error =
          "upload to s3://silo-backups/cluster was denied (403). Check that this node's cloud credentials are allowed to write to the bucket."
      }
      break
    case 'degraded':
      if (c) c.state = 'NODE_STATE_DEAD'
      if (w.backup) w.backup.running = true
      if (w.health) w.health.aliveNodes = 2
      for (const v of w.volumes) if (v.path === '/db') v.mapDamaged = true
      break
    case 'incident':
      if (c) c.usedBytes = String(Math.round(Number(c.capacityBytes) * 0.84))
      for (const v of w.volumes) {
        if (v.path === '/csi/volumes/pvc-3b0d') v.replicas = ['node-a', 'node-b']
      }
      // Of every seven runs one fails and one is skipped, offset so both show up
      // within a few minutes. The skip text is silod's own.
      if (w.gc) w.gc.lastError = GC_INCIDENT_ERRORS[gcK % 7] ?? ''
      break
    case 'empty':
      w.nodes = []
      w.volumes = []
      w.storage = null
      w.backup = null
      break
    case 'unconfigured':
      w.backup = { configured: false, target: '', intervalSeconds: '0', nextRunUnix: '0', running: false, lastRun: null }
      w.volumes = []
      break
    default:
      break
  }

  w.nodes.forEach((n, i) => {
    // node-c in the incident fills slowly so it stays under the critical line for hours.
    const gibPerHour = scenario === 'incident' && i === 2 ? 0.5 : 1.7 + ((i * 37) % 7) / 10
    const used = Number(n.usedBytes) + grow((gibPerHour * GIB) / 3600, elapsed, 600, i)
    n.usedBytes = String(Math.min(Number(n.capacityBytes), Math.round(used)))
    // node-a answers for itself, so its skew is the reference and stays zero.
    if (i > 0) n.clockSkewMs = String(Number(n.clockSkewMs) + Math.round(10 * Math.sin(elapsed / 20 + i)))
  })
  const [responding] = w.nodes
  if (w.storage && responding) {
    const capacity = Number(w.storage.capacityBytes)
    const used = Number(responding.usedBytes)
    w.storage.usedBytes = String(used)
    w.storage.availableBytes = String(Math.max(0, capacity - used))
    w.storage.chunkCount = String(Math.round(used / (256 * 1024)))
  }
  if (w.health) {
    w.health.clockSkewMs = String(w.nodes.reduce((m, n) => Math.max(m, Math.abs(Number(n.clockSkewMs))), 0))
  }
  evolveVolumes(w.volumes, elapsed)
  return w
}

let sessionStart: number | null = null

/** Forgets the session start so the next fetch begins a fresh timeline. */
export function resetMockSession(): void {
  sessionStart = null
}

/** Stands in for fetchStatus with a short delay so loading states are visible. */
export async function fetchMockStatus(scenario: MockScenario, delayMs = 300): Promise<Status> {
  if (sessionStart === null) sessionStart = Date.now()
  const since = sessionStart
  await new Promise((r) => setTimeout(r, delayMs))
  if (scenario === 'error') throw new StatusUnreachableError()
  return parseStatus(mockWire(scenario, Date.now(), since))
}
