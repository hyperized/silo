import fixture from './__fixtures__/status.json'
import type { MockScenario } from './mockMode'
import { parseStatus, StatusUnreachableError, type Status, type WireStatus } from './status'

// The fixture's timestamps are fixed; shifting them so "now" sits just after
// the last backup keeps ages like "2 minutes ago" meaningful during development.
const FIXTURE_NOW = 1761641000

function shift(v: string, delta: number): string {
  const n = Number(v)
  return n > 0 ? String(n + delta) : v
}

export function mockWire(scenario: MockScenario, nowMs: number): WireStatus {
  const delta = Math.floor(nowMs / 1000) - FIXTURE_NOW
  const w = structuredClone(fixture) as WireStatus
  for (const n of w.nodes) n.lastChangeUnix = shift(n.lastChangeUnix, delta)
  if (w.backup) {
    w.backup.nextRunUnix = shift(w.backup.nextRunUnix, delta)
    if (w.backup.lastRun) {
      w.backup.lastRun.startedUnix = shift(w.backup.lastRun.startedUnix, delta)
      w.backup.lastRun.finishedUnix = shift(w.backup.lastRun.finishedUnix, delta)
    }
  }
  if (w.health?.startedUnix) w.health.startedUnix = shift(w.health.startedUnix, delta)
  if (w.gc?.lastRunUnix) w.gc.lastRunUnix = shift(w.gc.lastRunUnix, delta)
  const [, b, c] = w.nodes
  switch (scenario) {
    case 'attention':
      if (b) {
        b.state = 'NODE_STATE_SUSPECT'
        b.lastChangeUnix = String(Math.floor(nowMs / 1000) - 40)
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
        w.health.clockSkewMs = '750'
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
  return w
}

/** Stands in for fetchStatus with a short delay so loading states are visible. */
export async function fetchMockStatus(scenario: MockScenario, delayMs = 300): Promise<Status> {
  await new Promise((r) => setTimeout(r, delayMs))
  if (scenario === 'error') throw new StatusUnreachableError()
  return parseStatus(mockWire(scenario, Date.now()))
}
