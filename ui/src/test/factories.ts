import type { Backup, BackupRun, ClusterHealth, ClusterNode, Gc, LocalStorage, Status, Volume } from '../api/status'

export const GIB = 1024 ** 3

export function node(over: Partial<ClusterNode> = {}): ClusterNode {
  return {
    id: 'node-a',
    gossipAddress: '10.0.0.1:7946',
    dataAddress: '10.0.0.1:7081',
    state: 'alive',
    incarnation: 1,
    lastChange: null,
    capacityBytes: 100 * GIB,
    usedBytes: 30 * GIB,
    pressured: false,
    clockSkewMs: 0,
    ...over,
  }
}

export function storage(over: Partial<LocalStorage> = {}): LocalStorage {
  return {
    dataDir: '/var/lib/silo',
    capacityBytes: 100 * GIB,
    usedBytes: 30 * GIB,
    availableBytes: 70 * GIB,
    chunkCount: 12345,
    ...over,
  }
}

export function backupRun(over: Partial<BackupRun> = {}): BackupRun {
  return {
    startedAt: null,
    finishedAt: null,
    ok: true,
    error: '',
    chunksUploaded: 1500,
    chunksSkipped: 20,
    bytesUploaded: 3 * GIB,
    manifest: 'manifest-1',
    ...over,
  }
}

export function backup(over: Partial<Backup> = {}): Backup {
  return {
    configured: true,
    target: 's3://bucket/silo',
    intervalSeconds: 6 * 3600,
    nextRunAt: null,
    running: false,
    lastRun: null,
    ...over,
  }
}

export function volume(over: Partial<Volume> = {}): Volume {
  return {
    path: '/db',
    sizeBytes: 10 * GIB,
    allocatedBytes: 2 * GIB,
    extentSizeBytes: 4 * 1024 ** 2,
    leaseHolder: 'node-a',
    replicas: [],
    mapDamaged: false,
    attachedHere: false,
    ...over,
  }
}

export function clusterHealth(over: Partial<ClusterHealth> = {}): ClusterHealth {
  return {
    replicationFactor: 3,
    aliveNodes: 3,
    isolated: false,
    rejoinAttempts: 0,
    clockSkewMs: 0,
    clockSkewAlerts: 0,
    startedAt: null,
    ...over,
  }
}

export function gc(over: Partial<Gc> = {}): Gc {
  return {
    enabled: true,
    running: false,
    lastRunAt: null,
    lastDurationMs: 1840,
    lastOrphansDeleted: 312,
    lastBytesFreed: 1.2 * GIB,
    lastError: '',
    ...over,
  }
}

export function status(over: Partial<Status> = {}): Status {
  return {
    respondingNodeId: 'node-a',
    version: '0.16.0',
    nodes: [node()],
    storage: storage(),
    backup: backup(),
    volumes: [volume()],
    health: null,
    gc: null,
    ...over,
  }
}
