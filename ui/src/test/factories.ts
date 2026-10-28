import type { Backup, BackupRun, ClusterNode, LocalStorage, Status, Volume } from '../api/status'
import type { Health } from '../lib/health'

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
    ...over,
  }
}

export function health(over: Partial<Health> = {}): Health {
  return {
    level: 'healthy',
    findings: [],
    backup: null,
    nodes: { total: 1, up: 1, suspect: 0, down: 0 },
    capacity: { usedBytes: 30 * GIB, capacityBytes: 100 * GIB },
    ...over,
  }
}
