// Wire types mirror GetStatusResponse as protojson renders it: lowerCamelCase
// keys, int64 as decimal strings, enums by name, unset messages as null.

export interface WireNode {
  id: string
  gossipAddress: string
  dataAddress: string
  state: string
  incarnation: string
  lastChangeUnix: string
  capacityBytes: string
  usedBytes: string
}

export interface WireStorage {
  dataDir: string
  capacityBytes: string
  usedBytes: string
  availableBytes: string
  chunkCount: string
}

export interface WireBackupRun {
  startedUnix: string
  finishedUnix: string
  ok: boolean
  error: string
  chunksUploaded: string
  chunksSkipped: string
  bytesUploaded: string
  manifest: string
}

export interface WireBackup {
  configured: boolean
  target: string
  intervalSeconds: string
  nextRunUnix: string
  running: boolean
  lastRun: WireBackupRun | null
}

export interface WireVolume {
  path: string
  inode: string
  sizeBytes: string
  extentSizeBytes: string
  leaseHolder: string
  extentsWritten: string
}

export interface WireStatus {
  respondingNodeId: string
  version: string
  nodes: WireNode[]
  storage: WireStorage | null
  backup: WireBackup | null
  volumes: WireVolume[]
}

export type NodeState = 'alive' | 'suspect' | 'dead' | 'left' | 'unknown'

export interface ClusterNode {
  id: string
  gossipAddress: string
  dataAddress: string
  state: NodeState
  incarnation: number
  lastChange: Date | null
  capacityBytes: number
  usedBytes: number
}

export interface LocalStorage {
  dataDir: string
  capacityBytes: number
  usedBytes: number
  availableBytes: number
  chunkCount: number
}

export interface BackupRun {
  startedAt: Date | null
  finishedAt: Date | null
  ok: boolean
  error: string
  chunksUploaded: number
  chunksSkipped: number
  bytesUploaded: number
  manifest: string
}

export interface Backup {
  configured: boolean
  target: string
  intervalSeconds: number
  nextRunAt: Date | null
  running: boolean
  lastRun: BackupRun | null
}

export interface Volume {
  path: string
  sizeBytes: number
  allocatedBytes: number
  extentSizeBytes: number
  leaseHolder: string
}

export interface Status {
  respondingNodeId: string
  version: string
  nodes: ClusterNode[]
  storage: LocalStorage | null
  backup: Backup | null
  volumes: Volume[]
}

export const STATUS_PATH = '/api/v1/status'

/** Raised when the response is not the shape this page was built for. */
export class StatusFormatError extends Error {
  constructor(detail: string) {
    super(
      `silod answered, but with data this page does not understand (${detail}). ` +
        'This usually means the page and silod are different versions: reload the page, ' +
        'and if it persists, upgrade silod so both come from the same release.',
    )
    this.name = 'StatusFormatError'
  }
}

/** Raised when the request never got an HTTP answer. */
export class StatusUnreachableError extends Error {
  constructor() {
    super(
      'Could not reach silod at this address. Check that silod is running and that its ' +
        'HTTP port (SILO_HTTP_ADDR, default 7080) is reachable from your browser.',
    )
    this.name = 'StatusUnreachableError'
  }
}

/** Raised when silod (or a proxy in front of it) answered with a non-2xx status. */
export class StatusServerError extends Error {
  constructor(readonly httpStatus: number) {
    super(
      `silod answered with an error (HTTP ${String(httpStatus)}). If a proxy sits in front of ` +
        'silod, check that it forwards /api/ to silod; otherwise look at the silod log for the cause.',
    )
    this.name = 'StatusServerError'
  }
}

type Obj = Record<string, unknown>

function isObj(v: unknown): v is Obj {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function str(o: Obj, key: string, where: string): string {
  const v = o[key]
  if (typeof v !== 'string') throw new StatusFormatError(`${where}.${key} is not text`)
  return v
}

function bool(o: Obj, key: string, where: string): boolean {
  const v = o[key]
  if (typeof v !== 'boolean') throw new StatusFormatError(`${where}.${key} is not true/false`)
  return v
}

/**
 * Parses a protojson int64. protojson emits strings, but accepts numbers on
 * input, so both are tolerated. Values above 2^53 lose precision, which is
 * acceptable for byte counts and timestamps shown to a human.
 */
export function parseInt64(v: unknown): number {
  if (typeof v === 'number' && Number.isInteger(v)) return v
  if (typeof v === 'string' && /^-?\d+$/.test(v)) return Number(v)
  return Number.NaN
}

function int(o: Obj, key: string, where: string): number {
  const n = parseInt64(o[key])
  if (Number.isNaN(n)) throw new StatusFormatError(`${where}.${key} is not a whole number`)
  return n
}

/** Zero means "not set" for every timestamp in the contract. */
function unixDate(o: Obj, key: string, where: string): Date | null {
  const n = int(o, key, where)
  return n > 0 ? new Date(n * 1000) : null
}

function list(o: Obj, key: string): unknown[] {
  const v = o[key]
  if (v === undefined || v === null) return []
  if (!Array.isArray(v)) throw new StatusFormatError(`${key} is not a list`)
  return v
}

function optional(o: Obj, key: string): Obj | null {
  const v = o[key]
  if (v === undefined || v === null) return null
  if (!isObj(v)) throw new StatusFormatError(`${key} is not an object`)
  return v
}

// A Map rather than an object literal so wire values such as "constructor"
// cannot resolve through the prototype chain.
const NODE_STATES: ReadonlyMap<string, NodeState> = new Map([
  ['NODE_STATE_ALIVE', 'alive'],
  ['NODE_STATE_SUSPECT', 'suspect'],
  ['NODE_STATE_DEAD', 'dead'],
  ['NODE_STATE_LEFT', 'left'],
])

export function parseNodeState(v: unknown): NodeState {
  return (typeof v === 'string' && NODE_STATES.get(v)) || 'unknown'
}

function parseNode(v: unknown, i: number): ClusterNode {
  const where = `nodes[${String(i)}]`
  if (!isObj(v)) throw new StatusFormatError(`${where} is not an object`)
  return {
    id: str(v, 'id', where),
    gossipAddress: str(v, 'gossipAddress', where),
    dataAddress: str(v, 'dataAddress', where),
    state: parseNodeState(v.state),
    incarnation: int(v, 'incarnation', where),
    lastChange: unixDate(v, 'lastChangeUnix', where),
    capacityBytes: int(v, 'capacityBytes', where),
    usedBytes: int(v, 'usedBytes', where),
  }
}

function parseStorage(v: Obj): LocalStorage {
  return {
    dataDir: str(v, 'dataDir', 'storage'),
    capacityBytes: int(v, 'capacityBytes', 'storage'),
    usedBytes: int(v, 'usedBytes', 'storage'),
    availableBytes: int(v, 'availableBytes', 'storage'),
    chunkCount: int(v, 'chunkCount', 'storage'),
  }
}

function parseRun(v: Obj): BackupRun {
  const where = 'backup.lastRun'
  return {
    startedAt: unixDate(v, 'startedUnix', where),
    finishedAt: unixDate(v, 'finishedUnix', where),
    ok: bool(v, 'ok', where),
    error: str(v, 'error', where),
    chunksUploaded: int(v, 'chunksUploaded', where),
    chunksSkipped: int(v, 'chunksSkipped', where),
    bytesUploaded: int(v, 'bytesUploaded', where),
    manifest: str(v, 'manifest', where),
  }
}

function parseBackup(v: Obj): Backup {
  const run = optional(v, 'lastRun')
  return {
    configured: bool(v, 'configured', 'backup'),
    target: str(v, 'target', 'backup'),
    intervalSeconds: int(v, 'intervalSeconds', 'backup'),
    nextRunAt: unixDate(v, 'nextRunUnix', 'backup'),
    running: bool(v, 'running', 'backup'),
    lastRun: run ? parseRun(run) : null,
  }
}

function parseVolume(v: unknown, i: number): Volume {
  const where = `volumes[${String(i)}]`
  if (!isObj(v)) throw new StatusFormatError(`${where} is not an object`)
  const size = int(v, 'sizeBytes', where)
  // The written-extent count can overshoot the logical size on the last,
  // partially used extent; clamp so the percentage never passes 100.
  const extent = int(v, 'extentSizeBytes', where)
  const allocated = Math.min(int(v, 'extentsWritten', where) * extent, size)
  return {
    path: str(v, 'path', where),
    sizeBytes: size,
    allocatedBytes: allocated,
    extentSizeBytes: extent,
    leaseHolder: str(v, 'leaseHolder', where),
  }
}

/** Converts the untrusted wire payload into the domain model, or throws StatusFormatError. */
export function parseStatus(raw: unknown): Status {
  if (!isObj(raw)) throw new StatusFormatError('the response is not an object')
  const storage = optional(raw, 'storage')
  const backup = optional(raw, 'backup')
  return {
    respondingNodeId: str(raw, 'respondingNodeId', 'status'),
    version: str(raw, 'version', 'status'),
    nodes: list(raw, 'nodes').map(parseNode).sort((a, b) => a.id.localeCompare(b.id)),
    storage: storage ? parseStorage(storage) : null,
    backup: backup ? parseBackup(backup) : null,
    volumes: list(raw, 'volumes')
      .map(parseVolume)
      .sort((a, b) => a.path.localeCompare(b.path)),
  }
}

/** Fetches and parses the status; rejects with one of the Status*Error classes, or AbortError. */
export async function fetchStatus(signal?: AbortSignal): Promise<Status> {
  let res: Response
  try {
    res = await fetch(STATUS_PATH, {
      headers: { Accept: 'application/json' },
      cache: 'no-store',
      ...(signal ? { signal } : {}),
    })
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') throw e
    throw new StatusUnreachableError()
  }
  if (!res.ok) throw new StatusServerError(res.status)
  let body: unknown
  try {
    body = await res.json()
  } catch {
    throw new StatusFormatError('the response is not JSON')
  }
  return parseStatus(body)
}
