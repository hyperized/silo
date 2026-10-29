import { describe, it, expect, vi, afterEach } from 'vitest'
import fixture from './__fixtures__/status.json'
import {
  fetchStatus,
  parseInt64,
  parseNodeState,
  parseStatus,
  StatusFormatError,
  StatusServerError,
  StatusUnreachableError,
  STATUS_PATH,
} from './status'

type Raw = Record<string, unknown>

function wire(): Raw {
  return structuredClone(fixture)
}

function mutated(fn: (w: Raw) => void): Raw {
  const w = wire()
  fn(w)
  return w
}

const nodes = (w: Raw): Raw[] => w.nodes as Raw[]
const volumes = (w: Raw): Raw[] => w.volumes as Raw[]
const backup = (w: Raw): Raw => w.backup as Raw
const lastRun = (w: Raw): Raw => backup(w).lastRun as Raw

describe('parseInt64', () => {
  it.each([
    ['123', 123],
    ['-5', -5],
    ['0', 0],
    [42, 42],
    [-7, -7],
  ])('parses %j as %d', (input, want) => {
    expect(parseInt64(input)).toBe(want)
  })

  it.each([['abc'], ['1.5'], [''], [1.5], [null], [undefined], [{}], [true], [Number.NaN]])(
    'returns NaN for %j',
    (input) => {
      expect(parseInt64(input)).toBeNaN()
    },
  )
})

describe('parseNodeState', () => {
  it.each([
    ['NODE_STATE_ALIVE', 'alive'],
    ['NODE_STATE_SUSPECT', 'suspect'],
    ['NODE_STATE_DEAD', 'dead'],
    ['NODE_STATE_LEFT', 'left'],
    ['NODE_STATE_SOMETHING_NEW', 'unknown'],
    ['constructor', 'unknown'],
    ['toString', 'unknown'],
    ['', 'unknown'],
    [3, 'unknown'],
    [null, 'unknown'],
    [undefined, 'unknown'],
  ])('maps %j to %s', (input, want) => {
    expect(parseNodeState(input)).toBe(want)
  })
})

describe('parseStatus', () => {
  it('parses the fixture into numbers and dates', () => {
    const s = parseStatus(wire())
    expect(s.respondingNodeId).toBe('node-a')
    expect(s.version).toBe('0.16.0')
    expect(s.nodes.map((n) => n.id)).toEqual(['node-a', 'node-b', 'node-c'])
    expect(s.nodes[0]).toEqual({
      id: 'node-a',
      gossipAddress: '10.0.0.1:7100',
      dataAddress: '10.0.0.1:7000',
      state: 'alive',
      incarnation: 3,
      lastChange: new Date(1761640000 * 1000),
      capacityBytes: 107374182400,
      usedBytes: 32212254720,
      pressured: false,
      clockSkewMs: 0,
    })
    expect(s.nodes.map((n) => n.clockSkewMs)).toEqual([0, 42, -17])
    expect(s.health).toEqual({
      replicationFactor: 3,
      aliveNodes: 3,
      isolated: false,
      rejoinAttempts: 0,
      clockSkewMs: 42,
      clockSkewAlerts: 0,
      startedAt: new Date(1761400000 * 1000),
    })
    expect(s.gc).toEqual({
      enabled: true,
      running: false,
      lastRunAt: new Date(1761637000 * 1000),
      lastDurationMs: 1840,
      lastOrphansDeleted: 312,
      lastBytesFreed: 1308622848,
      lastError: '',
    })
    expect(s.storage).toEqual({
      dataDir: '',
      capacityBytes: 107374182400,
      usedBytes: 32212254720,
      availableBytes: 75161927680,
      chunkCount: 12345,
    })
    expect(s.backup).toMatchObject({
      configured: true,
      target: 's3://silo-backups/cluster',
      intervalSeconds: 21600,
      nextRunAt: new Date(1761660000 * 1000),
      running: false,
    })
    expect(s.backup?.lastRun).toMatchObject({
      startedAt: new Date(1761638000 * 1000),
      finishedAt: new Date(1761638120 * 1000),
      ok: true,
      error: '',
      chunksUploaded: 42,
      chunksSkipped: 12303,
      bytesUploaded: 176160768,
    })
  })

  it('sorts volumes by path and computes allocated bytes', () => {
    const s = parseStatus(wire())
    expect(s.volumes.map((v) => v.path)).toEqual(['/cache', '/db'])
    expect(s.volumes[1]).toEqual({
      path: '/db',
      sizeBytes: 53687091200,
      allocatedBytes: 1800 * 4194304,
      extentSizeBytes: 4194304,
      leaseHolder: 'csi-node-3',
      replicas: ['node-a', 'node-b', 'node-c'],
      mapDamaged: false,
      attachedHere: true,
    })
    expect(s.volumes[0]?.allocatedBytes).toBe(0)
  })

  it('clamps allocated bytes to the volume size', () => {
    const s = parseStatus(
      mutated((w) => {
        const v = volumes(w)[0] as Raw
        v.extentsWritten = '999999'
      }),
    )
    const db = s.volumes.find((v) => v.path === '/db')
    expect(db?.allocatedBytes).toBe(db?.sizeBytes)
  })

  it('sorts nodes by id', () => {
    const s = parseStatus(mutated((w) => nodes(w).reverse()))
    expect(s.nodes.map((n) => n.id)).toEqual(['node-a', 'node-b', 'node-c'])
  })

  it('accepts int64 values sent as numbers', () => {
    const s = parseStatus(mutated((w) => (nodes(w)[0] as Raw).incarnation = 9))
    expect(s.nodes[0]?.incarnation).toBe(9)
  })

  it('turns unix 0 into null dates', () => {
    const s = parseStatus(
      mutated((w) => {
        ;(nodes(w)[0] as Raw).lastChangeUnix = '0'
        backup(w).nextRunUnix = '0'
        lastRun(w).startedUnix = '0'
        lastRun(w).finishedUnix = '0'
      }),
    )
    expect(s.nodes[0]?.lastChange).toBeNull()
    expect(s.backup?.nextRunAt).toBeNull()
    expect(s.backup?.lastRun?.startedAt).toBeNull()
    expect(s.backup?.lastRun?.finishedAt).toBeNull()
  })

  it.each([
    ['null', null],
    ['absent', undefined],
  ])('treats %s storage, backup and lastRun as null', (_name, value) => {
    const set = (o: Raw, k: string): void => {
      if (value === undefined) Reflect.deleteProperty(o, k)
      else o[k] = value
    }
    const s = parseStatus(
      mutated((w) => {
        set(w, 'storage')
        set(backup(w), 'lastRun')
      }),
    )
    expect(s.storage).toBeNull()
    expect(s.backup?.lastRun).toBeNull()
    expect(parseStatus(mutated((w) => { set(w, 'backup'); })).backup).toBeNull()
  })

  it.each([
    ['null', null],
    ['absent', undefined],
  ])('treats %s nodes and volumes as empty lists', (_name, value) => {
    const s = parseStatus(
      mutated((w) => {
        if (value === undefined) {
          Reflect.deleteProperty(w, 'nodes')
          Reflect.deleteProperty(w, 'volumes')
        } else {
          w.nodes = value
          w.volumes = value
        }
      }),
    )
    expect(s.nodes).toEqual([])
    expect(s.volumes).toEqual([])
  })

  describe('fields newer than the first release', () => {
    const health = (w: Raw): Raw => w.health as Raw
    const gc = (w: Raw): Raw => w.gc as Raw

    it.each([
      ['null', null],
      ['absent', undefined],
    ])('treats %s health and gc as null', (_name, value) => {
      const s = parseStatus(
        mutated((w) => {
          if (value === undefined) {
            Reflect.deleteProperty(w, 'health')
            Reflect.deleteProperty(w, 'gc')
          } else {
            w.health = value
            w.gc = value
          }
        }),
      )
      expect(s.health).toBeNull()
      expect(s.gc).toBeNull()
    })

    it.each([
      ['absent', (o: Raw, k: string) => Reflect.deleteProperty(o, k)],
      ['null', (o: Raw, k: string) => (o[k] = null)],
    ])('gives %s fields their zero value', (_name, drop) => {
      const s = parseStatus(
        mutated((w) => {
          for (const n of nodes(w)) for (const k of ['pressured', 'clockSkewMs']) drop(n, k)
          for (const v of volumes(w)) for (const k of ['replicas', 'mapDamaged', 'attachedHere']) drop(v, k)
          for (const k of Object.keys(health(w))) drop(health(w), k)
          for (const k of Object.keys(gc(w))) drop(gc(w), k)
        }),
      )
      expect(s.nodes.every((n) => !n.pressured && n.clockSkewMs === 0)).toBe(true)
      expect(s.volumes.every((v) => v.replicas.length === 0 && !v.mapDamaged && !v.attachedHere)).toBe(true)
      expect(s.health).toEqual({
        replicationFactor: 0,
        aliveNodes: 0,
        isolated: false,
        rejoinAttempts: 0,
        clockSkewMs: 0,
        clockSkewAlerts: 0,
        startedAt: null,
      })
      expect(s.gc).toEqual({
        enabled: false,
        running: false,
        lastRunAt: null,
        lastDurationMs: 0,
        lastOrphansDeleted: 0,
        lastBytesFreed: 0,
        lastError: '',
      })
    })

    it('parses set values', () => {
      const s = parseStatus(
        mutated((w) => {
          Object.assign(nodes(w)[1] as Raw, { pressured: true })
          Object.assign(volumes(w)[0] as Raw, { mapDamaged: true })
          Object.assign(health(w), { isolated: true, rejoinAttempts: '4', clockSkewMs: '-2500', clockSkewAlerts: 7 })
          Object.assign(gc(w), { running: true, lastError: 'disk gone' })
        }),
      )
      expect(s.nodes[1]?.pressured).toBe(true)
      expect(s.volumes.find((v) => v.path === '/db')?.mapDamaged).toBe(true)
      expect(s.health).toMatchObject({ isolated: true, rejoinAttempts: 4, clockSkewMs: 2500, clockSkewAlerts: 7 })
      expect(s.gc).toMatchObject({ running: true, lastError: 'disk gone' })
    })

    it('turns a zero gc last run into null', () => {
      expect(parseStatus(mutated((w) => (gc(w).lastRunUnix = '0'))).gc?.lastRunAt).toBeNull()
    })
  })

  describe('rejects malformed payloads with StatusFormatError', () => {
    it.each([
      ['string root', 'nope'],
      ['null root', null],
      ['array root', []],
      ['number root', 5],
    ])('%s', (_n, raw) => {
      expect(() => parseStatus(raw)).toThrow(StatusFormatError)
    })

    it.each<[string, (w: Raw) => void, string]>([
      ['non-string top-level field', (w) => (w.version = 1), 'status.version'],
      ['missing top-level field', (w) => delete w.respondingNodeId, 'status.respondingNodeId'],
      ['non-string node field', (w) => (nodes(w)[0] as Raw).id = 7, 'nodes[0].id'],
      ['non-integer node int64', (w) => (nodes(w)[1] as Raw).usedBytes = '1.5', 'nodes[1].usedBytes'],
      ['non-integer timestamp', (w) => (nodes(w)[0] as Raw).lastChangeUnix = 'x', 'lastChangeUnix'],
      ['non-string storage field', (w) => (w.storage as Raw).dataDir = null, 'storage.dataDir'],
      ['non-integer storage int64', (w) => (w.storage as Raw).chunkCount = {}, 'storage.chunkCount'],
      ['non-boolean backup field', (w) => (backup(w).configured = 'yes'), 'backup.configured'],
      ['non-boolean lastRun field', (w) => (lastRun(w).ok = 1), 'backup.lastRun.ok'],
      ['non-string lastRun field', (w) => (lastRun(w).manifest = 1), 'backup.lastRun.manifest'],
      ['non-integer volume int64', (w) => (volumes(w)[0] as Raw).sizeBytes = 'big', 'volumes[0].sizeBytes'],
      ['non-string volume field', (w) => (volumes(w)[0] as Raw).path = 1, 'volumes[0].path'],
      ['non-array nodes', (w) => (w.nodes = {}), 'nodes is not a list'],
      ['non-array volumes', (w) => (w.volumes = 'x'), 'volumes is not a list'],
      ['non-object storage', (w) => (w.storage = 'x'), 'storage is not an object'],
      ['array storage', (w) => (w.storage = []), 'storage is not an object'],
      ['non-object backup', (w) => (w.backup = 3), 'backup is not an object'],
      ['non-object lastRun', (w) => (backup(w).lastRun = 'x'), 'lastRun is not an object'],
      ['non-object node item', (w) => (nodes(w)[2] = 'x' as unknown as Raw), 'nodes[2] is not an object'],
      ['null node item', (w) => (nodes(w)[0] = null as unknown as Raw), 'nodes[0] is not an object'],
      ['non-object volume item', (w) => (volumes(w)[1] = 4 as unknown as Raw), 'volumes[1] is not an object'],
      ['non-boolean pressured', (w) => ((nodes(w)[0] as Raw).pressured = 'no'), 'nodes[0].pressured'],
      ['non-integer clock skew', (w) => ((nodes(w)[0] as Raw).clockSkewMs = 'x'), 'nodes[0].clockSkewMs'],
      ['non-array replicas', (w) => ((volumes(w)[0] as Raw).replicas = 'node-a'), 'volumes[0].replicas is not a list'],
      ['non-string replica', (w) => ((volumes(w)[0] as Raw).replicas = ['node-a', 3]), 'volumes[0].replicas[1]'],
      ['non-boolean mapDamaged', (w) => ((volumes(w)[0] as Raw).mapDamaged = 1), 'volumes[0].mapDamaged'],
      ['non-object health', (w) => (w.health = 'ok'), 'health is not an object'],
      ['non-integer replication factor', (w) => ((w.health as Raw).replicationFactor = 'x'), 'health.replicationFactor'],
      ['non-boolean isolated', (w) => ((w.health as Raw).isolated = 'no'), 'health.isolated'],
      ['non-object gc', (w) => (w.gc = []), 'gc is not an object'],
      ['non-string gc error', (w) => ((w.gc as Raw).lastError = 5), 'gc.lastError'],
    ])('%s', (_n, fn, where) => {
      const raw = mutated(fn)
      expect(() => parseStatus(raw)).toThrow(StatusFormatError)
      expect(() => parseStatus(raw)).toThrow(where)
    })
  })
})

describe('fetchStatus', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  function stubFetch(impl: () => Promise<unknown>): ReturnType<typeof vi.fn> {
    const f = vi.fn(impl)
    vi.stubGlobal('fetch', f)
    return f
  }

  const ok = (json: () => Promise<unknown>): { ok: boolean; status: number; json: () => Promise<unknown> } => ({
    ok: true,
    status: 200,
    json,
  })

  it('returns the parsed status without a signal in the request', async () => {
    const f = stubFetch(() => Promise.resolve(ok(() => Promise.resolve(wire()))))
    const s = await fetchStatus()
    expect(s.respondingNodeId).toBe('node-a')
    expect(f).toHaveBeenCalledWith(STATUS_PATH, {
      headers: { Accept: 'application/json' },
      cache: 'no-store',
    })
  })

  it('passes the signal when given', async () => {
    const f = stubFetch(() => Promise.resolve(ok(() => Promise.resolve(wire()))))
    const ctrl = new AbortController()
    await fetchStatus(ctrl.signal)
    expect(f).toHaveBeenCalledWith(STATUS_PATH, {
      headers: { Accept: 'application/json' },
      cache: 'no-store',
      signal: ctrl.signal,
    })
  })

  it('maps a network failure to StatusUnreachableError mentioning SILO_HTTP_ADDR', async () => {
    stubFetch(() => Promise.reject(new TypeError('Failed to fetch')))
    const err = await fetchStatus().catch((e: unknown) => e)
    expect(err).toBeInstanceOf(StatusUnreachableError)
    expect((err as Error).message).toContain('SILO_HTTP_ADDR')
  })

  it('rethrows an AbortError unchanged', async () => {
    const abort = new DOMException('aborted', 'AbortError')
    stubFetch(() => Promise.reject(abort))
    await expect(fetchStatus()).rejects.toBe(abort)
  })

  it('treats a non-abort DOMException as unreachable', async () => {
    stubFetch(() => Promise.reject(new DOMException('nope', 'NetworkError')))
    await expect(fetchStatus()).rejects.toBeInstanceOf(StatusUnreachableError)
  })

  it('maps a non-2xx answer to StatusServerError with the HTTP status', async () => {
    stubFetch(() => Promise.resolve({ ok: false, status: 502 }))
    const err = await fetchStatus().catch((e: unknown) => e)
    expect(err).toBeInstanceOf(StatusServerError)
    expect((err as StatusServerError).httpStatus).toBe(502)
    expect((err as Error).message).toContain('HTTP 502')
  })

  it('maps an unparsable body to StatusFormatError', async () => {
    stubFetch(() => Promise.resolve(ok(() => Promise.reject(new SyntaxError('bad json')))))
    await expect(fetchStatus()).rejects.toBeInstanceOf(StatusFormatError)
  })

  it('maps a well-formed JSON body of the wrong shape to StatusFormatError', async () => {
    stubFetch(() => Promise.resolve(ok(() => Promise.resolve([]))))
    await expect(fetchStatus()).rejects.toBeInstanceOf(StatusFormatError)
  })
})
