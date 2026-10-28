import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { effectScope, nextTick, ref, type EffectScope, type Ref } from 'vue'
import {
  backoffDelay,
  BACKOFF_MAX_MS,
  POLL_INTERVAL_MS,
  PollTimeoutError,
  REQUEST_TIMEOUT_MS,
  useStatusPoll,
} from './useStatusPoll'

function inScope<T>(scope: EffectScope, fn: () => T): T {
  const r = scope.run(fn)
  if (r === undefined) throw new Error('scope is not active')
  return r
}

interface Deferred<T> {
  promise: Promise<T>
  resolve: (v: T) => void
  reject: (e: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const flush = async (): Promise<void> => {
  await vi.advanceTimersByTimeAsync(0)
}

describe('backoffDelay', () => {
  it.each<[number, number]>([
    [0, 2000],
    [1, 2000],
    [2, 4000],
    [3, 8000],
    [4, 16000],
    [5, 30000],
    [6, 30000],
    [50, BACKOFF_MAX_MS],
  ])('after %d failures waits %d ms', (failures, want) => {
    expect(backoffDelay(failures)).toBe(want)
  })
})

describe('useStatusPoll', () => {
  let scope: EffectScope
  let active: Ref<boolean>

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(1_800_000_000_000)
    scope = effectScope()
    active = ref(true)
  })

  afterEach(() => {
    scope.stop()
    vi.useRealTimers()
  })

  function start<T>(fetcher: (signal: AbortSignal) => Promise<T>, extra: { intervalMs?: number; timeoutMs?: number } = {}) {
    return inScope(scope, () => useStatusPoll(fetcher, { active, ...extra }))
  }

  it('fetches immediately and exposes the result', async () => {
    const fetcher = vi.fn(() => Promise.resolve('one'))
    const poll = start(fetcher)
    expect(poll.inFlight.value).toBe(true)
    await flush()
    expect(fetcher).toHaveBeenCalledTimes(1)
    expect(poll.data.value).toBe('one')
    expect(poll.error.value).toBeNull()
    expect(poll.lastSuccessAt.value).toBe(Date.now())
    expect(poll.inFlight.value).toBe(false)
    expect(poll.failures.value).toBe(0)
  })

  it('schedules the next fetch after the interval', async () => {
    let n = 0
    const fetcher = vi.fn(() => Promise.resolve(`v${String(++n)}`))
    const poll = start(fetcher)
    await flush()
    expect(poll.nextAttemptAt.value).toBe(Date.now() + POLL_INTERVAL_MS)
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS - 1)
    expect(fetcher).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(fetcher).toHaveBeenCalledTimes(2)
    expect(poll.data.value).toBe('v2')
  })

  it('honours a custom interval', async () => {
    const fetcher = vi.fn(() => Promise.resolve(1))
    start(fetcher, { intervalMs: 1000 })
    await vi.advanceTimersByTimeAsync(3000)
    expect(fetcher).toHaveBeenCalledTimes(4)
  })

  it('keeps the previous data on failure and backs off 2, 4, 8, 16, 30, 30 s', async () => {
    let fail = false
    const fetcher = vi.fn(() => (fail ? Promise.reject(new Error('boom')) : Promise.resolve('good')))
    const poll = start(fetcher, { intervalMs: 1000 })
    await flush()
    fail = true
    await vi.advanceTimersByTimeAsync(1000)
    expect(poll.data.value).toBe('good')
    expect(poll.error.value?.message).toBe('boom')

    const seen: number[] = []
    for (let i = 1; i <= 6; i++) {
      expect(poll.failures.value).toBe(i)
      const wait = (poll.nextAttemptAt.value as number) - Date.now()
      seen.push(wait)
      await vi.advanceTimersByTimeAsync(wait)
    }
    expect(seen).toEqual([2000, 4000, 8000, 16000, 30000, 30000])
    expect(poll.data.value).toBe('good')
  })

  it('wraps a non-Error rejection in an Error', async () => {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- a non-Error rejection is the case under test
    const poll = start(() => Promise.reject('plain text'))
    await flush()
    expect(poll.error.value).toBeInstanceOf(Error)
    expect(poll.error.value?.message).toBe('plain text')
  })

  it('resets failures and the error after a success', async () => {
    let fail = true
    const fetcher = vi.fn(() => (fail ? Promise.reject(new Error('down')) : Promise.resolve('up')))
    const poll = start(fetcher)
    await flush()
    expect(poll.failures.value).toBe(1)
    fail = false
    await vi.advanceTimersByTimeAsync(2000)
    expect(poll.data.value).toBe('up')
    expect(poll.error.value).toBeNull()
    expect(poll.failures.value).toBe(0)
    expect(poll.nextAttemptAt.value).toBe(Date.now() + POLL_INTERVAL_MS)
  })

  it('ignores a stale success when a newer request has started', async () => {
    const first = deferred<string>()
    const second = deferred<string>()
    const queue = [first, second]
    const signals: AbortSignal[] = []
    const poll = start((s) => {
      signals.push(s)
      return (queue.shift() as Deferred<string>).promise
    })
    const refreshed = poll.refresh()
    second.resolve('new')
    await refreshed
    first.resolve('old')
    await flush()
    expect(poll.data.value).toBe('new')
    expect(signals[0]?.aborted).toBe(true)
    expect(signals[1]?.aborted).toBe(false)
    expect(poll.inFlight.value).toBe(false)
  })

  it('ignores a stale failure when a newer request has started', async () => {
    const first = deferred<string>()
    const second = deferred<string>()
    const queue = [first, second]
    const poll = start(() => (queue.shift() as Deferred<string>).promise)
    const refreshed = poll.refresh()
    second.resolve('new')
    await refreshed
    first.reject(new Error('late'))
    await flush()
    expect(poll.error.value).toBeNull()
    expect(poll.failures.value).toBe(0)
  })

  it('rejects a never-settling fetcher with PollTimeoutError and aborts its signal', async () => {
    let signal!: AbortSignal
    const poll = start(
      (s) => {
        signal = s
        return new Promise<never>(() => undefined)
      },
      { timeoutMs: 4000 },
    )
    await vi.advanceTimersByTimeAsync(3999)
    expect(poll.inFlight.value).toBe(true)
    expect(signal.aborted).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(signal.aborted).toBe(true)
    expect(poll.error.value).toBeInstanceOf(PollTimeoutError)
    expect(poll.error.value?.message).toContain('4 s')
    expect(poll.inFlight.value).toBe(false)
    expect(poll.failures.value).toBe(1)
  })

  it('uses a 10 s request timeout by default', async () => {
    const poll = start(() => new Promise<never>(() => undefined))
    await vi.advanceTimersByTimeAsync(REQUEST_TIMEOUT_MS)
    expect(poll.error.value).toBeInstanceOf(PollTimeoutError)
  })

  it('stops scheduling while inactive and fetches immediately when active again', async () => {
    const fetcher = vi.fn(() => Promise.resolve('x'))
    const poll = start(fetcher)
    await flush()
    active.value = false
    await nextTick()
    expect(poll.nextAttemptAt.value).toBeNull()
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 3)
    expect(fetcher).toHaveBeenCalledTimes(1)

    active.value = true
    await nextTick()
    await flush()
    expect(fetcher).toHaveBeenCalledTimes(2)
    expect(poll.nextAttemptAt.value).toBe(Date.now() + POLL_INTERVAL_MS)
  })

  it('does not fetch at all when it starts inactive', async () => {
    active.value = false
    const fetcher = vi.fn(() => Promise.resolve('x'))
    start(fetcher)
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 2)
    expect(fetcher).not.toHaveBeenCalled()
  })

  it('stops on togglePause and resumes with an immediate fetch', async () => {
    const fetcher = vi.fn(() => Promise.resolve('x'))
    const poll = start(fetcher)
    await flush()
    poll.togglePause()
    expect(poll.paused.value).toBe(true)
    await nextTick()
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 3)
    expect(fetcher).toHaveBeenCalledTimes(1)

    poll.togglePause()
    await nextTick()
    await flush()
    expect(poll.paused.value).toBe(false)
    expect(fetcher).toHaveBeenCalledTimes(2)
  })

  it('refresh while paused fetches once without scheduling', async () => {
    const fetcher = vi.fn(() => Promise.resolve('x'))
    const poll = start(fetcher)
    await flush()
    poll.togglePause()
    await nextTick()
    await poll.refresh()
    expect(fetcher).toHaveBeenCalledTimes(2)
    expect(poll.nextAttemptAt.value).toBeNull()
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 3)
    expect(fetcher).toHaveBeenCalledTimes(2)
  })

  it('clears timers and aborts the in-flight request when the scope stops', async () => {
    const pending = deferred<string>()
    let signal!: AbortSignal
    const poll = start((s) => {
      signal = s
      return pending.promise
    })
    scope.stop()
    expect(signal.aborted).toBe(true)
    expect(vi.getTimerCount()).toBe(1) // the request deadline, cleared once the fetch settles
    pending.resolve('late')
    await flush()
    expect(poll.data.value).toBeNull()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('does not reschedule after the scope stops between polls', async () => {
    const fetcher = vi.fn(() => Promise.resolve('x'))
    start(fetcher)
    await flush()
    expect(vi.getTimerCount()).toBe(1)
    scope.stop()
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS * 2)
    expect(fetcher).toHaveBeenCalledTimes(1)
  })
})
