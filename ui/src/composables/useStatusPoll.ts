import { computed, onScopeDispose, ref, shallowRef, watch, type Ref } from 'vue'

export interface PollOptions {
  intervalMs?: number
  timeoutMs?: number
  /** Pause the schedule while false; used for tab visibility. */
  active: Readonly<Ref<boolean>>
}

export const POLL_INTERVAL_MS = 5000
export const BACKOFF_BASE_MS = 2000
export const BACKOFF_MAX_MS = 30000
export const REQUEST_TIMEOUT_MS = 10000
/** Data older than this without a successful refresh is flagged as out of date. */
export const STALE_AFTER_MS = 30000

export class PollTimeoutError extends Error {
  constructor(ms: number) {
    super(
      `silod did not answer within ${String(ms / 1000)} s. It may be overloaded, or the network between ` +
        'your browser and silod is slow. This page keeps trying.',
    )
    this.name = 'PollTimeoutError'
  }
}

/** 2 s, 4 s, 8 s, 16 s, then 30 s for every further consecutive failure. */
export function backoffDelay(failures: number): number {
  return Math.min(BACKOFF_BASE_MS * 2 ** Math.max(failures - 1, 0), BACKOFF_MAX_MS)
}

/**
 * Polls `fetcher` on a fixed interval, backing off after failures. The last
 * good value is kept through errors so the page can show stale data instead of
 * going blank. Only the newest request may update state, so a slow response
 * can never overwrite a fresher one.
 */
export function useStatusPoll<T>(fetcher: (signal: AbortSignal) => Promise<T>, opts: PollOptions) {
  const intervalMs = opts.intervalMs ?? POLL_INTERVAL_MS
  const timeoutMs = opts.timeoutMs ?? REQUEST_TIMEOUT_MS
  const data = shallowRef<T | null>(null)
  const error = shallowRef<Error | null>(null)
  const lastSuccessAt = ref<number | null>(null)
  const nextAttemptAt = ref<number | null>(null)
  const inFlight = ref(false)
  const failures = ref(0)
  const paused = ref(false)

  let timer: ReturnType<typeof setTimeout> | undefined
  let controller: AbortController | null = null
  let seq = 0

  const running = computed(() => opts.active.value && !paused.value)

  function clearTimer(): void {
    clearTimeout(timer)
    timer = undefined
    nextAttemptAt.value = null
  }

  function schedule(delay: number): void {
    clearTimer()
    if (!running.value) return
    nextAttemptAt.value = Date.now() + delay
    timer = setTimeout(() => void run(), delay)
  }

  async function run(): Promise<void> {
    clearTimer()
    controller?.abort()
    const ctrl = new AbortController()
    controller = ctrl
    const id = ++seq
    inFlight.value = true
    let deadline: ReturnType<typeof setTimeout> | undefined
    // A request that never settles would otherwise freeze the schedule, so
    // the deadline rejects even when the fetcher ignores the abort signal.
    const timeout = new Promise<never>((_, reject) => {
      deadline = setTimeout(() => {
        ctrl.abort()
        reject(new PollTimeoutError(timeoutMs))
      }, timeoutMs)
    })
    try {
      const value = await Promise.race([fetcher(ctrl.signal), timeout])
      if (id !== seq) return
      data.value = value
      error.value = null
      failures.value = 0
      lastSuccessAt.value = Date.now()
      schedule(intervalMs)
    } catch (e) {
      if (id !== seq) return
      error.value = e instanceof Error ? e : new Error(String(e))
      failures.value++
      schedule(backoffDelay(failures.value))
    } finally {
      clearTimeout(deadline)
      if (id === seq) {
        inFlight.value = false
        controller = null
      }
    }
  }

  watch(
    running,
    (on) => {
      if (on) {
        void run()
      } else {
        clearTimer()
      }
    },
    { immediate: true },
  )

  onScopeDispose(() => {
    clearTimer()
    seq++
    controller?.abort()
  })

  return {
    data,
    error,
    lastSuccessAt,
    nextAttemptAt,
    inFlight,
    failures,
    paused,
    /** Fetch immediately, skipping any pending wait. */
    refresh: (): Promise<void> => run(),
    togglePause: (): void => {
      paused.value = !paused.value
    },
  }
}
