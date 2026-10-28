import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { effectScope, type EffectScope, nextTick, ref } from 'vue'
import { useNow } from './useNow'

function inScope<T>(scope: EffectScope, fn: () => T): T {
  const r = scope.run(fn)
  if (r === undefined) throw new Error('scope is not active')
  return r
}

describe('useNow', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(1_800_000_000_000)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts at the current time and ticks while active', async () => {
    const scope = effectScope()
    const now = inScope(scope, () => useNow(ref(true), 1000))
    expect(now.value).toBe(1_800_000_000_000)
    await vi.advanceTimersByTimeAsync(3000)
    expect(now.value).toBe(1_800_000_003_000)
    scope.stop()
  })

  it('ticks every second by default', async () => {
    const scope = effectScope()
    const now = inScope(scope, () => useNow(ref(true)))
    await vi.advanceTimersByTimeAsync(1000)
    expect(now.value).toBe(1_800_000_001_000)
    scope.stop()
  })

  it('does not tick while inactive', async () => {
    const scope = effectScope()
    const now = inScope(scope, () => useNow(ref(false), 1000))
    await vi.advanceTimersByTimeAsync(5000)
    expect(now.value).toBe(1_800_000_000_000)
    expect(vi.getTimerCount()).toBe(0)
    scope.stop()
  })

  it('refreshes immediately when it becomes active and stops when it becomes inactive', async () => {
    const active = ref(false)
    const scope = effectScope()
    const now = inScope(scope, () => useNow(active, 1000))
    vi.setSystemTime(1_800_000_010_000)
    active.value = true
    await nextTick()
    expect(now.value).toBe(1_800_000_010_000)
    await vi.advanceTimersByTimeAsync(2000)
    expect(now.value).toBe(1_800_000_012_000)

    active.value = false
    await nextTick()
    await vi.advanceTimersByTimeAsync(5000)
    expect(now.value).toBe(1_800_000_012_000)
    expect(vi.getTimerCount()).toBe(0)
    scope.stop()
  })

  it('clears its timer when the scope stops', () => {
    const scope = effectScope()
    scope.run(() => useNow(ref(true), 1000))
    expect(vi.getTimerCount()).toBe(1)
    scope.stop()
    expect(vi.getTimerCount()).toBe(0)
  })
})
