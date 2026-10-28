import { describe, it, expect, afterEach } from 'vitest'
import { effectScope, type EffectScope, nextTick } from 'vue'
import { usePageVisible } from './usePageVisible'

function inScope<T>(scope: EffectScope, fn: () => T): T {
  const r = scope.run(fn)
  if (r === undefined) throw new Error('scope is not active')
  return r
}

let state: DocumentVisibilityState = 'visible'

function setVisibility(next: DocumentVisibilityState): void {
  state = next
  document.dispatchEvent(new Event('visibilitychange'))
}

describe('usePageVisible', () => {
  afterEach(() => {
    state = 'visible'
  })

  function setup(initial: DocumentVisibilityState) {
    state = initial
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => state })
    const scope = effectScope()
    const visible = inScope(scope, () => usePageVisible())
    return { scope, visible }
  }

  it.each<[DocumentVisibilityState, boolean]>([
    ['visible', true],
    ['hidden', false],
  ])('starts as %s -> %s', (initial, want) => {
    const { scope, visible } = setup(initial)
    expect(visible.value).toBe(want)
    scope.stop()
  })

  it('follows visibilitychange events', async () => {
    const { scope, visible } = setup('visible')
    setVisibility('hidden')
    await nextTick()
    expect(visible.value).toBe(false)
    setVisibility('visible')
    await nextTick()
    expect(visible.value).toBe(true)
    scope.stop()
  })

  it('stops listening once the scope is stopped', () => {
    const { scope, visible } = setup('visible')
    scope.stop()
    setVisibility('hidden')
    expect(visible.value).toBe(true)
  })
})
