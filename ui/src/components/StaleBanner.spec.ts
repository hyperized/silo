import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import StaleBanner from './StaleBanner.vue'

function banner(retryInSeconds: number | null) {
  return mount(StaleBanner, { props: { age: '45 s ago', message: 'Connection lost', retryInSeconds } })
}

describe('StaleBanner', () => {
  it('announces the age and the reason as a status', () => {
    const w = banner(null)
    const status = w.get('[role="status"]')
    expect(status.text()).toContain('Stale, 45 s ago')
    expect(status.text()).toContain('Connection lost')
  })

  it('shows the countdown only when one is known', () => {
    expect(banner(8).text()).toContain('retry in 8 s')
    expect(banner(null).text()).not.toContain('retry in')
  })

  it('emits retry on click', async () => {
    const w = banner(null)
    await w.get('button').trigger('click')
    expect(w.emitted('retry')).toHaveLength(1)
  })
})
