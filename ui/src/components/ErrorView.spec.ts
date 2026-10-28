import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import ErrorView from './ErrorView.vue'

function view(over: Partial<{ retryInSeconds: number | null; retrying: boolean }> = {}) {
  return mount(ErrorView, { props: { message: 'Could not reach silod', retryInSeconds: null, retrying: false, ...over } })
}

describe('ErrorView', () => {
  it('is an alert carrying the message', () => {
    const w = view()
    expect(w.get('[role="alert"]').text()).toContain('No status from silod')
    expect(w.text()).toContain('Could not reach silod')
  })

  it('emits retry on click', async () => {
    const w = view()
    await w.get('button').trigger('click')
    expect(w.emitted('retry')).toHaveLength(1)
  })

  it('counts down to the next try', () => {
    const w = view({ retryInSeconds: 8 })
    expect(w.text()).toContain('next try in 8 s')
    expect(w.get('button').attributes('disabled')).toBeUndefined()
  })

  it('disables the button while retrying', () => {
    const w = view({ retrying: true, retryInSeconds: 8 })
    expect(w.get('button').attributes('disabled')).toBeDefined()
    expect(w.text()).toContain('retrying')
    expect(w.text()).not.toContain('next try')
  })

  it('says nothing about timing when idle with no countdown', () => {
    const w = view()
    expect(w.text()).not.toContain('retrying')
    expect(w.text()).not.toContain('next try')
  })
})
