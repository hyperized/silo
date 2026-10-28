import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import { formatDateTime } from '../lib/format'
import RelTime from './RelTime.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)

describe('RelTime', () => {
  it.each([
    ['ago', new Date(NOW - 48 * 60_000), '48 min ago'],
    ['in', new Date(NOW + 5 * 3600_000), 'in 5 h'],
    ['for', new Date(NOW - 16 * 60_000), '16 min'],
  ] as const)('renders mode %s', (mode, date, text) => {
    const w = mount(RelTime, { props: { date, now: NOW, mode } })
    expect(w.text()).toBe(text)
  })

  it('carries the machine-readable and absolute times', () => {
    const date = new Date(NOW - 60_000)
    const w = mount(RelTime, { props: { date, now: NOW, mode: 'ago' } })
    expect(w.element.tagName).toBe('TIME')
    expect(w.attributes('datetime')).toBe(date.toISOString())
    expect(w.attributes('title')).toBe(formatDateTime(date))
  })
})
