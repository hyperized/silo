import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Presentation } from '../lib/presentation'
import StateBadge from './StateBadge.vue'

const PLAIN: Presentation = { label: 'Healthy', tone: 'good', icon: 'check' }
const TIPPED: Presentation = { label: 'Off', tone: 'neutral', icon: 'minus', tip: 'Off: not set' }

describe('StateBadge', () => {
  it('renders the label next to a decorative icon', () => {
    const w = mount(StateBadge, { props: { presentation: PLAIN } })
    expect(w.text()).toBe('Healthy')
    expect(w.get('svg').attributes('aria-hidden')).toBe('true')
    expect(w.get('span.inline-flex').classes()).toContain('text-good')
  })

  it.each([
    ['good', 'bg-good-wash'],
    ['warn', 'bg-warn-wash'],
    ['crit', 'bg-crit-wash'],
    ['neutral', 'bg-raise'],
  ] as const)('adds the %s wash only as a chip', (tone, wash) => {
    const presentation: Presentation = { label: 'X', tone, icon: 'check' }
    expect(mount(StateBadge, { props: { presentation } }).get('span.inline-flex').classes()).not.toContain(wash)
    expect(mount(StateBadge, { props: { presentation, chip: true } }).get('span.inline-flex').classes()).toContain(wash)
  })

  it('has no tooltip without a tip', () => {
    expect(mount(StateBadge, { props: { presentation: PLAIN } }).find('[role="tooltip"]').exists()).toBe(false)
  })

  it('wraps in a tooltip when the presentation has a tip', () => {
    const w = mount(StateBadge, { props: { presentation: TIPPED } })
    expect(w.get('[role="tooltip"]').text()).toBe('Off: not set')
    expect(w.text()).toContain('Off')
  })
})
