import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import StatusIcon from './StatusIcon.vue'
import type { IconName } from '../lib/presentation'

const MARKERS: Record<IconName, string> = {
  check: 'M5 8.2 7 10.2 11 6',
  alert: 'M8 1.8 14.6 13.5H1.4Z',
  octagon: 'M5.3 1.5h5.4l3.8 3.8',
  minus: 'M5 8h6',
  question: 'M6.2 6.2a1.9',
  sync: 'M12.4 1.6v2.9',
  clock: 'M8 4.5V8l2.3 1.5',
}

describe('StatusIcon', () => {
  it.each(Object.entries(MARKERS) as [IconName, string][])('draws the %s icon', (name, marker) => {
    const wrapper = mount(StatusIcon, { props: { name } })
    expect(wrapper.html()).toContain(marker)
    for (const [other, otherMarker] of Object.entries(MARKERS)) {
      if (other !== name) expect(wrapper.html()).not.toContain(otherMarker)
    }
  })

  it('is hidden from assistive technology', () => {
    const wrapper = mount(StatusIcon, { props: { name: 'check' } })
    expect(wrapper.get('svg').attributes('aria-hidden')).toBe('true')
  })

  it('defaults to 16 px', () => {
    const svg = mount(StatusIcon, { props: { name: 'check' } }).get('svg')
    expect(svg.attributes('width')).toBe('16')
    expect(svg.attributes('height')).toBe('16')
  })

  it('honours the size prop', () => {
    const svg = mount(StatusIcon, { props: { name: 'check', size: 22 } }).get('svg')
    expect(svg.attributes('width')).toBe('22')
    expect(svg.attributes('height')).toBe('22')
  })
})
