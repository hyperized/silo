import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Meter from './Meter.vue'

function meter(used: number, capacity: number) {
  return mount(Meter, { props: { usedBytes: used, capacityBytes: capacity, label: 'Disk' } })
}

describe('Meter', () => {
  it('exposes the measurement to assistive technology', () => {
    const el = meter(30, 100).get('[role="meter"]')
    expect(el.attributes('aria-label')).toBe('Disk')
    expect(el.attributes('aria-valuemin')).toBe('0')
    expect(el.attributes('aria-valuemax')).toBe('100')
    expect(el.attributes('aria-valuenow')).toBe('30')
    expect(el.attributes('aria-valuetext')).toBe('30%, 70 B free')
  })

  it('scales the fill to the used ratio', () => {
    const fill = meter(25, 100).get('[role="meter"] > span')
    expect(fill.attributes('style')).toContain('scaleX(0.25)')
  })

  it.each([
    [79, 'bg-accent'],
    [80, 'bg-warn-bar'],
    [89, 'bg-warn-bar'],
    [90, 'bg-crit'],
  ])('colours the fill at %i%% as %s', (used, cls) => {
    expect(meter(used, 100).get('[role="meter"] > span').classes()).toContain(cls)
  })

  it('never reports negative free space when over capacity', () => {
    expect(meter(150, 100).get('[role="meter"]').attributes('aria-valuetext')).toBe('100%, 0 B free')
  })

  it('renders an inert placeholder when the capacity is not known', () => {
    const w = meter(0, 0)
    expect(w.find('[role="meter"]').exists()).toBe(false)
    expect(w.attributes('aria-hidden')).toBe('true')
  })
})
