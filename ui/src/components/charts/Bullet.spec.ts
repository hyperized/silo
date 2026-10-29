import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Bullet from './Bullet.vue'

function bullet(value: number, format?: (v: number) => string) {
  return mount(Bullet, {
    props: { value, warn: 500, crit: 2000, unit: 'ms', label: 'Skew', ...(format ? { format } : {}) },
  })
}

describe('Bullet', () => {
  it('places the fill and markers on the piecewise scale', () => {
    const w = bullet(24)
    expect(w.get('[data-part="fill"]').attributes('width')).toBe('2.88%')
    expect(w.get('[data-part="warn"]').attributes('x')).toBe('60%')
    expect(w.get('[data-part="crit"]').attributes('x')).toBe('99%')
  })

  it.each([
    [24, 'var(--c-good)', 'var(--c-warn)', 'var(--c-crit)', 'Skew 24 ms, warning at 500, critical at 2,000'],
    [
      740,
      'var(--c-warn-bar)',
      'var(--c-ink)',
      'var(--c-crit)',
      'Skew 740 ms, over the warning threshold, warning at 500, critical at 2,000',
    ],
    [
      2300,
      'var(--c-crit)',
      'var(--c-ink)',
      'var(--c-ink)',
      'Skew 2,300 ms, over the critical threshold, warning at 500, critical at 2,000',
    ],
  ])('at %i fills %s with markers %s/%s', (value, fill, warn, crit, aria) => {
    const w = bullet(value)
    expect(w.get('[data-part="fill"]').attributes('fill')).toBe(fill)
    expect(w.get('[data-part="warn"]').attributes('fill')).toBe(warn)
    expect(w.get('[data-part="crit"]').attributes('fill')).toBe(crit)
    expect(w.get('svg').attributes('role')).toBe('img')
    expect(w.get('svg').attributes('aria-label')).toBe(aria)
  })

  it('pins values beyond crit to the full width', () => {
    expect(bullet(9000).get('[data-part="fill"]').attributes('width')).toBe('100%')
  })

  it('draws no fill for zero', () => {
    expect(bullet(0).find('[data-part="fill"]').exists()).toBe(false)
  })

  it('labels the scale with the thresholds through the formatter', () => {
    const w = bullet(1, (v) => v.toFixed(1))
    expect(w.findAll('[aria-hidden="true"] span').map((s) => s.text())).toEqual(['0', '500.0', '2000.0'])
    expect(w.get('svg').attributes('aria-label')).toBe('Skew 1.0 ms, warning at 500.0, critical at 2000.0')
  })
})
