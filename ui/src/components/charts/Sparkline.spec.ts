import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Sparkline from './Sparkline.vue'

type Props = InstanceType<typeof Sparkline>['$props']

function spark(points: number[], props: Partial<Props> = {}) {
  return mount(Sparkline, { props: { points, label: 'Write rate 38 MB/s', ...props } })
}

describe('Sparkline', () => {
  it('draws the series scaled to its own range in a stretched viewBox', () => {
    const w = spark([0, 5, 10])
    expect(w.get('svg').attributes('role')).toBe('img')
    expect(w.get('svg').attributes('aria-label')).toBe('Write rate 38 MB/s')
    expect(w.get('svg svg').attributes('viewBox')).toBe('0 0 160 28')
    expect(w.get('svg svg').attributes('preserveAspectRatio')).toBe('none')
    expect(w.get('[data-part="line"]').attributes('points')).toBe('0,26 80,14 160,2')
    expect(w.get('[data-part="line"]').attributes('stroke')).toBe('var(--c-accent)')
  })

  it('honours a custom size and stroke', () => {
    const w = spark([1, 2], { width: 100, height: 22, stroke: 'good' })
    expect(w.get('svg').attributes('height')).toBe('22')
    expect(w.get('svg svg').attributes('viewBox')).toBe('0 0 100 22')
    expect(w.get('[data-part="line"]').attributes('stroke')).toBe('var(--c-good)')
  })

  it('marks the last point by percentage so it stays round', () => {
    const dot = spark([0, 10]).get('[data-part="last"]')
    expect(dot.attributes('cx')).toBe('100%')
    expect(dot.attributes('cy')).toBe('2')
    expect(dot.attributes('fill')).toBe('var(--c-accent)')
  })

  it('can leave the last point unmarked', () => {
    expect(spark([0, 10], { markLast: false }).find('[data-part="last"]').exists()).toBe(false)
  })

  it('draws a constant series at mid height', () => {
    expect(spark([4, 4, 4]).get('[data-part="line"]').attributes('points')).toBe('0,14 80,14 160,14')
  })

  it('draws a band behind the line and colours the dot when the last point is in it', () => {
    const w = spark([0, 10], { band: { from: 10, to: 5, level: 'degraded' }, stroke: 'warn' })
    const band = w.get('[data-part="band"]')
    expect(band.attributes('y')).toBe('2')
    expect(band.attributes('height')).toBe('12')
    expect(band.attributes('fill')).toBe('var(--c-crit)')
    expect(w.get('[data-part="last"]').attributes('fill')).toBe('var(--c-crit)')
  })

  it('clamps a band outside the series range and keeps the stroke colour outside it', () => {
    const w = spark([0, 10], { band: { from: 20, to: 40, level: 'attention' }, stroke: 'crit' })
    const band = w.get('[data-part="band"]')
    expect(band.attributes('y')).toBe('0')
    expect(band.attributes('height')).toBe('0')
    expect(band.attributes('fill')).toBe('var(--c-warn)')
    expect(w.get('[data-part="last"]').attributes('fill')).toBe('var(--c-crit)')
  })

  it('flags a value below the band as outside it', () => {
    const w = spark([10, 0], { band: { from: 5, to: 10, level: 'attention' } })
    expect(w.get('[data-part="last"]').attributes('fill')).toBe('var(--c-accent)')
  })

  it.each([[[]], [[7]]])('draws a flat dash and says so for %j', (points) => {
    const w = spark(points, { band: { from: 0, to: 1, level: 'attention' } })
    expect(w.find('[data-part="line"]').exists()).toBe(false)
    expect(w.find('[data-part="band"]').exists()).toBe(false)
    expect(w.find('[data-part="last"]').exists()).toBe(false)
    expect(w.get('[data-part="empty"]').attributes('y1')).toBe('14')
    expect(w.get('svg').attributes('aria-label')).toBe('Write rate 38 MB/s, no data yet')
  })
})
