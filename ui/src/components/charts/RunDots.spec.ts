import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import RunDots from './RunDots.vue'

type Run = { ok: boolean; label: string }

function dots(runs: Run[], scheduled?: string) {
  return mount(RunDots, {
    props: { runs, label: 'Backups', ...(scheduled === undefined ? {} : { scheduled }) },
  })
}

const ok: Run = { ok: true, label: '08:00 ok' }
const bad: Run = { ok: false, label: '14:00 failed' }

describe('RunDots', () => {
  it('spreads runs across the line, oldest left', () => {
    const r = dots([ok, ok, ok]).findAll('[data-part="run"] circle')
    expect(r.map((c) => c.attributes('cx'))).toEqual(['4%', '50%', '96%'])
    expect(r[0]?.attributes('fill')).toBe('var(--c-good)')
  })

  it('marks failures in crit with a cross', () => {
    const w = dots([ok, bad])
    expect(w.get('[data-ok="false"] circle').attributes('fill')).toBe('var(--c-crit)')
    expect(w.findAll('[data-part="cross"]')).toHaveLength(1)
    expect(w.get('[data-part="cross"]').attributes('x')).toBe('96%')
  })

  it('adds a dashed hollow dot for the next scheduled run', () => {
    const w = dots([ok, ok], 'in 4 h')
    const next = w.get('[data-part="scheduled"] circle')
    expect(next.attributes('cx')).toBe('96%')
    expect(next.attributes('fill')).toBe('none')
    expect(next.attributes('stroke-dasharray')).toBe('2 2')
    expect(w.get('[data-part="scheduled"] title').text()).toBe('in 4 h')
  })

  it('centres a lone dot', () => {
    expect(dots([], 'in 4 h').get('[data-part="scheduled"] circle').attributes('cx')).toBe('50%')
  })

  it.each([
    [[ok, bad], undefined, 'Backups: 2 runs, 1 failed, last failed'],
    [[ok], 'in 4 h', 'Backups: 1 run, 0 failed, last succeeded, next in 4 h'],
    [[], 'in 4 h', 'Backups: no runs seen yet, next in 4 h'],
  ] as [Run[], string | undefined, string][])('names %j as an image', (runs, scheduled, want) => {
    const svg = dots(runs, scheduled).get('svg')
    expect(svg.attributes('role')).toBe('img')
    expect(svg.attributes('aria-label')).toBe(want)
  })

  it('titles each run', () => {
    expect(dots([ok, bad]).findAll('[data-part="run"] title').map((t) => t.text())).toEqual(['08:00 ok', '14:00 failed'])
  })

  it('says so with nothing to show', () => {
    const w = dots([])
    expect(w.get('[data-part="empty"]').text()).toBe('no runs seen yet')
    expect(w.find('[data-part="scheduled"]').exists()).toBe(false)
  })
})
