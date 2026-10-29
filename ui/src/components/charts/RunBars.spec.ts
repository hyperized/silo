import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import RunBars from './RunBars.vue'

type Run = { value: number; ok: boolean; label: string }

function bars(runs: Run[], max?: number) {
  return mount(RunBars, { props: { runs, label: 'Bytes freed', ...(max === undefined ? {} : { max }) } })
}

const run = (value: number, ok = true): Run => ({ value, ok, label: `freed ${String(value)}` })

describe('RunBars', () => {
  it('lays out one bar per run in percent of the width', () => {
    const r = bars([run(10), run(20), run(40), run(30)]).findAll('[data-part="bar"]')
    expect(r.map((b) => b.attributes('x'))).toEqual(['3.5%', '28.5%', '53.5%', '78.5%'])
    expect(r[0]?.attributes('width')).toBe('18%')
    expect(r[2]?.attributes('height')).toBe('56')
    expect(r[2]?.attributes('y')).toBe('8')
    expect(r[0]?.attributes('height')).toBe('14')
  })

  it('colours failed runs crit and only the latest ok run at full opacity', () => {
    const r = bars([run(10), run(20), run(30, false)]).findAll('[data-part="bar"]')
    expect(r.map((b) => b.attributes('fill'))).toEqual(['var(--c-accent)', 'var(--c-accent)', 'var(--c-crit)'])
    expect(r.map((b) => b.attributes('opacity'))).toEqual(['0.6', '1', '1'])
  })

  it('titles each bar and summarises in its accessible name', () => {
    const w = bars([run(10), run(20, false)])
    expect(w.findAll('title').map((t) => t.text())).toEqual(['freed 10', 'freed 20'])
    expect(w.get('svg').attributes('role')).toBe('img')
    expect(w.get('svg').attributes('aria-label')).toBe('Bytes freed: 2 runs, 1 failed')
    expect(bars([run(1)]).get('svg').attributes('aria-label')).toBe('Bytes freed: 1 run, 0 failed')
  })

  it('scales to an explicit max and clamps above it', () => {
    const r = bars([run(50), run(200)], 100).findAll('[data-part="bar"]')
    expect(r.map((b) => b.attributes('height'))).toEqual(['28', '56'])
  })

  it('keeps a stub for zero, negative and non-finite runs', () => {
    const r = bars([run(0), run(-5), run(Number.NaN)], 0).findAll('[data-part="bar"]')
    expect(r.map((b) => b.attributes('height'))).toEqual(['2', '2', '2'])
  })

  it('shows an empty baseline and says so with no runs', () => {
    const w = bars([])
    expect(w.find('[data-part="bar"]').exists()).toBe(false)
    expect(w.find('[data-part="baseline"]').exists()).toBe(true)
    expect(w.get('[data-part="empty"]').text()).toBe('no runs seen yet')
    expect(w.get('svg').attributes('aria-label')).toBe('Bytes freed: no runs seen yet')
  })
})
