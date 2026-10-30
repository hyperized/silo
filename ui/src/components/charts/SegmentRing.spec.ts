import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { NodeState } from '../../api/status'
import SegmentRing from './SegmentRing.vue'

function ring(states: NodeState[], caption?: string) {
  const segments = states.map((state, i) => ({ state, label: `node-${String(i)}` }))
  return mount(SegmentRing, {
    props: { segments, value: '2/3', label: 'Nodes', ...(caption === undefined ? {} : { caption }) },
  })
}

describe('SegmentRing', () => {
  it('splits the ring into one segment per member, starting at the top', () => {
    const segs = ring(['alive', 'alive', 'alive']).findAll('[data-part="segment"]')
    expect(segs).toHaveLength(3)
    expect(segs.map((s) => s.attributes('stroke-dashoffset'))).toEqual(['0', '-87.96', '-175.93'])
    expect(segs[0]?.attributes('stroke-dasharray')).toBe('81.96 181.93')
    expect(segs[0]?.attributes('transform')).toBe('rotate(-90 60 57)')
  })

  it('fits the whole ring inside the same 120 x 114 box as the capacity gauge', () => {
    const w = ring(['alive'])
    expect(w.get('svg').attributes('viewBox')).toBe('0 0 120 114')
    const c = w.get('[data-part="segment"]')
    const [cy, r] = [Number(c.attributes('cy')), Number(c.attributes('r'))]
    const half = Number(c.attributes('stroke-width')) / 2
    expect(cy - r - half).toBeGreaterThan(0)
    expect(cy + r + half).toBeLessThan(114)
    expect(Number(c.attributes('cx')) + r + half).toBeLessThan(120)
  })

  it('styles each state by colour and line', () => {
    const w = ring(['alive', 'suspect', 'dead', 'left', 'unknown'])
    const stroke = (state: string) => w.get(`[data-state="${state}"] [data-part="segment"]`).attributes('stroke')
    expect(stroke('alive')).toBe('var(--c-good)')
    expect(stroke('suspect')).toBe('var(--c-warn-bar)')
    expect(stroke('dead')).toBe('var(--c-crit)')
    expect(stroke('left')).toBe('var(--c-track)')
    expect(stroke('unknown')).toBe('var(--c-track)')
    expect(w.get('[data-state="suspect"] [data-part="segment"]').attributes('stroke-dasharray')).toMatch(/^6 4 6 4 /)
    expect(w.findAll('[data-part="outline"]')).toHaveLength(2)
    expect(w.get('[data-state="left"] [data-part="outline"]').attributes('stroke')).toBe('var(--c-ink-3)')
  })

  it('names each segment for pointer users', () => {
    expect(ring(['alive', 'dead']).findAll('title').map((t) => t.text())).toEqual(['node-0: alive', 'node-1: dead'])
  })

  it('counts states in its accessible name', () => {
    expect(ring(['alive', 'suspect', 'alive'], '1 suspect').get('svg').attributes('aria-label')).toBe(
      'Nodes 2/3, 1 suspect, 2 alive, 1 suspect',
    )
  })

  it.each([
    [['alive'], 'var(--c-ink-3)'],
    [['alive', 'unknown'], 'var(--c-warn)'],
    [['suspect', 'dead'], 'var(--c-crit)'],
  ] as [NodeState[], string][])('colours the caption for %j as %s', (states, fill) => {
    expect(ring(states, 'c').findAll('text')[1]?.attributes('fill')).toBe(fill)
  })

  it('draws one member as the whole ring', () => {
    expect(ring(['alive']).get('[data-part="segment"]').attributes('stroke-dasharray')).toBe('263.89 0')
  })

  it('draws only the track with no members', () => {
    const w = ring([])
    expect(w.find('[data-part="segment"]').exists()).toBe(false)
    expect(w.get('[data-part="track"]').attributes('stroke')).toBe('var(--c-track)')
    expect(w.get('svg').attributes('aria-label')).toBe('Nodes 2/3, none')
    expect(w.findAll('text')).toHaveLength(1)
  })
})
