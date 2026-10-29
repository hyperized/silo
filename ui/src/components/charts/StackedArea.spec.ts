import { describe, expect, it, vi } from 'vitest'
import { nextTick } from 'vue'
import { mount, type VueWrapper } from '@vue/test-utils'
import StackedArea from './StackedArea.vue'

type Props = InstanceType<typeof StackedArea>['$props']

const T0 = Date.UTC(2026, 9, 29, 8, 0)
const MIN = 60_000

function chart(props: Partial<Props> = {}) {
  return mount(StackedArea, {
    props: {
      series: [
        { name: 'node-a', values: [10, 20, 30] },
        { name: 'node-b', values: [10, 10, 10] },
      ],
      times: [T0, T0 + MIN, T0 + 2 * MIN],
      label: 'Used capacity per node',
      formatY: (v: number) => `${v.toFixed(0)} GB`,
      formatX: (t: number) => `t+${String((t - T0) / MIN)}`,
      ...props,
    },
  })
}

function stubRect(el: Element, left = 100, width = 860) {
  vi.spyOn(el, 'getBoundingClientRect').mockReturnValue({
    left,
    width,
    top: 0,
    height: 220,
    right: left + width,
    bottom: 220,
    x: left,
    y: 0,
    toJSON: () => ({}),
  })
}

// jsdom has no PointerEvent and test-utils cannot set clientX on the event it
// builds, so a MouseEvent of the pointer type stands in; the handler only reads clientX.
async function point(w: VueWrapper, clientX: number) {
  w.get('svg').element.dispatchEvent(new MouseEvent('pointermove', { clientX, bubbles: true }))
  await nextTick()
}

describe('StackedArea', () => {
  it('is a focusable image with a name', () => {
    const svg = chart().get('svg')
    expect(svg.attributes('role')).toBe('img')
    expect(svg.attributes('aria-label')).toBe('Used capacity per node')
    expect(svg.attributes('tabindex')).toBe('0')
    expect(svg.attributes('viewBox')).toBe('0 0 860 220')
  })

  it('stacks the series with fading opacity and page-coloured separators', () => {
    const w = chart()
    const areas = w.findAll('[data-part="area"]')
    expect(areas.map((a) => a.attributes('opacity'))).toEqual(['0.8', '0.55'])
    expect(areas[0]?.attributes('fill')).toBe('var(--c-accent)')
    // yMax = 40 * 1.1 = 44 over a 160 px plot.
    expect(areas[0]?.attributes('d')).toBe('M0,123.64 L402,87.27 L804,50.91 L804,160 L402,160 L0,160 Z')
    const sep = w.findAll('[data-part="separator"]')
    expect(sep).toHaveLength(1)
    expect(sep[0]?.attributes('stroke')).toBe('var(--c-page)')
  })

  it('draws a dashed grid with y labels from the top value down to zero', () => {
    const w = chart({ yMax: 100 })
    const grid = w.findAll('[data-part="grid"]')
    expect(grid.map((g) => g.attributes('y1'))).toEqual(['20', '60', '100', '140', '180'])
    expect(grid[0]?.attributes('stroke-dasharray')).toBe('2 4')
    expect(grid[4]?.attributes('stroke-dasharray')).toBeUndefined()
    expect(w.findAll('[data-part="ylabel"]').map((t) => t.text())).toEqual(['100 GB', '75 GB', '50 GB', '25 GB', '0 GB'])
  })

  it('labels the first, middle and last sample on x', () => {
    const labels = chart().findAll('[data-part="xlabel"]')
    expect(labels.map((l) => l.text())).toEqual(['t+0', 't+1', 't+2'])
    expect(labels.map((l) => l.attributes('text-anchor'))).toEqual(['start', 'middle', 'end'])
    expect(labels.map((l) => l.attributes('x'))).toEqual(['56', '458', '860'])
  })

  it.each([
    [[T0], ['t+0']],
    [[T0, T0 + MIN], ['t+0', 't+1']],
  ])('labels short histories without duplicates (%j)', (times, want) => {
    const w = chart({ times, series: [{ name: 'a', values: [1, 2] }] })
    expect(w.findAll('[data-part="xlabel"]').map((l) => l.text())).toEqual(want)
  })

  it('draws the limit line when it is inside the scale', () => {
    const w = chart({ yMax: 100, limit: { value: 75, label: '80 % of 2.4 TB' } })
    expect(w.get('[data-part="limit"]').attributes('y1')).toBe('60')
    expect(w.get('[data-part="limit"]').attributes('stroke')).toBe('var(--c-warn)')
    expect(w.text()).toContain('80 % of 2.4 TB')
  })

  it.each([-1, 101])('hides a limit at %i outside the scale', (value) => {
    expect(chart({ yMax: 100, limit: { value, label: 'x' } }).find('[data-part="limit"]').exists()).toBe(false)
  })

  it('pads missing values with zero and falls back to a unit scale when everything is zero', () => {
    const w = chart({ series: [{ name: 'a', values: [] }] })
    expect(w.get('[data-part="area"]').attributes('d')).toBe('M0,160 L402,160 L804,160 L804,160 L402,160 L0,160 Z')
    expect(w.findAll('[data-part="ylabel"]')[0]?.text()).toBe('1 GB')
  })

  it('ignores a non-positive yMax', () => {
    expect(chart({ yMax: 0 }).findAll('[data-part="ylabel"]')[0]?.text()).toBe('44 GB')
  })

  it('ignores non-finite totals when picking the scale', () => {
    const w = chart({ series: [{ name: 'a', values: [Number.NaN, 10, 10] }] })
    expect(w.findAll('[data-part="ylabel"]')[0]?.text()).toBe('11 GB')
  })

  it('shows the grid and a waiting message with no samples', () => {
    const w = chart({ times: [], limit: { value: 0, label: 'x' } })
    expect(w.findAll('[data-part="grid"]')).toHaveLength(5)
    expect(w.get('[data-part="empty"]').text()).toBe('collecting samples')
    expect(w.find('[data-part="area"]').exists()).toBe(false)
    expect(w.find('[data-part="limit"]').exists()).toBe(false)
    expect(w.get('svg').attributes('aria-label')).toBe('Used capacity per node, collecting samples')
  })

  describe('pointer', () => {
    it('moves the cursor to the nearest sample and announces it', async () => {
      const w = chart()
      stubRect(w.get('svg').element)
      // clientX 100 + 56 (left gutter) + 402 lands on the middle sample.
      await point(w, 558)
      expect(w.emitted('hover')).toEqual([[1]])
      const readout = w.get('[data-part="readout"]')
      expect(readout.get('line').attributes('x1')).toBe('458')
      // Cursor right of centre puts the box to its left.
      expect(readout.get('rect').attributes('x')).toBe('298')
      expect(readout.text()).toContain('t+1')
      expect(w.get('[aria-live="polite"]').text()).toBe('t+1: node-a 20 GB, node-b 10 GB')
    })

    it('puts the box to the right of a cursor in the left half and scales client pixels', async () => {
      const w = chart()
      stubRect(w.get('svg').element, 0, 430)
      await point(w, 20)
      expect(w.emitted('hover')).toEqual([[0]])
      expect(w.get('[data-part="readout"] rect').attributes('x')).toBe('66')
    })

    it('does not re-emit when the sample is unchanged and clears on leave', async () => {
      const w = chart()
      stubRect(w.get('svg').element)
      await point(w, 900)
      await point(w, 950)
      await w.get('svg').trigger('pointerleave')
      expect(w.emitted('hover')).toEqual([[2], [null]])
      expect(w.find('[data-part="readout"]').exists()).toBe(false)
      expect(w.get('[aria-live="polite"]').text()).toBe('')
    })

    it('ignores a pointer over a box that has no layout yet', async () => {
      const w = chart()
      stubRect(w.get('svg').element, 0, 0)
      await point(w, 10)
      expect(w.emitted('hover')).toBeUndefined()
    })

    it('stays quiet over an empty chart', async () => {
      const w = chart({ times: [] })
      stubRect(w.get('svg').element)
      await point(w, 300)
      expect(w.emitted('hover')).toBeUndefined()
    })
  })

  describe('keyboard', () => {
    it('starts at the latest sample, steps with the arrows and stops at the ends', async () => {
      const w = chart()
      const svg = w.get('svg')
      await svg.trigger('keydown', { key: 'ArrowLeft' })
      await svg.trigger('keydown', { key: 'ArrowLeft' })
      await svg.trigger('keydown', { key: 'ArrowLeft' })
      await svg.trigger('keydown', { key: 'ArrowLeft' })
      await svg.trigger('keydown', { key: 'ArrowRight' })
      expect(w.emitted('hover')).toEqual([[2], [1], [0], [1]])
    })

    it('starts at the latest sample on ArrowRight too and clears on Escape', async () => {
      const w = chart()
      const svg = w.get('svg')
      await svg.trigger('keydown', { key: 'ArrowRight' })
      await svg.trigger('keydown', { key: 'ArrowRight' })
      await svg.trigger('keydown', { key: 'Escape' })
      expect(w.emitted('hover')).toEqual([[2], [null]])
    })

    it('prevents scrolling only for the keys it handles', () => {
      const w = chart()
      const arrow = new KeyboardEvent('keydown', { key: 'ArrowRight', cancelable: true })
      const tab = new KeyboardEvent('keydown', { key: 'Tab', cancelable: true })
      w.get('svg').element.dispatchEvent(arrow)
      w.get('svg').element.dispatchEvent(tab)
      expect(arrow.defaultPrevented).toBe(true)
      expect(tab.defaultPrevented).toBe(false)
    })

    it('ignores arrows with no samples', async () => {
      const w = chart({ times: [] })
      await w.get('svg').trigger('keydown', { key: 'ArrowLeft' })
      expect(w.emitted('hover')).toBeUndefined()
    })

    it('drops a cursor that points past the end after the history shrinks', async () => {
      const w = chart()
      await w.get('svg').trigger('keydown', { key: 'ArrowLeft' })
      await w.setProps({ times: [T0], series: [{ name: 'a', values: [1] }] })
      expect(w.find('[data-part="readout"]').exists()).toBe(false)
      await w.get('svg').trigger('keydown', { key: 'ArrowLeft' })
      expect(w.emitted('hover')).toEqual([[2], [0]])
    })
  })
})
