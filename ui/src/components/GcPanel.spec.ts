import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Gc } from '../api/status'
import { gc } from '../test/factories'
import GcPanel from './GcPanel.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const HOUR = 3600_000

function panel(g: Gc | null | undefined, nodeId = 'node-a') {
  return mount(GcPanel, { props: { gc: g, nodeId, now: NOW } })
}

function row(w: ReturnType<typeof panel>, label: string): string | undefined {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.replace(/\s+/g, ' ').trim()
}

const ran = gc({ lastRunAt: new Date(NOW - 2 * HOUR), lastDurationMs: 2000 })

describe('GcPanel', () => {
  it('shows skeletons while loading', () => {
    const w = panel(undefined)
    expect(w.findAllComponents(Skeleton)).toHaveLength(3)
    expect(w.find('dl').exists()).toBe(false)
  })

  it('says not reported for an older silod', () => {
    const w = panel(null)
    expect(row(w, 'State')).toBe('not reported')
    expect(row(w, 'Last run')).toBeUndefined()
  })

  it('shows the node id in the header only when known', () => {
    expect(panel(null).get('header [title="GC of the responding node"]').text()).toBe('node-a')
    expect(panel(null, '').find('header [title]').exists()).toBe(false)
  })

  it('shows Off and still the last dry run when disabled', () => {
    const w = panel(gc({ enabled: false, lastRunAt: new Date(NOW - HOUR), lastOrphansDeleted: 0 }))
    expect(row(w, 'State')).toContain('Off')
    expect(row(w, 'Last run')).toContain('1 h ago')
    expect(row(w, 'Freed')).toContain('0 orphan chunks')
  })

  it('says never when no run finished, and hides Freed', () => {
    const w = panel(gc())
    expect(row(w, 'State')).toContain('Never')
    expect(row(w, 'Last run')).toBe('never')
    expect(row(w, 'Freed')).toBeUndefined()
  })

  describe('after a run', () => {
    it('is OK and shows when it ran', () => {
      const w = panel(ran)
      expect(row(w, 'State')).toContain('OK')
      expect(row(w, 'Last run')).toBe('2 h ago, took 2 s')
    })

    it.each([
      [850, 'took 850 ms'],
      [2000, 'took 2 s'],
    ])('formats a %i ms duration', (ms, text) => {
      expect(row(panel(gc({ lastRunAt: ran.lastRunAt, lastDurationMs: ms })), 'Last run')).toContain(text)
    })

    it.each([
      [1, '1 orphan chunk (1.2 GiB)'],
      [312, '312 orphan chunks (1.2 GiB)'],
      [0, '0 orphan chunks (1.2 GiB)'],
    ])('reports %i orphans freed', (n, text) => {
      expect(row(panel(gc({ lastRunAt: ran.lastRunAt, lastOrphansDeleted: n })), 'Freed')).toBe(text)
    })

    it('has no Error row without an error', () => {
      expect(row(panel(ran), 'Error')).toBeUndefined()
    })
  })

  it('is running while a pass is in progress', () => {
    expect(row(panel(gc({ running: true })), 'State')).toContain('Running')
  })

  it('shows a failure with the error text verbatim in code', () => {
    const w = panel(gc({ ...ran, lastError: 'disk read error' }))
    expect(row(w, 'State')).toContain('Failed')
    expect(w.get('code.whitespace-pre-wrap').text()).toBe('disk read error')
  })

  it('shows the last error even while running', () => {
    const w = panel(gc({ running: true, lastError: 'disk read error' }))
    expect(row(w, 'State')).toContain('Running')
    expect(row(w, 'Error')).toBe('disk read error')
  })
})
