import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Gc } from '../api/status'
import type { GcRun } from '../composables/useHistory'
import { gc, GIB } from '../test/factories'
import GcPanel from './GcPanel.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'
import { GC_PRESENTATION } from '../lib/presentation'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const HOUR = 3600_000

function panel(g: Gc | null | undefined, runs: GcRun[] = []) {
  return mount(GcPanel, { props: { gc: g, runs, now: NOW } })
}

function cell(w: ReturnType<typeof panel>, label: string): string | undefined {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.replace(/\s+/g, ' ').trim()
}

function run(over: Partial<GcRun> = {}): GcRun {
  return { at: NOW - HOUR, outcome: 'ok', bytesFreed: GIB, orphans: 10, durationMs: 1000, ...over }
}

const ran = gc({ lastRunAt: new Date(NOW - 2 * HOUR), lastDurationMs: 2000 })

describe('GcPanel', () => {
  it('shows skeletons while loading, without a badge', () => {
    const w = panel(undefined)
    expect(w.findAllComponents(Skeleton)).toHaveLength(2)
    expect(w.find('dl').exists()).toBe(false)
    expect(w.text()).not.toMatch(/OK|Running|Off|Failed/)
  })

  it('says not reported for an older silod, without a badge', () => {
    const w = panel(null)
    expect(w.text()).toContain('not reported')
    expect(w.find('dl').exists()).toBe(false)
    expect(w.text()).not.toMatch(/OK|Running|Off|Failed|Never/)
  })

  describe('states', () => {
    it('says OK after a clean run', () => {
      const w = panel(ran)
      expect(w.get('header').text()).toContain('OK')
      expect(cell(w, 'Last run')).toBe('2 h ago')
      expect(cell(w, 'Orphans')).toBe('312')
      expect(cell(w, 'Freed')).toBe('1.2 GiB')
      expect(w.text()).toContain('Took 2 s.')
    })

    it('says Running while a sweep is in progress', () => {
      expect(panel(gc({ ...ran, running: true })).get('header').text()).toContain('Running')
    })

    it('says Failed in the warning tone when the last run errored', () => {
      const w = panel(gc({ ...ran, lastError: 'boom' }))
      expect(w.get('header').text()).toContain('Failed')
      expect(w.getComponent(StateBadge).props('presentation')).toEqual(GC_PRESENTATION.failed)
      expect(GC_PRESENTATION.failed.tone).toBe('warn')
    })

    it('says Skipped in the neutral tone when silod skipped the sweep', () => {
      const w = panel(gc({ ...ran, lastError: 'skipped: maps disagree' }))
      expect(w.get('header').text()).toContain('Skipped')
      expect(w.get('header').text()).not.toContain('Failed')
      expect(w.getComponent(StateBadge).props('presentation')).toEqual(GC_PRESENTATION.skipped)
      expect(GC_PRESENTATION.skipped.tone).toBe('neutral')
    })

    it('says Off, relabels Freed as Would free and explains it', () => {
      const w = panel(gc({ ...ran, enabled: false }))
      expect(w.get('header').text()).toContain('Off')
      expect(cell(w, 'Would free')).toBe('1.2 GiB')
      expect(w.findAll('dt').some((d) => d.text() === 'Freed')).toBe(false)
      expect(w.text()).toContain('GC is off: sweeps count orphans and delete nothing.')
    })

    it('says Off with n/a cells and no Took line when it never ran', () => {
      const w = panel(gc({ enabled: false }))
      expect(w.text()).toContain('GC is off')
      expect(w.text()).not.toContain('Took')
      expect(cell(w, 'Would free')).toBe('n/a')
    })
  })

  it('shows never and n/a for a gc that has not run yet', () => {
    const w = panel(gc())
    expect(w.get('header').text()).toContain('Never')
    expect(cell(w, 'Last run')).toBe('never')
    expect(cell(w, 'Orphans')).toBe('n/a')
    expect(cell(w, 'Freed')).toBe('n/a')
    expect(w.text()).not.toContain('Took')
    expect(w.findAll('p')).toHaveLength(0)
  })

  it('feeds the run bars from the runs seen', () => {
    const w = panel(ran, [run(), run({ outcome: 'failed' }), run()])
    expect(w.get('[role="img"]').attributes('aria-label')).toBe('Bytes freed per GC run: 3 runs, 1 failed')
  })

  describe('earlier failures', () => {
    it('names failures that came before the latest run', () => {
      const w = panel(ran, [run({ outcome: 'failed' }), run(), run()])
      expect(w.text()).toMatch(/1 of 3 runs failed, at \d{2}:\d{2}\./)
    })

    it('lists every earlier failure time', () => {
      const w = panel(ran, [run({ outcome: 'failed' }), run({ outcome: 'failed' }), run()])
      expect(w.text()).toMatch(/2 of 3 runs failed, at \d{2}:\d{2}, \d{2}:\d{2}\./)
    })

    it('leaves the latest failure to the error block', () => {
      const w = panel(gc({ ...ran, lastError: 'boom' }), [run(), run({ outcome: 'failed' })])
      expect(w.text()).not.toContain('runs failed')
    })

    it('shows the sentence even before silod reports a run', () => {
      const w = panel(gc(), [run({ outcome: 'failed' }), run()])
      expect(w.text()).toContain('1 of 2 runs failed')
    })
  })

  it('prints the latest run error in full', () => {
    const w = panel(gc({ ...ran, lastError: 'delete chunk: permission denied' }))
    expect(w.text()).toContain('The latest run failed:')
    expect(w.get('code').text()).toBe('delete chunk: permission denied')
  })

  it('shows a skipped sweep as Skipped with the reason and no failure text', () => {
    const w = panel(gc({ ...ran, lastError: 'skipped: this node\'s extent maps disagree with their replicas; wait for the extent scrubber to reconcile them' }))
    expect(w.text()).toContain('Skipped')
    expect(w.text()).not.toContain('failed')
    expect(w.get('[data-testid="gc-skipped"]').text()).toContain('clears by itself')
    expect(w.get('code').text()).toBe("this node's extent maps disagree with their replicas; wait for the extent scrubber to reconcile them")
  })

  it('labels skipped runs in the bars and leaves them out of the earlier failures', () => {
    const w = panel(ran, [run({ outcome: 'skipped' }), run({ outcome: 'failed' }), run()])
    expect(w.get('[role="img"]').attributes('aria-label')).toBe('Bytes freed per GC run: 3 runs, 1 failed, 1 skipped')
    expect(w.findAll('title')[0]?.text()).toMatch(/, skipped$/)
    expect(w.findAll('title')[1]?.text()).toMatch(/, failed$/)
    expect(w.text()).toMatch(/1 of 3 runs failed, at \d{2}:\d{2}\./)
  })

  it('has no error block on a clean run', () => {
    expect(panel(ran).find('code').exists()).toBe(false)
  })
})
