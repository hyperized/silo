import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Backup } from '../api/status'
import type { Health } from '../lib/health'
import { GIB, backup, backupRun, health, volume } from '../test/factories'
import Skeleton from './Skeleton.vue'
import StatsStrip from './StatsStrip.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const HOUR = 3600_000

interface Over {
  health?: Health | null
  volumes?: ReturnType<typeof volume>[]
  backup?: Backup | null
  version?: string
}

function strip(over: Over = {}) {
  return mount(StatsStrip, {
    props: { health: health(), volumes: [], backup: null, version: '0.16.0', now: NOW, ...over },
  })
}

function cell(w: ReturnType<typeof strip>, label: string): { text: string; el: Element } {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  if (!dt) throw new Error(`no cell ${label}`)
  const el = dt.element.parentElement as Element
  return { el, text: el.textContent.replace(/\s+/g, ' ').trim() }
}

describe('StatsStrip', () => {
  it('shows placeholders while the first answer is pending', () => {
    const w = strip({ health: null })
    expect(w.find('dl').exists()).toBe(false)
    expect(w.findAllComponents(Skeleton).length).toBeGreaterThan(0)
  })

  describe('nodes', () => {
    it('shows up over total and "up" when all are up', () => {
      const c = cell(strip({ health: health({ nodes: { total: 3, up: 3, suspect: 0, down: 0 } }) }), 'Nodes')
      expect(c.text).toBe('Nodes3 / 3up')
    })

    it('lists down, suspect and unknown in that order', () => {
      const c = cell(strip({ health: health({ nodes: { total: 5, up: 2, suspect: 1, down: 1 } }) }), 'Nodes')
      expect(c.text).toBe('Nodes2 / 51 down, 1 suspect, 1 unknown')
    })

    it.each([
      [{ total: 3, up: 2, suspect: 0, down: 1 }, '1 down'],
      [{ total: 3, up: 2, suspect: 1, down: 0 }, '1 suspect'],
      [{ total: 3, up: 2, suspect: 0, down: 0 }, '1 unknown'],
    ])('shows only the relevant note for %j', (nodes, note) => {
      expect(cell(strip({ health: health({ nodes }) }), 'Nodes').text).toContain(note)
    })
  })

  describe('capacity', () => {
    it('shows percent, a meter and used of total', () => {
      const w = strip({ health: health({ capacity: { usedBytes: 142 * GIB, capacityBytes: 300 * GIB } }) })
      const c = cell(w, 'Capacity')
      expect(c.text).toContain('47%')
      expect(c.text).toContain('142 / 300 GiB')
      expect(w.get('[role="meter"]').attributes('aria-label')).toBe('Cluster capacity')
    })

    it('says n/a and not reported without capacity', () => {
      const w = strip({ health: health({ capacity: null }) })
      const c = cell(w, 'Capacity')
      expect(c.text).toContain('n/a')
      expect(c.text).toContain('not reported')
      expect(w.find('[role="meter"]').exists()).toBe(false)
    })
  })

  describe('volumes', () => {
    it('counts them and sums the provisioned size', () => {
      const w = strip({ volumes: [volume({ path: '/a', sizeBytes: 10 * GIB }), volume({ path: '/b', sizeBytes: 50 * GIB })] })
      const c = cell(w, 'Volumes')
      expect(c.text).toContain('2')
      expect(c.text).toContain('60 GiB provisioned')
    })

    it('says none when there are no volumes', () => {
      expect(cell(strip(), 'Volumes').text).toBe('Volumes0none')
    })
  })

  describe('backup', () => {
    const ran = backupRun({ finishedAt: new Date(NOW - 3 * HOUR) })

    it('shows the state word and icon with the last run age', () => {
      const w = strip({ health: health({ backup: 'ok' }), backup: backup({ lastRun: ran }) })
      const c = cell(w, 'Backup')
      expect(c.text).toBe('BackupOK3 h ago')
      expect(c.el.querySelector('svg')).not.toBeNull()
    })

    it('prefixes the age while a run is in progress', () => {
      const w = strip({ health: health({ backup: 'running' }), backup: backup({ running: true, lastRun: ran }) })
      expect(cell(w, 'Backup').text).toBe('BackupRunninglast run 3 h ago')
    })

    it('falls back to the start time when the run has not finished', () => {
      const run = backupRun({ startedAt: new Date(NOW - 2 * HOUR), finishedAt: null })
      const w = strip({ health: health({ backup: 'ok' }), backup: backup({ lastRun: run }) })
      expect(cell(w, 'Backup').text).toContain('2 h ago')
    })

    it('says no target when backups are off', () => {
      const w = strip({ health: health({ backup: 'unconfigured' }), backup: backup({ configured: false }) })
      expect(cell(w, 'Backup').text).toContain('no target')
    })

    it('says no target when silod reported no backup section', () => {
      expect(cell(strip({ health: health({ backup: 'unconfigured' }), backup: null }), 'Backup').text).toContain('no target')
    })

    it('announces the first scheduled run', () => {
      const w = strip({
        health: health({ backup: 'never' }),
        backup: backup({ nextRunAt: new Date(NOW + 5 * HOUR) }),
      })
      expect(cell(w, 'Backup').text).toContain('first in 5 h')
    })

    it('says no runs when configured with nothing scheduled or done', () => {
      const w = strip({ health: health({ backup: 'never' }), backup: backup() })
      expect(cell(w, 'Backup').text).toContain('no runs')
    })

    it('shows n/a when the health has no backup state', () => {
      const c = cell(strip({ health: health({ backup: null }), backup: backup() }), 'Backup')
      expect(c.text).toContain('n/a')
      expect(c.el.querySelector('svg')).toBeNull()
    })
  })

  it('shows the version', () => {
    expect(cell(strip({ version: '1.2.3' }), 'Version').text).toBe('Version1.2.3')
  })
})
