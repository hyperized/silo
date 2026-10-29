import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Backup } from '../api/status'
import type { BackupRunPoint } from '../composables/useHistory'
import { backup, backupRun, GIB } from '../test/factories'
import BackupPanel from './BackupPanel.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const HOUR = 3600_000

function panel(b: Backup | null | undefined, runs: BackupRunPoint[] = []) {
  return mount(BackupPanel, { props: { backup: b, runs, now: NOW } })
}

function cell(w: ReturnType<typeof panel>, label: string): string | undefined {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.replace(/\s+/g, ' ').trim()
}

function point(over: Partial<BackupRunPoint> = {}): BackupRunPoint {
  return { at: NOW - HOUR, ok: true, bytesUploaded: GIB, ...over }
}

const finished = new Date(NOW - 2 * HOUR)
const good = backup({ lastRun: backupRun({ finishedAt: finished }), nextRunAt: new Date(NOW + 4 * HOUR) })

describe('BackupPanel', () => {
  it('shows skeletons while loading, without a badge', () => {
    const w = panel(undefined)
    expect(w.findAllComponents(Skeleton)).toHaveLength(2)
    expect(w.find('dl').exists()).toBe(false)
    expect(w.get('header').text()).toBe('Backups')
  })

  it.each([
    ['not reported', null],
    ['unconfigured', backup({ configured: false, target: '' })],
  ])('points at SILO_BACKUP_TARGET when backups are %s', (_name, b) => {
    const w = panel(b)
    expect(w.text()).toContain("Set SILO_BACKUP_TARGET to back up this node's chunks.")
    expect(w.get('header').text()).toContain('Off')
    expect(w.find('dl').exists()).toBe(false)
    expect(w.find('[title]').exists()).toBe(false)
  })

  describe('configured', () => {
    it('shows the badge word and a target with its credentials redacted', () => {
      const w = panel(backup({ target: 's3://AKIA:secret@s3.example.com/bucket/silo;token=abc' }))
      const meta = w.get('header').text()
      expect(meta).toContain('Never')
      expect(meta).toContain('s3://s3.example.com/bucket/silo')
      expect(meta).not.toContain('secret')
      expect(meta).not.toContain('AKIA')
      expect(meta).not.toContain('token')
    })

    it('says OK after a clean run', () => {
      expect(panel(good).get('header').text()).toContain('OK')
    })

    it('says Running during a run', () => {
      expect(panel(backup({ running: true })).get('header').text()).toContain('Running')
    })

    it('draws the scheduled dot only when a next run is set and nothing is running', () => {
      const dots = (b: Backup) => panel(b, [point()]).findAll('[data-part="scheduled"]').length
      expect(dots(backup({ nextRunAt: new Date(NOW + 4 * HOUR) }))).toBe(1)
      expect(dots(backup({ nextRunAt: null }))).toBe(0)
      expect(dots(backup({ nextRunAt: new Date(NOW + 4 * HOUR), running: true }))).toBe(0)
    })

    it('labels the dots with the runs seen', () => {
      const w = panel(good, [point(), point({ ok: false })])
      expect(w.get('[role="img"]').attributes('aria-label')).toContain('Backup runs')
      expect(w.get('[role="img"]').attributes('aria-label')).toContain('next in 4 h')
    })
  })

  describe('Last, Uploaded, Next', () => {
    it('shows the finish time, the bytes uploaded and the next run', () => {
      const w = panel(good)
      expect(cell(w, 'Last')).toBe('2 h ago')
      expect(cell(w, 'Uploaded')).toBe('3 GiB')
      expect(cell(w, 'Next')).toBe('in 4 h')
    })

    it('falls back to the start time while a run has not finished', () => {
      const b = backup({ lastRun: backupRun({ startedAt: new Date(NOW - 30 * 60_000) }) })
      expect(cell(panel(b), 'Last')).toBe('30 min ago')
    })

    it('says after this run while running', () => {
      const b = backup({ running: true, nextRunAt: new Date(NOW + HOUR) })
      expect(cell(panel(b), 'Next')).toBe('after this run')
    })

    it('says not scheduled without a next run', () => {
      expect(cell(panel(backup()), 'Next')).toBe('not scheduled')
    })

    it('says never and n/a before any run', () => {
      const w = panel(backup())
      expect(cell(w, 'Last')).toBe('never')
      expect(cell(w, 'Uploaded')).toBe('n/a')
    })
  })

  describe('schedule sentence', () => {
    it('states the interval', () => {
      expect(panel(backup()).text()).toContain('Every 6 h.')
    })

    it('says there is no schedule for an interval of zero', () => {
      expect(panel(backup({ intervalSeconds: 0 })).text()).toContain('No schedule.')
    })

    it('adds skipped chunks and the manifest after a run', () => {
      const b = backup({ lastRun: backupRun({ finishedAt: finished, chunksSkipped: 2500, manifest: 'm-42' }) })
      const text = panel(b).text()
      expect(text).toContain('Every 6 h.')
      expect(text).toContain('chunks skipped, already present.')
      expect(text).toContain('Manifest m-42.')
    })

    it('leaves the manifest out when the run produced none', () => {
      const b = backup({ lastRun: backupRun({ finishedAt: finished, manifest: '' }) })
      const text = panel(b).text()
      expect(text).toContain('chunks skipped')
      expect(text).not.toContain('Manifest')
    })

    it('has no skipped or manifest sentence before any run', () => {
      expect(panel(backup()).text()).not.toContain('skipped')
    })
  })

  describe('failed run', () => {
    it('prints the error text', () => {
      const b = backup({ lastRun: backupRun({ finishedAt: finished, ok: false, error: 'upload: 403 forbidden' }) })
      const w = panel(b)
      expect(w.get('header').text()).toContain('Failed')
      expect(w.text()).toContain('The last run failed:')
      expect(w.get('code').text()).toBe('upload: 403 forbidden')
    })

    it('says no error text when silod sent none', () => {
      const b = backup({ lastRun: backupRun({ finishedAt: finished, ok: false, error: '' }) })
      const w = panel(b)
      expect(w.text()).toContain('no error text')
      expect(w.find('code').exists()).toBe(false)
    })

    it('has no failure block after a good run', () => {
      expect(panel(good).text()).not.toContain('The last run failed')
    })
  })
})
