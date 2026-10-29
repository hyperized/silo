import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Backup } from '../../api/status'
import type { BackupRunPoint } from '../../composables/useHistory'
import { backup, backupRun } from '../../test/factories'
import Skeleton from '../Skeleton.vue'
import BackupTile from './BackupTile.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const MIN = 60 * 1000
const ago = (ms: number) => new Date(NOW - ms)
const ahead = (ms: number) => new Date(NOW + ms)

function tile(b: Backup | null | undefined, runs: BackupRunPoint[] = []) {
  return mount(BackupTile, { props: { backup: b, runs, now: NOW } })
}

const point = (i: number, ok = true): BackupRunPoint => ({ at: NOW - (100 - i) * MIN, ok, bytesUploaded: 0 })
const lines = (w: ReturnType<typeof tile>) => w.findAll('p').map((p) => p.text())

describe('BackupTile', () => {
  it('shows a skeleton while loading', () => {
    const w = tile(undefined)
    expect(w.findComponent(Skeleton).exists()).toBe(true)
    expect(w.find('p').exists()).toBe(false)
  })

  it('reads Off with no next line when silod reports no backup', () => {
    const w = tile(null)
    expect(w.text()).toContain('Off')
    expect(lines(w)).toEqual(['SILO_BACKUP_TARGET not set'])
  })

  it('reads Off for an unconfigured backup', () => {
    const w = tile(backup({ configured: false }))
    expect(w.text()).toContain('Off')
    expect(lines(w)).toEqual(['SILO_BACKUP_TARGET not set'])
  })

  it('adds the age to an OK run', () => {
    const w = tile(backup({ lastRun: backupRun({ finishedAt: ago(48 * MIN) }), nextRunAt: ahead(2 * 3600 * 1000) }))
    expect(w.text()).toContain('OK 48 min ago')
  })

  it('falls back to the start time for the age, and to no age without either', () => {
    const started = tile(backup({ lastRun: backupRun({ startedAt: ago(10 * MIN) }), nextRunAt: ahead(MIN * 60) }))
    expect(started.text()).toContain('OK 10 min ago')
    const bare = tile(backup({ lastRun: backupRun(), nextRunAt: ahead(MIN * 60) }))
    expect(bare.text()).toContain('OK')
    expect(bare.text()).not.toContain('ago')
  })

  it('adds the age to a failed run', () => {
    const w = tile(backup({ lastRun: backupRun({ ok: false, finishedAt: ago(5 * MIN) }) }))
    expect(w.text()).toContain('Failed 5 min ago')
  })

  it('adds the age to an overdue run', () => {
    const w = tile(
      backup({ lastRun: backupRun({ finishedAt: ago(3 * 3600 * 1000) }), nextRunAt: ago(60 * MIN) }),
    )
    expect(w.text()).toContain('Overdue 3 h ago')
  })

  it('reads Running without an age and promises the next run after it', () => {
    const w = tile(backup({ running: true, lastRun: backupRun({ finishedAt: ago(5 * MIN) }) }))
    expect(w.text()).toContain('Running')
    expect(w.text()).not.toContain('ago')
    expect(lines(w)).toContain('next after this run')
  })

  it('reads Never when configured with no run yet', () => {
    const w = tile(backup({ lastRun: null }))
    expect(w.text()).toContain('Never')
    expect(w.text()).not.toContain('chunks skipped')
  })

  it('shows when the next run is due, or that none is scheduled', () => {
    expect(lines(tile(backup({ nextRunAt: ahead(2 * 3600 * 1000) })))).toContain('next in 2 h')
    expect(lines(tile(backup({ nextRunAt: null })))).toContain('not scheduled')
  })

  it('shows the upload line with a compact skipped count', () => {
    const w = tile(
      backup({ lastRun: backupRun({ bytesUploaded: 3 * 1024 ** 3, chunksSkipped: 41000, finishedAt: ago(MIN) }) }),
    )
    expect(lines(w)).toContain('3 GiB up, 41K chunks skipped')
  })

  describe('run squares', () => {
    it('is absent without runs', () => {
      expect(tile(backup()).find('[role="img"]').exists()).toBe(false)
    })

    it('keeps only the last eight of more runs', () => {
      const runs = Array.from({ length: 12 }, (_, i) => point(i))
      const w = tile(backup(), runs)
      expect(w.findAll('[data-part="run"]')).toHaveLength(8)
      expect(w.get('[role="img"]').attributes('aria-label')).toBe('Last 8 runs, 0 failed')
    })

    it('marks a failed run with data-ok false and a cross', () => {
      const w = tile(backup(), [point(1), point(2, false)])
      const squares = w.findAll('[data-part="run"]')
      expect(squares.map((s) => s.attributes('data-ok'))).toEqual(['true', 'false'])
      expect(squares[0]?.find('svg').exists()).toBe(false)
      expect(squares[1]?.find('svg').exists()).toBe(true)
      expect(squares[1]?.attributes('title')).toMatch(/\d{2}:\d{2}, failed/)
      expect(squares[0]?.attributes('title')).toMatch(/\d{2}:\d{2}, succeeded/)
      expect(w.get('[role="img"]').attributes('aria-label')).toBe('Last 2 runs, 1 failed')
    })

    it('says run in the singular for one', () => {
      expect(tile(backup(), [point(1)]).get('[role="img"]').attributes('aria-label')).toBe('Last 1 run, 0 failed')
    })
  })
})
