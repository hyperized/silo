import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Backup } from '../api/status'
import { backup, backupRun } from '../test/factories'
import BackupPanel from './BackupPanel.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const HOUR = 3600_000

function panel(b: Backup | null | undefined, nodeId = 'node-a') {
  return mount(BackupPanel, { props: { backup: b, nodeId, now: NOW } })
}

function row(w: ReturnType<typeof panel>, label: string): string | undefined {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.replace(/\s+/g, ' ').trim()
}

const finished = backupRun({ startedAt: new Date(NOW - HOUR - 2 * 60_000), finishedAt: new Date(NOW - HOUR) })

describe('BackupPanel', () => {
  it('shows skeletons while loading', () => {
    const w = panel(undefined)
    expect(w.findAllComponents(Skeleton)).toHaveLength(5)
    expect(w.find('dl').exists()).toBe(false)
  })

  it.each([
    ['null', null],
    ['unconfigured', backup({ configured: false })],
  ])('shows Off and how to enable it for %s', (_name, b) => {
    const w = panel(b)
    expect(row(w, 'State')).toContain('Off')
    expect(row(w, 'Target')).toBe('SILO_BACKUP_TARGET not set')
    expect(row(w, 'Schedule')).toBeUndefined()
    expect(row(w, 'Last run')).toBeUndefined()
  })

  it('shows the node id in the header only when known', () => {
    expect(panel(null).get('header [title="Backups of the responding node"]').text()).toBe('node-a')
    expect(panel(null, '').find('header [title]').exists()).toBe(false)
  })

  describe('configured', () => {
    it('hides credentials in the target', () => {
      const w = panel(backup({ target: 's3://user:secret@bucket/silo?token=abc' }))
      expect(row(w, 'Target')).toBe('s3://bucket/silo')
    })

    it('shows the schedule', () => {
      expect(row(panel(backup()), 'Schedule')).toBe('every 6 h')
    })

    it('shows the last run age and how long it took', () => {
      const w = panel(backup({ lastRun: finished }))
      expect(row(w, 'Last run')).toBe('1 h ago, took 2 min')
    })

    it.each([
      ['start missing', backupRun({ startedAt: null, finishedAt: new Date(NOW - HOUR) })],
      ['finish missing', backupRun({ startedAt: new Date(NOW - HOUR), finishedAt: null })],
    ])('omits the duration when the %s', (_name, run) => {
      expect(row(panel(backup({ lastRun: run })), 'Last run')).toBe('1 h ago')
    })

    it('says never without a run and hides the chunks row', () => {
      const w = panel(backup())
      expect(row(w, 'Last run')).toBe('never')
      expect(row(w, 'Chunks')).toBeUndefined()
    })

    it('summarises the chunks of the last run', () => {
      expect(row(panel(backup({ lastRun: finished })), 'Chunks')).toBe('1,500 uploaded (3 GiB), 20 skipped')
    })

    describe('next run', () => {
      it('waits for the current run', () => {
        expect(row(panel(backup({ running: true })), 'Next run')).toBe('after this run')
      })

      it('counts down to the schedule', () => {
        expect(row(panel(backup({ nextRunAt: new Date(NOW + 5 * HOUR) })), 'Next run')).toBe('in 5 h')
      })

      it('says not scheduled otherwise', () => {
        expect(row(panel(backup()), 'Next run')).toBe('not scheduled')
      })
    })

    describe('error', () => {
      it('is absent for a successful run', () => {
        expect(row(panel(backup({ lastRun: finished })), 'Error')).toBeUndefined()
      })

      it('shows the error text verbatim in code', () => {
        const w = panel(backup({ lastRun: backupRun({ ...finished, ok: false, error: 'access denied' }) }))
        expect(w.get('code.whitespace-pre-wrap').text()).toBe('access denied')
      })

      it('admits when there is no error text', () => {
        const w = panel(backup({ lastRun: backupRun({ ...finished, ok: false, error: '' }) }))
        expect(row(w, 'Error')).toBe('no error text')
      })
    })
  })
})
