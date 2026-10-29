import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterHealth } from '../api/status'
import { clusterHealth } from '../test/factories'
import HealthPanel from './HealthPanel.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const DAY = 24 * 3600_000

function panel(health: ClusterHealth | null | undefined, over: { skewMs?: number; nodeId?: string; version?: string } = {}) {
  return mount(HealthPanel, {
    props: { health, skewMs: over.skewMs ?? 0, version: over.version ?? '0.16.0', nodeId: over.nodeId ?? 'node-a', now: NOW },
  })
}

function row(w: ReturnType<typeof panel>, label: string): string | undefined {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.replace(/\s+/g, ' ').trim()
}

describe('HealthPanel', () => {
  it('shows skeletons while loading', () => {
    const w = panel(undefined)
    expect(w.findAllComponents(Skeleton)).toHaveLength(4)
    expect(w.find('dl').exists()).toBe(false)
  })

  it('shows the node id in the header only when known', () => {
    expect(panel(clusterHealth()).get('header [title="As seen by the responding node"]').text()).toBe('node-a')
    expect(panel(clusterHealth(), { nodeId: '' }).find('header [title]').exists()).toBe(false)
  })

  describe('older silod without a health report', () => {
    it('says not reported for replication and membership', () => {
      const w = panel(null)
      expect(row(w, 'Replication')).toBe('not reported')
      expect(row(w, 'Membership')).toBe('not reported')
    })

    it('still shows the version, without uptime, and the skew from the node table', () => {
      const w = panel(null, { skewMs: 2500 })
      expect(row(w, 'silod')).toBe('0.16.0')
      expect(row(w, 'Clock skew')).toContain('2,500 ms')
    })
  })

  describe('replication', () => {
    it('is not reported when the factor is 0', () => {
      expect(row(panel(clusterHealth({ replicationFactor: 0, aliveNodes: 2 })), 'Replication')).toBe('not reported')
    })

    it('is healthy with enough alive nodes', () => {
      expect(row(panel(clusterHealth()), 'Replication')).toContain('Healthy')
      expect(row(panel(clusterHealth()), 'Replication')).toContain('3 / 3 nodes alive')
    })

    it('is degraded with fewer alive nodes than the factor', () => {
      const text = row(panel(clusterHealth({ aliveNodes: 2 })), 'Replication')
      expect(text).toContain('Degraded')
      expect(text).toContain('2 / 3 nodes alive')
    })
  })

  describe('membership', () => {
    it('is connected and omits rejoin attempts when there are none', () => {
      const text = row(panel(clusterHealth()), 'Membership')
      expect(text).toBe('Connected')
    })

    it('is isolated when the node reaches no peer', () => {
      expect(row(panel(clusterHealth({ isolated: true })), 'Membership')).toContain('Isolated')
    })

    it.each([
      [1, '1 rejoin attempt'],
      [4, '4 rejoin attempts'],
      [1500, '1,500 rejoin attempts'],
    ])('counts %i rejoin attempts', (n, text) => {
      expect(row(panel(clusterHealth({ rejoinAttempts: n })), 'Membership')).toContain(text)
    })
  })

  describe('clock skew', () => {
    it('shows a healthy skew as plain text without a badge', () => {
      const w = panel(clusterHealth(), { skewMs: 120 })
      const dd = w.findAll('dt').find((d) => d.text() === 'Clock skew')?.element.nextElementSibling
      expect(dd?.textContent.trim()).toBe('120 ms')
      expect(dd?.querySelector('svg')).toBeNull()
    })

    it.each([
      ['warn', 500, 'text-warn'],
      ['crit', 2000, 'text-crit'],
    ])('shows a %s skew as a badge', (_name, ms, tone) => {
      const w = panel(clusterHealth(), { skewMs: ms })
      const dd = w.findAll('dt').find((d) => d.text() === 'Clock skew')?.element.nextElementSibling
      expect(dd?.querySelector('svg')).not.toBeNull()
      expect(dd?.querySelector(`.${tone}`)?.textContent).toContain(`${ms.toLocaleString('en-US')} ms`)
    })

    it.each([
      [0, ''],
      [1, '1 alert'],
      [3, '3 alerts'],
    ])('shows %i skew alerts', (n, text) => {
      const shown = row(panel(clusterHealth({ clockSkewAlerts: n })), 'Clock skew') ?? ''
      if (text) expect(shown).toContain(text)
      else expect(shown).not.toContain('alert')
    })
  })

  describe('silod', () => {
    it('shows the version alone without a start time', () => {
      expect(row(panel(clusterHealth()), 'silod')).toBe('0.16.0')
    })

    it('shows the version and uptime', () => {
      const w = panel(clusterHealth({ startedAt: new Date(NOW - 3 * DAY) }))
      expect(row(w, 'silod')).toBe('0.16.0, up 3 d')
    })
  })
})
