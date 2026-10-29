import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode, Volume } from '../api/status'
import { node, volume } from '../test/factories'
import Skeleton from './Skeleton.vue'
import VolumesPanel from './VolumesPanel.vue'

const NODES: ClusterNode[] = [node({ id: 'node-a' }), node({ id: 'node-b' }), node({ id: 'node-c' })]

type Props = {
  volumes: Volume[] | null
  nodes?: ClusterNode[]
  replicationFactor?: number
  sparks?: Record<string, number[]>
}

function panel(p: Props) {
  return mount(VolumesPanel, {
    props: { nodes: NODES, replicationFactor: 0, sparks: {}, ...p },
  })
}

const paths = (w: ReturnType<typeof panel>) => w.findAll('tbody th[scope="row"] > span:first-child').map((e) => e.text())

function many(n: number): Volume[] {
  return Array.from({ length: n }, (_, i) => volume({ path: `/v${String(i).padStart(2, '0')}` }))
}

describe('VolumesPanel', () => {
  it('shows skeleton rows while loading', () => {
    const w = panel({ volumes: null })
    expect(w.findAllComponents(Skeleton)).toHaveLength(15)
    expect(w.find('input').exists()).toBe(false)
    expect(w.find('footer').exists()).toBe(false)
  })

  it('explains how to create a volume when there are none', () => {
    const w = panel({ volumes: [] })
    expect(w.text()).toContain('No volumes')
    expect(w.text()).toContain('siloctl volume create --size 10G /name')
    expect(w.find('input').exists()).toBe(false)
    expect(w.find('table').exists()).toBe(false)
  })

  describe('replicas', () => {
    const nodes = [node({ id: 'node-a' }), node({ id: 'node-b', state: 'suspect' }), node({ id: 'node-c', state: 'dead' })]

    it('names the state of each chip in its title and for screen readers', () => {
      const v = volume({ replicas: ['node-a', 'node-b', 'node-c', 'node-z'] })
      const chips = panel({ volumes: [v], nodes }).findAll('[title]').filter((e) => e.text().startsWith('node-'))
      expect(chips.map((c) => c.attributes('title'))).toEqual([
        'node-a: Alive',
        'node-b: Suspect',
        'node-c: Down',
        'node-z: Unknown',
      ])
      expect(chips.map((c) => c.get('.sr-only').text())).toEqual([', Alive', ', Suspect', ', Down', ', Unknown'])
    })

    it('shows none for a volume without a replica set when another has one', () => {
      const w = panel({ volumes: [volume({ path: '/a', replicas: ['node-a'] }), volume({ path: '/b' })] })
      expect(w.findAll('th').some((h) => h.text().startsWith('Replicas'))).toBe(true)
      expect(w.find('[data-label="Replicas"] .text-ink-3').text()).toBe('none')
    })

    it('hides the column when no volume reports replicas', () => {
      const w = panel({ volumes: [volume()] })
      expect(w.findAll('th').some((h) => h.text().startsWith('Replicas'))).toBe(false)
      expect(w.find('[data-label="Replicas"]').exists()).toBe(false)
    })
  })

  describe('sorting', () => {
    it('puts map damaged first, then degraded, then busier volumes, then by path', () => {
      const volumes = [
        volume({ path: '/b-quiet', replicas: ['node-a', 'node-b', 'node-c'] }),
        volume({ path: '/a-quiet', replicas: ['node-a', 'node-b', 'node-c'] }),
        volume({ path: '/busy', replicas: ['node-a', 'node-b', 'node-c'] }),
        volume({ path: '/degraded', replicas: ['node-a', 'node-b'] }),
        volume({ path: '/damaged', replicas: ['node-a', 'node-b', 'node-c'], mapDamaged: true }),
      ]
      const w = panel({ volumes, replicationFactor: 3, sparks: { '/busy': [1, 9], '/b-quiet': [0, 0] } })
      expect(paths(w)).toEqual(['/damaged', '/degraded', '/busy', '/a-quiet', '/b-quiet'])
    })
  })

  describe('degraded', () => {
    it('names each replica on a node that is not alive', () => {
      const nodes = [node({ id: 'node-a' }), node({ id: 'node-b', state: 'suspect' }), node({ id: 'node-c', state: 'dead' })]
      const v = volume({ replicas: ['node-a', 'node-b', 'node-c', 'node-z'] })
      const w = panel({ volumes: [v], nodes, replicationFactor: 3 })
      expect(w.text()).toContain('degraded, replica on node-b suspect, replica on node-c down, replica on node-z unknown')
    })

    it('counts replicas when the set is short but every member is alive', () => {
      const w = panel({ volumes: [volume({ replicas: ['node-a', 'node-b'] })], replicationFactor: 3 })
      expect(w.text()).toContain('degraded, 2 of 3 replicas')
    })

    it('flags nothing when the replication factor is unknown', () => {
      const w = panel({ volumes: [volume({ replicas: ['node-a'] })], replicationFactor: 0 })
      expect(w.text()).not.toContain('degraded')
    })
  })

  describe('markers and state', () => {
    it('shows all three markers and the worst state word', () => {
      const v = volume({ replicas: ['node-a'], mapDamaged: true, attachedHere: true })
      const w = panel({ volumes: [v], replicationFactor: 3 })
      const marks = w.get('tbody th').text()
      expect(marks).toContain('map damaged')
      expect(marks).toContain('degraded, 1 of 3 replicas')
      expect(marks).toContain('attached here')
      expect(w.get('[data-label=\"State\"]').text()).toMatch(/^Map damaged/)
    })

    it('says Degraded for a degraded volume with an intact map', () => {
      const w = panel({ volumes: [volume({ replicas: ['node-a'] })], replicationFactor: 3 })
      expect(w.get('[data-label=\"State\"]').text()).toMatch(/^Degraded/)
      expect(w.get('tbody th').text()).not.toContain('map damaged')
    })

    it('says Healthy and adds no marker line for a plain volume', () => {
      const w = panel({ volumes: [volume()] })
      expect(w.get('[data-label=\"State\"]').text()).toMatch(/^Healthy/)
      expect(w.get('tbody th').find('.flex').exists()).toBe(false)
    })

    it('says none when nobody holds the lease', () => {
      const w = panel({ volumes: [volume({ leaseHolder: '' })] })
      expect(w.get('[data-label="Lease"]').text()).toBe('none')
    })

    it('shows the lease holder', () => {
      const w = panel({ volumes: [volume({ leaseHolder: 'node-b' })] })
      expect(w.get('[data-label="Lease"]').text()).toBe('node-b')
    })
  })

  describe('write sparkline', () => {
    it('states the current rate in its label', () => {
      const w = panel({ volumes: [volume({ path: '/db' })], sparks: { '/db': [1, 2048] } })
      expect(w.get('[data-label="Writes"] [role="img"]').attributes('aria-label')).toBe('Writes to /db, 2 KiB/s now')
    })

    it('leaves the rate out with fewer than two points', () => {
      const w = panel({ volumes: [volume({ path: '/db' })], sparks: { '/db': [5] } })
      expect(w.get('[data-label="Writes"] [role="img"]').attributes('aria-label')).toContain('Writes to /db')
      expect(w.get('[data-label="Writes"] [role="img"]').attributes('aria-label')).not.toContain('now')
    })

    it('copes with a volume that has no entry in sparks', () => {
      const w = panel({ volumes: [volume({ path: '/db' })], sparks: { '/other': [1, 2] } })
      expect(w.get('[data-label="Writes"] [role="img"]').attributes('aria-label')).not.toContain('now')
    })
  })

  describe('filter', () => {
    const volumes = [
      volume({ path: '/Alpha', leaseHolder: 'node-a' }),
      volume({ path: '/beta', leaseHolder: 'NODE-B' }),
      volume({ path: '/gamma', leaseHolder: 'node-b' }),
    ]

    it('matches the path, ignoring case and surrounding spaces', async () => {
      const w = panel({ volumes })
      await w.get('input').setValue('  ALPHA ')
      expect(paths(w)).toEqual(['/Alpha'])
    })

    it('matches the lease holder', async () => {
      const w = panel({ volumes })
      await w.get('input').setValue('node-b')
      expect(paths(w)).toEqual(['/beta', '/gamma'])
    })

    it('offers to clear a filter that matches nothing', async () => {
      const w = panel({ volumes })
      await w.get('input').setValue(' nope ')
      expect(w.get('[role="status"]').text()).toContain('No volume matches “nope”')
      expect(w.find('table').exists()).toBe(false)
      expect(w.find('footer').exists()).toBe(false)
      await w.get('button').trigger('click')
      expect(w.find('[role="status"]').exists()).toBe(false)
      expect((w.get('input').element as HTMLInputElement).value).toBe('')
      expect(paths(w)).toHaveLength(3)
    })
  })

  describe('footer and paging', () => {
    it('counts the rows shown without a toggle for a short list', () => {
      const w = panel({ volumes: many(8) })
      expect(w.get('footer').text()).toContain('8 of 8 shown')
      expect(w.get('footer').text()).not.toContain('filtered')
      expect(w.find('footer button').exists()).toBe(false)
    })

    it('says filtered once a query is set', async () => {
      const w = panel({ volumes: many(3) })
      await w.get('input').setValue('v01')
      expect(w.get('footer').text()).toContain('1 of 3 shown, filtered')
    })

    it('shows the first eight and toggles to all', async () => {
      const w = panel({ volumes: many(11) })
      expect(paths(w)).toHaveLength(8)
      expect(w.get('footer').text()).toContain('8 of 11 shown')
      const toggle = w.get('footer button')
      expect(toggle.text()).toBe('Show all')
      expect(toggle.attributes('aria-expanded')).toBe('false')

      await toggle.trigger('click')
      expect(paths(w)).toHaveLength(11)
      expect(toggle.text()).toBe('Show first 8')
      expect(toggle.attributes('aria-expanded')).toBe('true')
      expect(w.get('footer').text()).toContain('11 of 11 shown')

      await toggle.trigger('click')
      expect(paths(w)).toHaveLength(8)
    })

    it('offers Show all only when more than eight match the filter', async () => {
      const w = panel({ volumes: many(12) })
      await w.get('input').setValue('v0')
      expect(w.get('footer').text()).toContain('8 of 12 shown, filtered')
      expect(w.find('footer button').exists()).toBe(true)
      await w.get('input').setValue('v00')
      expect(w.find('footer button').exists()).toBe(false)
    })
  })
})
