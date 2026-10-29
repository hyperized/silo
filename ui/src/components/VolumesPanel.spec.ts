import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode, Volume } from '../api/status'
import { GIB, node, volume } from '../test/factories'
import VolumesPanel from './VolumesPanel.vue'

const panel = (volumes: Volume[] | null, nodes: ClusterNode[] = [], replicationFactor = 0) =>
  mount(VolumesPanel, { props: { volumes, nodes, replicationFactor } })

describe('VolumesPanel', () => {
  it('shows skeleton rows and no count while loading', () => {
    const w = panel(null)
    expect(w.get('tbody').attributes('aria-hidden')).toBe('true')
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.get('h2').text()).toBe('Volumes')
  })

  it('explains how to create the first volume', () => {
    const w = panel([])
    expect(w.text()).toContain('No volumes')
    expect(w.get('code').text()).toBe('siloctl volume create --size 10G /name')
    expect(w.find('table').exists()).toBe(false)
    expect(w.get('h2').text()).toBe('Volumes0')
  })

  describe('rows', () => {
    const rows = [
      volume({ path: '/db', leaseHolder: 'node-a' }),
      volume({ path: '/empty', sizeBytes: 0, allocatedBytes: 0, leaseHolder: '' }),
    ]

    it('shows the path, size and extent size', () => {
      const r = panel(rows).findAll('tbody tr')[0]
      expect(r?.get('th').text()).toBe('/db')
      expect(r?.get('[data-label="Size"]').text()).toBe('10 GiB')
      expect(r?.get('[data-label="Extent"]').text()).toBe('4 MiB')
    })

    it('shows allocated bytes with their share of the size', () => {
      const cell = panel(rows).findAll('tbody tr')[0]?.get('[data-label="Allocated"]')
      expect(cell?.text()).toContain('2 GiB')
      expect(cell?.text()).toContain('20%')
    })

    it('omits the percentage for a zero-size volume', () => {
      const cell = panel(rows).findAll('tbody tr')[1]?.get('[data-label="Allocated"]')
      expect(cell?.text()).toBe('0 B')
    })

    it('shows the lease holder or none', () => {
      const r = panel(rows).findAll('tbody tr')
      expect(r[0]?.get('[data-label="Lease"]').text()).toBe('node-a')
      expect(r[1]?.get('[data-label="Lease"]').text()).toBe('none')
    })

    it('explains the technical headers', () => {
      const tips = panel([volume({ sizeBytes: GIB })]).findAll('thead [role="tooltip"]').map((t) => t.text())
      expect(tips).toEqual(['Extents written times extent size', 'Allocation unit', 'Node holding the write lease'])
    })
  })

  describe('replicas', () => {
    const nodes = [node({ id: 'node-a' }), node({ id: 'node-b', state: 'dead' }), node({ id: 'node-c', state: 'suspect' })]
    const cell = (w: ReturnType<typeof panel>, i = 0) => w.findAll('tbody tr')[i]?.get('[data-label="Replicas"]')
    const headers = (w: ReturnType<typeof panel>) => w.get('thead').text()

    it('hides the column when no volume has replicas', () => {
      const w = panel([volume()], nodes)
      expect(w.find('[data-label="Replicas"]').exists()).toBe(false)
      expect(headers(w)).not.toContain('Replicas')
    })

    it('shows the column and explains the header', () => {
      const w = panel([volume({ replicas: ['node-a'] })], nodes)
      expect(headers(w)).toContain('Replicas')
      expect(w.findAll('thead [role="tooltip"]').map((t) => t.text())).toContain('Nodes holding a copy, by state')
    })

    it('says none for a volume without replicas next to one with', () => {
      const w = panel([volume({ replicas: ['node-a'] }), volume({ path: '/other', replicas: [] })], nodes)
      expect(cell(w, 1)?.text()).toBe('none')
    })

    it('labels each chip with the state of its node', () => {
      const w = panel([volume({ replicas: ['node-a', 'node-b', 'node-c'] })], nodes, 3)
      const chips = cell(w)?.findAll('span[title]') ?? []
      expect(chips.map((c) => c.attributes('title'))).toEqual(['node-a: Up', 'node-b: Down', 'node-c: Suspect'])
      expect(chips.map((c) => c.get('.sr-only').text())).toEqual([', Up', ', Down', ', Suspect'])
      expect(chips[0]?.text()).toContain('node-a')
    })

    it('shows Unknown for a replica not in the node list', () => {
      const w = panel([volume({ replicas: ['node-z'] })], nodes)
      const chip = cell(w)?.get('span[title]')
      expect(chip?.attributes('title')).toBe('node-z: Unknown')
      expect(chip?.get('.sr-only').text()).toBe(', Unknown')
    })

    it('tones chips by node state', () => {
      const w = panel([volume({ replicas: ['node-a', 'node-b', 'node-c'] })], nodes)
      const chips = cell(w)?.findAll('span[title]') ?? []
      expect(chips[0]?.classes()).toContain('text-good')
      expect(chips[1]?.classes()).toContain('text-crit')
      expect(chips[2]?.classes()).toContain('text-warn')
    })
  })

  describe('markers', () => {
    const rowOf = (w: ReturnType<typeof panel>) => w.get('tbody tr th')

    it('marks a volume attached on the responding node', () => {
      const th = rowOf(panel([volume({ attachedHere: true })]))
      expect(th.get('[title="Attached on the responding node"]').text()).toBe('attached')
    })

    it('shows no markers for a plain volume', () => {
      const th = rowOf(panel([volume()]))
      expect(th.text()).toBe('/db')
    })

    it('flags a damaged map', () => {
      const th = rowOf(panel([volume({ mapDamaged: true })]))
      expect(th.text()).toContain('Map damaged')
      expect(th.text()).not.toContain('Degraded')
    })

    it.each([
      ['a replica node is down', ['node-a', 'node-b'], 2],
      ['the replica set is short of the factor', ['node-a'], 2],
    ])('flags a degraded volume when %s', (_name, replicas, factor) => {
      const nodes = [node({ id: 'node-a' }), node({ id: 'node-b', state: 'dead' })]
      const th = rowOf(panel([volume({ replicas })], nodes, factor))
      expect(th.text()).toContain('Degraded')
      expect(th.text()).not.toContain('Map damaged')
    })

    it('is not degraded when every replica is alive and the factor is met', () => {
      const nodes = [node({ id: 'node-a' }), node({ id: 'node-b' })]
      expect(rowOf(panel([volume({ replicas: ['node-a', 'node-b'] })], nodes, 2)).text()).toBe('/db')
    })

    it('shows both chips together', () => {
      const th = rowOf(panel([volume({ mapDamaged: true, replicas: ['node-z'] })], [], 1))
      expect(th.text()).toContain('Map damaged')
      expect(th.text()).toContain('Degraded')
    })
  })
})
