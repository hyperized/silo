import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode, LocalStorage } from '../api/status'
import { GIB, node, storage } from '../test/factories'
import LocalDiskPanel from './LocalDiskPanel.vue'
import Skeleton from './Skeleton.vue'

interface Over {
  nodes?: ClusterNode[] | null
  storage?: LocalStorage | null | undefined
  ratePerHour?: number | null
}

function panel(over: Over = {}) {
  return mount(LocalDiskPanel, {
    props: { nodes: [node()], storage: storage(), ratePerHour: null, ...over },
  })
}

const OCTAGON = 'M5.3 1.5h5.4l3.8 3.8'
const ALERT = 'M8 1.8 14.6 13.5H1.4Z'

function stat(w: ReturnType<typeof panel>, label: string): string {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return dt?.element.nextElementSibling?.textContent.trim() ?? ''
}

describe('LocalDiskPanel', () => {
  it('shows skeletons and no stats while loading', () => {
    const w = panel({ nodes: null })
    expect(w.findAllComponents(Skeleton)).toHaveLength(3)
    expect(w.find('dl').exists()).toBe(false)
  })

  it('says No nodes for an empty list but keeps the stats', () => {
    const w = panel({ nodes: [] })
    expect(w.text()).toContain('No nodes')
    expect(w.find('ul').exists()).toBe(false)
    expect(w.find('dl').exists()).toBe(true)
  })

  it('leaves nodes that have left out', () => {
    const w = panel({ nodes: [node({ id: 'node-a' }), node({ id: 'node-b', state: 'left' })] })
    expect(w.findAll('li')).toHaveLength(1)
    expect(w.text()).not.toContain('node-b')
  })

  it('shows used of capacity and the percentage for a healthy node', () => {
    const w = panel()
    const li = w.get('li')
    expect(li.text()).toContain('node-a')
    expect(li.text()).toContain('30 of 100 GiB · 30%')
    expect(li.html()).not.toContain(OCTAGON)
    expect(li.html()).not.toContain(ALERT)
  })

  it('says not reported for a node without capacity', () => {
    const w = panel({ nodes: [node({ capacityBytes: 0, usedBytes: 0 })] })
    expect(w.get('li').text()).toContain('not reported')
    expect(w.get('li').text()).not.toContain('%')
  })

  it('flags a node at 80 % with a warning icon next to its figures', () => {
    const w = panel({ nodes: [node({ usedBytes: 85 * GIB })] })
    expect(w.get('li').text()).toContain('85 of 100 GiB · 85%')
    expect(w.get('li').html()).toContain(ALERT)
  })

  it('flags a node at 90 % with a critical icon next to its figures', () => {
    const w = panel({ nodes: [node({ usedBytes: 95 * GIB })] })
    expect(w.get('li').text()).toContain('95 of 100 GiB · 95%')
    expect(w.get('li').html()).toContain(OCTAGON)
  })

  it('shows the data directory only when storage has one', () => {
    expect(panel().text()).toContain('/var/lib/silo')
    expect(panel({ storage: storage({ dataDir: '' }) }).text()).not.toContain('/var/lib/silo')
    expect(panel({ storage: null }).find('[title="Data directory of the responding node"]').exists()).toBe(false)
  })

  describe('stats', () => {
    it('shows chunks and free space of the responding node', () => {
      const w = panel()
      expect(stat(w, 'Chunks here')).toBe('12.3K')
      expect(stat(w, 'Free here')).toBe('70 GiB')
    })

    it.each([null, undefined])('shows n/a without storage (%s)', (s) => {
      const w = panel({ storage: s })
      expect(stat(w, 'Chunks here')).toBe('n/a')
      expect(stat(w, 'Free here')).toBe('n/a')
    })

    it('counts pressured nodes, singular and plural', () => {
      expect(stat(panel(), 'Pressured')).toBe('0 nodes')
      expect(stat(panel({ nodes: [node({ pressured: true })] }), 'Pressured')).toBe('1 node')
      const two = [node({ id: 'a', pressured: true }), node({ id: 'b', pressured: true }), node({ id: 'c', state: 'left', pressured: true })]
      expect(stat(panel({ nodes: two }), 'Pressured')).toBe('2 nodes')
    })

    it('says measuring until the growth rate is known', () => {
      expect(stat(panel({ ratePerHour: null }), 'Growth')).toBe('measuring')
      expect(stat(panel({ ratePerHour: 2 * GIB }), 'Growth')).toBe('+2 GiB/h')
    })
  })
})
