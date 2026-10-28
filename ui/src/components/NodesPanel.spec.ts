import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode, LocalStorage } from '../api/status'
import { GIB, node, storage } from '../test/factories'
import NodesPanel from './NodesPanel.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)

function panel(nodes: ClusterNode[] | null, ...st: [] | [LocalStorage | null | undefined]) {
  const disk = st.length === 0 ? storage() : st[0]
  return mount(NodesPanel, { props: { nodes, respondingNodeId: 'node-a', storage: disk, now: NOW } })
}

const bodyRows = (w: ReturnType<typeof panel>) => w.findAll('tbody tr')
const localDisk = (w: ReturnType<typeof panel>) => w.get('div.border-t')

describe('NodesPanel', () => {
  it('shows skeleton rows and no count while loading', () => {
    const w = panel(null, undefined)
    expect(w.get('tbody').attributes('aria-hidden')).toBe('true')
    expect(bodyRows(w)).toHaveLength(3)
    expect(w.get('h2').text()).toBe('Nodes')
  })

  it('says No nodes for an empty list', () => {
    const w = panel([])
    expect(w.text()).toContain('No nodes')
    expect(w.find('table').exists()).toBe(false)
    expect(w.get('h2').text()).toBe('Nodes0')
  })

  describe('rows', () => {
    const nodes = [
      node({ id: 'node-a', lastChange: new Date(NOW - 16 * 60_000) }),
      node({ id: 'node-b', state: 'suspect', gossipAddress: '10.0.0.2:7946', dataAddress: '10.0.0.2:7081' }),
      node({ id: 'node-c', state: 'left', capacityBytes: 0, usedBytes: 0 }),
    ]

    it('shows the id and marks only the responding node as local', () => {
      const rows = bodyRows(panel(nodes))
      expect(rows[0]?.get('th').text()).toBe('node-alocal')
      expect(rows[1]?.get('th').text()).toBe('node-b')
      expect(rows[1]?.find('[title="Responding node"]').exists()).toBe(false)
    })

    it('shows the state word', () => {
      const rows = bodyRows(panel(nodes))
      expect(rows[0]?.get('[data-label="State"]').text()).toContain('Up')
      expect(rows[1]?.get('[data-label="State"]').text()).toContain('Suspect')
    })

    it('shows the data address with the gossip address on hover', () => {
      const cell = bodyRows(panel(nodes))[1]?.get('[data-label="Data address"]')
      expect(cell?.text()).toBe('10.0.0.2:7081')
      expect(cell?.get('span').attributes('title')).toBe('Gossip 10.0.0.2:7946')
    })

    it('shows capacity as a meter, percent and used of total', () => {
      const cell = bodyRows(panel(nodes))[0]?.get('[data-label="Capacity"]')
      expect(cell?.get('[role="meter"]').attributes('aria-label')).toBe('Capacity of node-a')
      expect(cell?.text()).toContain('30%')
      expect(cell?.text()).toContain('30 / 100 GiB')
    })

    it('says not reported when a node has no capacity', () => {
      const cell = bodyRows(panel(nodes))[2]?.get('[data-label="Capacity"]')
      expect(cell?.text()).toBe('not reported')
      expect(cell?.find('[role="meter"]').exists()).toBe(false)
    })

    it('shows how long a node has been in its state', () => {
      const since = bodyRows(panel(nodes))[0]?.get('[data-label="Since"] time')
      expect(since?.text()).toBe('16 min')
    })

    it('leaves Since empty without a change time', () => {
      const cell = bodyRows(panel(nodes))[1]?.get('[data-label="Since"]')
      expect(cell?.find('time').exists()).toBe(false)
    })

    it('mutes rows of nodes that left', () => {
      const rows = bodyRows(panel(nodes))
      expect(rows[2]?.classes()).toContain('text-ink-3')
      expect(rows[0]?.classes()).not.toContain('text-ink-3')
    })

    it('explains the Since header', () => {
      const w = panel(nodes)
      const tips = w.findAll('thead [role="tooltip"]').map((t) => t.text())
      expect(tips).toEqual(['Time in the current state'])
    })
  })

  describe('local disk', () => {
    it('shows a skeleton while storage is loading', () => {
      expect(localDisk(panel(null, undefined)).find('span[aria-hidden="true"]').exists()).toBe(true)
    })

    it('says not reported when the node has no disk figures', () => {
      const d = localDisk(panel([node()], null))
      expect(d.text()).toContain('not reported')
      expect(d.find('[role="meter"]').exists()).toBe(false)
    })

    it('shows directory, usage, free space and chunk count', () => {
      const d = localDisk(panel([node()]))
      expect(d.text()).toContain('/var/lib/silo')
      expect(d.text()).toContain('30 / 100 GiB')
      expect(d.text()).toContain('70 GiB free')
      expect(d.text()).toContain('12,345 chunks')
      expect(d.get('[role="meter"]').attributes('aria-label')).toBe('Local disk of node-a')
    })

    it('omits the directory when it is empty', () => {
      const d = localDisk(panel([node()], storage({ dataDir: '', usedBytes: 5 * GIB })))
      expect(d.find('.break-all').exists()).toBe(false)
      expect(d.text()).toContain('5 / 100 GiB')
    })
  })
})
