import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode } from '../api/status'
import { node } from '../test/factories'
import NodeCard from './NodeCard.vue'
import NodeCards from './NodeCards.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)

function cards(nodes: ClusterNode[] | null, sparks: Record<string, number[]> = {}, respondingNodeId = 'node-a') {
  return mount(NodeCards, { props: { nodes, respondingNodeId, now: NOW, sparks } })
}

describe('NodeCards', () => {
  it('has an accessible Nodes heading', () => {
    expect(cards([node()]).get('h2').text()).toBe('Nodes')
  })

  it('shows three skeletons while loading', () => {
    const w = cards(null)
    expect(w.findAllComponents(Skeleton)).toHaveLength(3)
    expect(w.findAllComponents(NodeCard)).toHaveLength(0)
  })

  it('says No nodes for an empty list', () => {
    const w = cards([])
    expect(w.text()).toContain('No nodes')
    expect(w.findAllComponents(NodeCard)).toHaveLength(0)
  })

  it('renders one card per node', () => {
    const w = cards([node({ id: 'node-a' }), node({ id: 'node-b' }), node({ id: 'node-c' })])
    expect(w.findAllComponents(NodeCard)).toHaveLength(3)
    expect(w.findAll('h3').map((h) => h.text())).toEqual(['node-a', 'node-b', 'node-c'])
  })

  it('marks only the responding node as self', () => {
    const w = cards([node({ id: 'node-a' }), node({ id: 'node-b' })], {}, 'node-b')
    expect(w.findAllComponents(NodeCard).map((c) => c.props('self'))).toEqual([false, true])
  })

  it('hands each node its own spark and an empty line when none is known', () => {
    const w = cards([node({ id: 'node-a' }), node({ id: 'node-b' })], { 'node-a': [1, 2, 3] })
    expect(w.findAllComponents(NodeCard).map((c) => c.props('spark'))).toEqual([[1, 2, 3], []])
  })
})
