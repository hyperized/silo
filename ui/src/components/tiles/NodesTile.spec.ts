import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode } from '../../api/status'
import { node } from '../../test/factories'
import Skeleton from '../Skeleton.vue'
import NodesTile from './NodesTile.vue'

function tile(nodes: ClusterNode[] | null, isolated = false) {
  return mount(NodesTile, { props: { nodes, isolated } })
}

const n = (id: string, state: ClusterNode['state']) => node({ id, state })

describe('NodesTile', () => {
  it('shows a skeleton and no footer while loading', () => {
    const w = tile(null)
    expect(w.findComponent(Skeleton).classes()).toContain('aspect-[120/114]')
    expect(w.text()).not.toContain('suspect')
    expect(w.text()).not.toContain('down')
  })

  it('says no nodes for an empty list', () => {
    const w = tile([])
    expect(w.text()).toContain('no nodes')
    expect(w.text()).toContain('0/0')
    expect(w.text()).toContain('0 suspect')
    expect(w.text()).toContain('0 down')
  })

  it('counts up of total and leaves left nodes out of the ring', () => {
    const w = tile([n('a', 'alive'), n('b', 'alive'), n('c', 'left')])
    expect(w.text()).toContain('2/2')
    expect(w.text()).toContain('alive')
  })

  it('shows suspect and down counts, warming each only when above zero', () => {
    const quiet = tile([n('a', 'alive')])
    expect(quiet.get('span.mt-0, .mt-auto span').classes()).not.toContain('text-warn')
    const w = tile([n('a', 'alive'), n('b', 'suspect'), n('c', 'dead'), n('d', 'dead')])
    expect(w.text()).toContain('1/4')
    const spans = w.findAll('.mt-auto > span')
    expect(spans[0]?.text()).toBe('1 suspect')
    expect(spans[0]?.classes()).toContain('text-warn')
    expect(spans[1]?.text()).toBe('2 down')
    expect(spans[1]?.classes()).toContain('text-crit')
    expect(quiet.findAll('.mt-auto > span')[1]?.classes()).not.toContain('text-crit')
  })

  it('replaces the down count with the Isolated badge', () => {
    const w = tile([n('a', 'alive'), n('b', 'dead')], true)
    expect(w.text()).toContain('Isolated')
    expect(w.text()).not.toContain('down')
  })
})
