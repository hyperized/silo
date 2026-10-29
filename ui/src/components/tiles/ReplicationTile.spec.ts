import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterHealth } from '../../api/status'
import { clusterHealth } from '../../test/factories'
import Skeleton from '../Skeleton.vue'
import ReplicationTile from './ReplicationTile.vue'

function tile(health: ClusterHealth | null | undefined) {
  return mount(ReplicationTile, { props: { health } })
}

const bars = (w: ReturnType<typeof tile>) => w.findAll('[data-part="bar"]').map((b) => b.attributes('data-covered'))

describe('ReplicationTile', () => {
  it('shows a skeleton while loading', () => {
    const w = tile(undefined)
    expect(w.findComponent(Skeleton).exists()).toBe(true)
    expect(w.find('[data-part="bar"]').exists()).toBe(false)
  })

  it('says not reported for an older silod', () => {
    const w = tile(null)
    expect(w.text()).toContain('n/a')
    expect(w.text()).toContain('not reported')
  })

  it('says not reported for a replication factor of zero', () => {
    const w = tile(clusterHealth({ replicationFactor: 0 }))
    expect(w.text()).toContain('n/a')
    expect(w.text()).toContain('not reported')
    expect(w.find('[data-part="bar"]').exists()).toBe(false)
  })

  it('covers every bar when alive nodes match the factor', () => {
    const w = tile(clusterHealth({ replicationFactor: 3, aliveNodes: 3 }))
    expect(w.text()).toContain('3')
    expect(w.text()).toContain('factor')
    expect(w.text()).toContain('Healthy')
    expect(bars(w)).toEqual(['true', 'true', 'true'])
  })

  it('leaves the uncovered bars hollow and says Degraded', () => {
    const w = tile(clusterHealth({ replicationFactor: 3, aliveNodes: 1 }))
    expect(w.text()).toContain('Degraded')
    expect(bars(w)).toEqual(['true', 'false', 'false'])
  })

  it('counts rejoins in the singular and plural', () => {
    expect(tile(clusterHealth({ rejoinAttempts: 1 })).text()).toContain('1 rejoin')
    expect(tile(clusterHealth({ rejoinAttempts: 1 })).text()).not.toContain('1 rejoins')
    expect(tile(clusterHealth({ rejoinAttempts: 0 })).text()).toContain('0 rejoins')
    expect(tile(clusterHealth({ rejoinAttempts: 2 })).text()).toContain('2 rejoins')
  })

  it('adds the start time when known', () => {
    const started = new Date(Date.UTC(2026, 9, 28, 12, 0, 0))
    expect(tile(clusterHealth({ rejoinAttempts: 2, startedAt: started })).text()).toMatch(/2 rejoins since \d{2}:\d{2}/)
    expect(tile(clusterHealth({ rejoinAttempts: 2, startedAt: null })).text()).not.toContain('since')
  })
})
