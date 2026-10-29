import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Skeleton from '../Skeleton.vue'
import SkewTile from './SkewTile.vue'

type Props = InstanceType<typeof SkewTile>['$props']

function tile(over: Partial<Props> = {}) {
  return mount(SkewTile, { props: { skewMs: 12, alerts: 0, reported: true, ...over } })
}

describe('SkewTile', () => {
  it('shows a skeleton while loading', () => {
    const w = tile({ skewMs: null })
    expect(w.findComponent(Skeleton).exists()).toBe(true)
    expect(w.text()).not.toContain('alert')
  })

  it('says not reported for an older silod', () => {
    const w = tile({ reported: false })
    expect(w.text()).toContain('n/a')
    expect(w.text()).toContain('not reported')
  })

  it('shows the worst skew without an Over badge when healthy', () => {
    const w = tile({ skewMs: 12 })
    expect(w.text()).toContain('12')
    expect(w.text()).toContain('ms, worst peer')
    expect(w.text()).not.toContain('Over')
  })

  it('warns from 500 ms', () => {
    expect(tile({ skewMs: 499 }).text()).not.toContain('Over')
    expect(tile({ skewMs: 500 }).text()).toContain('Over 500 ms')
  })

  it('goes critical from 2,000 ms, for either sign', () => {
    expect(tile({ skewMs: 1999 }).text()).toContain('Over 500 ms')
    expect(tile({ skewMs: 2000 }).text()).toContain('Over 2,000 ms')
    expect(tile({ skewMs: -2500 }).text()).toContain('Over 2,000 ms')
  })

  it('counts alerts in the singular and plural', () => {
    expect(tile({ alerts: 1 }).text()).toContain('1 alert since start')
    expect(tile({ alerts: 0 }).text()).toContain('0 alerts since start')
    expect(tile({ alerts: 1234 }).text()).toContain('1,234 alerts since start')
  })
})
