import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Volume } from '../../api/status'
import { GIB, volume } from '../../test/factories'
import Skeleton from '../Skeleton.vue'
import VolumesTile from './VolumesTile.vue'

type Props = InstanceType<typeof VolumesTile>['$props']

function tile(over: Partial<Props> = {}) {
  return mount(VolumesTile, {
    props: { volumes: [volume()], degraded: 0, damaged: 0, spark: [1, 2, 3], writeRate: null, ...over },
  })
}

const vols = (...v: Partial<Volume>[]) => v.map((o) => volume(o))

describe('VolumesTile', () => {
  it('shows a skeleton while loading', () => {
    const w = tile({ volumes: null })
    expect(w.findComponent(Skeleton).exists()).toBe(true)
    expect(w.find('svg').exists()).toBe(false)
  })

  it('says None yet with no size for zero volumes', () => {
    const w = tile({ volumes: [] })
    expect(w.text()).toContain('0')
    expect(w.text()).toContain('None yet')
    expect(w.text()).not.toContain('GiB')
  })

  it('says All healthy and shows the provisioned size', () => {
    const w = tile({ volumes: vols({ sizeBytes: 10 * GIB }, { sizeBytes: 5 * GIB }) })
    expect(w.text()).toContain('All healthy')
    expect(w.text()).toContain('15 GiB')
  })

  it('counts degraded volumes', () => {
    expect(tile({ degraded: 2 }).text()).toContain('2 degraded')
  })

  it('lets damaged maps win over degraded', () => {
    const w = tile({ degraded: 2, damaged: 1 })
    expect(w.text()).toContain('1 map damaged')
    expect(w.text()).not.toContain('degraded')
  })

  it('leaves out the write rate until it is known', () => {
    expect(tile({ writeRate: null }).text()).toContain('0 attached here')
    expect(tile({ writeRate: null }).text()).not.toContain('written')
    expect(tile({ writeRate: 2 * 1024 ** 2 }).text()).toContain('2 MiB/s written, 0 attached here')
  })

  it('counts volumes attached here', () => {
    const w = tile({ volumes: vols({ attachedHere: true }, { attachedHere: false }, { attachedHere: true }) })
    expect(w.text()).toContain('2 attached here')
  })

  it('names the sparkline with the current rate when there is one', () => {
    expect(tile({ writeRate: null }).get('svg[role="img"]').attributes('aria-label')).toBe('Cluster write rate')
    expect(tile({ writeRate: 1024 ** 2 }).get('svg[role="img"]').attributes('aria-label')).toBe(
      'Cluster write rate, 1 MiB/s now',
    )
  })
})
