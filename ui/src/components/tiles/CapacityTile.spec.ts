import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import { GIB } from '../../test/factories'
import Skeleton from '../Skeleton.vue'
import CapacityTile from './CapacityTile.vue'

type Props = InstanceType<typeof CapacityTile>['$props']

function tile(over: Partial<Props> = {}) {
  return mount(CapacityTile, {
    props: {
      capacity: { usedBytes: 30 * GIB, capacityBytes: 100 * GIB },
      ratePerHour: null,
      secondsUntilFull: null,
      ...over,
    },
  })
}

const footer = (w: ReturnType<typeof tile>) => w.findAll('.mt-auto span').map((s) => s.text())

describe('CapacityTile', () => {
  it('shows a skeleton and no gauge while loading', () => {
    const w = tile({ capacity: undefined })
    expect(w.findComponent(Skeleton).classes()).toContain('aspect-[120/114]')
    expect(w.find('svg').exists()).toBe(false)
    expect(footer(w)).toEqual(['', ''])
  })

  it('says not reported when no node has a disk', () => {
    const w = tile({ capacity: null, ratePerHour: 1 * GIB, secondsUntilFull: 3600 })
    expect(w.get('svg').attributes('aria-label')).toContain('not available')
    expect(w.text()).toContain('not reported')
    expect(footer(w)).toEqual(['', ''])
  })

  it('names the percent and the used of total on the gauge', () => {
    const label = tile().get('svg').attributes('aria-label')
    expect(label).toContain('30%')
    expect(label).toContain('30 of 100 GiB')
  })

  it('reserves the same box for the skeleton as the captioned gauge draws', () => {
    const [, , w, h] = (tile().get('svg').attributes('viewBox') ?? '').split(' ')
    expect(tile({ capacity: undefined }).findComponent(Skeleton).classes()).toContain(`aspect-[${String(w)}/${String(h)}]`)
  })

  it('shows growth only when the rate is known', () => {
    expect(footer(tile({ ratePerHour: 2 * GIB }))[0]).toBe('+2 GiB/h')
    expect(footer(tile({ ratePerHour: null }))[0]).toBe('')
  })

  it.each([
    [0, ''],
    [-10, ''],
    [10 * 365 * 86400, ''],
    [null, ''],
    [3 * 86400, 'full in 3 d'],
  ] as [number | null, string][])('projects the time until full for %s', (secondsUntilFull, want) => {
    expect(footer(tile({ secondsUntilFull }))[1]).toBe(want)
  })
})
