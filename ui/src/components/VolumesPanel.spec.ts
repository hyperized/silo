import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Volume } from '../api/status'
import { GIB, volume } from '../test/factories'
import VolumesPanel from './VolumesPanel.vue'

const panel = (volumes: Volume[] | null) => mount(VolumesPanel, { props: { volumes } })

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
})
