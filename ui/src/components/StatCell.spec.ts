import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import StatCell from './StatCell.vue'

function cell(value: string) {
  return mount(StatCell, { props: { label: 'Nodes', value }, slots: { default: '<span>3 / 3</span>' } })
}

describe('StatCell', () => {
  it('renders the label and the slot', () => {
    const w = cell('a')
    expect(w.get('dt').text()).toBe('Nodes')
    expect(w.get('dd').text()).toBe('3 / 3')
  })

  it('does not flash on first render', () => {
    expect(cell('a').find('[aria-hidden="true"]').exists()).toBe(false)
  })

  it('flashes on each change with a fresh element so the animation restarts', async () => {
    const w = cell('a')
    await w.setProps({ value: 'b' })
    const first = w.get('[aria-hidden="true"]').element
    await w.setProps({ value: 'c' })
    const second = w.get('[aria-hidden="true"]').element
    expect(second).not.toBe(first)
  })

  it('does not flash when the value is unchanged', async () => {
    const w = cell('a')
    await w.setProps({ value: 'a' })
    expect(w.find('[aria-hidden="true"]').exists()).toBe(false)
  })
})
