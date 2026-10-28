import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Panel from './Panel.vue'

describe('Panel', () => {
  it('labels the section by its heading', () => {
    const w = mount(Panel, { props: { id: 'nodes', title: 'Nodes' }, slots: { default: '<p>body</p>' } })
    const h2 = w.get('h2')
    expect(h2.attributes('id')).toBe('nodes-title')
    expect(h2.text()).toBe('Nodes')
    expect(w.get('section').attributes('aria-labelledby')).toBe('nodes-title')
    expect(w.text()).toContain('body')
  })

  it('shows the count only when one is given', () => {
    expect(mount(Panel, { props: { id: 'a', title: 'A', count: 0 } }).get('h2').text()).toBe('A0')
    expect(mount(Panel, { props: { id: 'a', title: 'A' } }).get('h2').text()).toBe('A')
  })

  it('renders the meta slot in the header', () => {
    const w = mount(Panel, { props: { id: 'a', title: 'A' }, slots: { meta: '<span id="m">extra</span>' } })
    expect(w.get('header #m').text()).toBe('extra')
  })
})
