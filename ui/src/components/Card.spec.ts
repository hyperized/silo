import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Card from './Card.vue'

describe('Card', () => {
  it('renders the title in a heading the section is labelled by', () => {
    const w = mount(Card, { props: { id: 'c1', title: 'Volumes' } })
    const h = w.get('h2')
    expect(h.text()).toBe('Volumes')
    expect(h.attributes('id')).toBe('c1-title')
    expect(w.get('section').attributes('aria-labelledby')).toBe('c1-title')
    expect(w.get('section').attributes('id')).toBe('c1')
  })

  it('shows the count beside the title, including zero', () => {
    expect(mount(Card, { props: { id: 'c', title: 'Nodes', count: 3 } }).get('h2').text()).toBe('Nodes3')
    expect(mount(Card, { props: { id: 'c', title: 'Nodes', count: 0 } }).get('h2').text()).toBe('Nodes0')
  })

  it('omits the count when none is given', () => {
    expect(mount(Card, { props: { id: 'c', title: 'Nodes' } }).find('h2 span').exists()).toBe(false)
  })

  it('renders the meta and default slots', () => {
    const w = mount(Card, {
      props: { id: 'c', title: 'Nodes' },
      slots: { meta: '<em data-test="meta">hint</em>', default: '<p data-test="body">content</p>' },
    })
    expect(w.get('header [data-test="meta"]').text()).toBe('hint')
    expect(w.get('[data-test="body"]').text()).toBe('content')
  })

  it.each([
    ['normal', 'border-rule'],
    ['warn', 'border-warn-bar'],
    ['crit', 'border-crit'],
  ] as const)('outlines the %s tone with %s', (tone, cls) => {
    expect(mount(Card, { props: { id: 'c', title: 'T', tone } }).get('section').classes()).toContain(cls)
  })

  it('defaults to the normal tone', () => {
    expect(mount(Card, { props: { id: 'c', title: 'T' } }).get('section').classes()).toContain('border-rule')
  })
})
