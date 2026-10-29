import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Tile from './Tile.vue'

describe('Tile', () => {
  it('labels the section by its heading', () => {
    const w = mount(Tile, { props: { id: 'tile-x', title: 'Clock skew' }, slots: { default: '<p>body</p>' } })
    expect(w.get('h2').text()).toBe('Clock skew')
    expect(w.get('h2').attributes('id')).toBe('tile-x-title')
    expect(w.get('section').attributes('aria-labelledby')).toBe('tile-x-title')
  })

  it('renders its slot content', () => {
    const w = mount(Tile, { props: { id: 't', title: 'T' }, slots: { default: '<p data-test="body">hello</p>' } })
    expect(w.get('[data-test="body"]').text()).toBe('hello')
  })
})
