import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Tip from './Tip.vue'

function setup(props: { align?: 'start' | 'end' } = {}) {
  const w = mount(Tip, { props: { text: 'Helpful words', ...props }, slots: { default: 'Label' } })
  const tip = w.get('[role="tooltip"]')
  const shown = (): boolean => (tip.element as HTMLElement).style.display !== 'none'
  return { w, tip, shown }
}

describe('Tip', () => {
  it('is hidden until asked for', () => {
    const { tip, shown } = setup()
    expect(shown()).toBe(false)
    expect(tip.text()).toBe('Helpful words')
  })

  it('renders the slot', () => {
    expect(setup().w.text()).toContain('Label')
  })

  it('points aria-describedby at the tooltip', () => {
    const { w, tip } = setup()
    expect(tip.attributes('id')).toBeTruthy()
    expect(w.attributes('aria-describedby')).toBe(tip.attributes('id'))
  })

  it('is reachable by keyboard', () => {
    expect(setup().w.attributes('tabindex')).toBe('0')
  })

  it.each([
    ['mouseenter', 'mouseleave'],
    ['focus', 'blur'],
  ])('shows on %s and hides on %s', async (on, off) => {
    const { w, shown } = setup()
    await w.trigger(on)
    expect(shown()).toBe(true)
    await w.trigger(off)
    expect(shown()).toBe(false)
  })

  it('hides on Escape', async () => {
    const { w, shown } = setup()
    await w.trigger('focus')
    await w.trigger('keydown', { key: 'Escape' })
    expect(shown()).toBe(false)
  })

  it('aligns to the left edge by default and to the right edge for end', () => {
    expect(setup().tip.classes()).toContain('left-0')
    const end = setup({ align: 'end' }).tip
    expect(end.classes()).toContain('right-0')
    expect(end.classes()).not.toContain('left-0')
  })
})
