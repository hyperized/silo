import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import Skeleton from './Skeleton.vue'

describe('Skeleton', () => {
  it('is hidden from assistive technology', () => {
    expect(mount(Skeleton).attributes('aria-hidden')).toBe('true')
  })

  it('passes extra classes through for sizing', () => {
    expect(mount(Skeleton, { attrs: { class: 'h-4 w-10' } }).classes()).toEqual(expect.arrayContaining(['h-4', 'w-10']))
  })
})
