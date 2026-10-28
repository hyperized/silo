import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import { formatDateTime } from '../lib/format'
import type { Level } from '../lib/health'
import AppBar from './AppBar.vue'
import Skeleton from './Skeleton.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)

interface Props {
  nodeId: string
  level: Level | null
  lastSuccessAt: number | null
  now: number
  paused: boolean
  inFlight: boolean
}

function bar(over: Partial<Props> = {}) {
  return mount(AppBar, {
    props: { nodeId: 'node-a', level: 'healthy', lastSuccessAt: NOW - 3000, now: NOW, paused: false, inFlight: false, ...over },
  })
}

const age = (w: ReturnType<typeof bar>): string => w.get('span.whitespace-nowrap.text-ink-3').text()
const live = (w: ReturnType<typeof bar>): string => w.get('[aria-live="polite"]').text()

describe('AppBar', () => {
  it('names the product and the responding node', () => {
    const h1 = bar().get('h1')
    expect(h1.text()).toContain('silo')
    expect(h1.text()).toContain('node-a')
  })

  it('omits the node part when the id is empty', () => {
    const h1 = bar({ nodeId: '' }).get('h1')
    expect(h1.text()).toBe('silo')
    expect(h1.find('[title="Responding node"]').exists()).toBe(false)
  })

  it('shows the level as a word with a screen reader prefix', () => {
    const w = bar({ level: 'attention' })
    const chip = w.get('h1 ~ span')
    expect(chip.text()).toBe('Status Warning')
    expect(chip.get('.sr-only').text()).toBe('Status')
  })

  it('shows a skeleton while the first answer is in flight', () => {
    const w = bar({ level: null, lastSuccessAt: null, inFlight: true })
    expect(w.findComponent(Skeleton).exists()).toBe(true)
    expect(w.text()).not.toContain('Status')
  })

  it('shows neither level nor skeleton when idle without a level', () => {
    const w = bar({ level: null, lastSuccessAt: null })
    expect(w.findComponent(Skeleton).exists()).toBe(false)
    expect(w.text()).not.toContain('Status')
  })

  describe('age', () => {
    it('says Loading before the first success while in flight', () => {
      expect(age(bar({ lastSuccessAt: null, inFlight: true }))).toBe('Loading')
    })

    it('is empty with no data and nothing in flight', () => {
      expect(age(bar({ lastSuccessAt: null }))).toBe('')
    })

    it('says now under two seconds', () => {
      expect(age(bar({ lastSuccessAt: NOW - 1900 }))).toContain('now')
    })

    it('counts seconds since the last success', () => {
      expect(age(bar())).toBe('Updated 3 s ago')
    })

    it('carries the absolute time as a title only when there is data', () => {
      const w = bar()
      expect(w.get('span.whitespace-nowrap.text-ink-3').attributes('title')).toBe(formatDateTime(new Date(NOW - 3000)))
      expect(bar({ lastSuccessAt: null }).get('span.whitespace-nowrap.text-ink-3').attributes('title')).toBeUndefined()
    })
  })

  describe('pause', () => {
    it('offers Pause while running', () => {
      const btn = bar().get('button')
      expect(btn.text()).toBe('Pause')
      expect(btn.attributes('aria-pressed')).toBe('false')
    })

    it('offers Resume while paused', () => {
      const btn = bar({ paused: true }).get('button')
      expect(btn.text()).toBe('Resume')
      expect(btn.attributes('aria-pressed')).toBe('true')
      expect(btn.classes()).toContain('text-accent')
    })

    it('says Paused with the age when there is data', () => {
      expect(age(bar({ paused: true }))).toBe('Paused, 3 s ago')
    })

    it('says only Paused when there is no data', () => {
      expect(age(bar({ paused: true, lastSuccessAt: null }))).toBe('Paused')
    })

    it('emits togglePause on click', async () => {
      const w = bar()
      await w.get('button').trigger('click')
      expect(w.emitted('togglePause')).toHaveLength(1)
    })
  })

  describe('level announcements', () => {
    it('announces a change between two levels', async () => {
      const w = bar()
      expect(live(w)).toBe('')
      await w.setProps({ level: 'degraded' })
      expect(live(w)).toBe('Status Degraded')
    })

    it('stays quiet on the first level', async () => {
      const w = bar({ level: null, lastSuccessAt: null })
      await w.setProps({ level: 'healthy' })
      expect(live(w)).toBe('')
    })

    it('stays quiet when the level disappears', async () => {
      const w = bar()
      await w.setProps({ level: null })
      expect(live(w)).toBe('')
    })

    it('stays quiet when only the clock moves', async () => {
      const w = bar()
      await w.setProps({ now: NOW + 5000 })
      expect(live(w)).toBe('')
    })
  })
})
