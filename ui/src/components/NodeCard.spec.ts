import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { ClusterNode } from '../api/status'
import { GIB, node } from '../test/factories'
import NodeCard from './NodeCard.vue'
import Sparkline from './charts/Sparkline.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const MIB = 1024 ** 2

function card(over: Partial<ClusterNode> = {}, props: { self?: boolean; spark?: number[] } = {}) {
  return mount(NodeCard, { props: { node: node(over), self: false, now: NOW, spark: [], ...props } })
}

// The dl pairs are Used, Free, Skew, then the duration row.
function row(w: ReturnType<typeof card>, label: string) {
  const dt = w.findAll('dt').find((d) => d.text() === label)
  return { exists: dt !== undefined, value: dt?.element.nextElementSibling?.textContent.trim() ?? '' }
}

describe('NodeCard header', () => {
  it('names the node in an h3 that labels the card', () => {
    const w = card({ id: 'node-b' })
    expect(w.get('h3').text()).toBe('node-b')
    expect(w.get('article').attributes('aria-labelledby')).toBe(w.get('h3').attributes('id'))
  })

  it('draws the disk gauge without a caption, so it keeps the tight box', () => {
    const svg = card({ usedBytes: 30 * GIB, capacityBytes: 100 * GIB }).get('svg[role="img"]')
    expect(svg.attributes('viewBox')).toBe('0 0 120 108')
    expect(svg.findAll('text')).toHaveLength(1)
  })

  it('tags the responding node only', () => {
    expect(card({}, { self: true }).text()).toContain('this node')
    expect(card({}, { self: false }).text()).not.toContain('this node')
  })

  it('shows no disk tag for a healthy disk or an unreported one', () => {
    expect(card().find('[data-part="disk-tag"]').exists()).toBe(false)
    expect(card({ capacityBytes: 0, usedBytes: 0 }).find('[data-part="disk-tag"]').exists()).toBe(false)
  })

  it('tags a disk at 80 % as a warning', () => {
    const tag = card({ usedBytes: 84 * GIB }).get('[data-part="disk-tag"]')
    expect(tag.text()).toBe('Disk 84%')
    expect(tag.classes()).toContain('text-warn')
  })

  it('tags a disk at 90 % as critical', () => {
    const tag = card({ usedBytes: 93 * GIB }).get('[data-part="disk-tag"]')
    expect(tag.text()).toBe('Disk 93%')
    expect(tag.classes()).toContain('text-crit')
  })

  it('shows the Pressured badge only for a pressured node', () => {
    expect(card({ pressured: true }).text()).toContain('Pressured')
    expect(card().text()).not.toContain('Pressured')
  })
})

describe('NodeCard state and border', () => {
  it.each([
    ['alive', 'Alive'],
    ['suspect', 'Suspect'],
    ['dead', 'Down'],
    ['left', 'Left'],
    ['unknown', 'Unknown'],
  ] as const)('writes the %s state as the word %s', (state, word) => {
    expect(card({ state }).get('header').text()).toContain(word)
  })

  it('outlines a card with a quiet border when all is well', () => {
    expect(card().get('article').classes()).toContain('border-rule')
  })

  it('outlines a nearly full disk in the warning tone', () => {
    expect(card({ usedBytes: 85 * GIB }).get('article').classes()).toContain('border-warn-bar')
  })

  it('outlines a dead node in the critical tone', () => {
    expect(card({ state: 'dead' }).get('article').classes()).toContain('border-crit')
  })

  it('mutes a node that has left', () => {
    expect(card({ state: 'left' }).get('article').classes()).toContain('text-ink-3')
    expect(card().get('article').classes()).not.toContain('text-ink-3')
  })
})

describe('NodeCard figures', () => {
  it('shows used and free bytes', () => {
    const w = card()
    expect(row(w, 'Used').value).toBe('30 GiB')
    expect(row(w, 'Free').value).toBe('70 GiB')
  })

  it('says n/a for both when the capacity is not reported', () => {
    const w = card({ capacityBytes: 0, usedBytes: 0 })
    expect(row(w, 'Used').value).toBe('n/a')
    expect(row(w, 'Free').value).toBe('n/a')
  })

  it('never shows negative free space', () => {
    expect(row(card({ usedBytes: 120 * GIB }), 'Free').value).toBe('0 B')
  })

  it('calls the responding node the skew reference', () => {
    expect(row(card({ clockSkewMs: 0 }, { self: true }), 'Skew').value).toBe('reference')
  })

  it('shows the signed skew of a peer', () => {
    expect(row(card({ clockSkewMs: -42 }), 'Skew').value).toBe('-42 ms')
    expect(row(card({ clockSkewMs: 120 }), 'Skew').value).toBe('+120 ms')
  })

  it('turns a skew over 500 ms into a badge carrying the value, even on the reference node', () => {
    const w = card({ clockSkewMs: 650 }, { self: true })
    expect(row(w, 'Skew').value).toBe('+650 ms')
    expect(w.findAll('dd')[2]?.find('svg').exists()).toBe(true)
  })
})

describe('NodeCard duration', () => {
  const since = new Date(NOW - 41 * 60_000)

  it.each([
    ['alive', 'Up'],
    ['suspect', 'Suspect'],
    ['dead', 'Down'],
    ['left', 'Left'],
    ['unknown', 'Seen'],
  ] as const)('keys a %s node by %s', (state, key) => {
    const w = card({ state, lastChange: since })
    expect(row(w, key)).toEqual({ exists: true, value: '41 min' })
    expect(w.get('time').attributes('datetime')).toBe(since.toISOString())
  })

  it('says n/a without a last change', () => {
    const w = card({ lastChange: null })
    expect(row(w, 'Up').value).toBe('n/a')
    expect(w.find('time').exists()).toBe(false)
  })
})

describe('NodeCard write sparkline', () => {
  it('names the line without a rate when there is no data', () => {
    const w = card()
    const line = w.get('[aria-label^="Writes through"]')
    expect(line.attributes('aria-label')).toBe('Writes through node-a, no data yet')
    expect(w.text()).not.toContain('written')
  })

  it('puts the current rate in the label and the footer', () => {
    const w = card({}, { spark: [MIB, 2 * MIB, 38 * MIB] })
    expect(w.get('[aria-label^="Writes through"]').attributes('aria-label')).toBe('Writes through node-a, 38 MiB/s now')
    expect(w.text()).toContain('38 MiB/s written')
    expect(w.getComponent(Sparkline).props('points')).toEqual([MIB, 2 * MIB, 38 * MIB])
  })

  it('treats a single point as a rate, a zero as a real rate', () => {
    expect(card({}, { spark: [0] }).text()).toContain('0 B/s written')
  })
})

describe('NodeCard footer', () => {
  it('shows the data address with the gossip address in its title', () => {
    const w = card({ dataAddress: '10.0.0.9:7081', gossipAddress: '10.0.0.9:7946' })
    const addr = w.get('[title="Gossip 10.0.0.9:7946"]')
    expect(addr.text()).toBe('10.0.0.9:7081')
  })
})
