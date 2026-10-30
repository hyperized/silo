import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import ArcGauge from './ArcGauge.vue'

type Props = InstanceType<typeof ArcGauge>['$props']

function gauge(props: Partial<Props> = {}) {
  return mount(ArcGauge, { props: { ratio: 0.47, label: 'Capacity', value: '47%', ...props } })
}

describe('ArcGauge', () => {
  it('draws the track and fill with the mockup geometry', () => {
    const w = gauge()
    expect(w.get('[data-part="track"]').attributes('stroke-dasharray')).toBe('216.77 289.03')
    expect(w.get('[data-part="track"]').attributes('transform')).toBe('rotate(135 60 60)')
    expect(w.get('[data-part="fill"]').attributes('stroke-dasharray')).toBe('101.88 289.03')
    expect(w.get('[data-part="fill"]').attributes('stroke')).toBe('var(--c-accent)')
  })

  it('states the value and caption in its accessible name', () => {
    const svg = gauge({ caption: '1.1 of 2.4 TB' }).get('svg')
    expect(svg.attributes('role')).toBe('img')
    expect(svg.attributes('aria-label')).toBe('Capacity 47%, 1.1 of 2.4 TB')
    expect(svg.text()).toContain('47%')
    expect(svg.text()).toContain('1.1 of 2.4 TB')
  })

  it.each([
    [0.84, 'var(--c-warn-bar)', 'var(--c-warn)', 'warning', 1],
    [0.93, 'var(--c-crit)', 'var(--c-crit)', 'critical', 0],
  ])('at %f colours fill %s, caption %s, says %s and keeps %i ticks', (ratio, fill, caption, word, ticks) => {
    const w = gauge({ ratio, value: 'x', caption: 'c' })
    expect(w.get('[data-part="fill"]').attributes('stroke')).toBe(fill)
    expect(w.findAll('text')[1]?.attributes('fill')).toBe(caption)
    expect(w.get('svg').attributes('aria-label')).toBe(`Capacity x, c, ${word}`)
    expect(w.findAll('[data-part="tick"]')).toHaveLength(ticks)
  })

  it('keeps the tight box and centred value when there is no caption', () => {
    const w = gauge()
    expect(w.get('svg').attributes('viewBox')).toBe('0 0 120 108')
    expect(w.get('text').attributes('y')).toBe('62')
    expect(w.findAll('text')).toHaveLength(1)
  })

  it('puts the caption below the arc ends and grows the box to fit it', () => {
    const w = gauge({ caption: '455 GiB of 2.4 TiB' })
    const [value, caption] = w.findAll('text')
    expect(w.get('svg').attributes('viewBox')).toBe('0 0 120 114')
    expect(value?.attributes('y')).toBe('66')
    // The arc's outer edge ends near y=101; a 10.5 caption with its baseline
    // at 109 starts below that.
    expect(Number(caption?.attributes('y'))).toBeGreaterThan(101 + 7)
    expect(caption?.text()).toBe('455 GiB of 2.4 TiB')
  })

  it('shows a 20 character caption in full', () => {
    expect(gauge({ caption: '1023 GiB of 1023 TiB' }).findAll('text')[1]?.text()).toBe('1023 GiB of 1023 TiB')
  })

  it('cuts a longer caption with an ellipsis but keeps it whole in the accessible name', () => {
    const long = 'a caption far too long to fit'
    const w = gauge({ caption: long })
    expect(w.findAll('text')[1]?.text()).toBe('a caption far too l\u2026')
    expect(w.get('svg').attributes('aria-label')).toBe(`Capacity 47%, ${long}`)
  })

  it('draws warn and crit ticks at 80 and 90 percent by default', () => {
    const ticks = gauge().findAll('[data-part="tick"]')
    expect(ticks.map((t) => t.attributes('stroke'))).toEqual(['var(--c-warn)', 'var(--c-crit)'])
    expect(ticks.map((t) => t.attributes('stroke-dashoffset'))).toEqual(['-173.42', '-195.09'])
  })

  it('colours ticks past the second in ink-3', () => {
    const ticks = gauge({ ratio: 0.1, ticks: [0.5, 0.6, 0.7] }).findAll('[data-part="tick"]')
    expect(ticks[2]?.attributes('stroke')).toBe('var(--c-ink-3)')
  })

  it('lets an explicit level override the derived one', () => {
    expect(gauge({ level: 'degraded' }).get('[data-part="fill"]').attributes('stroke')).toBe('var(--c-crit)')
  })

  it('omits the fill at zero so the round caps do not draw a dot', () => {
    expect(gauge({ ratio: 0 }).find('[data-part="fill"]').exists()).toBe(false)
  })

  it('renders only the track and n/a when the ratio is unknown', () => {
    const w = gauge({ ratio: null })
    expect(w.find('[data-part="fill"]').exists()).toBe(false)
    expect(w.findAll('[data-part="tick"]')).toHaveLength(2)
    expect(w.get('text').text()).toBe('n/a')
    expect(w.get('svg').attributes('aria-label')).toBe('Capacity not available')
    expect(w.findAll('text')).toHaveLength(1)
  })
})
