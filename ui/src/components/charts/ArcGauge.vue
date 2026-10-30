<script setup lang="ts">
/*
 * A 270 degree gauge for one ratio. Each arc is a whole circle drawn with a
 * dash pattern and rotated so the dash starts at the lower left; that keeps
 * the geometry to one radius instead of computing arc endpoints.
 */
import { computed } from 'vue'
import { arcDash, arcTick } from '../../lib/chart'
import { fillLevel, type Level } from '../../lib/health'

const props = withDefaults(
  defineProps<{
    /** 0..1, or null when unknown: only the track and "n/a" are drawn. */
    ratio: number | null
    /** What is measured; starts the accessible name. */
    label: string
    /** Big centre text, e.g. "47%". */
    value: string
    caption?: string
    /** Ratios that get a tick; the first is drawn in warn, the second in crit, the rest in ink-3. */
    ticks?: number[]
    /** Fill colour; derived from the ratio by fillLevel when not given. */
    level?: Level | undefined
  }>(),
  { caption: '', ticks: () => [0.8, 0.9], level: undefined },
)

const R = 46
const SWEEP = 270
const FILL: Readonly<Record<Level, string>> = {
  healthy: 'var(--c-accent)',
  attention: 'var(--c-warn-bar)',
  degraded: 'var(--c-crit)',
}
const CAPTION: Readonly<Record<Level, string>> = {
  healthy: 'var(--c-ink-3)',
  attention: 'var(--c-warn)',
  degraded: 'var(--c-crit)',
}
const WORD: Readonly<Record<Level, string>> = { healthy: '', attention: 'warning', degraded: 'critical' }
const TICK = ['var(--c-warn)', 'var(--c-crit)']

// The arc ends reach down to about y=101 (outer edge, round caps included), so
// a caption between them would be drawn over the arc. It sits below them
// instead, where the full width is free, and the viewBox grows only when there
// is one so captionless gauges keep their tight box.
const CAPTION_Y = 109
const HEIGHT = 108
const HEIGHT_WITH_CAPTION = 114
// At font-size 10.5 a character is about 6 units wide, so 20 fill the 120-unit
// box. formatOf tops out at "1023 GiB of 1023 TiB" (20), which always fits.
const CAPTION_MAX = 20

const track = arcDash(1, R, SWEEP)
const level = computed<Level>(() => props.level ?? fillLevel(props.ratio))
const fill = computed(() => (props.ratio !== null && props.ratio > 0 ? arcDash(props.ratio, R, SWEEP) : null))

// A tick the fill has already passed would sit on top of the fill and read as
// a gap in it, so only the ticks still ahead are drawn.
const tickMarks = computed(() =>
  props.ticks
    .map((t, i) => ({ t, colour: TICK[i] ?? 'var(--c-ink-3)', ...arcTick(t, R, SWEEP) }))
    .filter(({ t }) => props.ratio === null || t > props.ratio),
)

const viewBox = computed(() => `0 0 120 ${String(props.caption ? HEIGHT_WITH_CAPTION : HEIGHT)}`)
const shownCaption = computed(() =>
  props.caption.length > CAPTION_MAX ? `${props.caption.slice(0, CAPTION_MAX - 1).trimEnd()}\u2026` : props.caption,
)

const ariaLabel = computed(() => {
  if (props.ratio === null) return `${props.label} not available`
  return [`${props.label} ${props.value}`, props.caption, WORD[level.value]].filter((s) => s !== '').join(', ')
})
</script>

<template>
  <svg
    width="100%"
    :viewBox="viewBox"
    role="img"
    :aria-label="ariaLabel"
    class="block"
  >
    <circle
      data-part="track"
      cx="60"
      cy="60"
      :r="R"
      fill="none"
      stroke="var(--c-track)"
      stroke-width="10"
      stroke-linecap="round"
      :stroke-dasharray="track.dasharray"
      transform="rotate(135 60 60)"
    />
    <circle
      v-if="fill"
      data-part="fill"
      cx="60"
      cy="60"
      :r="R"
      fill="none"
      :stroke="FILL[level]"
      stroke-width="10"
      stroke-linecap="round"
      :stroke-dasharray="fill.dasharray"
      transform="rotate(135 60 60)"
      class="transition-[stroke-dasharray] duration-300 ease-out motion-reduce:transition-none"
    />
    <circle
      v-for="tick in tickMarks"
      :key="tick.t"
      data-part="tick"
      cx="60"
      cy="60"
      :r="R"
      fill="none"
      :stroke="tick.colour"
      stroke-width="10"
      :stroke-dasharray="tick.dasharray"
      :stroke-dashoffset="tick.dashoffset"
      transform="rotate(135 60 60)"
    />
    <text
      x="60"
      :y="caption ? 66 : 62"
      text-anchor="middle"
      font-size="26"
      font-weight="600"
      fill="var(--c-ink)"
    >{{ ratio === null ? 'n/a' : value }}</text>
    <text
      v-if="caption"
      x="60"
      :y="CAPTION_Y"
      text-anchor="middle"
      font-size="10.5"
      :fill="CAPTION[level]"
    >{{ shownCaption }}</text>
  </svg>
</template>
