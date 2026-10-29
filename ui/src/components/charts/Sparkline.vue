<script setup lang="ts">
/*
 * A small trend line scaled to its own range. The line lives in an inner
 * viewBox stretched to the box (preserveAspectRatio="none"); the last-point dot
 * is placed in the outer, unscaled svg by percentage so it stays a circle
 * instead of an ellipse when the card is wide.
 */
import { computed } from 'vue'
import { linePoints, num, xAt, yScale } from '../../lib/chart'

type Stroke = 'accent' | 'good' | 'warn' | 'crit'

const props = withDefaults(
  defineProps<{
    /** y values, evenly spaced on x. */
    points: number[]
    /** Accessible name; put the current value in it, the line itself is not read out. */
    label: string
    width?: number
    height?: number
    /** Range in data units drawn as a translucent band behind the line. */
    band?: { from: number; to: number; level: 'attention' | 'degraded' } | undefined
    markLast?: boolean
    stroke?: Stroke
  }>(),
  { width: 160, height: 28, band: undefined, markLast: true, stroke: 'accent' },
)

const PAD = 2
const STROKE: Readonly<Record<Stroke, string>> = {
  accent: 'var(--c-accent)',
  good: 'var(--c-good)',
  warn: 'var(--c-warn)',
  crit: 'var(--c-crit)',
}
const BAND = { attention: 'var(--c-warn)', degraded: 'var(--c-crit)' } as const

const empty = computed(() => props.points.length < 2)
const scale = computed(() => yScale(props.points, props.height, PAD))
const line = computed(() => linePoints(props.points, props.width, props.height, PAD))

const bandRect = computed(() => {
  const b = props.band
  if (!b || empty.value) return null
  const clampY = (y: number): number => Math.min(Math.max(y, 0), props.height)
  const a = clampY(scale.value(b.from))
  const z = clampY(scale.value(b.to))
  return { y: Math.min(a, z), height: num(Math.abs(a - z)), fill: BAND[b.level] }
})

const last = computed(() => {
  if (!props.markLast || empty.value) return null
  const n = props.points.length
  const v = props.points[n - 1] as number
  const b = props.band
  const inBand = b !== undefined && v >= Math.min(b.from, b.to) && v <= Math.max(b.from, b.to)
  return {
    cx: `${String(num((xAt(n - 1, n, props.width) / props.width) * 100))}%`,
    cy: scale.value(v),
    fill: inBand ? BAND[b.level] : STROKE[props.stroke],
  }
})

const ariaLabel = computed(() => (empty.value ? `${props.label}, no data yet` : props.label))
</script>

<template>
  <svg
    width="100%"
    :height="height"
    role="img"
    :aria-label="ariaLabel"
    class="block overflow-visible"
  >
    <svg
      width="100%"
      height="100%"
      :viewBox="`0 0 ${String(width)} ${String(height)}`"
      preserveAspectRatio="none"
    >
      <rect
        v-if="bandRect"
        data-part="band"
        x="0"
        :y="bandRect.y"
        :width="width"
        :height="bandRect.height"
        :fill="bandRect.fill"
        opacity="0.12"
      />
      <line
        v-if="empty"
        data-part="empty"
        x1="0"
        :y1="height / 2"
        :x2="width"
        :y2="height / 2"
        stroke="var(--c-ink-3)"
        stroke-width="1.5"
        stroke-dasharray="3 3"
        vector-effect="non-scaling-stroke"
      />
      <polyline
        v-else
        data-part="line"
        :points="line"
        fill="none"
        :stroke="STROKE[stroke]"
        stroke-width="1.5"
        stroke-linejoin="round"
        vector-effect="non-scaling-stroke"
      />
    </svg>
    <circle
      v-if="last"
      data-part="last"
      :cx="last.cx"
      :cy="last.cy"
      r="2.5"
      :fill="last.fill"
    />
  </svg>
</template>
