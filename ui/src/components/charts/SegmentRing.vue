<script setup lang="ts">
/*
 * One ring segment per member (a node, usually). State is carried by colour
 * and by line style together, so the ring still reads in greyscale: suspect is
 * dashed, gone or unknown is an empty outline.
 */
import { computed } from 'vue'
import type { NodeState } from '../../api/status'
import { dashWithin, ringSegments } from '../../lib/chart'
import { nodeLevel, worst, type Level } from '../../lib/health'

const props = withDefaults(
  defineProps<{
    segments: { state: NodeState; label: string }[]
    /** Big centre text, e.g. "2/3". */
    value: string
    caption?: string
    /** What is shown; starts the accessible name. */
    label: string
  }>(),
  { caption: '' },
)

// Same 120x114 box as the capacity ArcGauge beside it, so the two tiles line up.
// The full circle reaches R + 5 (half the stroke) on every side; at R = 42 that is
// 10 units of air top and bottom, roughly where the gauge's arc starts and ends.
const R = 42
const CX = 60
const CY = 57
// Segments start at twelve o'clock.
const ROTATE = `rotate(-90 ${String(CX)} ${String(CY)})`
const GAP = 6
const ORDER: readonly NodeState[] = ['alive', 'suspect', 'dead', 'left', 'unknown']
const CAPTION: Readonly<Record<Level, string>> = {
  healthy: 'var(--c-ink-3)',
  attention: 'var(--c-warn)',
  degraded: 'var(--c-crit)',
}

const drawn = computed(() => {
  const geo = ringSegments(props.segments.length, R, GAP)
  return props.segments.map((s, i) => {
    const g = geo[i] as (typeof geo)[number]
    const solid = { dasharray: g.dasharray, dashoffset: g.dashoffset }
    if (s.state === 'alive') return { ...s, key: i, stroke: 'var(--c-good)', main: solid, outline: null }
    if (s.state === 'dead') return { ...s, key: i, stroke: 'var(--c-crit)', main: solid, outline: null }
    if (s.state === 'suspect') {
      return {
        ...s,
        key: i,
        stroke: 'var(--c-warn-bar)',
        main: { dasharray: dashWithin(g.length, g.circumference), dashoffset: g.dashoffset },
        outline: null,
      }
    }
    return {
      ...s,
      key: i,
      stroke: 'var(--c-track)',
      main: solid,
      outline: { dasharray: dashWithin(g.length, g.circumference, 3, 3), dashoffset: g.dashoffset },
    }
  })
})

const level = computed(() => props.segments.reduce<Level>((acc, s) => worst(acc, nodeLevel(s.state)), 'healthy'))

const ariaLabel = computed(() => {
  const counts = ORDER.map((state) => ({ state, n: props.segments.filter((s) => s.state === state).length }))
    .filter(({ n }) => n > 0)
    .map(({ state, n }) => `${String(n)} ${state}`)
  const summary = counts.length > 0 ? counts.join(', ') : 'none'
  return [`${props.label} ${props.value}`, props.caption, summary].filter((s) => s !== '').join(', ')
})
</script>

<template>
  <svg
    width="100%"
    viewBox="0 0 120 114"
    role="img"
    :aria-label="ariaLabel"
    class="block"
  >
    <circle
      v-if="drawn.length === 0"
      data-part="track"
      :cx="CX"
      :cy="CY"
      :r="R"
      fill="none"
      stroke="var(--c-track)"
      stroke-width="10"
    />
    <g
      v-for="seg in drawn"
      :key="seg.key"
      :data-state="seg.state"
    >
      <title>{{ seg.label }}: {{ seg.state }}</title>
      <circle
        data-part="segment"
        :cx="CX"
        :cy="CY"
        :r="R"
        fill="none"
        :stroke="seg.stroke"
        stroke-width="10"
        :stroke-dasharray="seg.main.dasharray"
        :stroke-dashoffset="seg.main.dashoffset"
        :transform="ROTATE"
      />
      <circle
        v-if="seg.outline"
        data-part="outline"
        :cx="CX"
        :cy="CY"
        :r="R"
        fill="none"
        stroke="var(--c-ink-3)"
        stroke-width="1.5"
        :stroke-dasharray="seg.outline.dasharray"
        :stroke-dashoffset="seg.outline.dashoffset"
        :transform="ROTATE"
      />
    </g>
    <text
      x="60"
      y="59"
      text-anchor="middle"
      font-size="26"
      font-weight="600"
      fill="var(--c-ink)"
    >{{ value }}</text>
    <text
      v-if="caption"
      x="60"
      y="77"
      text-anchor="middle"
      font-size="10.5"
      :fill="CAPTION[level]"
    >{{ caption }}</text>
  </svg>
</template>
