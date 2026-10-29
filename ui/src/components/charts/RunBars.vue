<script setup lang="ts">
/*
 * Last N runs as bars, oldest on the left. Bars are positioned in percent of
 * the width so the rounded corners are not stretched on wide cards.
 */
import { computed } from 'vue'
import { num } from '../../lib/chart'

const props = defineProps<{
  runs: { value: number; ok: boolean; label: string }[]
  label: string
  /** Value at full height; defaults to the largest run. */
  max?: number | undefined
}>()

const BASE = 64
const TALLEST = 56
// A run that freed nothing still happened; a 2 px stub keeps it visible.
const SHORTEST = 2

const scaleMax = computed(() => {
  if (props.max !== undefined && props.max > 0) return props.max
  const peak = Math.max(0, ...props.runs.map((r) => (Number.isFinite(r.value) ? r.value : 0)))
  return peak > 0 ? peak : 1
})

const latestOk = computed(() => props.runs.reduce((acc, r, i) => (r.ok ? i : acc), -1))

const bars = computed(() => {
  const slot = 100 / props.runs.length
  return props.runs.map((r, i) => {
    const v = Number.isFinite(r.value) ? Math.min(Math.max(r.value, 0), scaleMax.value) : 0
    const h = num(Math.max((v / scaleMax.value) * TALLEST, SHORTEST))
    return {
      key: i,
      label: r.label,
      ok: r.ok,
      x: `${String(num(i * slot + slot * 0.14))}%`,
      width: `${String(num(slot * 0.72))}%`,
      y: num(BASE - h),
      height: h,
      fill: r.ok ? 'var(--c-accent)' : 'var(--c-crit)',
      opacity: !r.ok || i === latestOk.value ? 1 : 0.6,
    }
  })
})

const ariaLabel = computed(() => {
  const n = props.runs.length
  if (n === 0) return `${props.label}: no runs seen yet`
  const failed = props.runs.filter((r) => !r.ok).length
  return `${props.label}: ${String(n)} ${n === 1 ? 'run' : 'runs'}, ${String(failed)} failed`
})
</script>

<template>
  <svg
    width="100%"
    height="72"
    role="img"
    :aria-label="ariaLabel"
    class="block"
  >
    <rect
      v-for="bar in bars"
      :key="bar.key"
      data-part="bar"
      :data-ok="bar.ok"
      :x="bar.x"
      :y="bar.y"
      :width="bar.width"
      :height="bar.height"
      rx="2"
      :fill="bar.fill"
      :opacity="bar.opacity"
    >
      <title>{{ bar.label }}</title>
    </rect>
    <line
      data-part="baseline"
      x1="0"
      y1="64.5"
      x2="100%"
      y2="64.5"
      stroke="var(--c-rule)"
    />
    <text
      v-if="bars.length === 0"
      data-part="empty"
      x="50%"
      y="40"
      text-anchor="middle"
      font-size="11.5"
      fill="var(--c-ink-3)"
    >no runs seen yet</text>
  </svg>
</template>
