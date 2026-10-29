<script setup lang="ts">
/*
 * A value against warn and crit thresholds on a piecewise scale (see bulletX).
 * Geometry is in percent of the width rather than a stretched viewBox, so the
 * rounded ends and the 1.5 px markers keep their shape at any width.
 */
import { computed } from 'vue'
import { bulletX } from '../../lib/chart'
import { formatCount } from '../../lib/format'
import type { Level } from '../../lib/health'

const props = withDefaults(
  defineProps<{
    value: number
    warn: number
    crit: number
    /** Unit spoken after each number in the accessible name, e.g. "ms". */
    unit: string
    label: string
    /** Formats a bare number for the scale and the accessible name; the unit is appended separately. */
    format?: (v: number) => string
  }>(),
  { format: formatCount },
)

const FILL: Readonly<Record<Level, string>> = {
  healthy: 'var(--c-good)',
  attention: 'var(--c-warn-bar)',
  degraded: 'var(--c-crit)',
}
const STATE: Readonly<Record<Level, string>> = {
  healthy: '',
  attention: ', over the warning threshold',
  degraded: ', over the critical threshold',
}

const level = computed<Level>(() => {
  if (props.value >= props.crit) return 'degraded'
  if (props.value >= props.warn) return 'attention'
  return 'healthy'
})

const width = computed(() => bulletX(props.value, props.warn, props.crit, 100))
const warnX = computed(() => bulletX(props.warn, props.warn, props.crit, 100))
const critX = computed(() => bulletX(props.crit, props.warn, props.crit, 100))

// A marker the fill has crossed switches to ink so it stays visible on top of the fill colour.
const warnStroke = computed(() => (level.value === 'healthy' ? 'var(--c-warn)' : 'var(--c-ink)'))
const critStroke = computed(() => (level.value === 'degraded' ? 'var(--c-ink)' : 'var(--c-crit)'))

const ariaLabel = computed(() => {
  const f = props.format
  return `${props.label} ${f(props.value)} ${props.unit}${STATE[level.value]}, warning at ${f(props.warn)}, critical at ${f(props.crit)}`
})
</script>

<template>
  <div>
    <svg
      width="100%"
      height="22"
      role="img"
      :aria-label="ariaLabel"
      class="block"
    >
      <rect
        data-part="track"
        x="0"
        y="8"
        width="100%"
        height="6"
        rx="3"
        fill="var(--c-track)"
      />
      <rect
        v-if="width > 0"
        data-part="fill"
        x="0"
        y="8"
        :width="`${String(width)}%`"
        height="6"
        rx="3"
        :fill="FILL[level]"
        class="transition-[width] duration-300 ease-out motion-reduce:transition-none"
      />
      <rect
        data-part="warn"
        :x="`${String(warnX)}%`"
        y="4"
        width="1.5"
        height="14"
        :fill="warnStroke"
      />
      <rect
        data-part="crit"
        :x="`${String(critX)}%`"
        y="4"
        width="1.5"
        height="14"
        :fill="critStroke"
      />
    </svg>
    <div
      class="relative h-4 text-[11px] leading-4 text-ink-3"
      aria-hidden="true"
    >
      <span class="absolute left-0">0</span>
      <span class="absolute left-3/5 -translate-x-1/2">{{ format(warn) }}</span>
      <span class="absolute right-0">{{ format(crit) }}</span>
    </div>
  </div>
</template>
