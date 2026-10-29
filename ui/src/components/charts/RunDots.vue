<script setup lang="ts">
/*
 * Run outcomes on a timeline, oldest on the left, with an optional hollow dot
 * for the next scheduled run. A failed dot carries a cross so the outcome does
 * not depend on telling red from green.
 */
import { computed } from 'vue'
import { num } from '../../lib/chart'

const props = withDefaults(
  defineProps<{
    runs: { ok: boolean; label: string }[]
    /** Label of the next scheduled run; draws a dashed hollow dot at the end when set. */
    scheduled?: string
    label: string
  }>(),
  { scheduled: '' },
)

const R = 5

const slots = computed(() => props.runs.length + (props.scheduled ? 1 : 0))

function at(i: number): number {
  return slots.value === 1 ? 50 : num(4 + (i * 92) / (slots.value - 1))
}

const dots = computed(() =>
  props.runs.map((r, i) => ({
    key: i,
    ok: r.ok,
    label: r.label,
    cx: `${String(at(i))}%`,
    fill: r.ok ? 'var(--c-good)' : 'var(--c-crit)',
  })),
)

const next = computed(() => (props.scheduled ? { cx: `${String(at(props.runs.length))}%` } : null))

const ariaLabel = computed(() => {
  const n = props.runs.length
  const parts: string[] = []
  if (n === 0) parts.push('no runs seen yet')
  else {
    const failed = props.runs.filter((r) => !r.ok).length
    const last = props.runs[n - 1] as { ok: boolean }
    parts.push(`${String(n)} ${n === 1 ? 'run' : 'runs'}, ${String(failed)} failed`)
    parts.push(`last ${last.ok ? 'succeeded' : 'failed'}`)
  }
  if (props.scheduled) parts.push(`next ${props.scheduled}`)
  return `${props.label}: ${parts.join(', ')}`
})
</script>

<template>
  <svg
    width="100%"
    height="40"
    role="img"
    :aria-label="ariaLabel"
    class="block"
  >
    <line
      x1="0"
      y1="20"
      x2="100%"
      y2="20"
      stroke="var(--c-rule)"
      stroke-width="2"
    />
    <g
      v-for="dot in dots"
      :key="dot.key"
      data-part="run"
      :data-ok="dot.ok"
    >
      <title>{{ dot.label }}</title>
      <circle
        :cx="dot.cx"
        cy="20"
        :r="R"
        :fill="dot.fill"
      />
      <svg
        v-if="!dot.ok"
        data-part="cross"
        :x="dot.cx"
        y="20"
        overflow="visible"
      >
        <path
          d="M-2.2,-2.2 L2.2,2.2 M2.2,-2.2 L-2.2,2.2"
          stroke="var(--c-panel)"
          stroke-width="1.5"
          stroke-linecap="round"
        />
      </svg>
    </g>
    <g
      v-if="next"
      data-part="scheduled"
    >
      <title>{{ scheduled }}</title>
      <circle
        :cx="next.cx"
        cy="20"
        :r="R"
        fill="none"
        stroke="var(--c-ink-3)"
        stroke-width="1.5"
        stroke-dasharray="2 2"
      />
    </g>
    <text
      v-if="slots === 0"
      data-part="empty"
      x="50%"
      y="14"
      text-anchor="middle"
      font-size="11.5"
      fill="var(--c-ink-3)"
    >no runs seen yet</text>
  </svg>
</template>
