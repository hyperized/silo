<script setup lang="ts">
/*
 * Stacked history with a hover readout. Pointer and keyboard both move one
 * cursor index; the readout is mirrored into a visually hidden live region
 * because text inside an svg with role="img" is not exposed to screen readers.
 */
import { computed, ref } from 'vue'
import { num, nearestIndex, seriesOpacity, stackPaths, xAt } from '../../lib/chart'

const props = withDefaults(
  defineProps<{
    /** One entry per stack, series[0] at the bottom; values align with times. */
    series: { name: string; values: number[] }[]
    /** Sample times in unix milliseconds. */
    times: number[]
    label: string
    /** Value at the top of the plot; defaults to the largest stacked total plus 10 %. */
    yMax?: number | undefined
    /** Dashed warn line, e.g. a capacity threshold. Hidden when it falls outside 0..yMax. */
    limit?: { value: number; label: string } | undefined
    formatY: (v: number) => string
    formatX: (t: number) => string
    width?: number
    height?: number
  }>(),
  { yMax: undefined, limit: undefined, width: 860, height: 220 },
)

const emit = defineEmits<{ hover: [index: number | null] }>()

// Room for a y label such as "1023 MiB" at 10.5 px.
const LEFT = 56
const TOP = 20
const BOTTOM = 40
const BOX_W = 150

const plotW = computed(() => props.width - LEFT)
const plotH = computed(() => props.height - TOP - BOTTOM)
const count = computed(() => props.times.length)
// Values padded or cut to the sample count, so every index below count is defined.
const aligned = computed(() =>
  props.series.map((s) => ({ name: s.name, values: props.times.map((_, i) => s.values[i] ?? 0) })),
)

const top = computed(() => {
  if (props.yMax !== undefined && props.yMax > 0) return props.yMax
  const totals = props.times.map((_, i) => aligned.value.reduce((a, s) => a + Math.max(s.values[i] as number, 0), 0))
  const peak = Math.max(0, ...totals.filter((t) => Number.isFinite(t)))
  return peak > 0 ? peak * 1.1 : 1
})

const paths = computed(() => stackPaths(aligned.value.map((s) => s.values), plotW.value, plotH.value, top.value))

const grid = computed(() =>
  [0, 1, 2, 3, 4].map((k) => ({
    y: num(TOP + (plotH.value * k) / 4),
    text: props.formatY((top.value * (4 - k)) / 4),
    base: k === 4,
  })),
)

const xLabels = computed(() => {
  const n = count.value
  const picks = n <= 2 ? [...Array(n).keys()] : [0, Math.floor((n - 1) / 2), n - 1]
  return picks.map((i) => ({
    x: num(LEFT + xAt(i, n, plotW.value)),
    anchor: i === 0 ? 'start' : i === n - 1 ? 'end' : 'middle',
    text: props.formatX(props.times[i] as number),
  }))
})

const limitLine = computed(() => {
  const l = props.limit
  if (!l || count.value === 0 || l.value < 0 || l.value > top.value) return null
  return { y: num(TOP + plotH.value - (l.value / top.value) * plotH.value), label: l.label }
})

const cursor = ref<number | null>(null)

function setCursor(i: number | null) {
  if (i === cursor.value) return
  cursor.value = i
  emit('hover', i)
}

const active = computed(() => (cursor.value !== null && cursor.value < count.value ? cursor.value : null))

const readout = computed(() => {
  const i = active.value
  if (i === null) return null
  const x = num(LEFT + xAt(i, count.value, plotW.value))
  const boxX = x < LEFT + plotW.value / 2 ? x + 10 : x - 10 - BOX_W
  return {
    x,
    boxX,
    boxY: TOP + 6,
    boxH: 26 + 14 * props.series.length,
    time: props.formatX(props.times[i] as number),
    rows: aligned.value.map((s) => ({ name: s.name, value: props.formatY(s.values[i] as number) })),
  }
})

const spoken = computed(() => {
  const r = readout.value
  if (!r) return ''
  return `${r.time}: ${r.rows.map((row) => `${row.name} ${row.value}`).join(', ')}`
})

function track(e: MouseEvent) {
  // The listener is bound on the svg itself, so currentTarget is always that element.
  const rect = (e.currentTarget as SVGSVGElement).getBoundingClientRect()
  if (rect.width <= 0) return
  const x = ((e.clientX - rect.left) / rect.width) * props.width - LEFT
  setCursor(nearestIndex(x, count.value, plotW.value))
}

function key(e: KeyboardEvent) {
  if (e.key === 'Escape') {
    setCursor(null)
    return
  }
  if ((e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') || count.value === 0) return
  e.preventDefault()
  const last = count.value - 1
  if (active.value === null) {
    setCursor(last)
    return
  }
  const step = e.key === 'ArrowLeft' ? -1 : 1
  setCursor(Math.min(Math.max(active.value + step, 0), last))
}

const ariaLabel = computed(() => (count.value === 0 ? `${props.label}, collecting samples` : props.label))
</script>

<template>
  <div>
    <svg
      :viewBox="`0 0 ${String(width)} ${String(height)}`"
      role="img"
      :aria-label="ariaLabel"
      tabindex="0"
      class="block w-full touch-pan-y"
      @pointermove="track"
      @pointerleave="setCursor(null)"
      @keydown="key"
    >
      <line
        v-for="g in grid"
        :key="g.y"
        data-part="grid"
        :x1="LEFT"
        :y1="g.y"
        :x2="width"
        :y2="g.y"
        stroke="var(--c-rule)"
        :stroke-dasharray="g.base ? undefined : '2 4'"
      />
      <text
        v-if="count === 0"
        data-part="empty"
        :x="LEFT + plotW / 2"
        :y="TOP + plotH / 2"
        text-anchor="middle"
        font-size="12"
        fill="var(--c-ink-3)"
      >collecting samples</text>
      <template v-else>
        <text
          v-for="g in grid"
          :key="g.y"
          data-part="ylabel"
          :x="LEFT - 6"
          :y="g.y + 4"
          text-anchor="end"
          font-size="10.5"
          fill="var(--c-ink-3)"
        >{{ g.text }}</text>
        <g :transform="`translate(${String(LEFT)},${String(TOP)})`">
          <path
            v-for="(d, i) in paths.areas"
            :key="i"
            data-part="area"
            :d="d"
            fill="var(--c-accent)"
            :opacity="seriesOpacity(i)"
          />
          <polyline
            v-for="(p, i) in paths.separators"
            :key="i"
            data-part="separator"
            :points="p"
            fill="none"
            stroke="var(--c-page)"
            stroke-width="1"
          />
        </g>
        <template v-if="limitLine">
          <line
            data-part="limit"
            :x1="LEFT"
            :y1="limitLine.y"
            :x2="width"
            :y2="limitLine.y"
            stroke="var(--c-warn)"
            stroke-dasharray="4 4"
          />
          <text
            :x="width - 4"
            :y="limitLine.y + 12"
            text-anchor="end"
            font-size="10.5"
            fill="var(--c-warn)"
          >{{ limitLine.label }}</text>
        </template>
        <text
          v-for="l in xLabels"
          :key="l.x"
          data-part="xlabel"
          :x="l.x"
          :y="height - 20"
          :text-anchor="l.anchor"
          font-size="10.5"
          fill="var(--c-ink-3)"
        >{{ l.text }}</text>
        <g
          v-if="readout"
          data-part="readout"
        >
          <line
            :x1="readout.x"
            :y1="TOP"
            :x2="readout.x"
            :y2="TOP + plotH"
            stroke="var(--c-ink-2)"
            stroke-dasharray="2 3"
          />
          <rect
            :x="readout.boxX"
            :y="readout.boxY"
            :width="BOX_W"
            :height="readout.boxH"
            rx="6"
            fill="var(--c-raise)"
            stroke="var(--c-rule)"
          />
          <text
            :x="readout.boxX + 10"
            :y="readout.boxY + 16"
            font-size="10.5"
            fill="var(--c-ink-3)"
          >{{ readout.time }}</text>
          <g
            v-for="(row, j) in readout.rows"
            :key="row.name"
            font-size="11"
            fill="var(--c-ink)"
          >
            <text
              :x="readout.boxX + 10"
              :y="readout.boxY + 32 + 14 * j"
            >{{ row.name }}</text>
            <text
              :x="readout.boxX + BOX_W - 10"
              :y="readout.boxY + 32 + 14 * j"
              text-anchor="end"
            >{{ row.value }}</text>
          </g>
        </g>
      </template>
    </svg>
    <p
      class="sr-only"
      aria-live="polite"
    >
      {{ spoken }}
    </p>
  </div>
</template>
