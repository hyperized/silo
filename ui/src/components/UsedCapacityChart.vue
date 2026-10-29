<!--
  Used bytes per node over time, stacked so the top edge is the cluster total.
  The chart is drawn at the pixel width of its container (measured with a
  ResizeObserver) so axis text keeps its size on a phone instead of scaling
  down with a fixed viewBox.
-->
<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { CHART_BUCKETS, SPAN_MS, type HistorySpan, type UsedByNode } from '../composables/useHistory'
import { POLL_INTERVAL_MS } from '../composables/useStatusPoll'
import { seriesOpacity } from '../lib/chart'
import { formatBytes, formatClock } from '../lib/format'
import { FULL_WARN } from '../lib/health'
import Card from './Card.vue'
import { StackedArea } from './charts'

const props = defineProps<{
  usedByNode: UsedByNode
  span: HistorySpan
  /** Cluster capacity in bytes; 0 when unknown, which hides the limit line. */
  capacityBytes: number
  now: number
}>()

const emit = defineEmits<{ 'update:span': [span: HistorySpan] }>()

const SPANS: readonly { value: HistorySpan; label: string }[] = [
  { value: '1h', label: '1 h' },
  { value: '6h', label: '6 h' },
  { value: '24h', label: '24 h' },
]
const SPAN_WORDS: Readonly<Record<HistorySpan, string>> = { '1h': 'hour', '6h': '6 hours', '24h': '24 hours' }
// The limit line stretches the scale only while usage is within this factor
// of it; further away it would squash the curves into a flat band.
const LIMIT_REACH = 2

const box = ref<HTMLElement | null>(null)
const width = ref(860)
let observer: ResizeObserver | null = null

onMounted(() => {
  if (typeof ResizeObserver === 'undefined') return
  observer = new ResizeObserver((entries) => {
    const w = entries[0]?.contentRect.width ?? 0
    if (w > 0) width.value = Math.max(240, Math.round(w))
  })
  // The box is rendered unconditionally, so it exists once mounted.
  observer.observe(box.value as HTMLElement)
})

onBeforeUnmount(() => observer?.disconnect())

const height = computed(() => (width.value < 520 ? 160 : 220))

const peak = computed(() => {
  const { times, series } = props.usedByNode
  return Math.max(0, ...times.map((_, i) => series.reduce((a, s) => a + (s.values[i] ?? 0), 0)))
})

const limit = computed(() => {
  if (props.capacityBytes <= 0) return null
  const value = props.capacityBytes * FULL_WARN
  return { value, label: `80 % of ${formatBytes(props.capacityBytes)}`, onScale: peak.value > 0 && value <= peak.value * LIMIT_REACH }
})

const yMax = computed(() => {
  const l = limit.value
  return l?.onScale ? Math.max(peak.value * 1.1, l.value * 1.08) : undefined
})

function formatX(t: number): string {
  return props.now - t < SPAN_MS[props.span] / CHART_BUCKETS ? 'now' : formatClock(new Date(t))
}

const legend = computed(() => props.usedByNode.series.map((s, i) => ({ name: s.name, opacity: seriesOpacity(i) })))
</script>

<template>
  <Card
    id="used-capacity"
    title="Used capacity, per node"
  >
    <template #meta>
      <div
        role="group"
        aria-label="Chart range"
        class="flex gap-1.5"
      >
        <button
          v-for="s in SPANS"
          :key="s.value"
          type="button"
          class="relative h-[26px] rounded-md border border-rule px-2.5 text-xs transition-colors duration-100 ease-out before:absolute before:inset-x-0 before:-inset-y-2.5 before:content-[''] motion-reduce:transition-none"
          :class="span === s.value ? 'bg-raise text-ink' : 'text-ink-3 hover:text-ink active:bg-track'"
          :aria-pressed="span === s.value"
          @click="emit('update:span', s.value)"
        >
          {{ s.label }}
        </button>
      </div>
    </template>
    <div ref="box">
      <StackedArea
        :series="usedByNode.series"
        :times="usedByNode.times"
        :label="`Used capacity per node over the last ${SPAN_WORDS[span]}`"
        :y-max="yMax"
        :limit="limit?.onScale ? limit : undefined"
        :format-y="formatBytes"
        :format-x="formatX"
        :width="width"
        :height="height"
      />
    </div>
    <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11.5px] text-ink-3">
      <span
        v-for="l in legend"
        :key="l.name"
        class="inline-flex items-center gap-1.5 font-mono"
      ><span
        class="size-2.5 rounded-sm bg-accent"
        :style="{ opacity: l.opacity }"
        aria-hidden="true"
      />{{ l.name }}</span>
      <span
        v-if="limit && !limit.onScale"
        class="inline-flex items-center gap-1.5"
        data-part="limit-note"
      ><span
        class="w-3 border-t border-dashed border-warn"
        aria-hidden="true"
      />{{ limit.label }}, far above the curves</span>
      <span class="ml-auto">Sampled by this page every {{ POLL_INTERVAL_MS / 1000 }} s; 6 h and 24 h fill in while it stays open</span>
    </div>
  </Card>
</template>
