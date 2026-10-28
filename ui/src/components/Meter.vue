<script setup lang="ts">
import { computed } from 'vue'
import { formatBytes, formatPercent } from '../lib/format'
import { fillLevel, usageRatio } from '../lib/health'

const props = defineProps<{
  usedBytes: number
  capacityBytes: number
  /** What is measured, for the meter's accessible name. */
  label: string
}>()

const FILL = { healthy: 'bg-accent', attention: 'bg-warn-bar', degraded: 'bg-crit' } as const

const meter = computed(() => {
  const ratio = usageRatio(props.usedBytes, props.capacityBytes)
  if (ratio === null) return null
  return {
    now: Math.round(ratio * 100),
    text: `${formatPercent(ratio)}, ${formatBytes(Math.max(props.capacityBytes - props.usedBytes, 0))} free`,
    fill: FILL[fillLevel(ratio)],
    style: { transform: `scaleX(${String(ratio)})` },
  }
})
</script>

<template>
  <span
    v-if="meter"
    role="meter"
    :aria-label="label"
    aria-valuemin="0"
    aria-valuemax="100"
    :aria-valuenow="meter.now"
    :aria-valuetext="meter.text"
    class="block h-1 overflow-hidden rounded-full bg-track"
  >
    <span
      class="block h-full origin-left transition-transform duration-300 ease-out motion-reduce:transition-none"
      :class="meter.fill"
      :style="meter.style"
    />
  </span>
  <span
    v-else
    class="block h-1 rounded-full bg-track"
    aria-hidden="true"
  />
</template>
