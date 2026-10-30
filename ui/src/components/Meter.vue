<script setup lang="ts">
import { computed } from 'vue'
import { formatBytes, formatPercent } from '../lib/format'
import { fillLevel, usageRatio } from '../lib/health'

const props = withDefaults(
  defineProps<{
    usedBytes: number
    capacityBytes: number
    /** What is measured, for the meter's accessible name. */
    label: string
    /** Bar thickness: xs 4 px, sm 6 px, md 8 px. */
    size?: 'xs' | 'sm' | 'md'
    /**
     * level colours the fill by how close it is to full; neutral keeps the accent
     * at every ratio, for a count where a full bar is not a problem.
     */
    tone?: 'level' | 'neutral'
    /** Word for the remainder in the spoken value, as in "70%, 3 GiB free". */
    rest?: string
  }>(),
  { size: 'xs', tone: 'level', rest: 'free' },
)

const HEIGHT = { xs: 'h-1', sm: 'h-1.5', md: 'h-2' } as const

const FILL = { healthy: 'bg-accent', attention: 'bg-warn-bar', degraded: 'bg-crit' } as const

const meter = computed(() => {
  const ratio = usageRatio(props.usedBytes, props.capacityBytes)
  if (ratio === null) return null
  return {
    now: Math.round(ratio * 100),
    text: `${formatPercent(ratio)}, ${formatBytes(Math.max(props.capacityBytes - props.usedBytes, 0))} ${props.rest}`,
    fill: FILL[props.tone === 'neutral' ? 'healthy' : fillLevel(ratio)],
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
    class="block overflow-hidden rounded-full bg-track"
    :class="HEIGHT[size]"
  >
    <span
      class="block h-full origin-left transition-transform duration-300 ease-out motion-reduce:transition-none"
      :class="meter.fill"
      :style="meter.style"
    />
  </span>
  <span
    v-else
    class="block rounded-full bg-track"
    :class="HEIGHT[size]"
    aria-hidden="true"
  />
</template>
