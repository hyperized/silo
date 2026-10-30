<script setup lang="ts">
import { computed } from 'vue'
import { formatHorizon, formatOf, formatPercent, formatRate } from '../../lib/format'
import type { Capacity } from '../../lib/health'
import { ArcGauge } from '../charts'
import Skeleton from '../Skeleton.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** undefined while loading, null when no node reports a disk. */
  capacity: Capacity | null | undefined
  /** Trend of used bytes over the chart span; null until two samples exist. */
  ratePerHour: number | null
  secondsUntilFull: number | null
}>()

// Beyond a few years the projection is noise from a nearly flat line.
const HORIZON_MAX_S = 5 * 365 * 86400

const ratio = computed(() => (props.capacity ? props.capacity.usedBytes / props.capacity.capacityBytes : null))
const caption = computed(() =>
  props.capacity ? formatOf(props.capacity.usedBytes, props.capacity.capacityBytes) : 'not reported',
)
const full = computed(() => {
  const s = props.secondsUntilFull
  return s !== null && s > 0 && s < HORIZON_MAX_S ? `full in ${formatHorizon(s)}` : ''
})
</script>

<template>
  <Tile
    id="tile-capacity"
    title="Cluster capacity"
  >
    <div class="mx-auto w-full max-w-[150px]">
      <Skeleton
        v-if="capacity === undefined"
        class="aspect-[120/114] w-full rounded-full"
      />
      <ArcGauge
        v-else
        :ratio="ratio"
        label="Capacity"
        :value="ratio === null ? '' : formatPercent(ratio)"
        :caption="caption"
      />
    </div>
    <div class="mt-auto flex h-4 justify-between gap-2 text-[11.5px] leading-4 whitespace-nowrap text-ink-3">
      <span>{{ capacity && ratePerHour !== null ? formatRate(ratePerHour, 'h') : '' }}</span>
      <span>{{ capacity ? full : '' }}</span>
    </div>
  </Tile>
</template>
