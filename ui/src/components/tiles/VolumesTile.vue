<script setup lang="ts">
import { computed } from 'vue'
import type { Volume } from '../../api/status'
import { formatBytes, formatRate } from '../../lib/format'
import type { Presentation } from '../../lib/presentation'
import { Sparkline } from '../charts'
import Skeleton from '../Skeleton.vue'
import StateBadge from '../StateBadge.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** null while the first answer is pending. */
  volumes: Volume[] | null
  degraded: number
  damaged: number
  /** Cluster write rate in bytes per second, one point per recent sample. */
  spark: number[]
  writeRate: number | null
}>()

// These are only read once volumes have arrived; the loading branch shows skeletons.
const loaded = computed(() => props.volumes as Volume[])
const provisioned = computed(() => loaded.value.reduce((a, v) => a + v.sizeBytes, 0))
const attached = computed(() => loaded.value.filter((v) => v.attachedHere).length)

const state = computed<Presentation>(() => {
  if (props.damaged > 0) return { label: `${String(props.damaged)} map damaged`, tone: 'crit', icon: 'octagon' }
  if (props.degraded > 0) return { label: `${String(props.degraded)} degraded`, tone: 'warn', icon: 'alert' }
  if (loaded.value.length === 0) return { label: 'None yet', tone: 'neutral', icon: 'minus' }
  return { label: 'All healthy', tone: 'good', icon: 'check' }
})

const footer = computed(() =>
  [props.writeRate === null ? '' : `${formatRate(props.writeRate, 's')} written`, `${String(attached.value)} attached here`]
    .filter((s) => s !== '')
    .join(', '),
)

const sparkLabel = computed(() =>
  props.writeRate === null ? 'Cluster write rate' : `Cluster write rate, ${formatRate(props.writeRate, 's')} now`,
)
</script>

<template>
  <Tile
    id="tile-volumes"
    title="Volumes"
  >
    <template v-if="volumes === null">
      <Skeleton class="mt-1.5 h-[30px] w-12" />
      <Skeleton class="mt-1 h-5 w-24" />
      <Skeleton class="mt-1.5 h-7" />
    </template>
    <template v-else>
      <p class="mt-1.5 flex items-baseline gap-2">
        <span class="text-[30px] leading-none font-semibold">{{ volumes.length }}</span>
        <span
          v-if="volumes.length > 0"
          class="text-xs text-ink-3"
        >{{ formatBytes(provisioned) }}</span>
      </p>
      <span class="mt-1 text-[13px]"><StateBadge :presentation="state" /></span>
      <div class="mt-1.5">
        <Sparkline
          :points="spark"
          :label="sparkLabel"
        />
      </div>
      <p class="mt-auto text-[11.5px] text-ink-3">
        {{ footer }}
      </p>
    </template>
  </Tile>
</template>
