<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterHealth } from '../../api/status'
import { formatClock, formatCount } from '../../lib/format'
import { replicationKind } from '../../lib/health'
import { REPLICATION_PRESENTATION } from '../../lib/presentation'
import Skeleton from '../Skeleton.vue'
import StateBadge from '../StateBadge.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** undefined while loading, null when silod is older than the health report. */
  health: ClusterHealth | null | undefined
}>()

const kind = computed(() => replicationKind(props.health))

// One bar per replica the factor asks for; the ones alive nodes cannot cover stay hollow.
const bars = computed(() => {
  const h = props.health as ClusterHealth
  return Array.from({ length: h.replicationFactor }, (_, i) => i < h.aliveNodes)
})

const rejoins = computed(() => {
  const h = props.health as ClusterHealth
  const n = `${formatCount(h.rejoinAttempts)} ${h.rejoinAttempts === 1 ? 'rejoin' : 'rejoins'}`
  return h.startedAt ? `${n} since ${formatClock(h.startedAt)}` : n
})
</script>

<template>
  <Tile
    id="tile-replication"
    title="Replication"
  >
    <template v-if="health === undefined">
      <Skeleton class="mt-1.5 h-[30px] w-12" />
      <Skeleton class="mt-1 h-5 w-20" />
    </template>
    <template v-else-if="health && kind">
      <p class="mt-1.5 flex items-baseline gap-2">
        <span class="text-[30px] leading-none font-semibold">{{ health.replicationFactor }}</span>
        <span class="text-xs text-ink-3">factor</span>
      </p>
      <span class="mt-1 text-[13px]"><StateBadge :presentation="REPLICATION_PRESENTATION[kind]" /></span>
      <div
        class="mt-2 flex gap-1"
        :title="`${health.aliveNodes} of ${health.replicationFactor} replicas can be placed on alive nodes`"
        aria-hidden="true"
      >
        <span
          v-for="(covered, i) in bars"
          :key="i"
          data-part="bar"
          :data-covered="covered"
          class="h-1.5 flex-1 rounded-full"
          :class="covered ? 'bg-good' : 'border border-crit'"
        />
      </div>
      <p class="mt-auto text-[11.5px] text-ink-3">
        {{ rejoins }}
      </p>
    </template>
    <template v-else>
      <p class="mt-1.5 text-[30px] leading-none font-semibold text-ink-3">
        n/a
      </p>
      <p class="mt-auto text-[11.5px] text-ink-3">
        not reported
      </p>
    </template>
  </Tile>
</template>
