<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterNode } from '../../api/status'
import { MEMBERSHIP_PRESENTATION } from '../../lib/presentation'
import { SegmentRing } from '../charts'
import Skeleton from '../Skeleton.vue'
import StateBadge from '../StateBadge.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** null while the first answer is pending. */
  nodes: ClusterNode[] | null
  /** The responding node cannot reach any peer. */
  isolated: boolean
}>()

// Only read once nodes have arrived; the loading branch shows a skeleton.
const active = computed(() => (props.nodes as ClusterNode[]).filter((n) => n.state !== 'left'))
const segments = computed(() => active.value.map((n) => ({ state: n.state, label: n.id })))
const up = computed(() => active.value.filter((n) => n.state === 'alive').length)
const suspect = computed(() => active.value.filter((n) => n.state === 'suspect').length)
const down = computed(() => active.value.filter((n) => n.state === 'dead').length)
</script>

<template>
  <Tile
    id="tile-nodes"
    title="Nodes"
  >
    <div class="mx-auto w-full max-w-[150px]">
      <Skeleton
        v-if="nodes === null"
        class="aspect-[120/108] w-full rounded-full"
      />
      <SegmentRing
        v-else
        :segments="segments"
        :value="`${up}/${active.length}`"
        :caption="active.length === 0 ? 'no nodes' : 'alive'"
        label="Nodes"
      />
    </div>
    <div
      v-if="nodes !== null"
      class="mt-auto flex h-4 justify-between gap-2 text-[11.5px] leading-4 whitespace-nowrap text-ink-3"
    >
      <span :class="{ 'font-medium text-warn': suspect > 0 }">{{ suspect }} suspect</span>
      <StateBadge
        v-if="isolated"
        :presentation="MEMBERSHIP_PRESENTATION.isolated"
      />
      <span
        v-else
        :class="{ 'font-medium text-crit': down > 0 }"
      >{{ down }} down</span>
    </div>
  </Tile>
</template>
