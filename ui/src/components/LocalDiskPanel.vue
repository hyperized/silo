<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterNode, LocalStorage } from '../api/status'
import { formatBytes, formatCompact, formatOf, formatPercent, formatRate } from '../lib/format'
import { fillLevel, usageRatio, type Level } from '../lib/health'
import Card from './Card.vue'
import Meter from './Meter.vue'
import Skeleton from './Skeleton.vue'
import StatusIcon from './StatusIcon.vue'

const props = defineProps<{
  /** null while the first answer is pending. */
  nodes: ClusterNode[] | null
  /** The responding node's disk; undefined while loading, null when not reported. */
  storage: LocalStorage | null | undefined
  /** Cluster growth from the history; null until two samples exist. */
  ratePerHour: number | null
}>()

const TONE: Readonly<Record<Level, string>> = { healthy: 'text-ink-2', attention: 'text-warn', degraded: 'text-crit' }

// Only read once nodes have arrived; the loading branch shows skeletons and no stats.
const rows = computed(() =>
  (props.nodes as ClusterNode[])
    .filter((n) => n.state !== 'left')
    .map((n) => {
      const ratio = usageRatio(n.usedBytes, n.capacityBytes)
      return { node: n, ratio, level: fillLevel(ratio) }
    }),
)

const pressured = computed(() => rows.value.filter((r) => r.node.pressured).length)

const stats = computed(() => [
  { label: 'Chunks here', value: props.storage ? formatCompact(props.storage.chunkCount) : 'n/a' },
  { label: 'Free here', value: props.storage ? formatBytes(props.storage.availableBytes) : 'n/a' },
  { label: 'Pressured', value: `${String(pressured.value)} ${pressured.value === 1 ? 'node' : 'nodes'}` },
  { label: 'Growth', value: props.ratePerHour === null ? 'measuring' : formatRate(props.ratePerHour, 'h') },
])
</script>

<template>
  <Card
    id="local-disk"
    title="Local disk, per node"
  >
    <template #meta>
      <span
        v-if="storage?.dataDir"
        class="min-w-0 truncate font-mono text-[11.5px] text-ink-3"
        title="Data directory of the responding node"
      >{{ storage.dataDir }}</span>
    </template>
    <div
      v-if="nodes === null"
      class="space-y-4"
      aria-hidden="true"
    >
      <Skeleton
        v-for="i in 3"
        :key="i"
        class="h-7"
      />
    </div>
    <p
      v-else-if="rows.length === 0"
      class="text-ink-3"
    >
      No nodes
    </p>
    <ul
      v-else
      class="flex flex-col gap-3.5"
    >
      <li
        v-for="r in rows"
        :key="r.node.id"
        class="flex flex-col gap-1.5"
      >
        <span class="flex items-center justify-between gap-2 text-[12.5px]">
          <span class="font-mono text-xs">{{ r.node.id }}</span>
          <span
            v-if="r.ratio !== null"
            class="inline-flex items-center gap-1 whitespace-nowrap"
            :class="TONE[r.level]"
          ><StatusIcon
            v-if="r.level !== 'healthy'"
            :name="r.level === 'degraded' ? 'octagon' : 'alert'"
            :size="12"
          />{{ formatOf(r.node.usedBytes, r.node.capacityBytes) }} · {{ formatPercent(r.ratio) }}</span>
          <span
            v-else
            class="text-ink-3"
          >not reported</span>
        </span>
        <Meter
          :used-bytes="r.node.usedBytes"
          :capacity-bytes="r.node.capacityBytes"
          :label="`Disk of ${r.node.id}`"
          size="md"
        />
      </li>
    </ul>
    <dl
      v-if="nodes !== null"
      class="mt-1 grid grid-cols-2 gap-2.5 border-t border-rule pt-3 text-xs text-ink-3"
    >
      <div
        v-for="s in stats"
        :key="s.label"
      >
        <dt>{{ s.label }}</dt>
        <dd class="text-base font-semibold text-ink">
          {{ s.value }}
        </dd>
      </div>
    </dl>
  </Card>
</template>
