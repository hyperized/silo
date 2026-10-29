<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterNode, NodeState } from '../api/status'
import { formatBytes, formatPercent, formatRate, formatSkew } from '../lib/format'
import { fillLevel, nodeLevel, skewLevel, usageRatio, worst } from '../lib/health'
import { NODE_PRESENTATION, PRESSURED_PRESENTATION, skewPresentation } from '../lib/presentation'
import { ArcGauge, Sparkline } from './charts'
import RelTime from './RelTime.vue'
import StateBadge from './StateBadge.vue'
import StatusIcon from './StatusIcon.vue'

const props = defineProps<{
  node: ClusterNode
  /** This is the node that answered the status request. */
  self: boolean
  now: number
  /** Bytes per second written to volumes whose lease this node holds, per recent sample. */
  spark: number[]
}>()

// The key reads as the state the duration belongs to: "Up 41 min", "Down 3 min".
const SINCE: Readonly<Record<NodeState, string>> = {
  alive: 'Up',
  suspect: 'Suspect',
  dead: 'Down',
  left: 'Left',
  unknown: 'Seen',
}
const BORDER = { healthy: 'border-rule', attention: 'border-warn-bar', degraded: 'border-crit' } as const
const TAG = { attention: 'bg-warn-wash text-warn', degraded: 'bg-crit-wash text-crit' } as const

const ratio = computed(() => usageRatio(props.node.usedBytes, props.node.capacityBytes))
const fill = computed(() => fillLevel(ratio.value))
const tone = computed(() => worst(fill.value, nodeLevel(props.node.state)))
const skew = computed(() => skewLevel(props.node.clockSkewMs))
const rate = computed(() => props.spark.at(-1) ?? null)
const titleId = computed(() => `node-${props.node.id}-title`)
</script>

<template>
  <article
    :aria-labelledby="titleId"
    class="flex min-w-0 flex-col gap-3 rounded-[10px] border bg-panel px-4 py-3.5 max-sm:p-3"
    :class="[BORDER[tone], { 'text-ink-3': node.state === 'left' }]"
  >
    <header class="flex items-center justify-between gap-2">
      <div class="flex min-w-0 flex-wrap items-center gap-2">
        <h3
          :id="titleId"
          class="truncate font-mono text-[13px] font-medium"
        >
          {{ node.id }}
        </h3>
        <span
          v-if="self"
          class="text-[11px] text-ink-3"
        >this node</span>
        <span
          v-if="fill !== 'healthy' && ratio !== null"
          data-part="disk-tag"
          class="inline-flex items-center gap-1 rounded px-1.5 py-px text-[11px]"
          :class="TAG[fill]"
        ><StatusIcon
          :name="fill === 'degraded' ? 'octagon' : 'alert'"
          :size="11"
        />Disk {{ formatPercent(ratio) }}</span>
        <span
          v-if="node.pressured"
          class="text-[11px]"
        ><StateBadge
          :presentation="PRESSURED_PRESENTATION"
          chip
        /></span>
      </div>
      <span class="text-xs"><StateBadge :presentation="NODE_PRESENTATION[node.state]" /></span>
    </header>

    <div class="flex items-center gap-3.5">
      <div class="w-16 shrink-0">
        <ArcGauge
          :ratio="ratio"
          :label="`Disk of ${node.id}`"
          :value="ratio === null ? '' : formatPercent(ratio)"
        />
      </div>
      <dl class="grid min-w-0 flex-1 grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs text-ink-3">
        <dt>Used</dt>
        <dd class="text-right text-ink">
          {{ node.capacityBytes > 0 ? formatBytes(node.usedBytes) : 'n/a' }}
        </dd>
        <dt>Free</dt>
        <dd class="text-right text-ink">
          {{ node.capacityBytes > 0 ? formatBytes(Math.max(node.capacityBytes - node.usedBytes, 0)) : 'n/a' }}
        </dd>
        <dt>Skew</dt>
        <dd class="text-right text-ink">
          <StateBadge
            v-if="skew !== 'healthy'"
            :presentation="skewPresentation(formatSkew(node.clockSkewMs), skew)"
          />
          <template v-else>
            {{ self ? 'reference' : formatSkew(node.clockSkewMs) }}
          </template>
        </dd>
        <dt>{{ SINCE[node.state] }}</dt>
        <dd class="text-right text-ink">
          <RelTime
            v-if="node.lastChange"
            :date="node.lastChange"
            :now="now"
            mode="for"
          />
          <template v-else>
            n/a
          </template>
        </dd>
      </dl>
    </div>

    <Sparkline
      :points="spark"
      :height="30"
      :width="240"
      :label="rate === null ? `Writes through ${node.id}` : `Writes through ${node.id}, ${formatRate(rate, 's')} now`"
    />
    <div class="flex justify-between gap-2 text-[11.5px] text-ink-3">
      <span
        class="truncate font-mono"
        :title="`Gossip ${node.gossipAddress}`"
      >{{ node.dataAddress }}</span>
      <span
        v-if="rate !== null"
        class="whitespace-nowrap"
      >{{ formatRate(rate, 's') }} written</span>
    </div>
  </article>
</template>
