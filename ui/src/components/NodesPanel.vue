<script setup lang="ts">
import type { ClusterNode, LocalStorage } from '../api/status'
import { formatBytes, formatCount, formatPercent, formatUsedOf } from '../lib/format'
import { usageRatio } from '../lib/health'
import { NODE_PRESENTATION } from '../lib/presentation'
import Meter from './Meter.vue'
import Panel from './Panel.vue'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'
import Tip from './Tip.vue'

defineProps<{
  /** null while the first answer is pending. */
  nodes: ClusterNode[] | null
  respondingNodeId: string
  /** undefined while loading, null when the responding node has not reported its disk. */
  storage: LocalStorage | null | undefined
  now: number
}>()

const TH = 'h-8 px-3 text-xs font-medium whitespace-nowrap text-ink-3'
const TD = 'h-9 px-3 py-1.5'

// Only rendered for nodes that report a capacity, so the ratio is never null here.
function pct(n: ClusterNode): string {
  return formatPercent(usageRatio(n.usedBytes, n.capacityBytes) as number)
}
</script>

<template>
  <Panel
    id="nodes"
    title="Nodes"
    :count="nodes?.length"
  >
    <p
      v-if="nodes && nodes.length === 0"
      class="px-3 py-3 text-ink-2"
    >
      No nodes
    </p>

    <table
      v-else
      class="stack w-full text-left text-[13px]"
    >
      <thead class="border-b border-rule">
        <tr>
          <th
            scope="col"
            :class="TH"
          >
            Node
          </th>
          <th
            scope="col"
            :class="TH"
          >
            State
          </th>
          <th
            scope="col"
            :class="TH"
          >
            Data address
          </th>
          <th
            scope="col"
            :class="[TH, 'w-[38%]']"
          >
            Capacity
          </th>
          <th
            scope="col"
            :class="[TH, 'text-right']"
          >
            <Tip
              text="Time in the current state"
              align="end"
            >
              Since
            </Tip>
          </th>
        </tr>
      </thead>

      <tbody
        v-if="nodes === null"
        aria-hidden="true"
      >
        <tr
          v-for="i in 3"
          :key="i"
          class="border-b border-rule last:border-0"
        >
          <td
            v-for="j in 5"
            :key="j"
            :class="TD"
          >
            <Skeleton class="h-3 w-3/4" />
          </td>
        </tr>
      </tbody>

      <tbody
        v-else
        class="divide-y divide-rule"
      >
        <tr
          v-for="n in nodes"
          :key="n.id"
          class="hover:bg-raise"
          :class="{ 'text-ink-3': n.state === 'left' }"
        >
          <th
            scope="row"
            :class="[TD, 'font-mono font-medium whitespace-nowrap']"
          >
            {{ n.id }}<span
              v-if="n.id === respondingNodeId"
              class="ml-2 rounded-sm border border-rule px-1 py-px font-sans text-[11px] font-normal text-ink-3"
              title="Responding node"
            >local</span>
          </th>
          <td
            :class="TD"
            data-label="State"
          >
            <StateBadge :presentation="NODE_PRESENTATION[n.state]" />
          </td>
          <td
            :class="[TD, 'font-mono break-all text-ink-2']"
            data-label="Data address"
          >
            <span :title="`Gossip ${n.gossipAddress}`">{{ n.dataAddress }}</span>
          </td>
          <td
            :class="TD"
            data-label="Capacity"
          >
            <span
              v-if="n.capacityBytes > 0"
              class="flex items-center gap-2 sm:gap-3"
            >
              <Meter
                class="w-14 shrink-0 sm:w-24"
                :used-bytes="n.usedBytes"
                :capacity-bytes="n.capacityBytes"
                :label="`Capacity of ${n.id}`"
              />
              <span class="w-9 shrink-0 text-right">{{ pct(n) }}</span>
              <span class="whitespace-nowrap text-ink-3">{{ formatUsedOf(n.usedBytes, n.capacityBytes) }}</span>
            </span>
            <span
              v-else
              class="text-ink-3"
            >not reported</span>
          </td>
          <td
            :class="[TD, 'text-right whitespace-nowrap text-ink-2']"
            data-label="Since"
          >
            <RelTime
              v-if="n.lastChange"
              :date="n.lastChange"
              :now="now"
              mode="for"
            />
          </td>
        </tr>
      </tbody>
    </table>

    <div
      class="flex flex-wrap items-center gap-x-4 gap-y-1.5 border-t border-rule bg-raise px-3 py-2 text-[13px] rounded-b-md"
    >
      <span class="text-xs font-medium text-ink-3">Local disk</span>
      <template v-if="storage === undefined">
        <Skeleton class="h-3 w-64" />
      </template>
      <span
        v-else-if="storage === null"
        class="text-ink-3"
      >not reported</span>
      <template v-else>
        <span
          v-if="storage.dataDir"
          class="font-mono break-all text-ink-2"
        >{{ storage.dataDir }}</span>
        <span class="flex items-center gap-3">
          <Meter
            class="w-24 shrink-0"
            :used-bytes="storage.usedBytes"
            :capacity-bytes="storage.capacityBytes"
            :label="`Local disk of ${respondingNodeId}`"
          />
          <span class="whitespace-nowrap">{{ formatUsedOf(storage.usedBytes, storage.capacityBytes) }}</span>
        </span>
        <span class="whitespace-nowrap text-ink-2">{{ formatBytes(storage.availableBytes) }} free</span>
        <span class="whitespace-nowrap text-ink-2">{{ formatCount(storage.chunkCount) }} chunks</span>
      </template>
    </div>
  </Panel>
</template>
