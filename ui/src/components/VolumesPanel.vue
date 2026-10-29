<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterNode, Volume } from '../api/status'
import { formatBytes, formatPercent } from '../lib/format'
import { stateById, volumeDegraded } from '../lib/health'
import {
  MAP_DAMAGED_PRESENTATION,
  NODE_PRESENTATION,
  VOLUME_DEGRADED_PRESENTATION,
  type Tone,
} from '../lib/presentation'
import Panel from './Panel.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'
import StatusIcon from './StatusIcon.vue'
import Tip from './Tip.vue'

const props = defineProps<{
  volumes: Volume[] | null
  /** Colours the replica chips; a replica on a node missing here shows as unknown. */
  nodes: ClusterNode[]
  /** 0 when silod does not report it; a shorter replica set then cannot be detected. */
  replicationFactor: number
}>()

const states = computed(() => stateById(props.nodes))
// An older silod sends no replica sets; an empty column would only take width.
const showReplicas = computed(() => props.volumes?.some((v) => v.replicas.length > 0) ?? false)

// Only read inside the rows branch, where volumes is never null.
const degraded = computed(
  () =>
    new Set(
      (props.volumes as Volume[]).filter((v) => volumeDegraded(v, states.value, props.replicationFactor)).map((v) => v.path),
    ),
)

function replica(id: string) {
  return { id, p: NODE_PRESENTATION[states.value.get(id) ?? 'unknown'] }
}

const CHIP: Readonly<Record<Tone, string>> = {
  good: 'text-good bg-good-wash',
  warn: 'text-warn bg-warn-wash',
  crit: 'text-crit bg-crit-wash',
  neutral: 'text-ink-2 bg-raise',
}

const TH = 'h-8 px-3 text-xs font-medium whitespace-nowrap text-ink-3'
const TD = 'h-9 px-3 py-1.5'
</script>

<template>
  <Panel
    id="volumes"
    title="Volumes"
    :count="volumes?.length"
  >
    <div
      v-if="volumes && volumes.length === 0"
      class="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-3 py-3"
    >
      <span class="text-ink-2">No volumes</span>
      <code class="rounded-sm bg-raise px-1.5 py-0.5 font-mono text-[12.5px] break-all text-ink select-all">siloctl volume create --size 10G /name</code>
    </div>

    <table
      v-else
      class="stack w-full text-left text-[13px]"
    >
      <thead class="border-b border-rule">
        <tr>
          <th
            scope="col"
            :class="[TH, 'w-[35%]']"
          >
            Path
          </th>
          <th
            scope="col"
            :class="[TH, 'text-right']"
          >
            Size
          </th>
          <th
            scope="col"
            :class="[TH, 'text-right']"
          >
            <Tip
              text="Extents written times extent size"
              align="end"
            >
              Allocated
            </Tip>
          </th>
          <th
            scope="col"
            :class="[TH, 'text-right']"
          >
            <Tip
              text="Allocation unit"
              align="end"
            >
              Extent
            </Tip>
          </th>
          <th
            v-if="showReplicas"
            scope="col"
            :class="TH"
          >
            <Tip text="Nodes holding a copy, by state">
              Replicas
            </Tip>
          </th>
          <th
            scope="col"
            :class="TH"
          >
            <Tip text="Node holding the write lease">
              Lease
            </Tip>
          </th>
        </tr>
      </thead>

      <tbody
        v-if="volumes === null"
        aria-hidden="true"
      >
        <tr
          v-for="i in 2"
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
          v-for="v in volumes"
          :key="v.path"
          class="hover:bg-raise"
        >
          <th
            scope="row"
            :class="[TD, 'font-mono font-medium break-all']"
          >
            {{ v.path }}<span
              v-if="v.attachedHere"
              class="ml-2 rounded-sm border border-rule px-1 py-px font-sans text-[11px] font-normal text-ink-3"
              title="Attached on the responding node"
            >attached</span>
            <span
              v-if="v.mapDamaged || degraded.has(v.path)"
              class="mt-1 flex flex-wrap gap-1.5 font-sans font-normal"
            >
              <StateBadge
                v-if="v.mapDamaged"
                :presentation="MAP_DAMAGED_PRESENTATION"
                chip
              />
              <StateBadge
                v-if="degraded.has(v.path)"
                :presentation="VOLUME_DEGRADED_PRESENTATION"
                chip
              />
            </span>
          </th>
          <td
            :class="[TD, 'text-right whitespace-nowrap']"
            data-label="Size"
          >
            {{ formatBytes(v.sizeBytes) }}
          </td>
          <td
            :class="[TD, 'text-right whitespace-nowrap']"
            data-label="Allocated"
          >
            <span>{{ formatBytes(v.allocatedBytes) }}<span
              v-if="v.sizeBytes > 0"
              class="ml-2 inline-block w-9 text-ink-3"
            >{{ formatPercent(v.allocatedBytes / v.sizeBytes) }}</span></span>
          </td>
          <td
            :class="[TD, 'text-right whitespace-nowrap text-ink-2']"
            data-label="Extent"
          >
            {{ formatBytes(v.extentSizeBytes) }}
          </td>
          <td
            v-if="showReplicas"
            :class="TD"
            data-label="Replicas"
          >
            <span
              v-if="v.replicas.length > 0"
              class="flex flex-wrap gap-1"
            >
              <span
                v-for="r in v.replicas.map(replica)"
                :key="r.id"
                class="inline-flex items-center gap-1 rounded-sm px-1.5 py-px font-mono text-[11.5px] whitespace-nowrap"
                :class="CHIP[r.p.tone]"
                :title="`${r.id}: ${r.p.label}`"
              ><StatusIcon
                :name="r.p.icon"
                :size="12"
              />{{ r.id }}<span class="sr-only">, {{ r.p.label }}</span></span>
            </span>
            <span
              v-else
              class="text-ink-3"
            >none</span>
          </td>
          <td
            :class="TD"
            data-label="Lease"
          >
            <span
              v-if="v.leaseHolder"
              class="font-mono break-all"
            >{{ v.leaseHolder }}</span>
            <span
              v-else
              class="text-ink-3"
            >none</span>
          </td>
        </tr>
      </tbody>
    </table>
  </Panel>
</template>
