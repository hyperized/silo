<script setup lang="ts">
import { computed, ref } from 'vue'
import type { ClusterNode, Volume } from '../api/status'
import { formatRate, formatUsedOf } from '../lib/format'
import { stateById, volumeDegraded } from '../lib/health'
import {
  MAP_DAMAGED_PRESENTATION,
  NODE_PRESENTATION,
  VOLUME_DEGRADED_PRESENTATION,
  type Presentation,
  type Tone,
} from '../lib/presentation'
import { Sparkline } from './charts'
import Meter from './Meter.vue'
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
  /** Write sparkline per volume path, bytes per second. */
  sparks: Readonly<Record<string, number[]>>
}>()

/** Rows shown before "Show all"; enough to see every problem row on a typical cluster. */
const FIRST = 8

const HEALTHY: Presentation = { label: 'Healthy', tone: 'good', icon: 'check' }
const CHIP: Readonly<Record<Tone, string>> = {
  good: 'text-good bg-good-wash',
  warn: 'text-warn bg-warn-wash',
  crit: 'text-crit bg-crit-wash',
  neutral: 'text-ink-2 bg-raise',
}
const TH = 'h-8 px-3 text-[11.5px] font-medium whitespace-nowrap text-ink-3'
const TD = 'px-3 py-1.5'

const query = ref('')
const showAll = ref(false)

const states = computed(() => stateById(props.nodes))
// An older silod sends no replica sets; an empty column would only take width.
const showReplicas = computed(() => props.volumes?.some((v) => v.replicas.length > 0) ?? false)

function reason(v: Volume): string {
  const down = v.replicas.filter((id) => states.value.get(id) !== 'alive')
  if (down.length > 0) {
    return down.map((id) => `replica on ${id} ${NODE_PRESENTATION[states.value.get(id) ?? 'unknown'].label.toLowerCase()}`).join(', ')
  }
  return `${String(v.replicas.length)} of ${String(props.replicationFactor)} replicas`
}

// Only read once volumes have arrived: every template path that uses rows checks for that first.
const rows = computed(() =>
  (props.volumes as Volume[])
    .map((v) => {
      const degraded = volumeDegraded(v, states.value, props.replicationFactor)
      const spark = props.sparks[v.path] ?? []
      return {
        v,
        degraded,
        why: degraded ? reason(v) : '',
        spark,
        rate: spark.at(-1) ?? 0,
        state: v.mapDamaged ? MAP_DAMAGED_PRESENTATION : degraded ? VOLUME_DEGRADED_PRESENTATION : HEALTHY,
        rank: v.mapDamaged ? 2 : degraded ? 1 : 0,
      }
    })
    .sort((a, b) => b.rank - a.rank || b.rate - a.rate || a.v.path.localeCompare(b.v.path)),
)

const matching = computed(() => {
  const q = query.value.trim().toLowerCase()
  if (!q) return rows.value
  return rows.value.filter((r) => r.v.path.toLowerCase().includes(q) || r.v.leaseHolder.toLowerCase().includes(q))
})

const shown = computed(() => (showAll.value ? matching.value : matching.value.slice(0, FIRST)))

function replica(id: string) {
  return { id, p: NODE_PRESENTATION[states.value.get(id) ?? 'unknown'] }
}
</script>

<template>
  <section
    id="volumes"
    aria-labelledby="volumes-title"
    class="flex min-w-0 flex-col rounded-[10px] border border-rule bg-panel"
  >
    <header class="flex flex-wrap items-center justify-between gap-x-3 gap-y-2 border-b border-rule px-4 py-2.5 max-sm:px-3">
      <h2
        id="volumes-title"
        class="text-[13px] font-semibold"
      >
        Volumes<span
          v-if="volumes"
          class="ml-1.5 font-normal text-ink-3"
        >{{ volumes.length }}</span>
      </h2>
      <label
        v-if="volumes && volumes.length > 0"
        class="inline-flex items-center gap-2 text-xs text-ink-3 max-sm:w-full"
      >
        <span>Filter</span>
        <input
          v-model="query"
          type="search"
          placeholder="path, lease holder"
          autocomplete="off"
          spellcheck="false"
          class="h-7 w-44 rounded-md border border-rule bg-page px-2.5 text-xs text-ink placeholder:text-ink-3 focus-visible:outline-2 focus-visible:outline-accent max-sm:h-11 max-sm:min-w-0 max-sm:flex-1"
        >
      </label>
    </header>

    <div
      v-if="volumes && volumes.length === 0"
      class="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-4 py-3"
    >
      <span class="text-ink-2">No volumes</span>
      <code class="rounded-sm bg-raise px-1.5 py-0.5 font-mono text-[12.5px] break-all text-ink select-all">siloctl volume create --size 10G /name</code>
    </div>

    <div
      v-else-if="volumes && matching.length === 0"
      class="flex flex-wrap items-center gap-x-3 gap-y-1 px-4 py-3"
      role="status"
    >
      <span class="text-ink-2">No volume matches “{{ query.trim() }}”</span>
      <button
        type="button"
        class="inline-flex min-h-11 items-center rounded px-2 font-medium text-accent hover:bg-raise active:bg-track"
        @click="query = ''"
      >
        Clear filter
      </button>
    </div>

    <div
      v-else
      class="overflow-x-auto"
    >
      <table class="stack w-full text-left text-[12.5px] sm:min-w-[720px]">
        <thead>
          <tr>
            <th
              scope="col"
              :class="[TH, 'pl-4']"
            >
              Path
            </th>
            <th
              scope="col"
              :class="TH"
            >
              <Tip text="Extents written of the volume size">
                Allocated
              </Tip>
            </th>
            <th
              scope="col"
              :class="TH"
            >
              <Tip text="Write rate over the last 20 samples">
                Writes
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
            <th
              scope="col"
              :class="[TH, 'pr-4 text-right']"
            >
              State
            </th>
          </tr>
        </thead>

        <tbody
          v-if="volumes === null"
          aria-hidden="true"
        >
          <tr
            v-for="i in 3"
            :key="i"
            class="border-t border-rule sm:h-11"
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

        <tbody v-else>
          <tr
            v-for="r in shown"
            :key="r.v.path"
            class="border-t border-rule hover:bg-raise sm:h-11"
          >
            <th
              scope="row"
              :class="[TD, 'pl-4 font-normal']"
            >
              <span class="font-mono text-xs break-all sm:break-normal sm:whitespace-nowrap">{{ r.v.path }}</span>
              <span
                v-if="r.v.mapDamaged || r.degraded || r.v.attachedHere"
                class="flex flex-wrap gap-x-2 text-[11px]"
              >
                <span
                  v-if="r.v.mapDamaged"
                  class="inline-flex items-center gap-1 whitespace-nowrap text-crit"
                ><StatusIcon
                  name="octagon"
                  :size="11"
                />map damaged</span>
                <span
                  v-if="r.degraded"
                  class="inline-flex items-center gap-1 whitespace-nowrap text-warn"
                ><StatusIcon
                  name="alert"
                  :size="11"
                />degraded, {{ r.why }}</span>
                <span
                  v-if="r.v.attachedHere"
                  class="text-ink-3"
                >attached here</span>
              </span>
            </th>
            <td
              :class="[TD, 'sm:w-[150px]']"
              data-label="Allocated"
            >
              <span class="flex items-center gap-2">
                <Meter
                  class="min-w-10 flex-1"
                  :used-bytes="r.v.allocatedBytes"
                  :capacity-bytes="r.v.sizeBytes"
                  :label="`Allocated of ${r.v.path}`"
                  size="sm"
                />
                <span class="text-[11.5px] whitespace-nowrap text-ink-2">{{ formatUsedOf(r.v.allocatedBytes, r.v.sizeBytes) }}</span>
              </span>
            </td>
            <td
              :class="[TD, 'sm:w-[120px]']"
              data-label="Writes"
            >
              <span class="block w-[100px]">
                <Sparkline
                  :points="r.spark"
                  :width="100"
                  :height="22"
                  :label="r.spark.length < 2 ? `Writes to ${r.v.path}` : `Writes to ${r.v.path}, ${formatRate(r.rate, 's')} now`"
                />
              </span>
            </td>
            <td
              v-if="showReplicas"
              :class="TD"
              data-label="Replicas"
            >
              <span
                v-if="r.v.replicas.length > 0"
                class="flex flex-wrap gap-1 sm:flex-nowrap"
              >
                <span
                  v-for="c in r.v.replicas.map(replica)"
                  :key="c.id"
                  class="inline-flex items-center gap-1 rounded-full px-1.5 py-px font-mono text-[11px] whitespace-nowrap"
                  :class="CHIP[c.p.tone]"
                  :title="`${c.id}: ${c.p.label}`"
                ><StatusIcon
                  :name="c.p.icon"
                  :size="11"
                />{{ c.id }}<span class="sr-only">, {{ c.p.label }}</span></span>
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
                v-if="r.v.leaseHolder"
                class="font-mono text-xs break-all text-ink-2 sm:break-normal sm:whitespace-nowrap"
              >{{ r.v.leaseHolder }}</span>
              <span
                v-else
                class="text-ink-3"
              >none</span>
            </td>
            <td
              :class="[TD, 'pr-4 text-right']"
              data-label="State"
            >
              <StateBadge :presentation="r.state" />
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <footer
      v-if="volumes && matching.length > 0"
      class="flex flex-wrap items-center justify-between gap-x-3 border-t border-rule px-4 text-[11.5px] text-ink-3 max-sm:px-3"
    >
      <span class="py-2">{{ shown.length }} of {{ volumes.length }} shown<template v-if="query.trim()">, filtered</template>, problems first, then by write rate</span>
      <button
        v-if="matching.length > FIRST"
        type="button"
        class="inline-flex min-h-9 items-center rounded px-2 font-medium text-accent hover:bg-raise active:bg-track max-sm:min-h-11"
        :aria-expanded="showAll"
        @click="showAll = !showAll"
      >
        {{ showAll ? `Show first ${FIRST}` : 'Show all' }}
      </button>
    </footer>
  </section>
</template>
