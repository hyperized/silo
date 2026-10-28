<script setup lang="ts">
import type { Volume } from '../api/status'
import { formatBytes, formatPercent } from '../lib/format'
import Panel from './Panel.vue'
import Skeleton from './Skeleton.vue'
import Tip from './Tip.vue'

defineProps<{ volumes: Volume[] | null }>()

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
            {{ v.path }}
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
