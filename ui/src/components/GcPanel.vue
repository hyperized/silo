<!--
  Chunk garbage collection on the responding node. Mirrors BackupPanel: the
  state badge first, then the last run. With GC off silod still sweeps and
  counts orphans without deleting them, so the last run is shown either way. The error text is shown here in full
  and also raised as an issue.
-->
<script setup lang="ts">
import { computed } from 'vue'
import type { Gc } from '../api/status'
import { formatBytes, formatCount, formatMillis } from '../lib/format'
import { gcKind } from '../lib/health'
import { GC_PRESENTATION } from '../lib/presentation'
import Panel from './Panel.vue'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  /** undefined while loading, null when silod does not report GC. */
  gc: Gc | null | undefined
  nodeId: string
  now: number
}>()

const kind = computed(() => (props.gc ? gcKind(props.gc) : null))

const ROW = 'grid grid-cols-[6rem_minmax(0,1fr)] gap-3 px-3 py-2'
const DT = 'text-xs leading-5 font-medium text-ink-3'
</script>

<template>
  <Panel
    id="gc"
    title="Garbage collection"
  >
    <template #meta>
      <span
        v-if="nodeId"
        class="font-mono text-xs text-ink-3"
        title="GC of the responding node"
      >{{ nodeId }}</span>
    </template>

    <div
      v-if="gc === undefined"
      class="space-y-3 p-3"
      aria-hidden="true"
    >
      <Skeleton
        v-for="i in 3"
        :key="i"
        class="h-3"
        :class="i % 2 ? 'w-3/4' : 'w-1/2'"
      />
    </div>

    <dl
      v-else
      class="divide-y divide-rule text-[13px]"
    >
      <div :class="ROW">
        <dt :class="DT">
          State
        </dt>
        <dd v-if="kind">
          <StateBadge :presentation="GC_PRESENTATION[kind]" />
        </dd>
        <dd
          v-else
          class="text-ink-3"
        >
          not reported
        </dd>
      </div>

      <template v-if="gc">
        <div :class="ROW">
          <dt :class="DT">
            Last run
          </dt>
          <dd v-if="gc.lastRunAt">
            <RelTime
              :date="gc.lastRunAt"
              :now="now"
              mode="ago"
            /><span class="text-ink-3">, took {{ formatMillis(gc.lastDurationMs) }}</span>
          </dd>
          <dd
            v-else
            class="text-ink-3"
          >
            never
          </dd>
        </div>

        <div
          v-if="gc.lastRunAt"
          :class="ROW"
        >
          <dt :class="DT">
            Freed
          </dt>
          <dd>
            {{ formatCount(gc.lastOrphansDeleted) }} orphan {{ gc.lastOrphansDeleted === 1 ? 'chunk' : 'chunks' }}<span class="text-ink-3"> ({{ formatBytes(gc.lastBytesFreed) }})</span>
          </dd>
        </div>

        <div
          v-if="gc.lastError"
          :class="ROW"
        >
          <dt :class="DT">
            Error
          </dt>
          <dd>
            <code class="block font-mono text-[12px] leading-[18px] break-words whitespace-pre-wrap text-ink">{{ gc.lastError }}</code>
          </dd>
        </div>
      </template>
    </dl>
  </Panel>
</template>
