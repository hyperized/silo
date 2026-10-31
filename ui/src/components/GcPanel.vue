<!--
  Chunk garbage collection on the responding node. The bars are the runs this
  page has seen; silod reports only the latest, so the strip fills while the
  page stays open. With GC off silod still sweeps and counts orphans without
  deleting them, so the last run is shown either way.
-->
<script setup lang="ts">
import { computed } from 'vue'
import type { Gc } from '../api/status'
import type { GcRun } from '../composables/useHistory'
import { formatBytes, formatClock, formatCount, formatMillis } from '../lib/format'
import { gcKind, gcSkipReason } from '../lib/health'
import { GC_PRESENTATION } from '../lib/presentation'
import Card from './Card.vue'
import { RunBars } from './charts'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  /** undefined while loading, null when silod does not report GC. */
  gc: Gc | null | undefined
  /** Runs this page has seen, oldest first. */
  runs: GcRun[]
  now: number
}>()

const kind = computed(() => (props.gc ? gcKind(props.gc) : null))

const OUTCOME_LABEL = { skipped: 'skipped', failed: 'failed' } as const

const bars = computed(() =>
  props.runs.map((r) => ({
    value: r.bytesFreed,
    ok: r.outcome === 'ok',
    skipped: r.outcome === 'skipped',
    label: `${formatClock(new Date(r.at))}, ${r.outcome === 'ok' ? `freed ${formatBytes(r.bytesFreed)}` : OUTCOME_LABEL[r.outcome]}`,
  })),
)

// The latest run's error is printed in full below; this names the older failures the bars show.
const earlierFailures = computed(() => {
  const older = props.runs.slice(0, -1).filter((r) => r.outcome === 'failed')
  if (older.length === 0) return ''
  const at = older.map((r) => formatClock(new Date(r.at))).join(', ')
  return `${String(older.length)} of ${String(props.runs.length)} runs failed, at ${at}.`
})
</script>

<template>
  <Card
    id="gc"
    title="Garbage collection"
  >
    <template #meta>
      <span
        v-if="kind"
        class="text-xs"
      ><StateBadge :presentation="GC_PRESENTATION[kind]" /></span>
    </template>

    <div
      v-if="gc === undefined"
      class="space-y-2"
      aria-hidden="true"
    >
      <Skeleton class="h-[72px]" />
      <Skeleton class="h-8" />
    </div>

    <p
      v-else-if="gc === null"
      class="text-[12.5px] text-ink-3"
    >
      not reported
    </p>

    <template v-else>
      <RunBars
        :runs="bars"
        label="Bytes freed per GC run"
      />
      <dl class="grid grid-cols-3 gap-2 text-[11.5px] text-ink-3">
        <div>
          <dt>Last run</dt>
          <dd class="text-sm font-semibold text-ink">
            <RelTime
              v-if="gc.lastRunAt"
              :date="gc.lastRunAt"
              :now="now"
              mode="ago"
            />
            <template v-else>
              never
            </template>
          </dd>
        </div>
        <div>
          <dt>Orphans</dt>
          <dd class="text-sm font-semibold text-ink">
            {{ gc.lastRunAt ? formatCount(gc.lastOrphansDeleted) : 'n/a' }}
          </dd>
        </div>
        <div>
          <dt>{{ kind === 'off' ? 'Would free' : 'Freed' }}</dt>
          <dd class="text-sm font-semibold text-ink">
            {{ gc.lastRunAt ? formatBytes(gc.lastBytesFreed) : 'n/a' }}
          </dd>
        </div>
      </dl>
      <p
        v-if="gc.lastRunAt || earlierFailures || kind === 'off'"
        class="text-[11.5px] text-ink-3"
      >
        <template v-if="kind === 'off'">
          GC is off: sweeps count orphans and delete nothing.
        </template>
        <template v-if="gc.lastRunAt">
          Took {{ formatMillis(gc.lastDurationMs) }}.
        </template>
        {{ earlierFailures }}
      </p>
      <div
        v-if="kind === 'skipped'"
        class="text-[11.5px]"
        data-testid="gc-skipped"
      >
        <p class="text-ink-3">
          silod skipped the latest sweep. GC holds off until this node's extent maps agree, and the skip clears by itself.
        </p>
        <code class="mt-0.5 block font-mono text-[11.5px] leading-[17px] break-words whitespace-pre-wrap text-ink">{{ gcSkipReason(gc.lastError) }}</code>
      </div>
      <div
        v-else-if="gc.lastError"
        class="text-[11.5px]"
      >
        <p class="text-warn">
          The latest run failed:
        </p>
        <code class="mt-0.5 block font-mono text-[11.5px] leading-[17px] break-words whitespace-pre-wrap text-ink">{{ gc.lastError }}</code>
      </div>
    </template>
  </Card>
</template>
