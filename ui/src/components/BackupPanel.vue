<script setup lang="ts">
import { computed } from 'vue'
import type { Backup } from '../api/status'
import { formatBytes, formatCount, formatDuration, formatInterval } from '../lib/format'
import { backupKind } from '../lib/health'
import { BACKUP_PRESENTATION, redactTarget } from '../lib/presentation'
import Panel from './Panel.vue'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  /** undefined while loading, null when silod did not report backups at all. */
  backup: Backup | null | undefined
  nodeId: string
  now: number
}>()

const kind = computed(() => (props.backup === undefined ? null : backupKind(props.backup, props.now)))
const run = computed(() => props.backup?.lastRun ?? null)
const when = computed(() => run.value?.finishedAt ?? run.value?.startedAt ?? null)
const took = computed(() => {
  const r = run.value
  return r?.startedAt && r.finishedAt ? formatDuration((r.finishedAt.getTime() - r.startedAt.getTime()) / 1000) : null
})

const ROW = 'grid grid-cols-[6rem_minmax(0,1fr)] gap-3 px-3 py-2'
const DT = 'text-xs leading-5 font-medium text-ink-3'
</script>

<template>
  <Panel
    id="backups"
    title="Backups"
  >
    <template #meta>
      <span
        v-if="nodeId"
        class="font-mono text-xs text-ink-3"
        title="Backups of the responding node"
      >{{ nodeId }}</span>
    </template>

    <div
      v-if="kind === null"
      class="space-y-3 p-3"
      aria-hidden="true"
    >
      <Skeleton
        v-for="i in 5"
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
        <dd><StateBadge :presentation="BACKUP_PRESENTATION[kind]" /></dd>
      </div>

      <div :class="ROW">
        <dt :class="DT">
          Target
        </dt>
        <dd
          v-if="kind === 'unconfigured'"
          class="text-ink-3"
        >
          <code class="font-mono text-ink-2">SILO_BACKUP_TARGET</code> not set
        </dd>
        <dd
          v-else
          class="font-mono break-all"
        >
          {{ redactTarget((backup as Backup).target) }}
        </dd>
      </div>

      <template v-if="backup && kind !== 'unconfigured'">
        <div :class="ROW">
          <dt :class="DT">
            Schedule
          </dt>
          <dd>{{ formatInterval(backup.intervalSeconds) }}</dd>
        </div>

        <div :class="ROW">
          <dt :class="DT">
            Last run
          </dt>
          <dd v-if="when">
            <RelTime
              :date="when"
              :now="now"
              mode="ago"
            /><span
              v-if="took"
              class="text-ink-3"
            >, took {{ took }}</span>
          </dd>
          <dd
            v-else
            class="text-ink-3"
          >
            never
          </dd>
        </div>

        <div
          v-if="run"
          :class="ROW"
        >
          <dt :class="DT">
            Chunks
          </dt>
          <dd>
            {{ formatCount(run.chunksUploaded) }} uploaded<span class="text-ink-3"> ({{ formatBytes(run.bytesUploaded) }})</span>,
            {{ formatCount(run.chunksSkipped) }} skipped
          </dd>
        </div>

        <div :class="ROW">
          <dt :class="DT">
            Next run
          </dt>
          <dd v-if="backup.running">
            after this run
          </dd>
          <dd v-else-if="backup.nextRunAt">
            <RelTime
              :date="backup.nextRunAt"
              :now="now"
              mode="in"
            />
          </dd>
          <dd
            v-else
            class="text-ink-3"
          >
            not scheduled
          </dd>
        </div>

        <div
          v-if="kind === 'failed' && run"
          :class="ROW"
        >
          <dt :class="DT">
            Error
          </dt>
          <dd>
            <code
              v-if="run.error"
              class="block font-mono text-[12px] leading-[18px] break-words whitespace-pre-wrap text-ink"
            >{{ run.error }}</code>
            <span
              v-else
              class="text-ink-3"
            >no error text</span>
          </dd>
        </div>
      </template>
    </dl>
  </Panel>
</template>
