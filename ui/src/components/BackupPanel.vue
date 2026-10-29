<script setup lang="ts">
import { computed } from 'vue'
import type { Backup } from '../api/status'
import type { BackupRunPoint } from '../composables/useHistory'
import { formatBytes, formatClock, formatCompact, formatIn, formatInterval } from '../lib/format'
import { backupKind } from '../lib/health'
import { BACKUP_PRESENTATION, redactTarget } from '../lib/presentation'
import Card from './Card.vue'
import { RunDots } from './charts'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  /** undefined while loading, null when silod did not report backups at all. */
  backup: Backup | null | undefined
  /** Runs this page has seen, oldest first. */
  runs: BackupRunPoint[]
  now: number
}>()

const kind = computed(() => (props.backup === undefined ? null : backupKind(props.backup, props.now)))
const run = computed(() => props.backup?.lastRun ?? null)
const when = computed(() => run.value?.finishedAt ?? run.value?.startedAt ?? null)

const dots = computed(() =>
  props.runs.map((r) => ({ ok: r.ok, label: `${formatClock(new Date(r.at))}, ${r.ok ? 'succeeded' : 'failed'}` })),
)
const scheduled = computed(() => {
  const b = props.backup
  return b && !b.running && b.nextRunAt ? formatIn(b.nextRunAt, props.now) : ''
})

// Only read once kind says a target is configured, which needs a backup report.
const b = computed(() => props.backup as Backup)

const schedule = computed(() => {
  const s = formatInterval(b.value.intervalSeconds)
  return s === 'none' ? 'No schedule.' : `${s.charAt(0).toUpperCase()}${s.slice(1)}.`
})
</script>

<template>
  <Card
    id="backups"
    title="Backups"
  >
    <template #meta>
      <span
        v-if="kind"
        class="flex min-w-0 items-center gap-2 text-xs"
      >
        <StateBadge :presentation="BACKUP_PRESENTATION[kind]" />
        <span
          v-if="kind !== 'unconfigured'"
          class="min-w-0 truncate font-mono text-[11.5px] text-ink-3"
          :title="redactTarget(b.target)"
        >{{ redactTarget(b.target) }}</span>
      </span>
    </template>

    <div
      v-if="kind === null"
      class="space-y-2"
      aria-hidden="true"
    >
      <Skeleton class="h-10" />
      <Skeleton class="h-8" />
    </div>

    <p
      v-else-if="kind === 'unconfigured'"
      class="text-[12.5px] text-ink-2"
    >
      Set <code class="font-mono text-ink">SILO_BACKUP_TARGET</code> to back up this node's chunks.
    </p>

    <template v-else>
      <RunDots
        :runs="dots"
        :scheduled="scheduled"
        label="Backup runs"
      />
      <dl class="grid grid-cols-3 gap-2 text-[11.5px] text-ink-3">
        <div>
          <dt>Last</dt>
          <dd class="text-sm font-semibold text-ink">
            <RelTime
              v-if="when"
              :date="when"
              :now="now"
              mode="ago"
            />
            <template v-else>
              never
            </template>
          </dd>
        </div>
        <div>
          <dt>Uploaded</dt>
          <dd class="text-sm font-semibold text-ink">
            {{ run ? formatBytes(run.bytesUploaded) : 'n/a' }}
          </dd>
        </div>
        <div>
          <dt>Next</dt>
          <dd class="text-sm font-semibold text-ink">
            <template v-if="b.running">
              after this run
            </template>
            <RelTime
              v-else-if="b.nextRunAt"
              :date="b.nextRunAt"
              :now="now"
              mode="in"
            />
            <template v-else>
              not scheduled
            </template>
          </dd>
        </div>
      </dl>
      <p class="text-[11.5px] break-words text-ink-3">
        {{ schedule }}<template v-if="run">
          {{ ` ${formatCompact(run.chunksSkipped)} chunks skipped, already present.` }}<template v-if="run.manifest">
            {{ ' Manifest ' }}<span class="font-mono break-all">{{ run.manifest }}</span>.
          </template>
        </template>
      </p>
      <div
        v-if="kind === 'failed' && run"
        class="text-[11.5px]"
      >
        <p class="text-warn">
          The last run failed:
        </p>
        <code
          v-if="run.error"
          class="mt-0.5 block font-mono text-[11.5px] leading-[17px] break-words whitespace-pre-wrap text-ink"
        >{{ run.error }}</code>
        <span
          v-else
          class="text-ink-3"
        >no error text</span>
      </div>
    </template>
  </Card>
</template>
