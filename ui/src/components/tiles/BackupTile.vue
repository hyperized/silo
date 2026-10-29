<script setup lang="ts">
import { computed } from 'vue'
import type { Backup } from '../../api/status'
import type { BackupRunPoint } from '../../composables/useHistory'
import { formatAgo, formatBytes, formatClock, formatCompact, formatIn } from '../../lib/format'
import { backupKind } from '../../lib/health'
import { BACKUP_PRESENTATION, type Presentation } from '../../lib/presentation'
import Skeleton from '../Skeleton.vue'
import StateBadge from '../StateBadge.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** undefined while loading, null when silod did not report backups at all. */
  backup: Backup | null | undefined
  /** Runs this page has seen, oldest first. */
  runs: BackupRunPoint[]
  now: number
}>()

const SHOWN_RUNS = 8

const kind = computed(() => (props.backup === undefined ? null : backupKind(props.backup, props.now)))
const run = computed(() => props.backup?.lastRun ?? null)

const state = computed<Presentation | null>(() => {
  if (kind.value === null) return null
  const p = BACKUP_PRESENTATION[kind.value]
  const r = run.value
  const when = r?.finishedAt ?? r?.startedAt ?? null
  const withAge = kind.value === 'ok' || kind.value === 'failed' || kind.value === 'overdue'
  return when && withAge ? { ...p, label: `${p.label} ${formatAgo(when, props.now)}` } : p
})

const recent = computed(() => props.runs.slice(-SHOWN_RUNS))
const runsLabel = computed(() => {
  const n = recent.value.length
  const failed = recent.value.filter((r) => !r.ok).length
  return `Last ${String(n)} ${n === 1 ? 'run' : 'runs'}, ${String(failed)} failed`
})

const upload = computed(() => {
  if (kind.value === 'unconfigured') return 'SILO_BACKUP_TARGET not set'
  const r = run.value
  return r ? `${formatBytes(r.bytesUploaded)} up, ${formatCompact(r.chunksSkipped)} chunks skipped` : ''
})

const next = computed(() => {
  const b = props.backup
  if (!b?.configured) return ''
  if (b.running) return 'next after this run'
  return b.nextRunAt ? `next ${formatIn(b.nextRunAt, props.now)}` : 'not scheduled'
})
</script>

<template>
  <Tile
    id="tile-backup"
    title="Backup"
  >
    <template v-if="state === null">
      <Skeleton class="mt-2.5 h-5 w-28" />
      <Skeleton class="mt-2 h-3.5 w-36" />
    </template>
    <template v-else>
      <span class="mt-2.5 text-[13px]"><StateBadge :presentation="state" /></span>
      <div
        v-if="recent.length > 0"
        role="img"
        :aria-label="runsLabel"
        class="mt-2 flex gap-1"
      >
        <span
          v-for="r in recent"
          :key="r.at"
          data-part="run"
          :data-ok="r.ok"
          class="relative grid size-3.5 place-items-center rounded-[3px]"
          :class="r.ok ? 'bg-good' : 'bg-crit'"
          :title="`${formatClock(new Date(r.at))}, ${r.ok ? 'succeeded' : 'failed'}`"
        ><svg
          v-if="!r.ok"
          width="8"
          height="8"
          viewBox="0 0 8 8"
          aria-hidden="true"
        ><path
          d="M1.5 1.5l5 5M6.5 1.5l-5 5"
          stroke="var(--c-panel)"
          stroke-width="1.6"
          stroke-linecap="round"
        /></svg></span>
      </div>
      <p
        v-if="upload"
        class="mt-1.5 text-[11.5px] text-ink-3"
      >
        {{ upload }}
      </p>
      <p
        v-if="next"
        class="mt-auto text-[11.5px] text-ink-3"
      >
        {{ next }}
      </p>
    </template>
  </Tile>
</template>
