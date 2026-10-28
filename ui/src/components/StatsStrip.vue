<script setup lang="ts">
import { computed } from 'vue'
import type { Backup, Volume } from '../api/status'
import { formatBytes, formatPercent, formatUsedOf } from '../lib/format'
import type { Health } from '../lib/health'
import { BACKUP_PRESENTATION, type Tone } from '../lib/presentation'
import Meter from './Meter.vue'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StatCell from './StatCell.vue'
import StatusIcon from './StatusIcon.vue'

const props = defineProps<{
  /** null while the first answer is pending. */
  health: Health | null
  volumes: Volume[]
  backup: Backup | null
  version: string
  now: number
}>()

const VALUE = 'block h-7 text-[28px] leading-7 font-[560] tracking-tight [font-stretch:80%]'
const SUB = 'mt-2 block text-[13px] text-ink-3'
const TONE: Readonly<Record<Tone, string>> = {
  good: 'text-good',
  warn: 'text-warn',
  crit: 'text-crit',
  neutral: 'text-ink-2',
}

const nodeNote = computed(() => {
  // Only read inside the v-if="health" branch.
  const n = (props.health as Health).nodes
  const parts: { text: string; tone: string }[] = []
  if (n.down) parts.push({ text: `${String(n.down)} down`, tone: 'text-crit' })
  if (n.suspect) parts.push({ text: `${String(n.suspect)} suspect`, tone: 'text-warn' })
  const other = n.total - n.up - n.down - n.suspect
  if (other) parts.push({ text: `${String(other)} unknown`, tone: 'text-ink-2' })
  return parts
})

const provisioned = computed(() => props.volumes.reduce((a, v) => a + v.sizeBytes, 0))
const lastRun = computed(() => {
  const r = props.backup?.lastRun
  return r?.finishedAt ?? r?.startedAt ?? null
})
</script>

<template>
  <section aria-label="Summary">
    <dl
      v-if="health"
      class="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-rule bg-rule md:grid-cols-5"
    >
      <StatCell
        label="Nodes"
        :value="`${health.nodes.up}/${health.nodes.total}`"
      >
        <span :class="VALUE">{{ health.nodes.up }}<span class="text-ink-3"> / {{ health.nodes.total }}</span></span>
        <span :class="SUB">
          <template v-if="nodeNote.length === 0">up</template>
          <template
            v-for="(p, i) in nodeNote"
            :key="p.text"
          ><template v-if="i > 0">, </template><span
            class="font-medium"
            :class="p.tone"
          >{{ p.text }}</span></template>
        </span>
      </StatCell>

      <StatCell
        label="Capacity"
        :value="health.capacity ? formatPercent(health.capacity.usedBytes / health.capacity.capacityBytes) : ''"
      >
        <template v-if="health.capacity">
          <span class="flex h-7 items-center gap-3">
            <span :class="VALUE">{{ formatPercent(health.capacity.usedBytes / health.capacity.capacityBytes) }}</span>
            <Meter
              class="max-w-32 flex-1"
              :used-bytes="health.capacity.usedBytes"
              :capacity-bytes="health.capacity.capacityBytes"
              label="Cluster capacity"
            />
          </span>
          <span :class="SUB">{{ formatUsedOf(health.capacity.usedBytes, health.capacity.capacityBytes) }}</span>
        </template>
        <template v-else>
          <span :class="[VALUE, 'text-ink-3']">n/a</span>
          <span :class="SUB">not reported</span>
        </template>
      </StatCell>

      <StatCell
        label="Volumes"
        :value="String(volumes.length)"
      >
        <span :class="VALUE">{{ volumes.length }}</span>
        <span :class="SUB">{{ volumes.length ? `${formatBytes(provisioned)} provisioned` : 'none' }}</span>
      </StatCell>

      <StatCell
        label="Backup"
        :value="health.backup ?? ''"
      >
        <span
          v-if="health.backup"
          class="flex h-7 items-center gap-2"
          :class="TONE[BACKUP_PRESENTATION[health.backup].tone]"
        ><StatusIcon
          :name="BACKUP_PRESENTATION[health.backup].icon"
          :size="20"
        /><span :class="VALUE">{{ BACKUP_PRESENTATION[health.backup].label }}</span></span>
        <span
          v-else
          :class="[VALUE, 'text-ink-3']"
        >n/a</span>
        <span :class="SUB">
          <template v-if="lastRun && health.backup === 'running'">last run </template><RelTime
            v-if="lastRun"
            :date="lastRun"
            :now="now"
            mode="ago"
          /><template v-else-if="!backup?.configured">no target</template><template v-else-if="backup.nextRunAt">first <RelTime
            :date="backup.nextRunAt"
            :now="now"
            mode="in"
          /></template><template v-else>no runs</template>
        </span>
      </StatCell>

      <StatCell
        label="Version"
        :value="version"
        class="max-md:col-span-2"
      >
        <span :class="VALUE">{{ version }}</span>
      </StatCell>
    </dl>

    <div
      v-else
      class="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-rule bg-rule md:grid-cols-5"
      aria-hidden="true"
    >
      <div
        v-for="i in 5"
        :key="i"
        class="bg-panel px-4 pt-3 pb-3.5"
        :class="{ 'max-md:col-span-2': i === 5 }"
      >
        <Skeleton class="h-4 w-16" />
        <Skeleton class="mt-1.5 h-7 w-20" />
        <Skeleton class="mt-2 h-5 w-24" />
      </div>
    </div>
  </section>
</template>
