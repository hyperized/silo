<!--
  Cluster-wide health as the responding node sees it: replication cover,
  membership, clock skew and the silod process. Problems found here also
  appear in the issue list; this panel shows the numbers behind them.
-->
<script setup lang="ts">
import { computed } from 'vue'
import type { ClusterHealth } from '../api/status'
import { formatCount } from '../lib/format'
import { replicationKind, skewLevel } from '../lib/health'
import { MEMBERSHIP_PRESENTATION, REPLICATION_PRESENTATION, skewPresentation } from '../lib/presentation'
import Panel from './Panel.vue'
import RelTime from './RelTime.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  /** undefined while loading, null when silod is older than the health report. */
  health: ClusterHealth | null | undefined
  /** Largest absolute clock skew known, from the health report or the node table. */
  skewMs: number
  version: string
  nodeId: string
  now: number
}>()

const replication = computed(() => replicationKind(props.health))
const skew = computed(() => {
  const level = skewLevel(props.skewMs)
  return { level, presentation: skewPresentation(`${formatCount(props.skewMs)} ms`, level) }
})

const ROW = 'grid grid-cols-[6rem_minmax(0,1fr)] gap-3 px-3 py-2'
const DT = 'text-xs leading-5 font-medium text-ink-3'
</script>

<template>
  <Panel
    id="health"
    title="Health"
  >
    <template #meta>
      <span
        v-if="nodeId"
        class="font-mono text-xs text-ink-3"
        title="As seen by the responding node"
      >{{ nodeId }}</span>
    </template>

    <div
      v-if="health === undefined"
      class="space-y-3 p-3"
      aria-hidden="true"
    >
      <Skeleton
        v-for="i in 4"
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
          Replication
        </dt>
        <dd
          v-if="health && replication"
          class="flex flex-wrap items-center gap-x-2"
        >
          <StateBadge :presentation="REPLICATION_PRESENTATION[replication]" />
          <span class="text-ink-2">{{ health.aliveNodes }} / {{ health.replicationFactor }} nodes alive</span>
        </dd>
        <dd
          v-else
          class="text-ink-3"
        >
          not reported
        </dd>
      </div>

      <div :class="ROW">
        <dt :class="DT">
          Membership
        </dt>
        <dd
          v-if="health"
          class="flex flex-wrap items-center gap-x-2"
        >
          <StateBadge :presentation="MEMBERSHIP_PRESENTATION[health.isolated ? 'isolated' : 'connected']" />
          <span
            v-if="health.rejoinAttempts > 0"
            class="text-ink-2"
          >{{ formatCount(health.rejoinAttempts) }} rejoin {{ health.rejoinAttempts === 1 ? 'attempt' : 'attempts' }}</span>
        </dd>
        <dd
          v-else
          class="text-ink-3"
        >
          not reported
        </dd>
      </div>

      <div :class="ROW">
        <dt :class="DT">
          Clock skew
        </dt>
        <dd class="flex flex-wrap items-center gap-x-2">
          <StateBadge
            v-if="skew.level !== 'healthy'"
            :presentation="skew.presentation"
          />
          <span v-else>{{ skew.presentation.label }}</span>
          <span
            v-if="health && health.clockSkewAlerts > 0"
            class="text-ink-2"
          >{{ formatCount(health.clockSkewAlerts) }} {{ health.clockSkewAlerts === 1 ? 'alert' : 'alerts' }}</span>
        </dd>
      </div>

      <div :class="ROW">
        <dt :class="DT">
          silod
        </dt>
        <dd>
          {{ version }}<span
            v-if="health?.startedAt"
            class="text-ink-2"
          >, up <RelTime
            :date="health.startedAt"
            :now="now"
            mode="for"
          /></span>
        </dd>
      </div>
    </dl>
  </Panel>
</template>
