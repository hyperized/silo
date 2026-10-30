<script setup lang="ts">
import { computed } from 'vue'
import { fetchStatus, type Status } from './api/status'
import { mockScenario } from './api/mockMode'
import AppBar from './components/AppBar.vue'
import BackupPanel from './components/BackupPanel.vue'
import ErrorView from './components/ErrorView.vue'
import GcPanel from './components/GcPanel.vue'
import IssueList from './components/IssueList.vue'
import IssueStrip from './components/IssueStrip.vue'
import LocalDiskPanel from './components/LocalDiskPanel.vue'
import NodeCards from './components/NodeCards.vue'
import StaleBanner from './components/StaleBanner.vue'
import BackupTile from './components/tiles/BackupTile.vue'
import CapacityTile from './components/tiles/CapacityTile.vue'
import NodesTile from './components/tiles/NodesTile.vue'
import ReplicationTile from './components/tiles/ReplicationTile.vue'
import SkewTile from './components/tiles/SkewTile.vue'
import VolumesTile from './components/tiles/VolumesTile.vue'
import UsedCapacityChart from './components/UsedCapacityChart.vue'
import VolumesPanel from './components/VolumesPanel.vue'
import { useHistory } from './composables/useHistory'
import { useNow } from './composables/useNow'
import { usePageVisible } from './composables/usePageVisible'
import { STALE_AFTER_MS, useStatusPoll } from './composables/useStatusPoll'
import { formatAgo } from './lib/format'
import { deriveHealth, maxSkewMs, stateById, volumeDegraded } from './lib/health'

// Mock data is a development aid only; production builds compile this to null.
const scenario =
  import.meta.env.DEV || import.meta.env.VITE_MOCK === '1'
    ? mockScenario(window.location.search, import.meta.env.VITE_MOCK)
    : null

const fetcher = scenario
  ? async (): Promise<Status> => (await import('./api/mock')).fetchMockStatus(scenario)
  : fetchStatus

const visible = usePageVisible()
const now = useNow(visible)
const poll = useStatusPoll(fetcher, { active: visible })
const { data, error, lastSuccessAt, nextAttemptAt, inFlight, paused } = poll
const history = useHistory(data, now)

const health = computed(() => (data.value ? deriveHealth(data.value, now.value) : null))

// Requires an error as well as age: right after the tab becomes visible again
// the clock jumps ahead before the catch-up request lands, and that is not stale.
const stale = computed(() => {
  const last = lastSuccessAt.value
  if (paused.value || error.value === null || last === null || now.value - last <= STALE_AFTER_MS) return null
  return { age: formatAgo(new Date(last), now.value), message: error.value.message }
})

const retryInSeconds = computed(() =>
  nextAttemptAt.value === null ? null : Math.max(0, Math.ceil((nextAttemptAt.value - now.value) / 1000)),
)

const volumeCounts = computed(() => {
  const s = data.value
  if (!s) return { degraded: 0, damaged: 0 }
  const states = stateById(s.nodes)
  const rf = s.health?.replicationFactor ?? 0
  return {
    degraded: s.volumes.filter((v) => volumeDegraded(v, states, rf)).length,
    damaged: s.volumes.filter((v) => v.mapDamaged).length,
  }
})

const skew = computed(() => {
  const s = data.value
  if (!s) return { ms: null, reported: false }
  return { ms: maxSkewMs(s), reported: s.health !== null || s.nodes.some((n) => n.clockSkewMs !== 0) }
})

const nodeSparks = computed(() =>
  Object.fromEntries((data.value?.nodes ?? []).map((n) => [n.id, history.nodeWriteSparkline(n.id)])),
)
const volumeSparks = computed(() =>
  Object.fromEntries((data.value?.volumes ?? []).map((v) => [v.path, history.volumeWriteSparkline(v.path)])),
)
</script>

<template>
  <a
    href="#main"
    class="sr-only z-40 rounded bg-panel px-3 py-2 text-ink focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
  >
    Skip to status
  </a>
  <AppBar
    :node-id="data?.respondingNodeId ?? ''"
    :level="health?.level ?? null"
    :last-success-at="lastSuccessAt"
    :now="now"
    :paused="paused"
    :in-flight="inFlight"
    :version="data?.version ?? ''"
    @toggle-pause="poll.togglePause()"
  />
  <IssueStrip
    v-if="health && health.findings.length > 0"
    :findings="health.findings"
  />

  <main
    id="main"
    class="mx-auto flex w-full max-w-[1440px] flex-col gap-5 px-6 pt-5 pb-12 max-sm:gap-3.5 max-sm:px-4 max-sm:pt-4"
    :aria-busy="data === null && error === null"
  >
    <ErrorView
      v-if="data === null && error"
      :message="error.message"
      :retry-in-seconds="retryInSeconds"
      :retrying="inFlight"
      @retry="poll.refresh()"
    />

    <template v-else>
      <StaleBanner
        v-if="stale"
        :age="stale.age"
        :message="stale.message"
        :retry-in-seconds="retryInSeconds"
        @retry="poll.refresh()"
      />

      <section
        aria-label="Instruments"
        class="grid grid-cols-2 gap-4 max-sm:gap-3 sm:grid-cols-3 wide:grid-cols-6"
      >
        <CapacityTile
          :capacity="health ? health.capacity : undefined"
          :rate-per-hour="history.clusterUsedRatePerHour.value"
          :seconds-until-full="history.secondsUntilFull.value"
        />
        <NodesTile
          :nodes="data?.nodes ?? null"
          :isolated="data?.health?.isolated ?? false"
        />
        <ReplicationTile :health="data ? data.health : undefined" />
        <VolumesTile
          :volumes="data?.volumes ?? null"
          :degraded="volumeCounts.degraded"
          :damaged="volumeCounts.damaged"
          :spark="history.clusterWriteSparkline.value"
          :write-rate="history.writeRateBytesPerSec.value"
        />
        <SkewTile
          :skew-ms="skew.ms"
          :alerts="data?.health?.clockSkewAlerts ?? 0"
          :reported="skew.reported"
        />
        <BackupTile
          :backup="data ? data.backup : undefined"
          :runs="history.backupRuns.value"
          :now="now"
        />
      </section>

      <div class="grid gap-4 max-sm:gap-3.5 wide:grid-cols-12">
        <UsedCapacityChart
          v-model:span="history.span.value"
          class="wide:col-span-8"
          :used-by-node="history.usedByNode.value"
          :capacity-bytes="health?.capacity?.capacityBytes ?? 0"
          :now="now"
        />
        <LocalDiskPanel
          class="wide:col-span-4"
          :nodes="data?.nodes ?? null"
          :storage="data ? data.storage : undefined"
          :rate-per-hour="history.clusterUsedRatePerHour.value"
        />
      </div>

      <NodeCards
        :nodes="data?.nodes ?? null"
        :responding-node-id="data?.respondingNodeId ?? ''"
        :now="now"
        :sparks="nodeSparks"
      />

      <VolumesPanel
        :volumes="data?.volumes ?? null"
        :nodes="data?.nodes ?? []"
        :replication-factor="data?.health?.replicationFactor ?? 0"
        :sparks="volumeSparks"
      />
      <div class="grid items-start gap-4 max-sm:gap-3.5 wide:grid-cols-3">
        <GcPanel
          :gc="data ? data.gc : undefined"
          :runs="history.gcRuns.value"
          :now="now"
        />
        <BackupPanel
          :backup="data ? data.backup : undefined"
          :runs="history.backupRuns.value"
          :now="now"
        />
        <IssueList :findings="health?.findings ?? null" />
      </div>
    </template>
  </main>
</template>
