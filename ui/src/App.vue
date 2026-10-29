<script setup lang="ts">
import { computed } from 'vue'
import { fetchStatus, type Status } from './api/status'
import { mockScenario } from './api/mockMode'
import AppBar from './components/AppBar.vue'
import BackupPanel from './components/BackupPanel.vue'
import ErrorView from './components/ErrorView.vue'
import GcPanel from './components/GcPanel.vue'
import HealthPanel from './components/HealthPanel.vue'
import IssueList from './components/IssueList.vue'
import NodesPanel from './components/NodesPanel.vue'
import StaleBanner from './components/StaleBanner.vue'
import StatsStrip from './components/StatsStrip.vue'
import VolumesPanel from './components/VolumesPanel.vue'
import { useNow } from './composables/useNow'
import { usePageVisible } from './composables/usePageVisible'
import { STALE_AFTER_MS, useStatusPoll } from './composables/useStatusPoll'
import { formatAgo } from './lib/format'
import { deriveHealth, maxSkewMs } from './lib/health'

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
    @toggle-pause="poll.togglePause()"
  />

  <main
    id="main"
    class="mx-auto max-w-[1400px] space-y-4 px-4 py-4"
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

      <StatsStrip
        :health="health"
        :volumes="data?.volumes ?? []"
        :backup="data?.backup ?? null"
        :version="data?.version ?? ''"
        :now="now"
      />

      <IssueList
        v-if="health && health.findings.length > 0"
        :findings="health.findings"
      />

      <div class="grid items-start gap-4 min-[1100px]:grid-cols-[minmax(0,1fr)_24rem]">
        <div class="min-w-0 space-y-4">
          <NodesPanel
            :nodes="data?.nodes ?? null"
            :responding-node-id="data?.respondingNodeId ?? ''"
            :storage="data ? data.storage : undefined"
            :now="now"
          />
          <VolumesPanel
            :volumes="data?.volumes ?? null"
            :nodes="data?.nodes ?? []"
            :replication-factor="data?.health?.replicationFactor ?? 0"
          />
        </div>
        <div class="min-w-0 space-y-4">
          <HealthPanel
            :health="data ? data.health : undefined"
            :skew-ms="data ? maxSkewMs(data) : 0"
            :version="data?.version ?? ''"
            :node-id="data?.respondingNodeId ?? ''"
            :now="now"
          />
          <GcPanel
            :gc="data ? data.gc : undefined"
            :node-id="data?.respondingNodeId ?? ''"
            :now="now"
          />
          <BackupPanel
            :backup="data ? data.backup : undefined"
            :node-id="data?.respondingNodeId ?? ''"
            :now="now"
          />
        </div>
      </div>
    </template>
  </main>
</template>
