<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { formatDateTime, formatDuration } from '../lib/format'
import type { Level } from '../lib/health'
import { LEVEL_PRESENTATION } from '../lib/presentation'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'

const props = defineProps<{
  nodeId: string
  level: Level | null
  lastSuccessAt: number | null
  now: number
  paused: boolean
  inFlight: boolean
  /** silod version of the responding node; empty until known. */
  version: string
}>()

defineEmits<{ togglePause: [] }>()

const age = computed(() => {
  if (props.lastSuccessAt === null) return props.inFlight ? 'Loading' : ''
  const s = (props.now - props.lastSuccessAt) / 1000
  return s < 2 ? 'now' : `${formatDuration(s)} ago`
})

// Only a change of level is announced; ages tick every second and would drown everything else.
const announcement = ref('')
watch(
  () => props.level,
  (level, prev) => {
    if (level && prev && level !== prev) announcement.value = `Status ${LEVEL_PRESENTATION[level].label}`
  },
)
</script>

<template>
  <header class="sticky top-0 z-30 border-b border-rule bg-panel">
    <div class="mx-auto flex h-14 max-w-[1440px] items-center gap-3.5 px-6 max-sm:h-13 max-sm:gap-2.5 max-sm:px-4">
      <svg
        width="22"
        height="22"
        viewBox="0 0 22 22"
        aria-hidden="true"
        class="shrink-0 max-sm:size-5"
      >
        <rect
          x="2"
          y="2"
          width="18"
          height="18"
          rx="4"
          fill="none"
          stroke="var(--c-accent)"
          stroke-width="2"
        />
        <rect
          x="6"
          y="6"
          width="10"
          height="4"
          rx="1"
          fill="var(--c-accent)"
        />
        <rect
          x="6"
          y="12"
          width="10"
          height="4"
          rx="1"
          fill="var(--c-accent)"
          opacity="0.55"
        />
      </svg>
      <h1 class="flex min-w-0 items-baseline gap-1.5 text-[15px] font-semibold">
        <span>silo</span>
        <span
          v-if="nodeId"
          class="flex min-w-0 items-baseline gap-1.5 font-normal"
        >
          <span
            class="text-ink-3"
            aria-hidden="true"
          >/</span>
          <span
            class="truncate font-mono text-ink-2"
            title="Responding node"
          >{{ nodeId }}</span>
        </span>
      </h1>

      <span
        v-if="level"
        class="text-[13px]"
      ><span class="sr-only">Status </span><StateBadge
        :presentation="LEVEL_PRESENTATION[level]"
        chip
      /></span>
      <Skeleton
        v-else-if="inFlight"
        class="h-6 w-20"
      />

      <span
        class="sr-only"
        aria-live="polite"
      >{{ announcement }}</span>

      <div class="ml-auto flex items-center gap-3 text-xs">
        <span
          class="whitespace-nowrap text-ink-3"
          :title="lastSuccessAt === null ? undefined : formatDateTime(new Date(lastSuccessAt))"
        >
          <template v-if="paused">Paused<template v-if="lastSuccessAt !== null">, </template></template><span
            v-else-if="lastSuccessAt !== null"
            class="max-sm:hidden"
          >Updated </span>{{ age }}
        </span>
        <button
          type="button"
          class="inline-flex min-h-11 min-w-11 items-center justify-center gap-1.5 rounded px-2 font-medium text-ink transition-colors duration-100 ease-out hover:bg-raise active:bg-track motion-reduce:transition-none"
          :class="{ 'text-accent': paused }"
          :aria-pressed="paused"
          @click="$emit('togglePause')"
        >
          <svg
            width="14"
            height="14"
            viewBox="0 0 14 14"
            aria-hidden="true"
          >
            <path
              v-if="paused"
              d="M3.5 2v10l8-5Z"
              fill="currentColor"
            />
            <path
              v-else
              d="M4 2.5v9M10 2.5v9"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
            />
          </svg>
          <span class="max-sm:sr-only">{{ paused ? 'Resume' : 'Pause' }}</span>
        </button>
        <span
          v-if="version"
          class="font-mono text-ink-3 max-sm:hidden"
          title="silod version"
        >{{ version }}</span>
      </div>
    </div>
  </header>
</template>
