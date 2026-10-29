<script setup lang="ts">
import type { Finding } from '../lib/health'
import Card from './Card.vue'
import Skeleton from './Skeleton.vue'
import StateBadge from './StateBadge.vue'
import StatusIcon from './StatusIcon.vue'

/** null while the first answer is pending. */
defineProps<{ findings: Finding[] | null }>()

const CLEAR = { label: 'No open issues', tone: 'good', icon: 'check' } as const
</script>

<template>
  <Card
    id="issues"
    title="Issues"
    :count="findings?.length || undefined"
  >
    <div
      v-if="findings === null"
      class="space-y-2"
      aria-hidden="true"
    >
      <Skeleton class="h-4 w-3/4" />
      <Skeleton class="h-4 w-1/2" />
    </div>
    <p
      v-else-if="findings.length === 0"
      class="text-[12.5px]"
    >
      <StateBadge :presentation="CLEAR" />
    </p>
    <ul
      v-else
      class="flex flex-col gap-2"
    >
      <li
        v-for="(f, i) in findings"
        :key="i"
        class="grid grid-cols-[14px_minmax(0,1fr)] gap-x-2 text-[12.5px]"
      >
        <span
          class="pt-0.5"
          :class="f.level === 'degraded' ? 'text-crit' : 'text-warn'"
        >
          <StatusIcon
            :name="f.level === 'degraded' ? 'octagon' : 'alert'"
            :size="14"
          />
          <span class="sr-only">{{ f.level === 'degraded' ? 'Critical' : 'Warning' }}: </span>
        </span>
        <span class="text-ink">{{ f.text }}</span>
        <code
          v-if="f.detail"
          class="col-start-2 mt-0.5 font-mono text-[11.5px] leading-[17px] break-words whitespace-pre-wrap text-ink-2"
        >{{ f.detail }}</code>
      </li>
    </ul>
  </Card>
</template>
