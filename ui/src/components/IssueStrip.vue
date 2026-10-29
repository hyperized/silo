<!--
  The worst open issue, one line under the top bar, so the reason for a
  Warning or Degraded pill is readable without scrolling. The full list lives
  in the Issues card.
-->
<script setup lang="ts">
import { computed } from 'vue'
import type { Finding } from '../lib/health'
import StatusIcon from './StatusIcon.vue'

const props = defineProps<{
  /** Worst first, as deriveHealth returns them; must not be empty. */
  findings: Finding[]
}>()

const worst = computed(() => props.findings[0] as Finding)
const crit = computed(() => worst.value.level === 'degraded')
const more = computed(() => props.findings.length - 1)
</script>

<template>
  <div
    class="border-b border-rule"
    :class="crit ? 'bg-crit-wash text-crit' : 'bg-warn-wash text-warn'"
  >
    <p class="mx-auto flex max-w-[1440px] min-w-0 items-center gap-2.5 px-6 py-2 text-[13px] max-sm:px-4">
      <StatusIcon
        :name="crit ? 'octagon' : 'alert'"
        :size="14"
      />
      <span class="sr-only">{{ crit ? 'Critical' : 'Warning' }}: </span>
      <span class="min-w-0 truncate font-medium">{{ worst.text }}</span>
      <a
        v-if="more > 0"
        href="#issues"
        class="shrink-0 whitespace-nowrap text-ink-2 underline-offset-2 hover:underline"
      >and {{ more }} more</a>
    </p>
  </div>
</template>
