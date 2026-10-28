<script setup lang="ts">
import type { Finding } from '../lib/health'
import Panel from './Panel.vue'
import StatusIcon from './StatusIcon.vue'

defineProps<{ findings: Finding[] }>()
</script>

<template>
  <Panel
    id="issues"
    title="Issues"
    :count="findings.length"
  >
    <ul class="divide-y divide-rule">
      <li
        v-for="(f, i) in findings"
        :key="i"
        class="grid grid-cols-[1rem_minmax(0,1fr)] gap-x-2.5 px-3 py-2"
      >
        <span
          class="pt-0.5"
          :class="f.level === 'degraded' ? 'text-crit' : 'text-warn'"
        >
          <StatusIcon
            :name="f.level === 'degraded' ? 'octagon' : 'alert'"
            :size="15"
          />
          <span class="sr-only">{{ f.level === 'degraded' ? 'Critical' : 'Warning' }}: </span>
        </span>
        <span class="font-medium text-ink">{{ f.text }}</span>
        <code
          v-if="f.detail"
          class="col-start-2 mt-1 font-mono text-[12px] leading-[18px] break-words whitespace-pre-wrap text-ink-2"
        >{{ f.detail }}</code>
      </li>
    </ul>
  </Panel>
</template>
