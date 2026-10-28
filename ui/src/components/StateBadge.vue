<script setup lang="ts">
import type { Presentation, Tone } from '../lib/presentation'
import StatusIcon from './StatusIcon.vue'
import Tip from './Tip.vue'

withDefaults(defineProps<{ presentation: Presentation; chip?: boolean }>(), { chip: false })

const INK: Readonly<Record<Tone, string>> = {
  good: 'text-good',
  warn: 'text-warn',
  crit: 'text-crit',
  neutral: 'text-ink-2',
}
const WASH: Readonly<Record<Tone, string>> = {
  good: 'bg-good-wash',
  warn: 'bg-warn-wash',
  crit: 'bg-crit-wash',
  neutral: 'bg-raise',
}
</script>

<template>
  <component
    :is="presentation.tip ? Tip : 'span'"
    v-bind="presentation.tip ? { text: presentation.tip } : {}"
  >
    <span
      class="inline-flex items-center gap-1.5 font-medium whitespace-nowrap"
      :class="[INK[presentation.tone], chip ? ['rounded px-2 py-0.5', WASH[presentation.tone]] : '']"
    >
      <StatusIcon
        :name="presentation.icon"
        :size="14"
      />{{ presentation.label }}
    </span>
  </component>
</template>
