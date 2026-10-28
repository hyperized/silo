<script setup lang="ts">
import { computed } from 'vue'
import { formatAgo, formatDateTime, formatDuration, formatIn } from '../lib/format'

const props = defineProps<{
  date: Date
  now: number
  /** ago: "48 min ago", in: "in 5 h", for: "16 min" (time elapsed since). */
  mode: 'ago' | 'in' | 'for'
}>()

const text = computed(() => {
  if (props.mode === 'ago') return formatAgo(props.date, props.now)
  if (props.mode === 'in') return formatIn(props.date, props.now)
  return formatDuration((props.now - props.date.getTime()) / 1000)
})
</script>

<template>
  <time
    :datetime="date.toISOString()"
    :title="formatDateTime(date)"
  >{{ text }}</time>
</template>
