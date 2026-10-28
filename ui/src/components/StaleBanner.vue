<script setup lang="ts">
import StatusIcon from './StatusIcon.vue'

defineProps<{ age: string; message: string; retryInSeconds: number | null }>()
defineEmits<{ retry: [] }>()
</script>

<template>
  <div
    role="status"
    class="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-md border border-rule bg-warn-wash px-3 py-1.5 text-[13px]"
  >
    <span class="inline-flex items-center gap-1.5 font-medium text-warn">
      <StatusIcon
        name="alert"
        :size="14"
      />Stale, {{ age }}
    </span>
    <span class="min-w-0 flex-1 text-ink-2">{{ message }}</span>
    <span
      v-if="retryInSeconds !== null"
      class="text-ink-3"
    >retry in {{ retryInSeconds }} s</span>
    <button
      type="button"
      class="inline-flex min-h-11 items-center rounded px-2 font-medium text-accent hover:bg-panel active:bg-track"
      @click="$emit('retry')"
    >
      Retry
    </button>
  </div>
</template>
