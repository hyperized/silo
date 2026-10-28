<script setup lang="ts">
import StatusIcon from './StatusIcon.vue'

defineProps<{ message: string; retryInSeconds: number | null; retrying: boolean }>()
defineEmits<{ retry: [] }>()
</script>

<template>
  <section
    role="alert"
    aria-labelledby="error-title"
    class="rounded-md border border-rule bg-panel px-4 py-4"
  >
    <div class="grid grid-cols-[1.25rem_minmax(0,1fr)] gap-x-3 gap-y-1">
      <span class="pt-0.5 text-crit"><StatusIcon
        name="octagon"
        :size="18"
      /></span>
      <h2
        id="error-title"
        class="text-[15px] font-semibold"
      >
        No status from silod
      </h2>
      <p class="col-start-2 max-w-[80ch] text-ink-2">
        {{ message }}
      </p>
      <div class="col-start-2 mt-2 flex items-center gap-3">
        <button
          type="button"
          class="inline-flex min-h-11 items-center rounded bg-accent px-4 font-medium text-panel transition-opacity duration-100 ease-out hover:opacity-90 active:opacity-80 disabled:opacity-60 motion-reduce:transition-none"
          :disabled="retrying"
          @click="$emit('retry')"
        >
          Retry
        </button>
        <span
          class="text-[13px] text-ink-3"
          aria-live="off"
        >
          <template v-if="retrying">retrying</template>
          <template v-else-if="retryInSeconds !== null">next try in {{ retryInSeconds }} s</template>
        </span>
      </div>
    </div>
  </section>
</template>
