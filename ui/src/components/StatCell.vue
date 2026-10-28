<script setup lang="ts">
import { ref, watch } from 'vue'

const props = defineProps<{
  label: string
  /** Compared between refreshes; a change briefly highlights the cell. */
  value: string
}>()

const flash = ref(0)
watch(
  () => props.value,
  () => {
    flash.value++
  },
)
</script>

<template>
  <div class="relative min-w-0 bg-panel px-4 pt-3 pb-3.5">
    <span
      v-if="flash > 0"
      :key="flash"
      class="animate-flash pointer-events-none absolute inset-0 bg-accent/8"
      aria-hidden="true"
    />
    <dt class="relative text-xs font-medium text-ink-3">
      {{ label }}
    </dt>
    <dd class="relative mt-1.5">
      <slot />
    </dd>
  </div>
</template>
