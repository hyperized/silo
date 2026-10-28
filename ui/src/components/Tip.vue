<script setup lang="ts">
import { ref, useId } from 'vue'

withDefaults(
  defineProps<{
    /** At most eight words. */
    text: string
    align?: 'start' | 'end'
  }>(),
  { align: 'start' },
)

const id = useId()
const open = ref(false)
</script>

<template>
  <span
    class="relative inline-flex cursor-help"
    tabindex="0"
    :aria-describedby="id"
    @mouseenter="open = true"
    @mouseleave="open = false"
    @focus="open = true"
    @blur="open = false"
    @keydown.esc="open = false"
  >
    <slot />
    <span
      v-show="open"
      :id="id"
      role="tooltip"
      class="pointer-events-none absolute top-full z-20 mt-1.5 w-max max-w-64 rounded bg-ink px-2 py-1 text-left text-xs font-normal whitespace-normal text-panel"
      :class="align === 'end' ? 'right-0' : 'left-0'"
    >{{ text }}</span>
  </span>
</template>
