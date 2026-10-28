<script setup lang="ts">
import type { IconName } from '../lib/presentation'

withDefaults(defineProps<{ name: IconName; size?: number }>(), { size: 16 })
</script>

<template>
  <svg
    :width="size"
    :height="size"
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    stroke-width="1.75"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    focusable="false"
    class="shrink-0"
  >
    <template v-if="name === 'check'">
      <circle
        cx="8"
        cy="8"
        r="6.5"
      />
      <path d="M5 8.2 7 10.2 11 6" />
    </template>
    <template v-else-if="name === 'alert'">
      <path d="M8 1.8 14.6 13.5H1.4Z" />
      <path d="M8 6.2v3.3M8 11.6v.1" />
    </template>
    <template v-else-if="name === 'octagon'">
      <path d="M5.3 1.5h5.4l3.8 3.8v5.4l-3.8 3.8H5.3l-3.8-3.8V5.3Z" />
      <path d="M5.8 5.8l4.4 4.4M10.2 5.8l-4.4 4.4" />
    </template>
    <template v-else-if="name === 'minus'">
      <circle
        cx="8"
        cy="8"
        r="6.5"
      />
      <path d="M5 8h6" />
    </template>
    <template v-else-if="name === 'question'">
      <circle
        cx="8"
        cy="8"
        r="6.5"
      />
      <path d="M6.2 6.2a1.9 1.9 0 1 1 2.6 1.8c-.5.2-.8.6-.8 1.1v.3M8 11.6v.1" />
    </template>
    <template v-else-if="name === 'sync'">
      <path d="M13.5 8a5.5 5.5 0 0 1-9.6 3.7M2.5 8a5.5 5.5 0 0 1 9.6-3.7" />
      <path d="M12.4 1.6v2.9H9.5M3.6 14.4v-2.9h2.9" />
    </template>
    <template v-else>
      <circle
        cx="8"
        cy="8"
        r="6.5"
      />
      <path d="M8 4.5V8l2.3 1.5" />
    </template>
  </svg>
</template>
