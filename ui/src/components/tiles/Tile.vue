<!-- One instrument in the top row: a small label over a gauge or a big number, with a footer line. -->
<script setup lang="ts">
defineProps<{ id: string; title: string }>()
</script>

<template>
  <section
    :aria-labelledby="`${id}-title`"
    class="flex min-w-0 flex-col gap-1.5 rounded-[10px] border border-rule bg-panel px-4 py-3.5 max-sm:p-3"
  >
    <h2
      :id="`${id}-title`"
      class="text-xs font-medium text-ink-3"
    >
      {{ title }}
    </h2>
    <slot />
  </section>
</template>
