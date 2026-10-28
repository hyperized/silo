<script setup lang="ts">
defineProps<{ id: string; title: string; count?: number | undefined }>()
</script>

<template>
  <section
    :aria-labelledby="`${id}-title`"
    class="min-w-0 rounded-md border border-rule bg-panel"
  >
    <header class="flex min-h-9 items-center justify-between gap-3 border-b border-rule px-3">
      <h2
        :id="`${id}-title`"
        class="text-[13px] font-semibold text-ink"
      >
        {{ title }}<span
          v-if="count !== undefined"
          class="ml-1.5 font-normal text-ink-3"
        >{{ count }}</span>
      </h2>
      <slot name="meta" />
    </header>
    <slot />
  </section>
</template>
