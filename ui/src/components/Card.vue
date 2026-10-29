<!-- The panel shell of the dashboard: a titled card with an optional meta slot beside the title. -->
<script setup lang="ts">
withDefaults(
  defineProps<{
    id: string
    title: string
    count?: number | undefined
    /** Border tone; a card in trouble is outlined so it stands out in a grid of equals. */
    tone?: 'normal' | 'warn' | 'crit'
  }>(),
  { count: undefined, tone: 'normal' },
)

const BORDER = { normal: 'border-rule', warn: 'border-warn-bar', crit: 'border-crit' } as const
</script>

<template>
  <section
    :id="id"
    :aria-labelledby="`${id}-title`"
    class="flex min-w-0 flex-col gap-2.5 rounded-[10px] border bg-panel px-4 py-3.5 max-sm:p-3"
    :class="BORDER[tone]"
  >
    <header class="flex min-h-5 items-center justify-between gap-3">
      <h2
        :id="`${id}-title`"
        class="min-w-0 text-[13px] font-semibold text-ink"
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
