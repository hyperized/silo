<script setup lang="ts">
import { computed } from 'vue'
import { formatCount } from '../../lib/format'
import { SKEW_CRIT_MS, SKEW_WARN_MS, skewLevel } from '../../lib/health'
import type { Presentation } from '../../lib/presentation'
import { Bullet } from '../charts'
import Skeleton from '../Skeleton.vue'
import StateBadge from '../StateBadge.vue'
import Tile from './Tile.vue'

const props = defineProps<{
  /** Largest absolute skew to any peer; null while the first answer is pending. */
  skewMs: number | null
  alerts: number
  /** False for an older silod that measures no skew at all. */
  reported: boolean
}>()

const over = computed<Presentation | null>(() => {
  // Only read in the reported branch, where skewMs is known.
  const level = skewLevel(props.skewMs as number)
  if (level === 'degraded') return { label: `Over ${formatCount(SKEW_CRIT_MS)} ms`, tone: 'crit', icon: 'octagon' }
  if (level === 'attention') return { label: `Over ${formatCount(SKEW_WARN_MS)} ms`, tone: 'warn', icon: 'alert' }
  return null
})

const alertText = computed(() => `${formatCount(props.alerts)} ${props.alerts === 1 ? 'alert' : 'alerts'} since start`)
</script>

<template>
  <Tile
    id="tile-skew"
    title="Clock skew"
  >
    <template v-if="skewMs === null">
      <Skeleton class="mt-1.5 h-[30px] w-16" />
      <Skeleton class="mt-2 h-[38px]" />
    </template>
    <template v-else-if="reported">
      <p class="mt-1.5 flex items-baseline gap-2">
        <span class="text-[30px] leading-none font-semibold">{{ formatCount(skewMs) }}</span>
        <span class="text-xs text-ink-3">ms, worst peer</span>
      </p>
      <div class="mt-2">
        <Bullet
          :value="skewMs"
          :warn="SKEW_WARN_MS"
          :crit="SKEW_CRIT_MS"
          unit="ms"
          label="Clock skew"
        />
      </div>
      <p class="mt-auto flex flex-wrap items-center gap-x-2 text-[11.5px] text-ink-3">
        <StateBadge
          v-if="over"
          :presentation="over"
        />
        <span>{{ alertText }}</span>
      </p>
    </template>
    <template v-else>
      <p class="mt-1.5 text-[30px] leading-none font-semibold text-ink-3">
        n/a
      </p>
      <p class="mt-auto text-[11.5px] text-ink-3">
        not reported
      </p>
    </template>
  </Tile>
</template>
