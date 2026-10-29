<script setup lang="ts">
import type { ClusterNode } from '../api/status'
import NodeCard from './NodeCard.vue'
import Skeleton from './Skeleton.vue'

defineProps<{
  /** null while the first answer is pending. */
  nodes: ClusterNode[] | null
  respondingNodeId: string
  now: number
  /** Write sparkline per node id; a node missing here gets an empty line. */
  sparks: Readonly<Record<string, number[]>>
}>()
</script>

<template>
  <section
    aria-labelledby="nodes-title"
    class="flex flex-col gap-3"
  >
    <h2
      id="nodes-title"
      class="sr-only"
    >
      Nodes
    </h2>
    <div
      v-if="nodes === null"
      class="grid gap-4 max-sm:gap-3 wide:grid-cols-3"
      aria-hidden="true"
    >
      <Skeleton
        v-for="i in 3"
        :key="i"
        class="h-[190px] rounded-[10px]"
      />
    </div>
    <p
      v-else-if="nodes.length === 0"
      class="rounded-[10px] border border-rule bg-panel px-4 py-3.5 text-ink-2"
    >
      No nodes
    </p>
    <div
      v-else
      class="grid gap-4 max-sm:gap-3 wide:grid-cols-3"
    >
      <NodeCard
        v-for="n in nodes"
        :key="n.id"
        :node="n"
        :self="n.id === respondingNodeId"
        :now="now"
        :spark="sparks[n.id] ?? []"
      />
    </div>
  </section>
</template>
