import { onScopeDispose, readonly, ref, watch, type Ref } from 'vue'

/** A clock ref for relative ages; it stops ticking while `active` is false. */
export function useNow(active: Readonly<Ref<boolean>>, intervalMs = 1000): Readonly<Ref<number>> {
  const now = ref(Date.now())
  let timer: ReturnType<typeof setInterval> | undefined
  const stop = (): void => {
    clearInterval(timer)
    timer = undefined
  }
  watch(
    active,
    (on) => {
      stop()
      if (!on) return
      now.value = Date.now()
      timer = setInterval(() => {
        now.value = Date.now()
      }, intervalMs)
    },
    { immediate: true },
  )
  onScopeDispose(stop)
  return readonly(now)
}
