import { onScopeDispose, readonly, ref, type Ref } from 'vue'

/** Tracks document visibility so timers can stop while nobody is looking. */
export function usePageVisible(): Readonly<Ref<boolean>> {
  const visible = ref(document.visibilityState !== 'hidden')
  const onChange = (): void => {
    visible.value = document.visibilityState !== 'hidden'
  }
  document.addEventListener('visibilitychange', onChange)
  onScopeDispose(() => {
    document.removeEventListener('visibilitychange', onChange)
  })
  return readonly(visible)
}
