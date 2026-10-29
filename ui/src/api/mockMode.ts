// Kept apart from mock.ts so choosing a scenario never pulls the fixture into
// the production bundle; mock.ts is only loaded through a dynamic import.

export const MOCK_SCENARIOS = ['healthy', 'attention', 'degraded', 'incident', 'empty', 'unconfigured', 'error'] as const
export type MockScenario = (typeof MOCK_SCENARIOS)[number]

/**
 * Picks a scenario from `?mock=<name>`. `?mock=1` or VITE_MOCK=1 alone means
 * attention, the scenario that fills every panel and the issue list.
 */
export function mockScenario(search: string, envFlag: string | undefined): MockScenario | null {
  const q = new URLSearchParams(search).get('mock')
  if (q && (MOCK_SCENARIOS as readonly string[]).includes(q)) return q as MockScenario
  if (q === '1' || envFlag === '1') return 'attention'
  return null
}
