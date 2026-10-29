import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Finding } from '../lib/health'
import IssueStrip from './IssueStrip.vue'

function strip(findings: Finding[]) {
  return mount(IssueStrip, { props: { findings } })
}

describe('IssueStrip', () => {
  it('shows the worst finding', () => {
    const w = strip([
      { level: 'degraded', text: 'Cluster degraded: 2 nodes alive' },
      { level: 'attention', text: 'Backups overdue' },
    ])
    expect(w.text()).toContain('Cluster degraded: 2 nodes alive')
    expect(w.text()).not.toContain('Backups overdue')
  })

  it('reads as critical for a degraded finding', () => {
    const w = strip([{ level: 'degraded', text: 'node-a isolated' }])
    expect(w.get('.sr-only').text()).toBe('Critical:')
    expect(w.classes()).toContain('bg-crit-wash')
  })

  it('reads as a warning for an attention finding', () => {
    const w = strip([{ level: 'attention', text: 'Backups overdue' }])
    expect(w.get('.sr-only').text()).toBe('Warning:')
    expect(w.classes()).toContain('bg-warn-wash')
  })

  it('links to the issues card with the number of other findings', () => {
    const w = strip([
      { level: 'attention', text: 'a' },
      { level: 'attention', text: 'b' },
      { level: 'attention', text: 'c' },
    ])
    const a = w.get('a')
    expect(a.text()).toBe('and 2 more')
    expect(a.attributes('href')).toBe('#issues')
  })

  it('has no link for a single finding', () => {
    expect(strip([{ level: 'attention', text: 'a' }]).find('a').exists()).toBe(false)
  })
})
