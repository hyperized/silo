import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Finding } from '../lib/health'
import IssueList from './IssueList.vue'
import Skeleton from './Skeleton.vue'

function list(findings: Finding[] | null) {
  return mount(IssueList, { props: { findings } })
}

describe('IssueList', () => {
  it('shows skeletons and no count while the first answer is pending', () => {
    const w = list(null)
    expect(w.findAllComponents(Skeleton)).toHaveLength(2)
    expect(w.get('h2').text()).toBe('Issues')
    expect(w.find('ul').exists()).toBe(false)
  })

  it('says there are no open issues', () => {
    const w = list([])
    expect(w.text()).toContain('No open issues')
    expect(w.get('h2').text()).toBe('Issues')
    expect(w.find('ul').exists()).toBe(false)
  })

  it('lists findings with the level as a word', () => {
    const w = list([
      { level: 'degraded', text: 'Cluster degraded' },
      { level: 'attention', text: 'Backups overdue' },
    ])
    const items = w.findAll('li')
    expect(items).toHaveLength(2)
    expect(items[0]?.text()).toBe('Critical: Cluster degraded')
    expect(items[0]?.get('.sr-only').text()).toBe('Critical:')
    expect(items[1]?.text()).toBe('Warning: Backups overdue')
    expect(items[1]?.get('.sr-only').text()).toBe('Warning:')
    expect(w.text()).not.toContain('No open issues')
  })

  it('counts the findings in the heading', () => {
    const w = list([
      { level: 'attention', text: 'a' },
      { level: 'attention', text: 'b' },
      { level: 'attention', text: 'c' },
    ])
    expect(w.get('h2').text()).toBe('Issues3')
  })

  it('prints a finding detail verbatim in a code block', () => {
    const w = list([
      { level: 'attention', text: 'Last backup failed', detail: 'upload: 403\n  forbidden' },
      { level: 'attention', text: 'No detail here' },
    ])
    const codes = w.findAll('code')
    expect(codes).toHaveLength(1)
    expect(codes[0]?.text()).toBe('upload: 403\n  forbidden')
  })
})
