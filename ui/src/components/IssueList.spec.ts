import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Finding } from '../lib/health'
import IssueList from './IssueList.vue'

const FINDINGS: Finding[] = [
  { level: 'degraded', text: 'node-b down for 5 min' },
  { level: 'attention', text: 'Backup failed 1 h ago', detail: 'access denied' },
]

describe('IssueList', () => {
  it('shows the number of findings in the heading', () => {
    expect(mount(IssueList, { props: { findings: FINDINGS } }).get('h2').text()).toBe('Issues2')
  })

  it('renders one item per finding', () => {
    const items = mount(IssueList, { props: { findings: FINDINGS } }).findAll('li')
    expect(items).toHaveLength(2)
    expect(items[0]?.text()).toContain('node-b down for 5 min')
    expect(items[1]?.text()).toContain('Backup failed 1 h ago')
  })

  it('names the severity in words for assistive technology', () => {
    const items = mount(IssueList, { props: { findings: FINDINGS } }).findAll('li')
    expect(items[0]?.get('.sr-only').text()).toBe('Critical:')
    expect(items[0]?.classes().length).toBeGreaterThan(0)
    expect(items[0]?.get('span').classes()).toContain('text-crit')
    expect(items[1]?.get('.sr-only').text()).toBe('Warning:')
    expect(items[1]?.get('span').classes()).toContain('text-warn')
  })

  it('shows the detail in a code element only when present', () => {
    const items = mount(IssueList, { props: { findings: FINDINGS } }).findAll('li')
    expect(items[0]?.find('code').exists()).toBe(false)
    expect(items[1]?.get('code').text()).toBe('access denied')
  })

  it('renders an empty list with a zero count', () => {
    const w = mount(IssueList, { props: { findings: [] } })
    expect(w.get('h2').text()).toBe('Issues0')
    expect(w.findAll('li')).toHaveLength(0)
  })
})
