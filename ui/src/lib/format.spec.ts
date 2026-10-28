import { describe, it, expect } from 'vitest'
import {
  formatAgo,
  formatBytes,
  formatCount,
  formatDateTime,
  formatDuration,
  formatIn,
  formatInterval,
  formatPercent,
  formatUsedOf,
} from './format'

const KiB = 1024
const GiB = KiB ** 3

describe('formatBytes', () => {
  it.each<[number, string]>([
    [0, '0 B'],
    [-5, '0 B'],
    [Number.NaN, '0 B'],
    [Number.POSITIVE_INFINITY, '0 B'],
    [512, '512 B'],
    [1023, '1023 B'],
    [1024, '1 KiB'],
    [1536, '1.5 KiB'],
    [10 * KiB, '10 KiB'],
    [7.5 * GiB, '7.5 GiB'],
    [30 * GiB, '30 GiB'],
    [1024 ** 4, '1 TiB'],
    [1024 ** 5, '1 PiB'],
    [1024 ** 6, '1 EiB'],
    [1024 ** 7, '1024 EiB'],
    [5 * 1024 ** 8, '5242880 EiB'],
  ])('formats %d as %s', (input, want) => {
    expect(formatBytes(input)).toBe(want)
  })
})

describe('formatCount', () => {
  it.each<[number, string]>([
    [0, '0'],
    [999, '999'],
    [12345, '12,345'],
    [1234567, '1,234,567'],
  ])('formats %d as %s', (input, want) => {
    expect(formatCount(input)).toBe(want)
  })
})

describe('formatPercent', () => {
  it.each<[number, string]>([
    [0, '0%'],
    [0.001, '<1%'],
    [0.0099, '<1%'],
    [0.01, '1%'],
    [0.844, '84%'],
    [0.845, '85%'],
    [1, '100%'],
    [1.5, '100%'],
    [-0.2, '0%'],
  ])('formats %d as %s', (input, want) => {
    expect(formatPercent(input)).toBe(want)
  })
})

describe('formatDuration', () => {
  it.each<[number, string]>([
    [0, '0 s'],
    [-10, '0 s'],
    [1, '1 s'],
    [45, '45 s'],
    [59.9, '59 s'],
    [60, '1 min'],
    [119, '1 min'],
    [960, '16 min'],
    [3599, '59 min'],
    [3600, '1 h'],
    [7200, '2 h'],
    [86399, '23 h'],
    [86400, '1 d'],
    [3 * 86400, '3 d'],
  ])('formats %d s as %s', (input, want) => {
    expect(formatDuration(input)).toBe(want)
  })
})

describe('formatAgo and formatIn', () => {
  const now = 1_800_000_000_000
  const at = (offsetS: number): Date => new Date(now + offsetS * 1000)

  it.each<[number, string]>([
    [0, 'now'],
    [-4, 'now'],
    [10, 'now'],
    [-5, '5 s ago'],
    [-48 * 60, '48 min ago'],
    [-3 * 3600, '3 h ago'],
  ])('formatAgo at offset %d s is %s', (offset, want) => {
    expect(formatAgo(at(offset), now)).toBe(want)
  })

  it.each<[number, string]>([
    [0, 'due'],
    [4, 'due'],
    [-100, 'due'],
    [5, 'in 5 s'],
    [5 * 3600, 'in 5 h'],
    [86400, 'in 1 d'],
  ])('formatIn at offset %d s is %s', (offset, want) => {
    expect(formatIn(at(offset), now)).toBe(want)
  })
})

describe('formatInterval', () => {
  it.each<[number, string]>([
    [0, 'none'],
    [-1, 'none'],
    [30, 'every 30 s'],
    [60, 'every 1 min'],
    [21600, 'every 6 h'],
    [172800, 'every 2 d'],
  ])('formats %d s as %s', (input, want) => {
    expect(formatInterval(input)).toBe(want)
  })
})

describe('formatUsedOf', () => {
  it.each<[number, number, string]>([
    [84 * GiB, 100 * GiB, '84 / 100 GiB'],
    [512 * 1024 ** 2, 100 * GiB, '512 MiB / 100 GiB'],
    [0, 100 * GiB, '0 B / 100 GiB'],
    [0, 0, '0 / 0 B'],
  ])('formats %d of %d as %s', (used, total, want) => {
    expect(formatUsedOf(used, total)).toBe(want)
  })
})

describe('formatDateTime', () => {
  it('returns a non-empty string containing the year', () => {
    const text = formatDateTime(new Date(Date.UTC(2026, 9, 28, 12, 0)))
    expect(text.length).toBeGreaterThan(0)
    expect(text).toContain('2026')
  })
})
