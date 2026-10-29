import { describe, it, expect } from 'vitest'
import {
  formatAgo,
  formatClock,
  formatCompact,
  formatHorizon,
  formatOf,
  formatRate,
  formatBytes,
  formatCount,
  formatDateTime,
  formatDuration,
  formatIn,
  formatInterval,
  formatMillis,
  formatPercent,
  formatSkew,
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

describe('formatMillis', () => {
  it.each([
    [0, '0 ms'],
    [-3, '0 ms'],
    [849.6, '850 ms'],
    [999, '999 ms'],
    [1000, '1 s'],
    [1840, '1 s'],
    [125_000, '2 min'],
  ])('formats %d as %s', (ms, want) => {
    expect(formatMillis(ms)).toBe(want)
  })
})

describe('formatSkew', () => {
  it.each([
    [0, '0 ms'],
    [42, '+42 ms'],
    [-17, '-17 ms'],
    [2500, '+2,500 ms'],
  ])('formats %d as %s', (ms, want) => {
    expect(formatSkew(ms)).toBe(want)
  })
})

describe('dashboard formats', () => {
  it('formats used of total', () => {
    expect(formatOf(1.1 * 1024 ** 4, 2.4 * 1024 ** 4)).toBe('1.1 of 2.4 TiB')
    expect(formatOf(512 * 1024 ** 2, 2 * 1024 ** 3)).toBe('512 MiB of 2 GiB')
  })

  it('formats rates per second and signed per hour', () => {
    expect(formatRate(38 * 1024 ** 2, 's')).toBe('38 MiB/s')
    expect(formatRate(2.1 * 1024 ** 3, 'h')).toBe('+2.1 GiB/h')
    expect(formatRate(-300 * 1024 ** 2, 'h')).toBe('-300 MiB/h')
    expect(formatRate(0, 'h')).toBe('0 B/h')
  })

  it('formats horizons in days, months and years', () => {
    expect(formatHorizon(16 * 3600)).toBe('16 h')
    expect(formatHorizon(12 * 86400)).toBe('12 d')
    expect(formatHorizon(270 * 86400)).toBe('9 mo')
    expect(formatHorizon(3 * 365 * 86400)).toBe('3 y')
  })

  it('formats compact counts', () => {
    expect(formatCompact(4_200_000)).toBe('4.2M')
    expect(formatCompact(41_000)).toBe('41K')
    expect(formatCompact(312)).toBe('312')
  })

  it('formats a wall-clock time', () => {
    expect(formatClock(new Date(2026, 9, 29, 8, 52))).toMatch(/^08:52/)
  })
})
