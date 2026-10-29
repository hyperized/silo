import { describe, expect, it } from 'vitest'
import {
  arcDash,
  arcTick,
  bulletX,
  dashWithin,
  linePoints,
  nearestIndex,
  num,
  ringSegments,
  seriesOpacity,
  stackPaths,
  xAt,
  yScale,
} from './chart'

const sum = (s: string): number => s.split(' ').reduce((a, v) => a + Number(v), 0)

describe('num', () => {
  it.each([
    [1.234, 1.23],
    [1.235, 1.24],
    [Number.NaN, 0],
    [Number.POSITIVE_INFINITY, 0],
  ])('%f -> %f', (x, want) => {
    expect(num(x)).toBe(want)
  })
})

describe('arcDash', () => {
  it('matches the mockup gauge at 47 % of a 270 degree arc', () => {
    expect(arcDash(0.47, 46, 270)).toEqual({ dasharray: '101.88 289.03', dashoffset: 0 })
    expect(arcDash(1, 46, 270).dasharray).toBe('216.77 289.03')
  })

  it.each([
    ['below zero', -1, '0 289.03'],
    ['above one', 2, '216.77 289.03'],
    ['NaN', Number.NaN, '0 289.03'],
  ])('clamps a ratio %s', (_, ratio, want) => {
    expect(arcDash(ratio, 46, 270).dasharray).toBe(want)
  })

  it('survives a negative radius', () => {
    expect(arcDash(0.5, -3, 270).dasharray).toBe('0 0')
  })
})

describe('arcTick', () => {
  it('places a 2 px mark at the ratio along the arc', () => {
    expect(arcTick(0.8, 46, 270)).toEqual({ dasharray: '2 287.03', dashoffset: -173.42 })
    expect(arcTick(0.9, 46, 270, 3).dasharray).toBe('3 286.03')
  })

  it('never produces a negative gap', () => {
    expect(arcTick(0.5, 0, 270).dasharray).toBe('2 0')
  })
})

describe('dashWithin', () => {
  it('fills the length with dashes and pads to the total', () => {
    expect(dashWithin(20, 100)).toBe('6 4 6 84')
    expect(sum(dashWithin(20, 100))).toBe(100)
  })

  it('ends on a partial dash and adds the rest as a gap', () => {
    expect(dashWithin(13, 50)).toBe('6 4 3 37')
  })

  it('always has an even count so the browser does not repeat it', () => {
    for (const len of [1, 6, 7, 10, 11, 16, 90]) {
      expect(dashWithin(len, 289).split(' ').length % 2).toBe(0)
      expect(sum(dashWithin(len, 289))).toBeCloseTo(289, 1)
    }
  })

  it.each([
    ['zero length', 0, 50, 6, '0 50'],
    ['negative total', 10, -5, 6, '0 0'],
    ['zero dash', 10, 50, 0, '0 50'],
  ])('handles %s', (_, len, total, dash, want) => {
    expect(dashWithin(len, total, dash)).toBe(want)
  })

  it('treats a negative space as none', () => {
    expect(dashWithin(10, 20, 4, -1)).toBe('4 0 4 0 2 10')
  })
})

describe('ringSegments', () => {
  it('splits a ring into equal segments with gaps, like the mockup', () => {
    const segs = ringSegments(3, 46, 6.33)
    expect(segs.map((s) => s.dashoffset)).toEqual([0, -96.34, -192.68])
    expect(segs[0]).toMatchObject({ length: 90.01, circumference: 289.03, dasharray: '90.01 199.01' })
  })

  it('draws a single segment as the whole ring', () => {
    expect(ringSegments(1, 46, 6)).toEqual([
      { length: 289.03, circumference: 289.03, dasharray: '289.03 0', dashoffset: 0 },
    ])
  })

  it.each([
    ['none', 0],
    ['negative', -2],
    ['NaN', Number.NaN],
  ])('returns no segments for %s', (_, n) => {
    expect(ringSegments(n, 46, 6)).toEqual([])
  })

  it('keeps segment length non-negative when the gap is too wide', () => {
    expect(ringSegments(4, 10, 100)[0]?.length).toBe(0)
    expect(ringSegments(2, 10, -5)[0]?.length).toBe(31.42)
  })
})

describe('bulletX', () => {
  it.each([
    ['zero', 0, 0],
    ['negative', -5, 0],
    ['NaN', Number.NaN, 0],
    ['healthy', 24, 4.61],
    ['at warn', 500, 96],
    ['between', 740, 105.98],
    ['at crit', 2000, 158.4],
    ['beyond crit', 2300, 160],
  ])('places %s', (_, value, want) => {
    expect(bulletX(value, 500, 2000, 160)).toBe(want)
  })

  it('handles collapsed and missing thresholds', () => {
    expect(bulletX(10, 0, 0, 160)).toBe(160)
    expect(bulletX(10, 10, 10, 160)).toBe(96)
    expect(bulletX(10, 5, 5, 160)).toBe(160)
    expect(bulletX(10, 5, 20, Number.NaN)).toBe(0)
    expect(bulletX(10, 5, 20, -1)).toBe(0)
  })
})

describe('yScale', () => {
  it('maps min to the bottom and max to the top with padding', () => {
    const y = yScale([0, 10], 28, 2)
    expect(y(0)).toBe(26)
    expect(y(10)).toBe(2)
    expect(y(5)).toBe(14)
  })

  it('draws a constant or empty series at mid height', () => {
    expect(yScale([3, 3], 28, 2)(3)).toBe(14)
    expect(yScale([], 28, 2)(1)).toBe(14)
  })

  it('treats non-finite samples as zero', () => {
    const y = yScale([Number.NaN, 10], 20, 0)
    expect(y(Number.POSITIVE_INFINITY)).toBe(20)
  })

  it('never inverts when the padding exceeds the height', () => {
    expect(yScale([0, 1], 2, 5)(0)).toBe(5)
  })
})

describe('xAt', () => {
  it('spreads samples across the width', () => {
    expect(xAt(2, 3, 100)).toBe(100)
    expect(xAt(0, 1, 100)).toBe(0)
  })
})

describe('linePoints', () => {
  it('returns nothing for no values', () => {
    expect(linePoints([], 160, 28, 2)).toBe('')
  })

  it('draws a single value as a flat line across the box', () => {
    expect(linePoints([5], 160, 28, 2)).toBe('0,14 160,14')
  })

  it('spaces points evenly and scales to the series range', () => {
    expect(linePoints([0, 5, 10], 160, 28, 2)).toBe('0,26 80,14 160,2')
  })
})

describe('stackPaths', () => {
  it('returns nothing for empty input', () => {
    expect(stackPaths([], 100, 50, 10)).toEqual({ areas: [], separators: [] })
    expect(stackPaths([[]], 100, 50, 10)).toEqual({ areas: [], separators: [] })
  })

  it('stacks series bottom-up with separators between them', () => {
    const { areas, separators } = stackPaths([[1, 2], [3, 2]], 100, 50, 10)
    expect(areas).toEqual(['M0,45 L100,40 L100,50 L0,50 Z', 'M0,30 L100,30 L100,40 L0,45 Z'])
    expect(separators).toEqual(['0,45 100,40'])
  })

  it('stretches a single sample across the width', () => {
    expect(stackPaths([[5]], 100, 50, 10).areas).toEqual(['M0,25 L100,25 L100,50 L0,50 Z'])
  })

  it('pads short series, ignores bad samples and clamps above yMax', () => {
    const { areas } = stackPaths([[Number.NaN, -1, 20], [1]], 100, 10, 10)
    expect(areas[0]).toBe('M0,10 L50,10 L100,0 L100,10 L50,10 L0,10 Z')
    expect(areas[1]).toBe('M0,9 L50,10 L100,0 L100,0 L50,10 L0,10 Z')
  })

  it.each([0, -1, Number.NaN])('falls back to a unit scale for yMax %f', (yMax) => {
    expect(stackPaths([[1]], 10, 10, yMax).areas[0]).toBe('M0,0 L10,0 L10,10 L0,10 Z')
  })
})

describe('seriesOpacity', () => {
  it.each([
    [0, 0.8],
    [1, 0.55],
    [2, 0.3],
    [3, 0.15],
    [9, 0.15],
  ])('series %i -> %f', (i, want) => {
    expect(seriesOpacity(i)).toBe(want)
  })
})

describe('nearestIndex', () => {
  it.each([
    ['no samples', 10, 0, 100, null],
    ['NaN count', 10, Number.NaN, 100, null],
    ['one sample', 80, 1, 100, 0],
    ['zero width', 10, 5, 0, 0],
    ['left edge', -20, 5, 100, 0],
    ['right edge', 500, 5, 100, 4],
    ['nearest', 60, 5, 100, 2],
    ['NaN x', Number.NaN, 5, 100, 0],
  ])('%s', (_, x, count, w, want) => {
    expect(nearestIndex(x, count, w)).toBe(want)
  })
})
