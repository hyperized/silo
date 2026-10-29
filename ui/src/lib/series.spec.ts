import { describe, it, expect } from 'vitest'
import { bucketMeans, deltaCounter, fillGaps, linearTrend, rate, resample, RingBuffer, window, type Sample } from './series'

const s = (t: number, v: number): Sample => ({ t, v })

describe('RingBuffer', () => {
  it.each([0, -1, 1.5, Number.NaN])('rejects capacity %s', (c) => {
    expect(() => new RingBuffer<number>(c)).toThrow(RangeError)
  })

  it('starts empty', () => {
    const b = new RingBuffer<number>(3)
    expect(b.capacity).toBe(3)
    expect(b.size).toBe(0)
    expect(b.toArray()).toEqual([])
    expect(b.last()).toBeUndefined()
  })

  it.each([
    { name: 'partial', push: [1, 2], want: [1, 2] },
    { name: 'exactly full', push: [1, 2, 3], want: [1, 2, 3] },
    { name: 'wrapped once', push: [1, 2, 3, 4], want: [2, 3, 4] },
    { name: 'wrapped twice', push: [1, 2, 3, 4, 5, 6, 7], want: [5, 6, 7] },
  ])('keeps the newest entries oldest first ($name)', ({ push, want }) => {
    const b = new RingBuffer<number>(3)
    for (const v of push) b.push(v)
    expect(b.toArray()).toEqual(want)
    expect(b.size).toBe(want.length)
    expect(b.last()).toBe(want.at(-1))
  })

  it('hands out a copy', () => {
    const b = new RingBuffer<number>(2)
    b.push(1)
    b.toArray().push(9)
    expect(b.toArray()).toEqual([1])
  })

  it('clears and reuses', () => {
    const b = new RingBuffer<number>(2)
    b.push(1)
    b.push(2)
    b.push(3)
    b.clear()
    expect(b.size).toBe(0)
    expect(b.last()).toBeUndefined()
    b.push(4)
    expect(b.toArray()).toEqual([4])
  })
})

describe('rate', () => {
  it.each([
    { name: 'empty', in: [], want: null },
    { name: 'one sample', in: [s(0, 5)], want: null },
    { name: 'steady', in: [s(0, 0), s(1000, 10), s(3000, 30)], want: 10 },
    { name: 'flat', in: [s(0, 7), s(2000, 7)], want: 0 },
    { name: 'counter reset', in: [s(0, 100), s(1000, 3)], want: null },
    { name: 'time did not advance', in: [s(1000, 1), s(1000, 2)], want: null },
    { name: 'time went back', in: [s(2000, 1), s(1000, 2)], want: null },
  ])('$name', ({ in: input, want }) => {
    expect(rate(input)).toBe(want)
  })
})

describe('window', () => {
  it('keeps samples inside the span, boundary included', () => {
    const in_ = [s(0, 1), s(500, 2), s(1000, 3), s(2000, 4)]
    expect(window(in_, 2000, 1500)).toEqual([s(500, 2), s(1000, 3), s(2000, 4)])
    expect(window(in_, 10_000, 1000)).toEqual([])
  })
})

describe('bucketMeans', () => {
  it.each([
    { name: 'zero buckets', buckets: 0, span: 100 },
    { name: 'fractional buckets', buckets: 1.5, span: 100 },
    { name: 'zero span', buckets: 4, span: 0 },
    { name: 'NaN span', buckets: 4, span: Number.NaN },
  ])('is empty for $name', ({ buckets, span }) => {
    expect(bucketMeans([s(0, 1)], 0, span, buckets)).toEqual({ times: [], means: [] })
  })

  it('averages per bucket, stamps bucket ends and drops out-of-span or non-finite samples', () => {
    const got = bucketMeans(
      [s(-1, 99), s(0, 2), s(10, 4), s(30, 8), s(40, 6), s(41, 99), s(35, Number.NaN)],
      40,
      40,
      4,
    )
    expect(got.times).toEqual([10, 20, 30, 40])
    expect(got.means).toEqual([2, 4, null, 7])
  })
})

describe('fillGaps', () => {
  it.each([
    { name: 'empty', in: [], want: [] },
    { name: 'all null', in: [null, null], want: [] },
    { name: 'no gaps', in: [1, 2], want: [1, 2] },
    { name: 'leading', in: [null, null, 3, 4], want: [3, 3, 3, 4] },
    { name: 'inner and trailing', in: [1, null, 2, null, null], want: [1, 1, 2, 2, 2] },
    { name: 'zero is a value', in: [0, null], want: [0, 0] },
  ])('$name', ({ in: input, want }) => {
    expect(fillGaps(input)).toEqual(want)
  })
})

describe('resample', () => {
  it('is empty when nothing is in the span', () => {
    expect(resample([s(0, 1)], 10_000, 100, 4)).toEqual({ times: [], values: [] })
  })

  it('fills every bucket without NaN', () => {
    const got = resample([s(25, 5), s(30, 7), s(80, 1)], 100, 100, 4)
    expect(got.times).toEqual([25, 50, 75, 100])
    expect(got.values).toEqual([6, 6, 6, 1])
    expect(got.values.some(Number.isNaN)).toBe(false)
  })
})

describe('linearTrend', () => {
  it.each([
    { name: 'empty', in: [] as Sample[] },
    { name: 'one sample', in: [s(0, 5)] },
    { name: 'same instant', in: [s(1000, 1), s(1000, 9)] },
  ])('has no slope for $name', ({ in: input }) => {
    const tr = linearTrend(input)
    expect(tr.perSecond).toBe(0)
    expect(tr.secondsUntil(100)).toBeNull()
  })

  it('fits a growing line', () => {
    const t0 = 1_800_000_000_000
    const tr = linearTrend([s(t0, 10), s(t0 + 1000, 12), s(t0 + 2000, 14)])
    expect(tr.perSecond).toBeCloseTo(2)
    expect(tr.secondsUntil(20)).toBeCloseTo(3)
    expect(tr.secondsUntil(10)).toBe(0)
  })

  it('fits noisy data by least squares', () => {
    const tr = linearTrend([s(0, 0), s(1000, 3), s(2000, 2), s(3000, 5)])
    expect(tr.perSecond).toBeCloseTo(1.4)
  })

  it.each([
    { name: 'flat', in: [s(0, 5), s(1000, 5)] },
    { name: 'shrinking', in: [s(0, 9), s(1000, 5)] },
  ])('never reaches the target when $name', ({ in: input }) => {
    expect(linearTrend(input).secondsUntil(100)).toBeNull()
  })
})

describe('deltaCounter', () => {
  it.each([
    { name: 'empty', in: [], want: [] },
    { name: 'one', in: [s(0, 3)], want: [] },
    { name: 'growing', in: [s(0, 1), s(1, 4), s(2, 4), s(3, 10)], want: [3, 0, 6] },
    { name: 'reset clamps to 0', in: [s(0, 50), s(1, 2), s(2, 5)], want: [0, 3] },
  ])('$name', ({ in: input, want }) => {
    expect(deltaCounter(input)).toEqual(want)
  })
})
