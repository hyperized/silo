// Time-series helpers over client-side samples. Every function is total: bad
// or short input gives an empty result or null, never NaN and never a throw
// (except constructing a RingBuffer with a nonsensical capacity, which is a
// programming error).

/** One observation: `t` in epoch milliseconds, `v` in the series' own unit. */
export interface Sample {
  t: number
  v: number
}

/**
 * Fixed-size FIFO that overwrites its oldest entry once full. Push is O(1)
 * because it writes into a circular index instead of shifting an array, so a
 * buffer polled every few seconds for hours never reallocates.
 */
export class RingBuffer<T> {
  readonly capacity: number
  private readonly slots: (T | undefined)[]
  private next = 0
  private count = 0

  /** @throws RangeError when capacity is not a positive integer. */
  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(`RingBuffer capacity must be a positive integer, got ${String(capacity)}`)
    }
    this.capacity = capacity
    this.slots = new Array<T | undefined>(capacity)
  }

  get size(): number {
    return this.count
  }

  push(v: T): void {
    this.slots[this.next] = v
    this.next = (this.next + 1) % this.capacity
    if (this.count < this.capacity) this.count++
  }

  /** Oldest first. A fresh array, safe for the caller to keep. */
  toArray(): T[] {
    const out: T[] = []
    const start = (this.next - this.count + this.capacity) % this.capacity
    for (let i = 0; i < this.count; i++) out.push(this.slots[(start + i) % this.capacity] as T)
    return out
  }

  last(): T | undefined {
    return this.count === 0 ? undefined : this.slots[(this.next - 1 + this.capacity) % this.capacity]
  }

  clear(): void {
    // Drop references so cleared entries can be collected.
    this.slots.fill(undefined)
    this.next = 0
    this.count = 0
  }
}

/**
 * Per-second rate of a monotonic counter from its last two samples. Null with
 * fewer than two samples, when time did not advance, or when the counter went
 * down: a reset (silod restart) says nothing about the real rate.
 */
export function rate(samples: readonly Sample[]): number | null {
  const a = samples.at(-2)
  const b = samples.at(-1)
  if (!a || !b) return null
  const dt = (b.t - a.t) / 1000
  if (dt <= 0 || b.v < a.v) return null
  return (b.v - a.v) / dt
}

/** The samples with `t >= nowMs - spanMs`, in their original order. */
export function window<S extends { t: number }>(samples: readonly S[], nowMs: number, spanMs: number): S[] {
  const from = nowMs - spanMs
  return samples.filter((s) => s.t >= from)
}

/** Bucket timestamps plus the mean per bucket; null where no sample fell. */
export interface BucketMeans {
  times: number[]
  means: (number | null)[]
}

/**
 * Splits `[now - span, now]` into `buckets` equal slots and averages the
 * samples in each. A bucket's timestamp is its end, so the last one is `now`.
 * Samples outside the span are ignored. Zero buckets or a non-positive span
 * give empty arrays.
 */
export function bucketMeans(samples: readonly Sample[], nowMs: number, spanMs: number, buckets: number): BucketMeans {
  if (!Number.isInteger(buckets) || buckets < 1 || !(spanMs > 0)) return { times: [], means: [] }
  const start = nowMs - spanMs
  const width = spanMs / buckets
  const acc = new Map<number, { sum: number; count: number }>()
  for (const s of samples) {
    if (s.t < start || s.t > nowMs || !Number.isFinite(s.v)) continue
    // t == now lands one past the end; it belongs to the last bucket.
    const i = Math.min(Math.floor((s.t - start) / width), buckets - 1)
    const b = acc.get(i)
    if (b) {
      b.sum += s.v
      b.count++
    } else {
      acc.set(i, { sum: s.v, count: 1 })
    }
  }
  const times: number[] = []
  const means: (number | null)[] = []
  for (let i = 0; i < buckets; i++) {
    times.push(start + (i + 1) * width)
    const b = acc.get(i)
    means.push(b ? b.sum / b.count : null)
  }
  return { times, means }
}

/**
 * Replaces nulls so a chart never draws a gap or NaN: a null repeats the value
 * before it, leading nulls take the first real value. All null gives `[]`.
 */
export function fillGaps(means: readonly (number | null)[]): number[] {
  const first = means.find((m) => m !== null)
  if (first === undefined) return []
  let prev: number = first
  return means.map((m) => (prev = m ?? prev))
}

/** Evenly bucketed series for charting. Both arrays have the same length. */
export interface Resampled {
  times: number[]
  values: number[]
}

/**
 * Resamples onto `buckets` evenly spaced points over the span (see
 * bucketMeans and fillGaps). When no sample falls inside the span both arrays
 * are empty, so callers can check `values.length` for "no data yet".
 */
export function resample(samples: readonly Sample[], nowMs: number, spanMs: number, buckets: number): Resampled {
  const { times, means } = bucketMeans(samples, nowMs, spanMs, buckets)
  const values = fillGaps(means)
  return values.length === 0 ? { times: [], values: [] } : { times, values }
}

export interface Trend {
  /** Least-squares slope in units per second; 0 with fewer than two samples. */
  perSecond: number
  /**
   * Seconds until the series reaches `target`, counted from the latest
   * sample's value at the fitted slope. Null when the slope is not positive
   * (it will never get there) or there are fewer than two samples; 0 when the
   * latest value is already at or past the target.
   */
  secondsUntil(target: number): number | null
}

/** Ordinary least-squares line through the samples. */
export function linearTrend(samples: readonly Sample[]): Trend {
  const first = samples[0]
  let slope = 0
  let lastV = 0
  if (first !== undefined && samples.length >= 2) {
    // Centre time on the first sample: epoch milliseconds squared would lose
    // the low digits in a double.
    const n = samples.length
    let sx = 0
    let sy = 0
    let sxx = 0
    let sxy = 0
    for (const s of samples) {
      const x = (s.t - first.t) / 1000
      sx += x
      sy += s.v
      sxx += x * x
      sxy += x * s.v
      lastV = s.v
    }
    const den = n * sxx - sx * sx
    slope = den === 0 ? 0 : (n * sxy - sx * sy) / den
  }
  return {
    perSecond: slope,
    secondsUntil(target: number): number | null {
      // slope stays 0 below two samples, so this also covers that case.
      if (!(slope > 0)) return null
      return Math.max((target - lastV) / slope, 0)
    },
  }
}

/**
 * Differences between consecutive samples of a counter, for sparklines. A
 * drop (counter reset) counts as 0 rather than a negative spike. Length is
 * one less than the input, or 0.
 */
export function deltaCounter(samples: readonly Sample[]): number[] {
  const out: number[] = []
  let prev: Sample | undefined
  for (const s of samples) {
    if (prev) out.push(Math.max(s.v - prev.v, 0))
    prev = s
  }
  return out
}
