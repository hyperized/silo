/**
 * Geometry for the hand-drawn SVG charts. Everything here is total: empty
 * series, zero ranges and non-finite samples produce a drawable answer rather
 * than NaN, because a single NaN in a path's `d` makes the browser drop the
 * whole element without a word.
 */

/** Rounds to two decimals so attributes stay short and tests can compare strings. */
export function num(x: number): number {
  // Adding zero turns -0 into 0, which would otherwise print as "-0" in test diffs.
  return Number.isFinite(x) ? Math.round(x * 100) / 100 + 0 : 0
}

function clamp(x: number, lo: number, hi: number): number {
  if (!Number.isFinite(x)) return lo
  return Math.min(Math.max(x, lo), hi)
}

function finite(v: number | undefined): number {
  return v !== undefined && Number.isFinite(v) ? v : 0
}

export interface Dash {
  dasharray: string
  dashoffset: number
}

/**
 * Dash pattern that draws the first `ratio` of an arc spanning `sweepDeg` on a
 * circle of radius `r`. The gap is a whole circumference so the pattern never
 * repeats; the circle element is rotated by the caller to put the start where
 * the gauge begins.
 */
export function arcDash(ratio: number, r: number, sweepDeg: number): Dash {
  const circ = 2 * Math.PI * Math.max(r, 0)
  const arc = (circ * clamp(sweepDeg, 0, 360)) / 360
  return { dasharray: `${String(num(arc * clamp(ratio, 0, 1)))} ${String(num(circ))}`, dashoffset: 0 }
}

/** A short mark of `width` at `ratio` along the same arc as {@link arcDash}. */
export function arcTick(ratio: number, r: number, sweepDeg: number, width = 2): Dash {
  const circ = 2 * Math.PI * Math.max(r, 0)
  const arc = (circ * clamp(sweepDeg, 0, 360)) / 360
  return {
    dasharray: `${String(num(width))} ${String(num(Math.max(circ - width, 0)))}`,
    dashoffset: num(-arc * clamp(ratio, 0, 1)),
  }
}

/**
 * A dashed run of `length` followed by a gap filling the rest of `total`, as
 * one dasharray. The values always come in dash/gap pairs: an odd-length
 * dasharray is repeated by the browser, which would shift every other copy.
 */
export function dashWithin(length: number, total: number, dash = 6, space = 4): string {
  const len = clamp(length, 0, Math.max(total, 0))
  const rest = Math.max(total, 0) - len
  if (len === 0 || dash <= 0) return `0 ${String(num(Math.max(total, 0)))}`
  const parts: number[] = []
  let left = len
  for (;;) {
    const d = Math.min(dash, left)
    parts.push(d)
    left -= d
    if (left <= 0) break
    const s = Math.min(Math.max(space, 0), left)
    parts.push(s)
    left -= s
    if (left <= 0) break
  }
  if (parts.length % 2 === 1) parts.push(rest)
  else parts[parts.length - 1] = (parts[parts.length - 1] as number) + rest
  return parts.map((p) => String(num(p))).join(' ')
}

export interface RingSegment extends Dash {
  /** Arc length of the segment, for building a dashed variant with {@link dashWithin}. */
  length: number
  circumference: number
}

/**
 * N equal segments around a full ring with `gap` between neighbours. One
 * segment is the whole ring with no gap, since a gap there would read as a
 * missing node.
 */
export function ringSegments(n: number, r: number, gap: number): RingSegment[] {
  const count = Number.isFinite(n) ? Math.max(Math.floor(n), 0) : 0
  if (count === 0) return []
  const circ = 2 * Math.PI * Math.max(r, 0)
  const step = circ / count
  const length = count === 1 ? circ : clamp(step - Math.max(gap, 0), 0, step)
  return Array.from({ length: count }, (_, i) => ({
    length: num(length),
    circumference: num(circ),
    dasharray: `${String(num(length))} ${String(num(circ - length))}`,
    dashoffset: num(-i * step),
  }))
}

/** Share of a bullet's width given to 0..warn; warn..crit gets BULLET_CRIT minus this. */
export const BULLET_WARN = 0.6
export const BULLET_CRIT = 0.99

/**
 * Position of `value` on a bullet bar of `width` with a piecewise scale: the
 * healthy range takes the first 60 %, warn..crit the next 39 % and anything past
 * crit pins to the end. A linear scale would squash a 24 ms skew against a
 * 2000 ms crit threshold into an invisible sliver.
 */
export function bulletX(value: number, warn: number, crit: number, width: number): number {
  const w = Math.max(finite(width), 0)
  if (!(value > 0)) return 0
  if (value <= warn) return num((value / warn) * BULLET_WARN * w)
  if (value <= crit) return num((BULLET_WARN + ((value - warn) / (crit - warn)) * (BULLET_CRIT - BULLET_WARN)) * w)
  return num(w)
}

/**
 * Maps a series value to a y coordinate in a box of height `h`, using the
 * series' own min..max with `pad` px kept free top and bottom. A constant (or
 * empty) series maps everything to mid height.
 */
export function yScale(values: readonly number[], h: number, pad: number): (v: number) => number {
  const vs = values.map((v) => finite(v))
  const min = Math.min(...vs)
  const max = Math.max(...vs)
  const span = max - min
  const inner = Math.max(h - 2 * pad, 0)
  if (vs.length === 0 || span === 0) return () => num(h / 2)
  return (v) => num(pad + ((max - finite(v)) / span) * inner)
}

/** x of sample `i` of `count` spread over `w`; a lone sample sits at 0. */
export function xAt(i: number, count: number, w: number): number {
  return count > 1 ? num((i * w) / (count - 1)) : 0
}

/**
 * Polyline points for `values` evenly spaced across `w`. A single value is
 * drawn as a flat line across the box so there is something to see.
 */
export function linePoints(values: readonly number[], w: number, h: number, pad: number): string {
  if (values.length === 0) return ''
  const y = yScale(values, h, pad)
  if (values.length === 1) return `0,${String(y(0))} ${String(num(w))},${String(y(0))}`
  return values.map((v, i) => `${String(xAt(i, values.length, w))},${String(y(v))}`).join(' ')
}

export interface StackPaths {
  /** One closed area per series, series[0] at the bottom. */
  areas: string[]
  /** Top edge of every series but the last, as polyline points, drawn as separators. */
  separators: string[]
}

/**
 * Stacked areas in a `w` x `h` box with `yMax` at the top. Negative and
 * non-finite samples count as zero; shorter series are padded with zeros. A
 * single sample is stretched across the width, otherwise the area would have
 * no width at all.
 */
export function stackPaths(series: readonly (readonly number[])[], w: number, h: number, yMax: number): StackPaths {
  const count = series.reduce((m, s) => Math.max(m, s.length), 0)
  if (count === 0) return { areas: [], separators: [] }
  const top = yMax > 0 && Number.isFinite(yMax) ? yMax : 1
  const xs = count === 1 ? [0, num(w)] : Array.from({ length: count }, (_, i) => xAt(i, count, w))
  const at = (s: readonly number[], i: number): number => Math.max(finite(s[count === 1 ? 0 : i]), 0)
  const y = (v: number): number => num(h - (clamp(v, 0, top) / top) * h)

  let below = xs.map(() => 0)
  const areas: string[] = []
  const edges: string[] = []
  for (const s of series) {
    const above = below.map((b, i) => b + at(s, i))
    const upper = xs.map((x, i) => `${String(x)},${String(y(above[i] as number))}`)
    const lower = xs.map((x, i) => `${String(x)},${String(y(below[i] as number))}`).reverse()
    areas.push(`M${upper.join(' L')} L${lower.join(' L')} Z`)
    edges.push(upper.join(' '))
    below = above
  }
  return { areas, separators: edges.slice(0, -1) }
}

/** Fill opacity of stacked series `i`: darkest first, never fainter than 0.15. */
export function seriesOpacity(i: number): number {
  return num(Math.max(0.8 - i * 0.25, 0.15))
}

/** Index of the sample nearest to `x` in a `w`-wide plot of `count` samples, or null when there are none. */
export function nearestIndex(x: number, count: number, w: number): number | null {
  if (!(count >= 1)) return null
  if (count === 1 || !(w > 0)) return 0
  return Math.round(clamp(x / w, 0, 1) * (count - 1))
}
