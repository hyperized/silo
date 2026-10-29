const UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB'] as const

/** Formats a byte count in binary units: "512 B", "7.5 GiB", "30 GiB". */
export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  let unit = 0
  let v = bytes
  while (v >= 1024 && unit < UNITS.length - 1) {
    v /= 1024
    unit++
  }
  if (unit === 0) return `${String(Math.round(v))} B`
  // One decimal below 10 keeps small values informative without making
  // large ones noisy ("7.5 GiB" but "30 GiB").
  const text = v < 10 ? v.toFixed(1).replace(/\.0$/, '') : String(Math.round(v))
  return `${text} ${UNITS[unit] as string}`
}

const COUNT = new Intl.NumberFormat('en')

export function formatCount(n: number): string {
  return COUNT.format(n)
}

/** Whole percentage of a 0..1 ratio; tiny non-zero values show "<1%" so they are not mistaken for empty. */
export function formatPercent(ratio: number): string {
  if (ratio > 0 && ratio < 0.01) return '<1%'
  return `${String(Math.round(Math.min(Math.max(ratio, 0), 1) * 100))}%`
}

interface Step {
  limit: number
  size: number
  unit: string
}

const STEPS: readonly Step[] = [
  { limit: 60, size: 1, unit: 's' },
  { limit: 3600, size: 60, unit: 'min' },
  { limit: 86400, size: 3600, unit: 'h' },
  { limit: Number.POSITIVE_INFINITY, size: 86400, unit: 'd' },
]

/** Formats a span of seconds in its largest whole unit: "45 s", "16 min", "3 d". */
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds))
  const step = STEPS.find((x) => s < x.limit) as Step
  return `${String(Math.floor(s / step.size))} ${step.unit}`
}

/** A span of milliseconds: "850 ms" below a second, else as formatDuration ("2 min"). */
export function formatMillis(ms: number): string {
  if (ms < 1000) return `${String(Math.max(0, Math.round(ms)))} ms`
  return formatDuration(ms / 1000)
}

/** A signed clock offset: "+42 ms", "-17 ms", "0 ms". */
export function formatSkew(ms: number): string {
  const sign = ms > 0 ? '+' : ms < 0 ? '-' : ''
  return `${sign}${COUNT.format(Math.abs(ms))} ms`
}

/** "48 min ago", or "now" within the first few seconds. */
export function formatAgo(date: Date, nowMs: number): string {
  const s = (nowMs - date.getTime()) / 1000
  if (s < 5) return 'now'
  return `${formatDuration(s)} ago`
}

/** "in 5 h", or "due" once the time has come. */
export function formatIn(date: Date, nowMs: number): string {
  const s = (date.getTime() - nowMs) / 1000
  if (s < 5) return 'due'
  return `in ${formatDuration(s)}`
}

/** "every 6 h"; an interval of zero means no schedule. */
export function formatInterval(seconds: number): string {
  if (seconds <= 0) return 'none'
  return `every ${formatDuration(seconds)}`
}

/** "84 / 100 GiB" when both sides share a unit, else "512 MiB / 100 GiB". */
export function formatUsedOf(used: number, total: number): string {
  const u = formatBytes(used)
  const t = formatBytes(total)
  const [un, uu] = u.split(' ')
  const tu = t.split(' ')[1]
  return uu === tu ? `${un as string} / ${t}` : `${u} / ${t}`
}

/** Absolute local time for tooltips next to relative ages. */
export function formatDateTime(date: Date): string {
  return date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' })
}

/** "1.1 of 2.4 TiB" when both sides share a unit, else "512 MiB of 2.4 TiB". */
export function formatOf(used: number, total: number): string {
  return formatUsedOf(used, total).replace(' / ', ' of ')
}

/** A byte rate: "38 MiB/s", or per hour with a sign for growth: "+2.1 GiB/h", "-300 MiB/h". */
export function formatRate(bytes: number, per: 's' | 'h'): string {
  if (per === 's') return `${formatBytes(bytes)}/s`
  const sign = bytes > 0 ? '+' : bytes < 0 ? '-' : ''
  return `${sign}${formatBytes(Math.abs(bytes))}/h`
}

/**
 * A long, rough horizon such as a time until full: "16 h", "12 d", "9 mo",
 * "3 y". Months are 30 days; nobody plans disk purchases to the day.
 */
export function formatHorizon(seconds: number): string {
  const d = seconds / 86400
  if (d < 60) return formatDuration(seconds)
  if (d < 730) return `${String(Math.floor(d / 30))} mo`
  return `${String(Math.floor(d / 365))} y`
}

const COMPACT = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 })

/** "4.2M", "41K", "312". */
export function formatCompact(n: number): string {
  return COMPACT.format(n)
}

/** Local wall-clock time, "08:52". */
export function formatClock(date: Date): string {
  return date.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false })
}
