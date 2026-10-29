import type { NodeState } from '../api/status'
import type { BackupKind, GcKind, Level, ReplicationKind } from './health'

export type Tone = 'good' | 'warn' | 'crit' | 'neutral'
export type IconName = 'check' | 'alert' | 'octagon' | 'minus' | 'question' | 'sync' | 'clock'

export interface Presentation {
  label: string
  tone: Tone
  icon: IconName
  /** At most eight words, shown on hover and focus. */
  tip?: string
}

export const LEVEL_PRESENTATION: Readonly<Record<Level, Presentation>> = {
  healthy: { label: 'Healthy', tone: 'good', icon: 'check' },
  attention: { label: 'Warning', tone: 'warn', icon: 'alert' },
  degraded: { label: 'Degraded', tone: 'crit', icon: 'octagon' },
}

export const NODE_PRESENTATION: Readonly<Record<NodeState, Presentation>> = {
  alive: { label: 'Up', tone: 'good', icon: 'check', tip: 'Up: answering heartbeats' },
  suspect: { label: 'Suspect', tone: 'warn', icon: 'alert', tip: 'Suspect: missed heartbeats, may recover' },
  dead: { label: 'Down', tone: 'crit', icon: 'octagon', tip: 'Down: declared dead by its peers' },
  left: { label: 'Left', tone: 'neutral', icon: 'minus', tip: 'Left: removed from the cluster' },
  unknown: { label: 'Unknown', tone: 'neutral', icon: 'question', tip: 'Unknown: state newer than this page' },
}

export const BACKUP_PRESENTATION: Readonly<Record<BackupKind, Presentation>> = {
  unconfigured: { label: 'Off', tone: 'neutral', icon: 'minus', tip: 'Off: SILO_BACKUP_TARGET is not set' },
  running: { label: 'Running', tone: 'good', icon: 'sync' },
  never: { label: 'Never', tone: 'neutral', icon: 'clock', tip: 'Never: no run since silod started' },
  ok: { label: 'OK', tone: 'good', icon: 'check' },
  failed: { label: 'Failed', tone: 'warn', icon: 'alert' },
  overdue: { label: 'Overdue', tone: 'warn', icon: 'clock', tip: 'Overdue: missed its scheduled run' },
}

export const REPLICATION_PRESENTATION: Readonly<Record<ReplicationKind, Presentation>> = {
  healthy: { label: 'Healthy', tone: 'good', icon: 'check', tip: 'Enough alive nodes for every replica' },
  degraded: { label: 'Degraded', tone: 'crit', icon: 'octagon', tip: 'Fewer alive nodes than the replication factor' },
}

export const GC_PRESENTATION: Readonly<Record<GcKind, Presentation>> = {
  off: { label: 'Off', tone: 'neutral', icon: 'minus', tip: 'Off: sweeps count orphans, delete nothing' },
  running: { label: 'Running', tone: 'good', icon: 'sync' },
  never: { label: 'Never', tone: 'neutral', icon: 'clock', tip: 'Never: no run since silod started' },
  ok: { label: 'OK', tone: 'good', icon: 'check' },
  failed: { label: 'Failed', tone: 'warn', icon: 'alert' },
}

export const MEMBERSHIP_PRESENTATION: Readonly<Record<'connected' | 'isolated', Presentation>> = {
  connected: { label: 'Connected', tone: 'good', icon: 'check' },
  isolated: { label: 'Isolated', tone: 'crit', icon: 'octagon', tip: 'Isolated: cannot reach any peer' },
}

export const PRESSURED_PRESENTATION: Presentation = {
  label: 'Pressured',
  tone: 'warn',
  icon: 'alert',
  tip: 'Pressured: node reports resource pressure',
}

export const VOLUME_DEGRADED_PRESENTATION: Presentation = {
  label: 'Degraded',
  tone: 'warn',
  icon: 'alert',
  tip: 'Degraded: a replica is down or missing',
}

export const MAP_DAMAGED_PRESENTATION: Presentation = {
  label: 'Map damaged',
  tone: 'crit',
  icon: 'octagon',
  tip: 'Extent map failed its checks on load',
}

const SKEW_TONE = { healthy: 'neutral', attention: 'warn', degraded: 'crit' } as const satisfies Record<Level, Tone>
const SKEW_ICON = { healthy: 'check', attention: 'alert', degraded: 'octagon' } as const satisfies Record<Level, IconName>

/** A clock skew value as a badge; the level comes from skewLevel. */
export function skewPresentation(text: string, level: Level): Presentation {
  return { label: text, tone: SKEW_TONE[level], icon: SKEW_ICON[level] }
}

export const HIDDEN_TARGET = '(hidden)'

/**
 * Renders a backup target without anything that can carry a secret: user info,
 * query, fragment and ";"-parameters (where signed URLs keep their tokens).
 * Only "scheme://" forms are parsed as URLs, because the URL parser reads
 * "user:pass@host:/path" as scheme "user:" and would echo the password back.
 * Anything it cannot parse is shown only when it has no character that could
 * introduce credentials.
 */
export function redactTarget(target: string): string {
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(target)) {
    try {
      const u = new URL(target)
      return `${u.protocol}//${u.host}${u.pathname.replace(/;.*$/, '')}`
    } catch {
      // Unparseable, e.g. a colon-split secret that looks like a bad port.
    }
  }
  return /[@?#;]/.test(target) ? HIDDEN_TARGET : target
}
