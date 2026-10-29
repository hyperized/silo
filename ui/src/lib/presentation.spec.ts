import { describe, it, expect } from 'vitest'
import type { NodeState } from '../api/status'
import type { BackupKind, GcKind, Level, ReplicationKind } from './health'
import {
  BACKUP_PRESENTATION,
  GC_PRESENTATION,
  HIDDEN_TARGET,
  LEVEL_PRESENTATION,
  MAP_DAMAGED_PRESENTATION,
  MEMBERSHIP_PRESENTATION,
  NODE_PRESENTATION,
  PRESSURED_PRESENTATION,
  redactTarget,
  REPLICATION_PRESENTATION,
  skewPresentation,
  VOLUME_DEGRADED_PRESENTATION,
} from './presentation'

describe('redactTarget', () => {
  it.each<[string, string]>([
    ['s3://key:secret@bucket/path', 's3://bucket/path'],
    ['s3://key@bucket/path', 's3://bucket/path'],
    ['https://user:pass@example.com:9000/b', 'https://example.com:9000/b'],
    ['s3://bucket/path?X-Amz-Signature=abc', 's3://bucket/path'],
    ['s3://bucket/path#frag', 's3://bucket/path'],
    ['s3://k:s@bucket/p?token=1#x', 's3://bucket/p'],
    ['s3://silo-backups/cluster', 's3://silo-backups/cluster'],
    ['/var/backups/silo', '/var/backups/silo'],
    ['', ''],
    ['s3://AKIA:se/cret@bucket/p', HIDDEN_TARGET],
    ['s3://user:p@ss@host/b', 's3://host/b'],
    ['azblob://acct:key@c/p;sig=xyz', 'azblob://c/p'],
    ['user:pass@host:/path', HIDDEN_TARGET],
    ['s3://bucket/prefix', 's3://bucket/prefix'],
    ['gs://bucket/prefix', 'gs://bucket/prefix'],
    ['gs://k:s@bucket/prefix?x=1', 'gs://bucket/prefix'],
    ['az://account/container/prefix', 'az://account/container/prefix'],
    ['az://account/container/prefix;sv=2024&sig=abc', 'az://account/container/prefix'],
    ['file:///var/backups/silo', 'file:///var/backups/silo'],
    ['backups/silo;token', HIDDEN_TARGET],
    ['relative/dir', 'relative/dir'],
  ])('turns %j into %j', (input, want) => {
    expect(redactTarget(input)).toBe(want)
  })

  it('does not treat an @ in the path as user info', () => {
    expect(redactTarget('s3://bucket/a@b')).toBe('s3://bucket/a@b')
  })
})

describe('presentation tables', () => {
  const levels: Level[] = ['healthy', 'attention', 'degraded']
  const states: NodeState[] = ['alive', 'suspect', 'dead', 'left', 'unknown']
  const kinds: BackupKind[] = ['unconfigured', 'running', 'never', 'ok', 'failed', 'overdue']
  const gcKinds: GcKind[] = ['off', 'running', 'never', 'ok', 'failed']
  const replication: ReplicationKind[] = ['healthy', 'degraded']

  it.each<[Level, string]>([
    ['healthy', 'Healthy'],
    ['attention', 'Warning'],
    ['degraded', 'Degraded'],
  ])('level %s is labelled %s', (l, label) => {
    expect(LEVEL_PRESENTATION[l].label).toBe(label)
  })

  it.each<[NodeState, string]>([
    ['alive', 'Up'],
    ['suspect', 'Suspect'],
    ['dead', 'Down'],
    ['left', 'Left'],
    ['unknown', 'Unknown'],
  ])('node state %s is labelled %s', (s, label) => {
    expect(NODE_PRESENTATION[s].label).toBe(label)
  })

  it.each<[BackupKind, string]>([
    ['unconfigured', 'Off'],
    ['running', 'Running'],
    ['never', 'Never'],
    ['ok', 'OK'],
    ['failed', 'Failed'],
    ['overdue', 'Overdue'],
  ])('backup kind %s is labelled %s', (k, label) => {
    expect(BACKUP_PRESENTATION[k].label).toBe(label)
  })

  const tips = [
    ...levels.map((l) => [`level ${l}`, LEVEL_PRESENTATION[l]] as const),
    ...states.map((s) => [`node ${s}`, NODE_PRESENTATION[s]] as const),
    ...kinds.map((k) => [`backup ${k}`, BACKUP_PRESENTATION[k]] as const),
    ...gcKinds.map((k) => [`gc ${k}`, GC_PRESENTATION[k]] as const),
    ...replication.map((k) => [`replication ${k}`, REPLICATION_PRESENTATION[k]] as const),
    ['membership connected', MEMBERSHIP_PRESENTATION.connected] as const,
    ['membership isolated', MEMBERSHIP_PRESENTATION.isolated] as const,
    ['pressured', PRESSURED_PRESENTATION] as const,
    ['volume degraded', VOLUME_DEGRADED_PRESENTATION] as const,
    ['map damaged', MAP_DAMAGED_PRESENTATION] as const,
  ]

  it.each(tips)('%s has a tone, an icon and a tip of at most eight words when present', (_name, p) => {
    expect(p.tone).not.toBe('')
    expect(p.icon).not.toBe('')
    if (p.tip !== undefined) expect(p.tip.trim().split(/\s+/).length).toBeLessThanOrEqual(8)
  })

  it('gives the states that need explaining a tip', () => {
    for (const s of states) expect(NODE_PRESENTATION[s].tip).toBeTruthy()
    for (const k of ['unconfigured', 'never', 'overdue'] as const) expect(BACKUP_PRESENTATION[k].tip).toBeTruthy()
    for (const k of ['off', 'never'] as const) expect(GC_PRESENTATION[k].tip).toBeTruthy()
    for (const k of replication) expect(REPLICATION_PRESENTATION[k].tip).toBeTruthy()
    expect(MEMBERSHIP_PRESENTATION.isolated.tip).toBeTruthy()
  })

  it.each<[Level, string, string]>([
    ['healthy', 'neutral', 'check'],
    ['attention', 'warn', 'alert'],
    ['degraded', 'crit', 'octagon'],
  ])('skew at level %s is %s with icon %s', (level, tone, icon) => {
    expect(skewPresentation('750 ms', level)).toEqual({ label: '750 ms', tone, icon })
  })
})
