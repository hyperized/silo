# silo status page: design

## Direction

An operator console, in the family of Proxmox and Grafana table panels. The page is
for someone who runs storage and wants to see the state of the cluster at a glance:
numbers, states and addresses, laid out densely, with no prose. Words are the domain
words (chunks, allocated, suspect, lease, extent). The one distinctive element is the
stats strip: condensed numerals set large, so the five numbers that matter read like
an instrument row before anything else on the page.

## Type

- IBM Plex Sans (variable, weight and width axes), bundled. Body 14 px / 20 px.
  Tabular figures everywhere numbers line up.
- Stats strip values: Plex Sans at width 80, weight 560, 28 px / 1. Panel titles
  13 px / 600. Column headers and labels 12 px / 500 in ink-3, sentence case.
- JetBrains Mono (variable), bundled, for ids, addresses, paths, targets, commands and
  raw error text. 12.5 px.

## Colour

Tokens live once in `src/style.css` (`@theme`), switched by `prefers-color-scheme`.

| Token | Light | Dark |
|---|---|---|
| page | #eceef2 | #0f131a |
| panel | #ffffff | #161b24 |
| ink / ink-2 / ink-3 | #151a21 / #454d59 / #5f6773 | #e3e7ee / #aab2bf / #8c95a3 |
| rule | #d9dde4 | #273040 |
| accent (cobalt) | #2a52be | #86a1ff |
| good / warn / crit | #17803a / #925f00 / #c42b2b | #4cc472 / #e6b54a / #ff7f78 |

Accent marks focus, the pause control and capacity fills below 80 %. Fills turn warn at
80 % and crit at 90 %. Every state carries an icon and a word, never colour alone.
Dark mode is a blue graphite set with its own lighter accent, not an inversion.

## Layout

```
[silo / node-a] [! Warning]                         Updated 3 s ago [Pause]   sticky, 52 px
Nodes 2 / 3 | Capacity 47 % ___ | Volumes 2 | Backup Failed | Version 0.16.0
! node-b suspect for 41 s
! node-c disk 84% full
! Backup failed 48 min ago
  upload to s3://... denied (403) ...                                         mono
+-- Nodes ----------------------------------+  +-- Health --------------+
| table: node, state (+ pressured), data    |  | replication, membership,|
|        addr, capacity, skew, since        |  | clock skew, silod       |
| local disk row: node-a, dir, bar, chunks  |  +-------------------------+
+-------------------------------------------+  +-- Garbage collection --+
+-- Volumes --------------------------------+  | state, last run, freed, |
| path (+ markers), size, allocated, extent,|  | error                   |
| replicas, lease                           |  +-------------------------+
+-------------------------------------------+  +-- Backups -------------+
                                               | key / value rows        |
                                               +-------------------------+
```

The main column holds tables, the side column holds key / value panels. Health and
garbage collection are key / value panels, so they sit in the side column above
Backups, in the order an operator reads them: is the cluster whole, is this node
cleaning up, is it backed up. Their problems also show in the issue list at the top,
so on narrow screens, where the side column falls below the tables, nothing is missed.

The stats strip stays at five cells. Degraded volumes and alive nodes against the
replication factor were considered for it, but a sixth and seventh cell would break
the instrument row; both numbers are in the Health panel and the issue list instead.

Max width 1400 px. Two columns from 1100 px (main plus a 22 rem side column), one
below. Panels are flat: 1 px border, 6 px radius, no shadow, a 36 px title row. Tables
use 36 px rows with hover tint. Below 640 px each table row becomes a key / value block
so nothing scrolls sideways.

## Thresholds and issues

The issue list uses two levels, Warning (amber, alert icon) and Critical (red,
octagon). The overall state in the top bar is the worst open issue.

| Issue | Level | Rule |
|---|---|---|
| Cluster degraded | Critical | alive nodes below the replication factor |
| Node isolated | Critical | the responding node reports it cannot reach any peer |
| Node down | Critical | state dead |
| Node suspect, state unknown | Warning | |
| Node pressured | Warning | the node reports resource pressure |
| Clock skew | Warning at 500 ms, Critical at 2000 ms | largest absolute skew from the health report or any node |
| Disk full | Warning at 80 %, Critical at 90 % | |
| Volume map damaged | Critical | count of volumes whose extent map is damaged |
| Volume degraded | Warning | count of volumes with a replica on a node that is not up (or not in the node table), or fewer replicas than the replication factor |
| Backup failed, overdue | Warning | |
| GC failed | Warning | the last GC run left an error |
| GC never ran | Warning | GC enabled, no run yet, silod up for more than 24 h |

Half a second of skew is worth a look; at two seconds lease expiry and timestamp
ordering can disagree between nodes. Replication is Healthy when alive nodes are at
least the replication factor and Degraded below it.

Fields that an older silod does not send take their zero value, and the page hides
what it cannot judge: the Skew column only appears when some node has a non-zero
skew, the Replicas column only when some volume reports a replica set, and the Health
and GC panels say "not reported" when silod sends no report at all.

## States

- Loading: skeleton stats and table rows in the final geometry, no shimmer.
- Replica chips: one per node of the set, with the node's state icon and colour
  (unknown when the id is not in the node table). Volume markers sit under the path:
  Map damaged, Degraded, and an "attached" tag for volumes attached on this node.
- Empty: "No nodes"; "No volumes" with `siloctl volume create --size 10G /name`.
- Error with no data: one line, the instruction from `src/api/status.ts`, retry
  countdown and a Retry button.
- Stale: one line under the top bar with the age, the error and Retry.
- Paused: the top bar says Paused; no stale line.

Explanations exist only as tooltips of at most eight words on state badges and column
headers, shown on hover and focus.

## Motion

Only two things move: capacity fills (`scaleX`, 300 ms ease-out) and a 400 ms fade of a
highlight behind a stats value that changed. With `prefers-reduced-motion` both are off.
