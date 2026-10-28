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
+-- Nodes ----------------------------------+  +-- Backups -------------+
| table: node, state, data addr, capacity,  |  | key / value rows        |
|        since                              |  | error text in mono      |
| local disk row: node-a, dir, bar, chunks  |  +-------------------------+
+-------------------------------------------+
+-- Volumes --------------------------------+
| path, size, allocated, extent, lease      |
+-------------------------------------------+
```

Max width 1400 px. Two columns from 1100 px (main plus a 22 rem side column), one
below. Panels are flat: 1 px border, 6 px radius, no shadow, a 36 px title row. Tables
use 36 px rows with hover tint. Below 640 px each table row becomes a key / value block
so nothing scrolls sideways.

## States

- Loading: skeleton stats and table rows in the final geometry, no shimmer.
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
