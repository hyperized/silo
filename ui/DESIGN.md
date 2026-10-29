# silo status page: design

## Direction

An operator dashboard. The page is for someone who runs storage and wants to see the
state of the cluster at a glance, and whether it is getting better or worse. Where a
number has a trend (used capacity, write rate, skew, GC and backup runs) it is drawn as
a gauge or a curve; tables are kept for what is a list by nature, the volumes. Words
are the domain words (chunks, allocated, suspect, lease, extent), with no prose beyond
one sentence where a card needs to explain a failure.

The top of the page is an instrument row of six tiles, each answering one question:
how full, how many nodes, are replicas covered, how many volumes and how busy, how far
apart are the clocks, is it backed up. Everything below gives the detail behind one of
those tiles.

## Type

- IBM Plex Sans (variable, weight and width axes), bundled. Body 14 px / 20 px.
  Tabular figures everywhere numbers line up.
- Tile values 30 px / 1, weight 600; gauge centres 26 px inside the SVG. Card titles
  13 px / 600, tile titles and labels 12 px / 500 in ink-3, footers 11.5 px in ink-3.
  Sentence case throughout.
- JetBrains Mono (variable), bundled, for ids, addresses, paths, targets, manifests,
  commands and raw error text. 12 to 13 px.

## Colour

Tokens live once in `src/style.css` (`@theme`), switched by `prefers-color-scheme`.
SVG uses the same tokens as CSS variables (`var(--c-accent)` and so on), never a hex
value, so the charts follow the scheme too.

| Token | Light | Dark |
|---|---|---|
| page | #eceef2 | #0f131a |
| panel | #ffffff | #161b24 |
| ink / ink-2 / ink-3 | #151a21 / #454d59 / #5f6773 | #e3e7ee / #aab2bf / #8c95a3 |
| rule | #d9dde4 | #273040 |
| accent (cobalt) | #2a52be | #86a1ff |
| good / warn / crit | #17803a / #925f00 / #c42b2b | #4cc472 / #e6b54a / #ff7f78 |

Accent is the colour of data: capacity fills below 80 %, curves, bars of successful
runs, the stacked areas (one hue, falling opacity per node). Fills turn warn at 80 %
and crit at 90 %. Good, warn and crit are reserved for states.

Every colour carries a word or an icon, never colour alone. A failed run strip square
has a cross, a suspect ring segment is dashed, a full disk gets a "Disk 84 %" tag as
well as an amber border, a replica chip has its state icon, and every state badge
pairs an icon with its label. The page still reads for someone who cannot tell red
from green, and on a greyscale printout.

Dark mode is a blue graphite set with its own lighter accent, not an inversion.

## Primitives

Seven hand-drawn SVG components in `src/components/charts`, no chart library. Each
takes plain numbers, has `role="img"` and an accessible name that states the value in
words ("Capacity 47%, 142 of 300 GiB, warning").

| Primitive | Used for | States |
|---|---|---|
| ArcGauge | cluster capacity, each node's disk | 270 degree arc; accent, warn at 80 %, crit at 90 %; ticks at 80 and 90 that the fill has not reached yet; "n/a" with only the track when unknown |
| SegmentRing | nodes | one segment per node: alive solid green, suspect dashed amber, dead solid red, left or unknown an empty dashed outline |
| Bullet | clock skew | value against warn 500 ms and crit 2000 ms markers on a scale that gives the healthy range room; fill green, amber, red |
| Sparkline | write rates per volume, per node, cluster wide | own range per line, last point marked; a dashed flat line until two samples exist |
| StackedArea | used capacity per node | one hue at three opacities, 80 % of capacity as a dashed warn line, hover and arrow keys move a readout that is mirrored into a live region |
| RunBars | GC runs | bytes freed per run, latest run at full opacity, failed runs in crit with a title naming the time |
| RunDots | backup runs | green dot per success, red dot with a cross per failure, a dashed hollow dot for the next scheduled run |

## History

silod reports a snapshot, so the page keeps its own history. `useHistory` samples every
status as it arrives (one per poll, every 5 s, never closer than 3 s) into a ring buffer
of 1200 samples, which covers the last hour and a bit. A second tier keeps one sample a
minute for 24 h, so the 6 h and 24 h ranges of the capacity chart fill in while the page
stays open. Nothing is stored: a reload starts empty, and the chart starts at the first
sample instead of drawing the part of the span it never saw.

GC and backup runs are kept as a separate list of the last 24 runs seen. silod only
reports the latest of each, so the strips also fill while the page is open.

Rates and projections ("+2.1 GiB/h", "full in 9 mo") are a least-squares line through
the samples in the chosen range and stay hidden until there are two samples. Write rates
are allocation growth between samples divided by the time between them, so a pause does
not show up as a spike. When silod serves its own history this buffer becomes the cache
for it rather than the source.

## Layout

```
[logo silo / node-a] [! Warning]                 Updated 3 s ago [Pause] 0.17.0  sticky, 56 px
! node-c disk 84% full   and 1 more                                          worst issue only
+capacity-+ +nodes---+ +replication+ +volumes--+ +clock skew+ +backup---+
| arc     | | ring    | | 3 factor  | | 12      | | 24 ms     | | OK 48 min|
| +GiB/h  | | suspect | | bars      | | spark   | | bullet    | | squares  |
+---------+ +---------+ +-----------+ +---------+ +-----------+ +----------+
+-- Used capacity, per node ---- [1 h][6 h][24 h] -+ +-- Local disk, per node ---+
| stacked area, 80 % line, legend                   | | meter per node            |
+--------------------------------------------- 8/12-+ | chunks, free, pressured,  |
                                                      | growth               4/12 |
                                                      +---------------------------+
+-- node-a  this node ---+ +-- node-b -------------+ +-- node-c  Disk 84 % ----+
| gauge, used, free,     | | ...                   | | amber border             |
| skew, up; spark; addr  | |                       | |                          |
+------------------------+ +-----------------------+ +--------------------------+
+-- Volumes ------------------------- Filter [ ] -+ +-- Garbage collection -----+
| path (+ markers), allocated, writes, replicas,   | | run bars, last / orphans /|
| lease, state; problems first; 8 then Show all    | | freed, failure sentence   |
+------------------------------------------- 8/12-+ +-- Backups ----------------+
                                                     | run dots, last / uploaded |
                                                     | / next, schedule, manifest|
                                                     +-- Issues -----------------+
                                                     | full list           4/12  |
                                                     +---------------------------+
```

Max width 1440 px, 24 px side gutter (16 px on a phone). From 1100 px: six tiles in a
row, the 8 / 4 column split, three node cards per row. From 640 px: tiles three per
row. Below 640 px: tiles two per row, every card full width, and each volume row turns
into a key / value block so nothing scrolls sideways. Between 640 and 1100 px the
volume table scrolls inside its card rather than squeezing paths onto several lines.

Cards are flat: 1 px border, 10 px radius, no shadow. A card in trouble (a node with a
full disk or that is down) changes its border to warn or crit; the tag or badge inside
says why.

The issue strip under the top bar shows the worst issue only, so the reason for an
amber or red pill is readable without scrolling. The full list sits in the side column
under GC and Backups, which on narrow screens falls below the tables; the strip keeps
the most important line in view there too.

The volume table sorts map-damaged volumes first, then degraded ones, then by current
write rate, and shows eight rows until "Show all". The filter matches path and lease
holder.

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
| Volume degraded | Warning | count of volumes with a replica on a node that is not alive (or not in the node table), or fewer replicas than the replication factor |
| Backup failed, overdue | Warning | |
| GC failed | Warning | the last GC run left an error |
| GC never ran | Warning | GC enabled, no run yet, silod up for more than 24 h |

Half a second of skew is worth a look; at two seconds lease expiry and timestamp
ordering can disagree between nodes. Replication is Healthy when alive nodes are at
least the replication factor and Degraded below it.

Fields that an older silod does not send take their zero value, and the page hides
what it cannot judge: the Replicas column only appears when some volume reports a
replica set, and the Replication, Clock skew and GC cards say "not reported" when silod
sends no report at all.

## States

- Loading: skeleton tiles, cards and table rows in the final geometry, no shimmer.
  Charts that have data but no history yet say "collecting samples" or draw a dashed
  flat line.
- Replica chips: one per node of the set, with the node's state icon and colour
  (unknown when the id is not in the node table). Volume markers sit under the path:
  map damaged, degraded with the reason ("replica on node-c down", "2 of 3 replicas"),
  and "attached here" for volumes attached on this node.
- Empty: "No nodes"; "No volumes" with `siloctl volume create --size 10G /name`; a filter
  that matches nothing says so and offers "Clear filter"; the Issues card says "No open
  issues".
- Error with no data: one line, the instruction from `src/api/status.ts`, retry
  countdown and a Retry button.
- Stale: one line under the top bar with the age, the error and Retry.
- Paused: the top bar says Paused; no stale line.

Explanations exist as tooltips of at most eight words on state badges and column
headers, shown on hover and focus, and as one sentence in the GC and Backups cards when
a run failed.

## Motion

Few things move. Meter fills (`scaleX`), gauge arcs (`stroke-dasharray`) and the skew
bullet ease to their new value over 300 ms ease-out, so a change between polls reads as
a change rather than a jump. Buttons change colour in 100 ms. Curves are redrawn, not
animated. With `prefers-reduced-motion` every transition is off.
