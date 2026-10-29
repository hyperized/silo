# silo status page

A read-only status page for a silo cluster, laid out as an operator dashboard. A sticky
top bar carries the responding node, the overall status (Healthy, Warning or Degraded),
the time since the last update with a pause control, and the silod version; the worst
open issue sits on one line under it. Below that: a row of six instrument tiles
(cluster capacity, nodes, replication, volumes, clock skew, backup), a chart of used
capacity per node over the last 1, 6 or 24 hours next to each node's local disk, one
card per node, the volumes table with each volume's replica set, write rate and state,
and a side column with garbage collection, backups and the full issue list. Below
640 px the tiles go two per row and the volume rows turn into key / value blocks.

The curves come from the page itself: it samples each status into a bounded in-memory
history (an hour at full rate, a day at one sample a minute), so they fill in while the
page stays open and start empty after a reload.

The page is built with Vue 3, TypeScript and Tailwind CSS 4, with IBM Plex Sans and
JetBrains Mono bundled (latin subset only, no external font requests). The finished build is
embedded into the silod binary and served by silod itself, so a cluster needs nothing
extra to show it.

## Running it locally

You need Node 22 or newer.

```sh
npm ci
npm run dev
```

The dev server forwards `/api` to `http://localhost:7080`, so start a silod with its
HTTP port on 7080 first.

Without a running silod, use the built-in sample data:

```sh
VITE_MOCK=1 npm run dev
```

or open the dev server with `?mock=<scenario>`, where the scenario is one of
`healthy`, `attention`, `degraded`, `incident`, `empty`, `unconfigured` or `error`.
`incident` is the picture from the design mockup: one node at 84 % disk, one degraded
volume and a GC run that fails now and then. The sample data changes over time (disks
fill, volumes take writes, clocks drift a little, GC runs every minute and backups every
two), so the charts have something to draw. `VITE_MOCK=1` or
`?mock=1` alone shows `attention`, which puts something in every panel and the issue
list. Sample data only
exists in development builds and builds made with `VITE_MOCK=1`; a normal production
build never contains it.

## Tests and checks

```sh
npm test               # unit tests
npm run test:coverage  # unit tests with coverage; anything below 100% fails
npm run lint           # ESLint (zero warnings allowed) and a type check
```

## Building

```sh
npm run build
```

This type-checks the code and writes the result to `../internal/webui/dist`, replacing
whatever is there. That directory sits inside the Go tree because silod embeds it at
compile time, so build the page before building silod. Asset file names contain a
content hash, which lets silod serve them with long cache lifetimes.

## Where the data comes from

The page makes one request, `GET /api/v1/status`, every 5 seconds while the tab is
visible. The response is silod's `GetStatusResponse` protobuf message rendered as JSON.
A few details of that encoding matter to the code in `src/api/status.ts`:

- Field names are camelCase.
- 64-bit integers arrive as strings (`"capacityBytes": "1073741824"`) and are turned
  into numbers in one place, `parseInt64`.
- Node states arrive as enum names such as `NODE_STATE_ALIVE` and become
  `alive`, `suspect`, `dead`, `left` or `unknown`.
- The original fields are always present, but the `storage`, `backup` and
  `backup.lastRun` objects can be `null`. Timestamps of `0` mean "not set".
- Newer fields (`health`, `gc`, and on nodes `pressured` and `clockSkewMs`, on volumes
  `replicas`, `mapDamaged` and `attachedHere`) may be missing when silod is older than
  the page. A missing field takes its zero value and a missing `health` or `gc` object
  becomes `null`; a field that is present with the wrong type is still an error.

`parseStatus` checks the shape of every response and turns it into the types the
components use. If silod sends something unexpected, the page shows an error that says
so instead of rendering wrong numbers. `src/api/__fixtures__/status.json` holds a
sample response for tests and the sample-data mode.

When a request fails, the page keeps showing the last good data, retries after 2, 4, 8
and 16 seconds and then every 30 seconds, and puts up a banner once the data is more
than 30 seconds old.

`DESIGN.md` covers the layout, type, colours, states, motion, and the thresholds behind
each issue (clock skew warns at 500 ms and is critical at 2000 ms).
