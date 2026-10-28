# silo status page

A read-only status page for a silo cluster. A sticky top bar carries the responding
node, the overall status (Healthy, Warning or Degraded) and the time since the last
update, with a pause control. Below it: a stats strip (nodes up, capacity, volumes,
backup state, version), the open issues, a nodes table with this node's local disk,
a volumes table and the backup details. Below 640 px the tables turn into key / value
rows.

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
`healthy`, `attention`, `degraded`, `empty`, `unconfigured` or `error`. Sample data only
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
- Every field is present, but the `storage`, `backup` and `backup.lastRun` objects can
  be `null`. Timestamps of `0` mean "not set".

`parseStatus` checks the shape of every response and turns it into the types the
components use. If silod sends something unexpected, the page shows an error that says
so instead of rendering wrong numbers. `src/api/__fixtures__/status.json` holds a
sample response for tests and the sample-data mode.

When a request fails, the page keeps showing the last good data, retries after 2, 4, 8
and 16 seconds and then every 30 seconds, and puts up a banner once the data is more
than 30 seconds old.

`DESIGN.md` covers the layout, type, colours, states and motion.
