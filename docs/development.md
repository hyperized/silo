# Development

Requirements: Go 1.25+, and Docker for the local cluster and image builds.

## Build and test

```sh
make build              # ./bin/{silod,siloctl,silo-csi}
make images             # silo/silod and silo/silo-csi container images
make ui                 # build the web UI into internal/webui/dist/ (needs Node.js)
make ui-dev             # web UI dev server with hot reload
make ui-test            # web UI unit tests
make ui-lint            # web UI lint
make test               # unit tests
make test-integration   # end-to-end against a real silod (build-tag 'integration')
make test-nbd-kernel    # NBD attach/reconnect against a real kernel (privileged docker)
make check              # fmt + vet + lint + test
```

`make check` is what CI runs. Run it before opening a pull request.

## Test layout

Tests are split by what they can see:

- `*_internal_test.go` (`package foo`) covers unpublished behaviour and error
  paths that are unreachable from outside the package.
- `*_external_test.go` (`package foo_test`) covers the API as a caller sees it.
- Integration tests sit behind the `integration` build tag so they never run in a
  plain `go test ./...`. Anything touching a real silod, a real kernel NBD device,
  or a real object store belongs there.

New code is expected to keep the package at its existing coverage. Several
packages sit at 100% and should stay there.

## Backup integration test

`test/integration/backup_test.go` (build tag `integration`) runs a backup and a
restore against a real S3-compatible store. Run it with `make test-integration`,
or on its own:

```sh
go test -tags integration ./test/integration/... -run Backup
```

It starts a throwaway RustFS container (an S3-compatible server) with docker. To
use an S3-compatible server you already have, set `SILO_TEST_S3_ENDPOINT`,
`SILO_TEST_S3_ACCESS_KEY` and `SILO_TEST_S3_SECRET_KEY`. The test is skipped when
docker is not available and no endpoint is set.

## Web UI

The status page lives in `ui/` (Vue 3, TypeScript, built with Vite). You need
Node.js LTS and npm to work on it; Go-only changes do not.

- `make ui` installs the dependencies and builds into `internal/webui/dist/`.
  silod embeds that directory at compile time, so rebuild silod afterwards.
- `make ui-dev` starts the Vite dev server with hot reload and proxies `/api` to
  a local silod on :7080. Start one first with `make up-local` or `make up`.
- `make ui-test` runs the unit tests and `make ui-lint` runs the linter.
- `make build` builds the UI first when npm is installed. Without npm it prints
  a warning and keeps the placeholder page. `make build SKIP_UI=1` skips the UI
  step, which is what CI does after it downloads the UI built in an earlier job.

Without a build, `internal/webui/dist/` holds only a `.gitkeep`, and silod
serves a small placeholder page that explains how to build the UI. Build output
in that directory is git-ignored, so `make ui` never dirties the working tree.
`ui/` carries a stub `go.mod` so that `go test ./...` and `gofmt` skip the Go
files some npm packages ship inside `node_modules`.

## Local cluster

`make up` boots three silod nodes with Prometheus on :9090, Grafana on :3030 and
an S3-compatible server (RustFS) the nodes back up to, with its console on :9001, then prints a paste-ready `siloctl auth init` command scraped from silo-a's logs.
`make status` shows cluster health, `make backup-demo` triggers a backup on silo-a
and lists the result, and `make down` tears it down.

The local stack deliberately paces some subsystems faster than production so
healing is visible while you watch it. `SILO_SCRUB_INTERVAL` is the main one.
