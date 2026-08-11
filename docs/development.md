# Development

Requirements: Go 1.25+, and Docker for the local cluster and image builds.

## Build and test

```sh
make build              # ./bin/{silod,siloctl,silo-csi}
make images             # silo/silod and silo/silo-csi container images
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

## Local cluster

`make up` boots three silod nodes with Prometheus on :9090 and Grafana on :3030,
then prints a paste-ready `siloctl auth init` command scraped from silo-a's logs.
`make status` shows cluster health and `make down` tears it down.

The local stack deliberately paces some subsystems faster than production so
healing is visible while you watch it. `SILO_SCRUB_INTERVAL` is the main one.
