# syntax=docker/dockerfile:1.7
#
# Privileged one-shot demo image: builds siloctl, then runs the NBD mkfs+mount
# flow against a running cluster (see deploy/nbd-demo/demo.sh). Requires a Linux
# Docker host with the nbd kernel module — it cannot run on the macOS Docker VM.

# ---- Build stage ------------------------------------------------------------
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod ./
COPY go.su[m] ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/siloctl ./cmd/siloctl

# ---- Runtime stage ----------------------------------------------------------
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends nbd-client e2fsprogs ca-certificates kmod && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/siloctl /usr/local/bin/siloctl
COPY deploy/nbd-demo/demo.sh /usr/local/bin/demo.sh
RUN chmod +x /usr/local/bin/demo.sh
ENTRYPOINT ["/usr/local/bin/demo.sh"]
