# syntax=docker/dockerfile:1.7

# ---- Build stage ------------------------------------------------------------
FROM golang:1.24-alpine AS builder
WORKDIR /src

# Dependencies first for layer caching.
COPY go.mod ./
# go.sum is committed once we have external deps; the wildcard makes the
# COPY tolerant of its current absence.
COPY go.su[m] ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags "-s -w -X main.version=${VERSION}" \
        -o /out/silod \
        ./cmd/silod

# ---- Runtime stage ----------------------------------------------------------
FROM alpine:3.21

# tini ensures signals are forwarded to silod (PID 1 quirks).
# ca-certificates lets silod talk TLS once we add it in later milestones.
RUN apk add --no-cache ca-certificates tini && \
    addgroup -S silo && adduser -S -G silo silo && \
    mkdir -p /var/lib/silo && chown silo:silo /var/lib/silo

COPY --from=builder /out/silod /usr/local/bin/silod

USER silo
VOLUME ["/var/lib/silo"]
EXPOSE 7000 7080

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/silod"]
