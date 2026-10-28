# syntax=docker/dockerfile:1.7

# ---- Web UI stage -----------------------------------------------------------
FROM node:26-alpine3.24 AS ui
WORKDIR /src/ui

COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci

# Vite writes to ../internal/webui/dist, i.e. /src/internal/webui/dist here.
COPY ui/ ./
RUN npm run build

# ---- Build stage ------------------------------------------------------------
FROM golang:1.25-alpine AS builder
WORKDIR /src

# Dependencies first for layer caching.
COPY go.mod ./
# go.sum is committed once we have external deps; the wildcard makes the
# COPY tolerant of its current absence.
COPY go.su[m] ./
RUN go mod download

COPY . .

# The image always embeds the real UI; a plain go build without this step serves a placeholder page.
COPY --from=ui /src/internal/webui/dist ./internal/webui/dist

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
    mkdir -p /var/lib/silo /var/lib/silo/cluster && \
    chown -R silo:silo /var/lib/silo

COPY --from=builder /out/silod /usr/local/bin/silod

USER silo
VOLUME ["/var/lib/silo"]
EXPOSE 7000 7001 7080

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/silod"]
