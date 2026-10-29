#!/usr/bin/env bash
# Runs the full Go test suite inside Docker (no local Go toolchain needed).
# Integration tests use the compose Postgres on the compose network, against a
# separate jobs_test database, so the running demo's data is untouched.
#   ./scripts/test.sh          # go test ./...
#   ./scripts/test.sh -race    # go test -race ./...
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d --wait postgres >/dev/null
NET="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(docker compose ps -q postgres)")"
IMAGE=golang:1.23  # Debian image ships gcc, so -race (cgo) works
docker run --rm --network "$NET" \
  -v "$PWD":/src -w /src \
  -v gojobqueue-gomod:/go/pkg/mod -v gojobqueue-gocache:/root/.cache/go-build \
  -e TEST_DATABASE_URL="postgres://jobs:jobs@postgres:5432/jobs_test?sslmode=disable" \
  "$IMAGE" go test -count=1 "$@" ./...
