#!/usr/bin/env bash
#
# One-click build & run script for the GroveMQ MQTT broker.
#
# Usage:
#   ./scripts/run.sh                 # build (if needed) + run in foreground
#   ./scripts/run.sh -config x.yaml  # build + run foreground with extra args
#   ./scripts/run.sh daemon          # build + run in background (nohup, pidfile)
#   ./scripts/run.sh build           # only build the binary
#   ./scripts/run.sh stop            # stop the background instance
#   ./scripts/run.sh status          # show whether the instance is running
#
# Required toolchain (this project needs Go >= 1.25 and CGO disabled):
#   - Prefers $GROVEMQ_GO, then PATH `go` (or /usr/local/go/bin).
#   - CGO_ENABLED=0 is forced (old macOS SDK lacks SecTrustCopyCertificateChain).
#
set -euo pipefail

# ---- Colors ----
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[run]${NC} $*"; }
warn()  { echo -e "${YELLOW}[run]${NC} $*"; }
err()   { echo -e "${RED}[run]${NC} $*" >&2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN_DIR="$ROOT/bin"
BIN="$BIN_DIR/grovemq"
PIDFILE="$BIN_DIR/grovemq.pid"

# ---- Resolve Go toolchain ----
resolve_go() {
  local candidates=()
  [ -n "${GROVEMQ_GO:-}" ] && candidates+=("$GROVEMQ_GO/bin")
  [ -x "/usr/local/go/bin/go" ] && candidates+=("/usr/local/go/bin")
  candidates+=("$(dirname "$(command -v go 2>/dev/null || true)")")

  for d in "${candidates[@]}"; do
    [ -z "$d" ] && continue
    local g="$d/go"
    [ -x "$g" ] || continue
    local ver
    ver="$("$g" version 2>/dev/null | awk '{print $3}' | sed 's/^go//')" || continue
    local maj min
    maj="$(echo "$ver" | cut -d. -f1)"
    min="$(echo "$ver" | cut -d. -f2)"
    if [ "${maj:-0}" -gt 1 ] || { [ "${maj:-0}" -eq 1 ] && [ "${min:-0}" -ge 25 ]; }; then
      echo "$g"
      return 0
    fi
  done
  return 1
}

GO_BIN="$(resolve_go)" || { err "Need Go >= 1.25. Install it or set GROVEMQ_GO. (system go is too old)"; exit 1; }
info "Using Go: $($GO_BIN version | awk '{print $3}')"

# ---- Build ----
needs_build() {
  [ -x "$BIN" ] || return 0
  # rebuild if any Go source is newer than the binary
  if find . -name '*.go' -newer "$BIN" -print -quit 2>/dev/null | grep -q .; then
    return 0
  fi
  return 1
}

build() {
  if needs_build; then
    info "Building $BIN ..."
    CGO_ENABLED=0 GOFLAGS=-mod=mod "$GO_BIN" build -o "$BIN" ./cmd/grovemq
    info "Build done."
  else
    info "Binary up to date, skipping build."
  fi
}

# ---- Instance control ----
start_foreground() {
  build
  info "Starting GroveMQ in foreground (Ctrl-C to stop)..."
  exec "$BIN" "$@"
}

start_daemon() {
  build
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    warn "Already running (pid $(cat "$PIDFILE")). Use 'stop' first."
    return 0
  fi
  info "Starting GroveMQ in background..."
  nohup "$BIN" "$@" > "$BIN_DIR/grovemq.out" 2>&1 &
  echo $! > "$PIDFILE"
  sleep 1
  if kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    info "Running (pid $(cat "$PIDFILE")). Logs: $BIN_DIR/grovemq.out"
  else
    err "Failed to start; see $BIN_DIR/grovemq.out"
    rm -f "$PIDFILE"
    exit 1
  fi
}

stop() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    info "Stopping (pid $(cat "$PIDFILE"))..."
    kill "$(cat "$PIDFILE")" && rm -f "$PIDFILE"
  else
    warn "No running instance found (or stale pidfile removed)."
  fi
}

status() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    info "Running: pid $(cat "$PIDFILE")"
    for p in 1883 8081 18083 8082; do
      if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
        info "  port $p: LISTEN"
      else
        warn "  port $p: not listening"
      fi
    done
  else
    warn "Not running."
  fi
}

# ---- Dispatch ----
case "${1:-}" in
  build)  build ;;
  stop)   stop ;;
  status) status ;;
  daemon) shift; start_daemon "$@" ;;
  cluster)     shift; exec "$ROOT/scripts/start-cluster.sh" "$@" ;;
  stop-cluster) shift; exec "$ROOT/scripts/stop-cluster.sh" "$@" ;;
  "")     start_foreground ;;
  -*)     start_foreground "$@" ;;          # pass flags (e.g. -config) through
  *)      start_foreground "$@" ;;          # unknown arg -> run with it
esac
