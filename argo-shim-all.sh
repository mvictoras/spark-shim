#!/bin/bash
# argo-shim-all — one command per location: Spark, Hermes and Argo.
#
# Mac-only. Asks spark-shim whether we're at the lab (titan directly
# reachable) or away, then brings Argo up in the matching mode:
#
#   lab  ->  argo-shim --direct (no tunnels, no Duo prompts)
#   away ->  argo-shim --opencode (OpenCode's Argo tunnel, one Duo prompt),
#            then the Codex/Claude shim on 17950
#
# spark-shim runs FIRST: it decides the location, configures OpenCode and
# Hermes, serves the stable Spark address, and owns the Hermes dashboard
# tunnel. Argo steps come after so a failing Argo can never block Spark.
# The shim itself is started in the background with a log file, so no step
# blocks the next. A stale argo-shim holding the port is replaced via
# --restart. Run it from anywhere, including through the old
# ~/scripts/argo-shim-all.sh symlink.

set -u

# Resolve through symlinks so the old ~/scripts path keeps working and the
# companion spark-shim.sh is found next to this script's real location.
src="${BASH_SOURCE[0]}"
while [ -L "$src" ]; do
  dir="$(cd "$(dirname "$src")" && pwd)"
  src="$(readlink "$src")"
  case "$src" in /*) ;; *) src="$dir/$src" ;; esac
done
here="$(cd "$(dirname "$src")" && pwd)"
SPARK_SHIM="$here/spark-shim.sh"

ARGO_PORT=17950
STATE_DIR="$HOME/.cache/spark-shim"
ARGO_LOG="$STATE_DIR/argo-shim.log"

export CELS_USERNAME=vmateevitsi

failures=0
report() {
  echo "argo-shim-all: FAILED step: $1 (continuing)" >&2
  failures=1
}

mkdir -p "$STATE_DIR"

# 1. Spark: location decision, client configs, stable address, Hermes tunnel.
"$SPARK_SHIM" || report "spark-shim"

# 2. Argo, in the mode matching the location. --where prints the location;
#    its exit code encodes it too (0 = lab, 1 = away), so success is judged
#    by the output, never by the status.
where=$("$SPARK_SHIM" --where 2>/dev/null)

argo_away() {
  echo "argo-shim-all: away -- tunnelling Argo through CELS"
  # OpenCode's Argo provider rides an SSH tunnel to apps.inside.anl.gov.
  # This is the step that costs a Duo prompt; the shim below reuses the
  # healthy master via --restart instead of re-prompting.
  uvx argo-shim --opencode || report "argo-shim --opencode (OpenCode Argo provider may be stale)"
  nohup uvx argo-shim --restart --codex --port "$ARGO_PORT" \
    >>"$ARGO_LOG" 2>&1 &
  echo "argo-shim-all: shim starting in background on port $ARGO_PORT (log: $ARGO_LOG)"
}

case "$where" in
lab)
  echo "argo-shim-all: at the lab -- Argo in direct mode (no tunnels, no Duo prompt)"
  nohup uvx argo-shim --direct --restart --codex --port "$ARGO_PORT" \
    >>"$ARGO_LOG" 2>&1 &
  echo "argo-shim-all: shim starting in background on port $ARGO_PORT (log: $ARGO_LOG)"
  ;;
away)
  argo_away
  ;;
*)
  report "could not get a location from 'spark-shim --where' (said: ${where:-nothing}); defaulting to away"
  argo_away
  ;;
esac

exit $failures
