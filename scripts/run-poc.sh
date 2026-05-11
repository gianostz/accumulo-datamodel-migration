#!/usr/bin/env bash
set -euo pipefail

# End-to-end PoC runner. See docs/05-runbook.md §4.1.

cd "$(dirname "$0")/.."

MINI_DIR=${MINI_DIR:-/tmp/mini-accumulo-poc}
STAGING_DIR=${STAGING_DIR:-/tmp/poc-staging}
REPORTS_DIR=${REPORTS_DIR:-/tmp/poc-reports}

echo "[run-poc] Cleaning previous run state..."
rm -rf "$MINI_DIR" "$STAGING_DIR"
mkdir -p "$STAGING_DIR" "$REPORTS_DIR"

JAR=$(ls target/accumulo-rfile-migration-*-shaded.jar 2>/dev/null | head -1 || true)
if [[ -z "${JAR}" ]]; then
    echo "[run-poc] Shaded jar not found. Build first:" >&2
    echo "    mvn clean package" >&2
    exit 1
fi

JAVA_OPTS=${JAVA_OPTS:--Xmx6g -XX:+UseG1GC}

echo "[run-poc] Launching $(basename "$JAR")"
# shellcheck disable=SC2086
exec java $JAVA_OPTS -jar "$JAR" "$@"
