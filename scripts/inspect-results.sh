#!/usr/bin/env bash
set -euo pipefail

# Inspect produced RFiles via Accumulo's PrintInfo. See docs/05-runbook.md §8.1.

cd "$(dirname "$0")/.."

STAGING_DIR=${STAGING_DIR:-/tmp/poc-staging}

JAR=$(ls target/accumulo-rfile-migration-*-shaded.jar 2>/dev/null | head -1 || true)
if [[ -z "${JAR}" ]]; then
    echo "[inspect] Shaded jar not found. Build first: mvn clean package" >&2
    exit 1
fi

if [[ ! -d "$STAGING_DIR" ]]; then
    echo "[inspect] Staging dir $STAGING_DIR not found." >&2
    exit 1
fi

mapfile -t rfiles < <(find "$STAGING_DIR" -name '*.rf' | sort)
if [[ ${#rfiles[@]} -eq 0 ]]; then
    echo "[inspect] No .rf files under $STAGING_DIR" >&2
    exit 1
fi

for f in "${rfiles[@]}"; do
    echo "==================== $f ===================="
    java -cp "$JAR" org.apache.accumulo.core.file.rfile.PrintInfo --dump "$f"
done
