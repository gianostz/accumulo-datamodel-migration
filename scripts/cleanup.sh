#!/usr/bin/env bash
set -euo pipefail

# Post-run cleanup. See docs/05-runbook.md §7.

MINI_DIR=${MINI_DIR:-/tmp/mini-accumulo-poc}
STAGING_DIR=${STAGING_DIR:-/tmp/poc-staging}
REPORTS_DIR=${REPORTS_DIR:-/tmp/poc-reports}

echo "[cleanup] Removing $MINI_DIR"
rm -rf "$MINI_DIR"

echo "[cleanup] Removing $STAGING_DIR"
rm -rf "$STAGING_DIR"

if [[ -d "$REPORTS_DIR" ]]; then
    read -r -p "[cleanup] Remove reports at $REPORTS_DIR? [y/N] " ans
    if [[ "$ans" == "y" || "$ans" == "Y" ]]; then
        rm -rf "$REPORTS_DIR"
        echo "[cleanup] Reports removed."
    else
        echo "[cleanup] Reports kept."
    fi
fi
