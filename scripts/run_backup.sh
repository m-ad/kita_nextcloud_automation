#!/usr/bin/env bash
# Run the backup_tables.py script using `uv` ensuring env vars are set
# Usage: ./scripts/run_backup.sh /path/to/backups 2

set -euo pipefail

# Load common helpers
source "$(dirname "$0")/_common.sh"

# Directory of this script and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Read positional args or fall back to defaults
BACKUP_PATH_DEFAULT="./backups"
KEEP_N_BACKUPS_DEFAULT="2"

BACKUP_PATH="${1:-$BACKUP_PATH_DEFAULT}"
KEEP_N_BACKUPS="${2:-$KEEP_N_BACKUPS_DEFAULT}"

export BACKUP_PATH
export KEEP_N_BACKUPS

echo "Using BACKUP_PATH=$BACKUP_PATH"
echo "Using KEEP_N_BACKUPS=$KEEP_N_BACKUPS"

 # Ensure uv is installed (see  _common.sh)
ensure_uv

echo "Changing to repo root: $REPO_ROOT"
cd "$REPO_ROOT"

echo "Running: uv run backup_tables.py"
uv run backup_tables.py
