#!/usr/bin/env bash
# Run the pipeline.py script using `uv`, reusing shared helpers
# Usage: ./scripts/run_pipeline.sh

set -euo pipefail

# Load common helpers
source "$(dirname "$0")/_common.sh"

# Ensure uv is installed (see  _common.sh)
ensure_uv

# Directory of this script and repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Changing to repo root: $REPO_ROOT"
cd "$REPO_ROOT"

echo "Running: uv run pipeline.py"
uv run pipeline.py
