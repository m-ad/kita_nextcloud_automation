#!/usr/bin/env bash
# Run the pipeline.py script using `uv`, reusing shared helpers
# Usage: ./scripts/run_pipeline.sh

set -euo pipefail

# Load common helpers
source "$(dirname "$0")/_common.sh"

# Ensure uv is installed (see  _common.sh)
ensure_uv

echo "Running: uv run pipeline.py"
uv run pipeline.py
