#!/usr/bin/env bash
# Common helper functions for scripts in this folder

set -euo pipefail

# Common install locations the installer may use
USER_BIN="$HOME/.local/bin"
USR_BIN="/usr/local/bin"

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

ensure_uv() {
  if command_exists uv; then
    echo "uv is already installed"
    return 0
  fi

  if [ -x "$USER_BIN/uv" ]; then
    export PATH="$USER_BIN:$PATH"
    echo "Found uv at $USER_BIN/uv; added to PATH"
    return 0
  fi

  if [ -x "$USR_BIN/uv" ]; then
    export PATH="$USR_BIN:$PATH"
    echo "Found uv at $USR_BIN/uv; added to PATH"
    return 0
  fi

  echo "Installing uv via official installer"
  if command_exists curl; then
    curl -LsSf https://astral.sh/uv/install.sh | sh || {
      echo "curl installer failed" >&2
      return 1
    }
  elif command_exists wget; then
    wget -qO- https://astral.sh/uv/install.sh | sh || {
      echo "wget installer failed" >&2
      return 1
    }
  else
    echo "Neither curl nor wget available" >&2
    return 1
  fi

  if [ -d "$USER_BIN" ] && [[ ":$PATH:" != *":$USER_BIN:"* ]]; then
    export PATH="$USER_BIN:$PATH"
  fi

  if [ -d "$USR_BIN" ] && [[ ":$PATH:" != *":$USR_BIN:"* ]]; then
    export PATH="$USR_BIN:$PATH"
  fi

  if command_exists uv; then
    echo "uv is now available"
    return 0
  fi

  echo "Failed to make uv available" >&2
  return 1
}
