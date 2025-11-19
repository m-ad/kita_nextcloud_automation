#!/usr/bin/env bash
# Common helper functions for scripts in this folder

set -euo pipefail

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

ensure_uv() {
  if command_exists uv; then
    echo "uv is already installed"
    return 0
  fi

  echo "Installing uv via official installer (astral.sh)"
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
    echo "Neither curl nor wget available to run the installer" >&2
    return 1
  fi

  # Common install locations the installer may use — add them to PATH for this session if present
  USER_BIN="$HOME/.local/bin"
  USR_BIN="/usr/local/bin"
  if [ -d "$USER_BIN" ] && [[ ":$PATH:" != *":$USER_BIN:"* ]]; then
    export PATH="$USER_BIN:$PATH"
    echo "Added $USER_BIN to PATH for this session"
  fi
  if [ -d "$USR_BIN" ] && [[ ":$PATH:" != *":$USR_BIN:"* ]]; then
    export PATH="$USR_BIN:$PATH"
    echo "Added $USR_BIN to PATH for this session"
  fi

  if command_exists uv; then
    echo "uv is now available"
    return 0
  fi

  echo "Failed to make uv available" >&2
  return 1
}
