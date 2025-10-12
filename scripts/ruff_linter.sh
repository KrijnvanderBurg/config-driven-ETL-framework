#!/bin/bash
target_path="${1:-$PWD}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"

# Install ruff if not already installed
if ! command -v ruff &> /dev/null; then
    echo "Installing ruff..."
    pip install ruff --quiet
fi
echo -n "ruff version: " && ruff --version

ruff check "$target_path" \
  --config "$config_filepath" \
  --no-fix \
  --diff \
  --show-files
