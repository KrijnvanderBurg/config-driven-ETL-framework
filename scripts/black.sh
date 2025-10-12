#!/bin/bash
target_path="${1:-$PWD}" && echo "Formatting folder: $target_path"
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"

# Install black if not already installed
if ! command -v black &> /dev/null; then
    echo "Installing black..."
    pip install black --quiet
fi
echo -n "black version: " && black --version

black "$target_path" \
  --config "$config_filepath" \
  --diff \
  --check
