#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/.github/config/pyrightconfig.json}" && echo "Config file: $config_filepath"

# Install pyright if not already installed
if ! command -v pyright &> /dev/null; then
    echo "Installing pyright..."
    pip install pyright --quiet
fi
echo -n "pyright version: " && pyright --version

pyright "$target_path" \
  --project "$config_filepath" \
  --level warning \
  --warnings
