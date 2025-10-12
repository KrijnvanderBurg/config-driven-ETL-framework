#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"

# Install flake8 if not already installed
if ! command -v flake8 &> /dev/null; then
    echo "Installing flake8..."
    pip install flake8 --quiet
fi
echo -n "flake8 version: " && flake8 --version

flake8 "$target_path" \
  --config "$config_filepath"
