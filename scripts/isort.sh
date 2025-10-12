#!/bin/bash
target_path="${1:-$PWD}" && echo "Formatting folder: $target_path"
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"

# Install isort if not already installed
if ! command -v isort &> /dev/null; then
    echo "Installing isort..."
    pip install isort --quiet
fi
echo -n "isort version: " && isort --version

isort "$target_path" \
  --settings-path "$config_filepath" \
  --check-only \
  --diff
