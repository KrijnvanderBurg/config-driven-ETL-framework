#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path" 
config_filepath="${2:-$PWD/config/vulture.toml}" && echo "Config file: $config_filepath"

# Install vulture if not already installed
if ! command -v vulture &> /dev/null; then
    echo "Installing vulture..."
    pip install vulture --quiet
fi
echo -n "Vulture version: " && vulture --version

vulture "$target_path" \
  --config "$config_filepath"
