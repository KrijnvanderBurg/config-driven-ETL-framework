#!/bin/bash
target_path="${1:-$PWD}" && echo "Scanning folder: $target_path"
config_filepath="${2}" && echo "Config file: $config_filepath"

# Install bandit if not already installed
if ! command -v bandit &> /dev/null; then
    echo "Installing bandit..."
    pip install bandit --quiet
fi
echo -n "bandit version: " && bandit --version

bandit -r "$target_path" \
  -c "$config_filepath"
