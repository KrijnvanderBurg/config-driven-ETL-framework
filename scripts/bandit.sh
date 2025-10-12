#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path" 
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"

# Install bandit if not already installed
if ! command -v bandit &> /dev/null; then
    echo "Installing bandit..."
    pip install bandit --quiet
fi
echo -n "bandit version: " && bandit --version

# Bandit auto-discovers pyproject.toml, so don't pass -c flag for it
if [[ "$config_filepath" == *"pyproject.toml"* ]]; then
    bandit -r "$target_path"
else
    bandit -r "$target_path" -c "$config_filepath"
fi
