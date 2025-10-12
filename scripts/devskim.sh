#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/.devskim.json}" && echo "Config file: $config_filepath"

# Install devskim if not already installed
if ! command -v devskim &> /dev/null; then
    echo "Installing devskim..."
    dotnet tool install --global Microsoft.CST.DevSkim.CLI
fi
echo -n "devskim version: " && devskim --version

devskim analyze "$target_path" \
  --options-json "$config_filepath" \
  --file-format text
