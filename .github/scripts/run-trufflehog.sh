#!/bin/bash
target_path="${1:-file://$PWD}" && echo "Scanning repository: $target_path"
# config_filepath="${2:-$PWD/config/trufflehog.toml}" && echo "Config file: $config_filepath"

# Install trufflehog if not already installed
if ! command -v trufflehog &> /dev/null; then
    echo "Installing trufflehog..."
    curl -sSfL https://raw.githubusercontent.com/trufflesecurity/trufflehog/main/scripts/install.sh | sh -s -- -b $HOME/.local/bin
fi
echo -n "TruffleHog version: " && trufflehog --version

# --config "$config_filepath" \
trufflehog filesystem "$target_path" \
    --fail \
    --no-update \
    --include-detectors="all"
