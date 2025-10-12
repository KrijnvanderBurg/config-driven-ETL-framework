#!/bin/bash
target_path="${1:-file://$PWD}" && echo "Scanning repository: $target_path"
config_filepath="${2:-$PWD/config/trufflehog.toml}" && echo "Config file: $config_filepath"

# Install trufflehog if not already installed
if ! command -v trufflehog &> /dev/null; then
    echo "Installing trufflehog..."
    # Install trufflehog from GitHub releases
    TRUFFLEHOG_VERSION="3.63.2"
    wget -q "https://github.com/trufflesecurity/trufflehog/releases/download/v${TRUFFLEHOG_VERSION}/trufflehog_${TRUFFLEHOG_VERSION}_linux_amd64.tar.gz"
    tar -xzf "trufflehog_${TRUFFLEHOG_VERSION}_linux_amd64.tar.gz"
    sudo mv trufflehog /usr/local/bin/
    rm "trufflehog_${TRUFFLEHOG_VERSION}_linux_amd64.tar.gz"
fi
echo -n "trufflehog version: " && trufflehog --version

trufflehog filesystem "$target_path" \
  --config "$config_filepath" \
  --fail \
  --no-update
