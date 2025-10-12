#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/config/.pyre_configuration}" && echo "Config file: $config_filepath"

# Install pyre if not already installed
if ! command -v pyre &> /dev/null; then
    echo "Installing pyre..."
    pip install pyre-check --quiet
fi
echo -n "pyre version: " && pyre --version=client_and_binary

# Copy the config file to the root of the project
cp "$config_filepath" "$target_path/.pyre_configuration"

pyre --source-directory "$target_path" \
  --log-level=INFO \
  --noninteractive \
  --sequential \
  --version=none \
  check
exit_code=$?

# Remove the config file from the root of the project
rm "$target_path/.pyre_configuration"

exit $exit_code
