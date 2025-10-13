#!/bin/bash
target_path="${1:-$PWD}" && echo "Target path: $target_path"

# Update lock file
echo "Updating lock file in $target_path"
poetry lock --directory "$target_path" --no-interaction

# Install dependencies
echo "Installing dependencies in $target_path"
poetry install --directory "$target_path" --no-interaction
