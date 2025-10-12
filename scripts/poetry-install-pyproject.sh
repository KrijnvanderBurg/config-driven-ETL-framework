#!/bin/bash
target_path="${1:-$PWD}" && echo "Target path: $target_path"

# Install Poetry if not already installed
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
fi
echo -n "Poetry version: " && poetry --version

# Update lock file
echo "Updating lock file in $target_path"
poetry lock --directory "$target_path" --no-interaction

# Install dependencies
echo "Installing dependencies in $target_path"
poetry install --directory "$target_path" --no-interaction
