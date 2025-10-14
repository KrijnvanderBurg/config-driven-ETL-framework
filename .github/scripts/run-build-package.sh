#!/bin/bash
workspace_path="${1:-$PWD}" && echo "Workspace path: $workspace_path"
output_path="${2:-$workspace_path/dist/}" && echo "Output path: $output_path"

# Install Poetry via pipx if not already installed
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry via pipx..."
    python -m pip install --user pipx
    python -m pipx ensurepath
    pipx install poetry
fi
echo -n "Poetry version: " && poetry --version

# Build the package
echo "Building package from $workspace_path"
poetry build --directory "$workspace_path" --output "$output_path" --no-interaction

echo "Build complete. Package(s) created in $output_path"
