#!/bin/bash
workspace_path="${1:-$PWD}" && echo "Workspace path: $workspace_path"

# Install Poetry via pipx if not already installed
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry via pipx..."
    python -m pip install --user pipx
    python -m pipx ensurepath
    pipx install poetry
fi
echo -n "Poetry version: " && poetry --version

# Lock dependencies
echo "Locking dependencies in $workspace_path"
poetry lock --directory "$workspace_path" --no-interaction

echo "Lock file updated successfully."
