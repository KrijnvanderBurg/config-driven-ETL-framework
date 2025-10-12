#!/bin/bash
target_path="${1:-$PWD}" && echo "Target path: $target_path"

# Install Poetry via pipx if not already installed
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry via pipx..."
    python -m pip install --user pipx
    python -m pipx ensurepath
    pipx install poetry
fi
echo -n "Poetry version: " && poetry --version

# Configure Poetry to not create virtualenvs
echo "Configuring Poetry..."
poetry config virtualenvs.create false
poetry config virtualenvs.in-project false

# Update lock file
echo "Updating lock file in $target_path"
poetry lock --directory "$target_path" --no-interaction

# Install dependencies
echo "Installing dependencies in $target_path"
poetry install --directory "$target_path" --no-interaction
