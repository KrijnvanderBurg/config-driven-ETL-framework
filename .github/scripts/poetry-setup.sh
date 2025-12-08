#!/bin/bash

# Set pipx to use user-writable directories
export PIPX_HOME="${HOME}/.local/pipx"
export PIPX_BIN_DIR="${HOME}/.local/bin"

# Install Poetry via pipx if not already installed
if ! command -v poetry &> /dev/null; then
    echo "Installing Poetry via pipx..."
    python -m pip install --user pipx
    python -m pipx ensurepath
    pipx install poetry
fi
echo -n "Poetry version: " && poetry --version
