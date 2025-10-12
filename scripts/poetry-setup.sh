#!/bin/bash

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
