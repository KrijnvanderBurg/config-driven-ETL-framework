#!/bin/bash
target_path="${1:-$PWD}" && echo "Target path: $target_path"

# Run poetry-setup.sh if it exists
if [ -f ".github/scripts/poetry-setup.sh" ]; then
    echo "Running poetry-setup.sh..."
    bash .github/scripts/poetry-setup.sh
fi

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
