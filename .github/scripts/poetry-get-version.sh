#!/bin/bash
target_path="${1:-$PWD}" && echo "Target path: $target_path"

# Get version from pyproject.toml
poetry version -s --directory "$target_path"
