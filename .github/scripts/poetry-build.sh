#!/bin/bash
workspace_path="${1:-$PWD}" && echo "Workspace path: $workspace_path"
output_path="${2:-$workspace_path/dist/}" && echo "Output path: $output_path"

# Build the package
echo "Building package from $workspace_path"
poetry build --directory "$workspace_path" --output "$output_path" --no-interaction

echo "Build complete. Package(s) created in $output_path"
