#!/bin/bash
dist_path="${1:-$PWD/dist/}" && echo "Distribution path: $dist_path"

# Check if dist directory exists
if [ ! -d "$dist_path" ]; then
    echo "Error: Distribution directory not found at $dist_path"
    exit 1
fi

# Find the wheel file
wheel_file=$(find "$dist_path" -name "*.whl" -type f | head -n 1)

if [ -z "$wheel_file" ]; then
    echo "Error: No wheel file (.whl) found in $dist_path"
    echo "Please run the build package script first."
    exit 1
fi

echo "Found wheel file: $wheel_file"

# Install the package
echo "Installing package..."
pip install "$wheel_file" --force-reinstall

echo "Package installation complete."
