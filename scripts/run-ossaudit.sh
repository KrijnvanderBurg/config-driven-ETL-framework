#!/bin/bash
#
# ISSUES
# Ossaudit ignores the configuration file entirely. And thus cannot ignore vulnerability IDs by config file.
#
target_dirpath="${1:-$PWD}" 
echo "Target dirpath: $target_dirpath"

config_filepath="${2:-$PWD/config/ossaudit.ini}" 
echo "Config filepath: $config_filepath"

# Set temporary directory parameter with default to /tmp
temp_dir="${3:-/tmp}"
echo "Temporary directory: $temp_dir"

# Install dependencies
if ! command -v poetry &> /dev/null; then
    echo "Installing poetry..."
    pip install poetry --quiet
fi
poetry self add poetry-plugin-export

# Export requirements from poetry
# https://github.com/python-poetry/poetry-plugin-export
requirements_file="$temp_dir/requirements.txt"
poetry export --format requirements.txt \
  --all-extras \
  --all-groups \
  --without-hashes \
  | tee "$requirements_file"

# Install and run ossaudit
if ! command -v ossaudit &> /dev/null; then
    echo "Installing ossaudit..."
    pip install ossaudit --quiet
fi
echo "ossaudit has no option to print version."

# Run ossaudit and capture the exit code
ossaudit -f "$requirements_file" \
  --config "$config_filepath"
ossaudit_exit_code=$?

# Clean up and exit with ossaudit's exit code
rm -f "$requirements_file"
exit $ossaudit_exit_code
