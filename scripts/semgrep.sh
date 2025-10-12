#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/pyproject.toml}" && echo "Config file: $config_filepath"
output_filepath="${3:-$PWD/semgrep-junit.xml}" && echo "Output will be saved to: $output_filepath"

# Install semgrep if not already installed
if ! command -v semgrep &> /dev/null; then
    echo "Installing semgrep..."
    pip install semgrep --quiet
fi

semgrep scan "$target_path" \
  --config "p/default" \
  --config "p/python" \
  --config "$config_filepath" \
  --junit-xml \
  -o "$output_filepath" \
  --error \
  --text \
  --no-autofix \
  --force-color \
  --metrics "off" \
  --oss-only
exit_code=$?

exit $exit_code
