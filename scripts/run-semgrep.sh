#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/config/semgrep.yaml}" && echo "Config file: $config_filepath"
sarif_output="${3:-$PWD/semgrep.sarif}" && echo "SARIF output: $sarif_output"

# Install semgrep if not already installed
if ! command -v semgrep &> /dev/null; then
    echo "Installing semgrep..."
    pip install semgrep --quiet
fi

# add to run custom rules
# --config "$config_filepath" \

semgrep scan "$target_path" \
  --config "p/default" \
  --config "p/python" \
  --sarif \
  -o "$sarif_output" \
  --error \
  --text \
  --no-autofix \
  --force-color \
  --metrics "off" \
  --oss-only
exit_code=$?

exit $exit_code
