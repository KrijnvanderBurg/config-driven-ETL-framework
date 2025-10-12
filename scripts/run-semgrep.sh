#!/bin/bash
target_path="${1:-$PWD/src}" && echo "Scanning folder: $target_path"
config_filepath="${2:-$PWD/config/semgrep.yaml}" && echo "Config file: $config_filepath"
junit_output="${3:-$PWD/semgrep-junit.xml}" && echo "JUnit output: $junit_output"
sarif_output="${4:-$PWD/semgrep.sarif}" && echo "SARIF output: $sarif_output"

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
  --junit-xml \
  -o "$junit_output" \
  --sarif \
  --sarif-output="$sarif_output" \
  --error \
  --text \
  --no-autofix \
  --force-color \
  --metrics "off" \
  --oss-only
exit_code=$?

exit $exit_code
