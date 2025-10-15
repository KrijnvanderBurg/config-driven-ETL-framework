#!/bin/bash
tests_path="${1:-$PWD/tests/}" && echo "Tests path: $tests_path"
coverage_path="${2:-$PWD/src/}" && echo "Coverage path: $coverage_path"
config_filepath_pytest="${3:-$PWD/.github/config/pytest.ini}" && echo "Config file pytest: $config_filepath_pytest"
config_filepath_coverage="${4:-$PWD/.github/config/.coveragerc}" && echo "Config file coverage: $config_filepath_coverage"
output_coverage_filepath="${5:-$PWD/coverage.xml}" && echo "Coverage output will be saved to: $output_coverage_filepath"
output_junit_filepath="${6:-$PWD/JUNIT-TEST.xml}" && echo "Pytest junit output will be saved to: $output_junit_filepath"

# Install pytest and related packages if not already installed
if ! command -v pytest &> /dev/null || ! python3 -c "import pytest_cov" &> /dev/null || ! python3 -c "import xdist" &> /dev/null; then
    echo "Installing pytest and related packages..."
    pip3 install pytest pytest-cov pytest-xdist --quiet
fi

cd "$PWD" && pytest "$tests_path" \
    --rootdir="$PWD" \
    -c="$config_filepath_pytest" \
    -o "cache_dir=$PWD/.pytest_cache" \
    -n 0 \
    --durations=0 \
    --durations-min=0.01 \
    --cov="$coverage_path" \
    --cov-report="term-missing" \
    --cov-config="$config_filepath_coverage" \
    --junit-xml="$output_junit_filepath"
    # --verbose

# Combine parallel coverage files from E2E subprocess executions
echo "Combining parallel coverage data files..."
coverage combine --rcfile="$config_filepath_coverage"

# Generate XML report from combined coverage data
echo "Generating coverage XML report..."
coverage xml -o "$output_coverage_filepath" --rcfile="$config_filepath_coverage"

