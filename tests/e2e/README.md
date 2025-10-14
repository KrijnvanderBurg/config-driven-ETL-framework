# E2E Test Framework

## Overview

This framework provides isolated, production-like testing for Samara data processing jobs. Tests execute via CLI commands in temporary directories, ensuring no repository pollution.

## Architecture

### Components

1. **`JobTestExecutor`** (`tests/e2e/framework/executor.py`)
   - Executes jobs in isolated tmp directories
   - Runs jobs via CLI exactly as in production
   - Handles configuration preparation and subprocess execution

2. **`PathRedirector`** (`tests/e2e/framework/redirector.py`)
   - Redirects input paths to tmp/inputs
   - Redirects output paths to tmp/outputs
   - Copies input files to isolated location

3. **`ResultVerifier`** (`tests/e2e/framework/verifier.py`)
   - Compares actual outputs with expected results
   - Verifies both schemas and data
   - Uses PySpark for DataFrame comparison

## Test Structure

```
tests/e2e/job/
├── test_job_command.py          # Main test file
└── <test_name>/
    ├── job.json                 # Job configuration
    ├── input_data.csv           # Test input data
    ├── input_schema.json        # Input schema
    └── <load_name>/             # Expected outputs folder
        ├── expected_output.csv  # Expected output data
        └── expected_schema.json # Expected output schema
```

## Key Features

✅ **Complete Isolation** - All execution happens in pytest tmp_path
✅ **Production-like** - Uses actual CLI commands, not test shortcuts  
✅ **Source Agnostic** - Framework supports any data source type
✅ **Git Safe** - No writes to repository directories
✅ **Simple** - Clean code without unnecessary complexity

## Adding New Tests

1. Create a new directory under `tests/e2e/job/`
2. Add your `job.json` configuration (must include `hooks` field)
3. Add input data files
4. Create `<load_name>/` directory with expected outputs
5. Test runs automatically via pytest discovery
