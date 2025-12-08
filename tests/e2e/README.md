# E2E Test Framework

## Overview

This framework provides isolated, production-like testing for Samara data processing jobs. Tests execute via CLI commands in temporary directories, ensuring no repository pollution.

## Architecture

All e2e test code is consolidated in a single file: `tests/e2e/test_job_command.py`

This file contains:
- **`PathRedirector`** - Redirects output paths to tmp directories
- **`JobTestExecutor`** - Executes jobs via CLI exactly as in production
- **`ResultVerifier`** - Verifies both schemas and data using PySpark
- **`TestJobExecution`** - Single parametrized test that discovers all job.json files

## Test Structure

```
tests/e2e/
├── test_job_command.py          # All e2e test code in one file
└── job/
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
✅ **Git Safe** - No writes to repository directories
✅ **Simple** - All code in a single file for easy understanding and maintenance

## Adding New Tests

1. Create a new directory under `tests/e2e/job/`
2. Add your `job.json` configuration (must include `hooks` field)
3. Add input data files
4. Create `<load_name>/` directory with expected outputs
5. Test runs automatically via pytest discovery
