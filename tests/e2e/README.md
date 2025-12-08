# E2E Test Framework

## Overview

This framework provides isolated, production-like testing for Samara data processing jobs. Tests execute via CLI commands in temporary directories, ensuring no repository pollution.

## Architecture

All e2e test code is in a single file: `tests/e2e/test_job_command.py` (99 lines)

This file contains:
- **One parametrized test function** that discovers and tests all job.json files
- **One simple fixture** (`spark`) that provides a Spark session for verification
- **Inline logic** for path redirection, CLI execution, and result verification

## Test Structure

```
tests/e2e/
├── test_job_command.py          # Single test file (99 lines)
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
✅ **Ultra Simple** - Single test function with inline logic (99 lines total)

## Adding New Tests

1. Create a new directory under `tests/e2e/job/`
2. Add your `job.json` configuration (must include `hooks` field)
3. Add input data files
4. Create `<load_name>/` directory with expected outputs
5. Test runs automatically via pytest discovery
