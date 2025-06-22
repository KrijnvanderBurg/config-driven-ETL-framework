"""Test fixtures for the PySpark Ingestion Framework.

This module defines pytest fixtures that can be reused across test modules
to set up test environments, create test data, and manage resources.

Fixtures defined here are automatically available to all test modules in the project.
"""

import os

# Set environment variable to suppress pyarrow timezone warning
# This is required for pyarrow>=2.0.0 with PySpark
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

pytest_plugins: list[str] = [
    # "tests.unit. ...
]
