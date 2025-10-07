"""Core functionality for the PySpark ingestion framework.

This package contains the main components that make up the ETL pipeline:

- Extract: Components for retrieving data from various sources
- Transform: Components for manipulating and processing data
- Load: Components for writing data to destinations
- Job: The orchestrator that ties together extract, transform, and load operations

These components form the backbone of the ingestion framework and implement
the ETL patterns for data processing with Apache PySpark.
"""

from . import extract, job

# Make commonly used components available at the top level
__all__ = [
    "extract",
    "job",
]
