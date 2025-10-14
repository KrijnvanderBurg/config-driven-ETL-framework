"""
Spark auto-initialization script for Jupyter notebooks.
This script runs automatically when a notebook kernel starts."""

import logging
import sys

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.WARN)

print("Initializing Spark session...", file=sys.stderr)

# Build the SparkSession with sensible defaults for notebook usage
spark = SparkSession.Builder() \
    .appName("Jupyter Notebook") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "devcontainer") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.ui.port", "4040") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.python.worker.reuse", "true") \
    .getOrCreate()

# Create a SparkContext variable for backward compatibility
sc = spark.sparkContext

# Set a higher log level to reduce verbosity
sc.setLogLevel("WARN")

# Print Spark session info
print(f"SparkSession successfully initialized!", file=sys.stderr)
print(f"Spark version: {spark.version}", file=sys.stderr)
print(f"Using {sc.defaultParallelism} cores by default", file=sys.stderr)
print("Spark Web UI available at http://localhost:4040", file=sys.stderr)

# Print connection information to help with debugging
print(f"Connected to Spark master: {sc.master}", file=sys.stderr)
print(f"Application ID: {sc.applicationId}", file=sys.stderr)