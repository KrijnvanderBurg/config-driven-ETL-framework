"""Command-line interface for the PySpark Ingestion Framework.

This module provides a command-line entry point for executing data ingestion
jobs defined by configuration files. It parses command-line arguments,
validates inputs, initializes the job from a configuration file, and
executes the ETL pipeline.

Example:
    ```
    python -m ingestion_framework --filepath path/to/config.json
    ```
"""

import logging
from argparse import ArgumentParser
from pathlib import Path

from ingestion_framework.core.job import Job
from ingestion_framework.utils.logger import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """Run the ingestion framework pipeline from command line arguments.

    Parses command line arguments, validates the input filepath,
    creates a job from the specified configuration file, and
    executes the ETL pipeline.

    Returns:
        None

    Raises:
        ValueError: If the filepath argument is empty.
    """
    logger.info("Starting something...")

    parser = ArgumentParser(description="config driven etl.")
    parser.add_argument("--filepath", required=True, type=str, help="dict_ filepath")
    args = parser.parse_args()
    logger.info("args: %s", args)

    if args.filepath == "":
        raise ValueError("Filepath is required.")
    filepath: Path = Path(args.filepath)

    job = Job.from_file(filepath=filepath)
    job.execute()

    logger.info("Exiting.")


if __name__ == "__main__":
    main()
