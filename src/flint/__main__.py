"""Command-line interface for the PySpark Ingestion Framework.

This module provides a command-line entry point for executing data ingestion
jobs defined by configuration files. It parses command-line arguments,
validates inputs, initializes the job from a configuration file, and
executes the ETL pipeline.

Example:
    ```
    python -m flint --filepath path/to/config.json
    ```
"""

import logging
from argparse import ArgumentParser
from pathlib import Path

from flint.core.job import Job
from flint.utils.logger import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """
    Run the ingestion framework pipeline from command line arguments.

    Parses command line arguments, validates the input filepath,
    creates a job from the specified configuration file, and
    executes the ETL pipeline.

    Raises:
        ValueError: If the filepath argument is empty.
    """
    logger.info("Starting job.")

    parser = ArgumentParser(description="config driven etl.")
    parser.add_argument("--config-filepath", required=True, type=str, help="config filepath")
    args = parser.parse_args()
    logger.debug("Parsed command line arguments: %s", args)

    if args.config_filepath == "":
        raise ValueError("Config filepath is required.")

    config_filepath: Path = Path(args.config_filepath)
    logger.info("Using configuration file: %s", config_filepath)

    job = Job.from_file(filepath=config_filepath)

    job.validate()
    job.execute()

    logger.info("Job completed. Exiting.")


if __name__ == "__main__":
    main()
