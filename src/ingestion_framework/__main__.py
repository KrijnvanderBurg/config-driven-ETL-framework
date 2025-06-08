"""
TODO
"""

import logging
from argparse import ArgumentParser
from pathlib import Path

from ingestion_framework.core.job import Job
from ingestion_framework.utils.logger import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """TODO"""
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
