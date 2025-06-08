"""
TODO


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import logging
from argparse import ArgumentParser

from stratum.job import Job
from stratum.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """TODO"""
    logger.info("Starting something...")

    parser = ArgumentParser(description="config driven etl.")
    parser.add_argument("--filepath", required=True, type=str, help="confeti filepath")
    args = parser.parse_args()
    logger.info("args: %s", args)

    filepath: str = args.filepath

    if filepath != "":
        job = Job.from_file(filepath=filepath)
        job.execute()

    logger.info("Exiting.")


if __name__ == "__main__":
    main()
