"""
TODO


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from argparse import ArgumentParser

from datastore.logger import set_logger

# from datastore.models.job_spec import JobSpecFactory

logger = set_logger(__name__)


if __name__ == "__main__":
    logger.info("Starting datastore driver config validation.")
    parser = ArgumentParser(description="Upload file to Databricks UC volume.")
    parser.add_argument("--confeti-filepath", required=True, type=str, help="Databricks URL.")
    args = parser.parse_args()
    logger.info("args: %s", args)

    confeti_filepath: str = args.confeti_filepath

    # job = JobSpecFactory.from_file(filepath=confeti_filepath)

    logger.info("Successfully validated job specification.")
