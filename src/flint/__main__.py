"""Flint module entry point.

Handles CLI argument parsing, command dispatch, and process exit.
"""

import logging
from argparse import ArgumentParser
from importlib.metadata import version

from flint.cli import RunCommand
from flint.utils.logger import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """Main entry point for Flint CLI (python -m flint).

    Parses arguments, dispatches to the appropriate command, and exits with the correct code.
    """
    parser = ArgumentParser(description="Flint: Configuration-driven PySpark ETL framework.")
    parser.add_argument("-v", "--version", action="version", version=version("flint"))
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        help="Set the logging level (default: INFO). Options: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
    )
    parser.add_argument("--log-file", type=str, help="Path to log file (default: flint.log).")
    subparsers = parser.add_subparsers(required=True, help="Command to run")

    # Register subcommands
    RunCommand.from_subparser(subparsers=subparsers)
    # ValidateCommand.from_subparser(subparsers=subparsers)

    print("Parsing command line arguments...")
    args = parser.parse_args()
    print("Parsed args:", args)

    print("Calling the handler function for the chosen subcommand...")
    args.handler(args)  # This runs the appropriate handler based on the command


if __name__ == "__main__":
    main()
