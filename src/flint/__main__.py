"""Flint module entry point.

Handles CLI argument parsing, command dispatch, and process exit.
"""

import logging
import sys
from argparse import ArgumentParser
from importlib.metadata import version

from flint.cli import RunCommand, ValidateCommand
from flint.utils.logger import get_logger, set_logger

set_logger()  # Configure root logger for all modules
logger: logging.Logger = get_logger(__name__)


def main() -> int:
    """Main entry point for Flint CLI (python -m flint).

    Parses arguments, dispatches to the appropriate command, and exits with the correct code.

    Returns:
        int: The exit code (0 for success, non-zero for errors)
    """
    parser = ArgumentParser(description="Flint: Configuration-driven PySpark ETL framework.")

    parser.add_argument("-v", "--version", action="version", version=version("flint"))
    parser.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        help="Set the logging level (default: INFO). Options: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Command to run")

    # Register subcommands
    ValidateCommand.add_subparser(subparsers=subparsers)
    RunCommand.add_subparser(subparsers=subparsers)
    args = parser.parse_args()

    if args.command == "validate":
        logger.info("Running 'validate' command...")
        validate_command = ValidateCommand.from_args(args)
        return validate_command.execute()

    if args.command == "run":
        logger.info("Running 'run' command...")
        run_command = RunCommand.from_args(args)
        return run_command.execute()

    raise NotImplementedError(f"Unknown command '{args.command}'.")


if __name__ == "__main__":
    exit_code = main()
    logger.info("Application finished with exit code %d. Exiting.", exit_code)
    sys.exit(exit_code)
