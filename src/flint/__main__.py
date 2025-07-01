"""Flint module entry point.

Handles CLI argument parsing, command dispatch, and process exit.
"""

import logging
import sys
from argparse import ArgumentParser
from importlib.metadata import version

from flint.cli import RunCommand, ValidateCommand
from flint.types import ExitCode
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

    exit_code = ExitCode.GENERAL_ERROR  # Default to error until we successfully complete a command

    try:
        if args.command == "validate":
            logger.info("Running 'validate' command...")
            validate_command = ValidateCommand.from_args(args)
            exit_code = validate_command.execute()

        elif args.command == "run":
            logger.info("Running 'run' command...")
            run_command = RunCommand.from_args(args)
            exit_code = run_command.execute()

        else:
            logger.error("Unknown command: %s", args.command)
            exit_code = ExitCode.INVALID_ARGUMENTS

    except Exception as e:
        logger.error("Uncaught exception: %s", str(e))
        logger.debug("Exception details:", exc_info=e)
        exit_code = ExitCode.UNEXPECTED_ERROR

    logger.info("Application finished with exit code %d (%s). Exiting.", exit_code, exit_code.name)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
