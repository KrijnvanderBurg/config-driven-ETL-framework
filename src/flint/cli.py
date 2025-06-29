"""Flint CLI command definitions and registry.

Contains Command base class, command implementations, and command registry.
No CLI argument parsing or dispatch logic is present here.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from flint.core.job import Job
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Command(ABC):
    """Abstract base class for CLI commands."""

    @staticmethod
    @abstractmethod
    def add_subparser(subparsers) -> None:
        """Add this command's subparser and arguments to the CLI.

        Args:
            subparsers: The subparsers action from argparse.
        """

    @classmethod
    @abstractmethod
    def from_args(cls, args) -> "Command":
        """Create a Command instance from parsed arguments."""
        ...

    @abstractmethod
    def execute(self) -> None:
        """Execute the command. Returns exit code."""


class RunCommand(Command):
    def __init__(self, config_filepath: str) -> None:
        self.config_filepath = config_filepath

    @staticmethod
    def add_subparser(subparsers) -> None:
        """Register the 'run' subcommand and its arguments."""
        parser = subparsers.add_parser("run", help="Run the ETL pipeline")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")

    @classmethod
    def from_args(cls, args) -> "RunCommand":
        """Create a RunCommand instance from parsed arguments."""
        return cls(config_filepath=args.config_filepath)

    def execute(self) -> None:
        path = Path(self.config_filepath)
        print(logger.level)
        logger.info("Running ETL pipeline with config: %s", path)

        job = Job.from_file(filepath=path)
        job.validate()
        job.execute()
        logger.info("Job completed successfully.")


class ValidateCommand(Command):
    def __init__(self, config_filepath: str) -> None:
        self.config_filepath = config_filepath

    @staticmethod
    def add_subparser(subparsers) -> None:
        """Register the 'validate' subcommand and its arguments."""
        parser = subparsers.add_parser("validate", help="Validate the ETL pipeline config.")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")

    @classmethod
    def from_args(cls, args) -> "ValidateCommand":
        """Create a ValidateCommand instance from parsed arguments."""
        return cls(config_filepath=args.config_filepath)

    def execute(self) -> None:
        path = Path(self.config_filepath)
        print(logger.level)
        logger.info("Validating ETL pipeline with config: %s", path)

        job = Job.from_file(filepath=path)
        job.validate()
        logger.info("Validation completed successfully.")
