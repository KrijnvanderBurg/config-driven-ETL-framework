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

    @classmethod
    @abstractmethod
    def from_subparser(cls, subparsers) -> None:
        """Add this command's subparser and arguments to the CLI.

        Args:
            subparsers: The subparsers action from argparse.
        """

    @abstractmethod
    def execute(self) -> None:
        """Execute the command. Returns exit code."""


class RunCommand(Command):
    def __init__(self, config_filepath: str) -> None:
        self.config_filepath = config_filepath

    @classmethod
    def from_subparser(cls, subparsers) -> None:
        parser = subparsers.add_parser("run", help="Run the ETL pipeline")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")
        parser.set_defaults(command_cls=cls)

    def execute(self) -> None:
        path = Path(self.config_filepath)
        logger.info("Running ETL pipeline with config: %s", path)

        job = Job.from_file(filepath=path)
        job.validate()
        job.execute()
        logger.info("Job completed successfully.")


class ValidateCommand(Command):
    def __init__(self, config_filepath: str) -> None:
        self.config_filepath = config_filepath

    @classmethod
    def from_subparser(cls, subparsers) -> None:
        parser = subparsers.add_parser("validate", help="Validate the job configuration")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")
        parser.set_defaults(command_cls=cls)

    def execute(self) -> None:
        path = Path(self.config_filepath)
        logger.info("Validating job configuration: %s", path)
        job = Job.from_file(filepath=path)
        job.validate()
        logger.info("Validation successful.")
