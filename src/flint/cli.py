"""Flint CLI command definitions and registry.

Contains Command base class, command implementations, and command registry.
No CLI argument parsing or dispatch logic is present here.
"""

import logging
from abc import ABC, abstractmethod
from argparse import Namespace, _SubParsersAction  # type: ignore
from pathlib import Path

from flint.core.job import Job
from flint.exceptions import ValidationError
from flint.types import ExitCode
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Command(ABC):
    """Abstract base class for CLI commands.

    Defines the interface for all CLI commands in Flint. Each command must
    implement methods for argument parsing and execution.
    """

    @staticmethod
    @abstractmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Add this command's subparser and arguments to the CLI.

        Args:
            subparsers (argparse._SubParsersAction): The subparsers action from argparse.
        """

    @classmethod
    @abstractmethod
    def from_args(cls, args: Namespace) -> "Command":
        """Create a Command instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            Command: An instance of the command.
        """

    @abstractmethod
    def execute(self) -> ExitCode:
        """Execute the command.

        Executes the command's main logic. Should be implemented by all subclasses.
        Should handle all exceptions and return appropriate exit codes.

        Returns:
            ExitCode: The exit code indicating success or specific failure reason
        """


class RunCommand(Command):
    """Command to run the ETL pipeline using a configuration file."""

    def __init__(self, config_filepath: Path) -> None:
        """Initialize RunCommand.

        Args:
            config_filepath: Path to the ETL pipeline configuration file.
        """
        self.config_filepath = config_filepath

    @staticmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Register the 'run' subcommand and its arguments.

        Args:
            subparsers: The subparsers action from argparse.
        """
        parser = subparsers.add_parser("run", help="Run the ETL pipeline")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")

    @classmethod
    def from_args(cls, args: Namespace) -> "RunCommand":
        """Create a RunCommand instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            RunCommand: An instance of RunCommand.
        """
        return cls(config_filepath=Path(args.config_filepath))

    def execute(self) -> ExitCode:
        """Execute the ETL pipeline as defined in the configuration file.

        Loads, validates, and runs the ETL job using the provided configuration file.
        Logs progress and completion status.

        Returns:
            ExitCode: SUCCESS if the job completes without errors, otherwise an appropriate error code
        """
        path = Path(self.config_filepath)
        logger.info("Running ETL pipeline with config: %s", path)

        if not path.exists():
            logger.error("Configuration file not found: %s", path)
            return ExitCode.CONFIGURATION_ERROR

        try:
            job = Job.from_file(filepath=path)
            job.validate()
            job.execute()
            logger.info("ETL pipeline completed successfully")
            return ExitCode.SUCCESS
        except ValidationError as e:
            # Directly return appropriate exit code for validation errors
            logger.error("Validation failed: %s", str(e))
            return ExitCode.VALIDATION_ERROR
        except RuntimeError as e:
            # Wrap runtime errors as general errors using exception chaining
            logger.error("Runtime error: %s", str(e))
            # No need to raise here, just return the appropriate exit code
            return ExitCode.GENERAL_ERROR
        except Exception as e:
            # Log and return appropriate exit code for unexpected exceptions
            logger.error("Unexpected exception: %s", str(e))
            logger.debug("Exception details:", exc_info=e)
            return ExitCode.UNEXPECTED_ERROR


class ValidateCommand(Command):
    """Command to validate the ETL pipeline configuration file."""

    def __init__(self, config_filepath: Path) -> None:
        """Initialize ValidateCommand.

        Args:
            config_filepath: Path to the ETL pipeline configuration file.
        """
        self.config_filepath = config_filepath

    @staticmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Register the 'validate' subcommand and its arguments.

        Args:
            subparsers: The subparsers action from argparse.
        """
        parser = subparsers.add_parser("validate", help="Validate the ETL pipeline config.")
        parser.add_argument("--config-filepath", required=True, type=str, help="Path to config file")

    @classmethod
    def from_args(cls, args: Namespace) -> "ValidateCommand":
        """Create a ValidateCommand instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            ValidateCommand: An instance of ValidateCommand.
        """
        return cls(config_filepath=Path(args.config_filepath))

    def execute(self) -> ExitCode:
        """Validate the ETL pipeline configuration file.

        Loads and validates the ETL job configuration file. Logs progress and completion status.

        Returns:
            ExitCode: SUCCESS if the validation completes without errors, otherwise an appropriate error code
        """
        path = Path(self.config_filepath)
        logger.info("Validating ETL pipeline with config: %s", path)

        if not path.exists():
            logger.error("Configuration file not found: %s", path)
            return ExitCode.CONFIGURATION_ERROR

        try:
            job = Job.from_file(filepath=path)
            job.validate()
            logger.info("ETL pipeline validation completed successfully")
            return ExitCode.SUCCESS
        except ValidationError as e:
            # Directly return appropriate exit code for validation errors
            logger.error("Validation failed: %s", str(e))
            return ExitCode.VALIDATION_ERROR
        except RuntimeError as e:
            # Wrap runtime errors as general errors using exception chaining
            logger.error("Runtime error: %s", str(e))
            # No need to raise here, just return the appropriate exit code
            return ExitCode.GENERAL_ERROR
        except Exception as e:
            # Log and return appropriate exit code for unexpected exceptions
            logger.error("Unexpected exception: %s", str(e))
            logger.debug("Exception details:", exc_info=e)
            return ExitCode.UNEXPECTED_ERROR
