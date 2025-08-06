"""Flint CLI command definitions and registry.

Contains Command base class, command implementations, and command registry.
No CLI argument parsing or dispatch logic is present here.
"""

import logging
from abc import ABC, abstractmethod
from argparse import Namespace, _SubParsersAction  # type: ignore
from pathlib import Path
from typing import Self

from flint.core.job import Job
from flint.exceptions import ValidationError
from flint.types import ExitCode
from flint.utils.alert import Alert
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
            subparsers: The subparsers action from argparse.
        """

    @classmethod
    @abstractmethod
    def from_args(cls, args: Namespace) -> Self:
        """Create a Command instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            Command: An instance of the command.
        """

    def execute(self) -> ExitCode:
        """Execute the command with standardized exception handling.

        Template method that provides consistent error handling for system-level exceptions.
        Subclasses implement _execute() with their specific logic and business exception handling.

        Returns:
            ExitCode: The exit code indicating success or specific failure reason.
        """
        try:
            exit_code = self._execute()
            logger.info("Command executed successfully with exit code %d (%s).", exit_code, exit_code.name)
            return exit_code
        except KeyboardInterrupt:
            logger.info("Operation cancelled by user")
            raise
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
            logger.debug("Exception details:", exc_info=True)
            return ExitCode.UNEXPECTED_ERROR

    @abstractmethod
    def _execute(self) -> ExitCode:
        """Execute the command's core logic.

        This method should be implemented by subclasses to contain their specific
        business logic and handle any command-specific exceptions.

        Returns:
            ExitCode: The exit code indicating success or specific failure reason.
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
    def from_args(cls, args: Namespace) -> Self:
        """Create a RunCommand instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            RunCommand: An instance of RunCommand.
        """
        config_filepath = Path(args.config_filepath)

        return cls(config_filepath=config_filepath)

    def _execute(self) -> ExitCode:
        """Execute the ETL pipeline as defined in the configuration file.

        Loads, validates, and runs the ETL job using the provided configuration file.
        Logs progress and completion status.

        Returns:
            ExitCode: SUCCESS if the pipeline completes without errors, otherwise an appropriate error code.
        """
        logger.info("Running ETL pipeline with config: %s", self.config_filepath)

        try:
            alert = Alert.from_file(filepath=self.config_filepath)
        except NotImplementedError:
            return ExitCode.INVALID_ARGUMENTS
        except FileNotFoundError:
            return ExitCode.CONFIGURATION_ERROR
        except PermissionError:
            return ExitCode.CONFIGURATION_ERROR
        except OSError:
            return ExitCode.CONFIGURATION_ERROR

        try:
            job = Job.from_file(filepath=self.config_filepath)
            job.validate()
            job.execute()
            logger.info("ETL pipeline completed successfully")
            return ExitCode.SUCCESS
        except ValidationError as e:
            alert.trigger_if_conditions_met(
                body="Validation failed", title="ETL Pipeline Validation Error", exception=e
            )
            return ExitCode.VALIDATION_ERROR
        except RuntimeError as e:
            alert.trigger_if_conditions_met(
                body="Runtime error occurred", title="ETL Pipeline Runtime Error", exception=e
            )
            return ExitCode.RUNTIME_ERROR


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
    def from_args(cls, args: Namespace) -> Self:
        """Create a ValidateCommand instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            ValidateCommand: An instance of ValidateCommand.
        """
        config_filepath = Path(args.config_filepath)

        return cls(config_filepath=config_filepath)

    def _execute(self) -> ExitCode:
        """Validate the ETL pipeline configuration file.

        Loads and validates the ETL job configuration file. Logs progress and completion status.

        Returns:
            ExitCode: SUCCESS if validation completes without errors, otherwise an appropriate error code.
        """
        logger.info("Validating ETL pipeline with config: %s", self.config_filepath)

        try:
            alert = Alert.from_file(filepath=self.config_filepath)
        except NotImplementedError:
            return ExitCode.INVALID_ARGUMENTS
        except FileNotFoundError:
            return ExitCode.CONFIGURATION_ERROR
        except PermissionError:
            return ExitCode.CONFIGURATION_ERROR
        except OSError:
            return ExitCode.CONFIGURATION_ERROR

        try:
            job = Job.from_file(filepath=self.config_filepath)
            job.validate()
            job.execute()
            logger.info("ETL pipeline validation completed successfully")
            return ExitCode.SUCCESS
        except ValidationError as e:
            alert.trigger_if_conditions_met(
                body="Validation failed", title="ETL Pipeline Validation Error", exception=e
            )
            return ExitCode.VALIDATION_ERROR
