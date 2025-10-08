"""Flint CLI command definitions and registry.

Contains Command base class, command implementations, and command registry.
No CLI argument parsing or dispatch logic is present here.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from argparse import Namespace, _SubParsersAction  # type: ignore
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from flint.alert import AlertController
from flint.exceptions import (
    ExitCode,
    FlintAlertConfigurationError,
    FlintAlertTestError,
    FlintIOError,
    FlintJobError,
    FlintRuntimeConfigurationError,
    FlintValidationError,
)
from flint.runtime.controller import RuntimeController
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


@dataclass
class Command(ABC):
    """Abstract base class for CLI commands.

    Defines the interface for all CLI commands in Flint. Each command must
    implement methods for argument parsing and execution.
    """

    alert_filepath: Path
    runtime_filepath: Path

    @staticmethod
    @abstractmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Add this command's subparser and arguments to the CLI.

        Args:
            subparsers: The subparsers action from argparse.
        """

    @classmethod
    def from_args(cls, args: Namespace) -> Self:
        """Create a ValidateCommand instance from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            ValidateCommand: An instance of ValidateCommand.
        """
        runtime_filepath = Path(args.runtime_filepath)
        alert_filepath = Path(args.alert_filepath)

        return cls(
            alert_filepath=alert_filepath,
            runtime_filepath=runtime_filepath,
        )

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
        except NotImplementedError:
            return ExitCode.UNEXPECTED_ERROR
        except KeyboardInterrupt:
            logger.error("Operation cancelled by user", exc_info=True)
            return ExitCode.KEYBOARD_INTERRUPT
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
            logger.error("Exception details:", exc_info=True)
            return ExitCode.UNEXPECTED_ERROR

    @abstractmethod
    def _execute(self) -> ExitCode:
        """Execute the command's core logic.

        This method should be implemented by subclasses to contain their specific
        business logic and handle any command-specific exceptions.

        Returns:
            ExitCode: The exit code indicating success or specific failure reason.
        """


@dataclass
class ValidateCommand(Command):
    """Command to validate the ETL pipeline configuration file."""

    test_exception_message: str | None = None
    test_env_vars: dict[str, str] | None = None

    @staticmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Register the 'validate' subcommand and its arguments."""
        parser = subparsers.add_parser("validate", help="Validate the ETL pipeline configuration")
        parser.add_argument("--alert-filepath", required=True, type=str, help="Path to alert configuration file")
        parser.add_argument("--runtime-filepath", required=True, type=str, help="Path to runtime configuration file")
        parser.add_argument("--test-exception", type=str, help="Test exception message to trigger alert testing")
        parser.add_argument("--test-env-var", action="append", help="Test env vars (KEY=VALUE)")

    @classmethod
    def from_args(cls, args: Namespace) -> Self:
        """Create ValidateCommand from args."""
        test_env_vars = None
        if args.test_env_var:
            test_env_vars = {}
            for env_var in args.test_env_var:
                key, value = env_var.split("=", 1)
                test_env_vars[key] = value

        return cls(
            alert_filepath=Path(args.alert_filepath),
            runtime_filepath=Path(args.runtime_filepath),
            test_exception_message=args.test_exception,
            test_env_vars=test_env_vars,
        )

    def _execute(self) -> ExitCode:
        """Validate the ETL pipeline configuration file."""
        # Set test env vars if provided
        if self.test_env_vars:
            for key, value in self.test_env_vars.items():
                os.environ[key] = value

        try:
            alert = AlertController.from_file(filepath=self.alert_filepath)
        except FlintIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            return e.exit_code
        except FlintAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            return e.exit_code

        try:
            _ = RuntimeController.from_file(filepath=self.runtime_filepath)
            # Not alerting on exceptions as a validate command is often run locally or from CICD
            # and thus an alert would be drowning out real alerts
        except FlintIOError as e:
            logger.error("Cannot access runtime configuration file: %s", e)
            return e.exit_code
        except FlintRuntimeConfigurationError as e:
            logger.error("Runtime configuration is invalid: %s", e)
            return e.exit_code
        except FlintValidationError as e:
            logger.error("Validation failed: %s", e)
            return e.exit_code

        # Trigger test exception if specified (either message or env vars)
        if self.test_exception_message or self.test_env_vars:
            try:
                message = self.test_exception_message or "Test alert triggered"
                raise FlintAlertTestError(message)
            except FlintAlertTestError as e:
                alert.evaluate_trigger_and_alert(title="Test Alert", body="Test alert", exception=e)
                return e.exit_code

        logger.info("ETL pipeline validation completed successfully")
        return ExitCode.SUCCESS


@dataclass
class RunCommand(Command):
    """Command to run the ETL pipeline using a configuration file."""

    @staticmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Register the 'run' subcommand and its arguments.

        Args:
            subparsers: The subparsers action from argparse.
        """
        parser = subparsers.add_parser("run", help="Run the ETL pipeline")
        parser.add_argument("--alert-filepath", required=True, type=str, help="Path to alert configuration file")
        parser.add_argument("--runtime-filepath", required=True, type=str, help="Path to runtime configuration file")

    def _execute(self) -> ExitCode:
        """Execute the ETL pipeline as defined in the configuration file."""
        logger.info("Running ETL pipeline with config: %s", self.runtime_filepath)

        try:
            alert = AlertController.from_file(filepath=self.alert_filepath)
        except FlintIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            return e.exit_code
        except FlintAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            return e.exit_code

        try:
            runtime = RuntimeController.from_file(filepath=self.runtime_filepath)
            runtime.execute_all()
            logger.info("ETL pipeline completed successfully")
            return ExitCode.SUCCESS
        except FlintIOError as e:
            logger.error("Cannot access runtime configuration file: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Configuration File Error", body="Failed to read runtime configuration file", exception=e
            )
            return e.exit_code
        except FlintRuntimeConfigurationError as e:
            logger.error("Runtime configuration is invalid: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Configuration Error", body="Invalid runtime configuration", exception=e
            )
            return e.exit_code
        except FlintValidationError as e:
            logger.error("Configuration validation failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Validation Error", body="Configuration validation failed", exception=e
            )
            return e.exit_code
        except FlintJobError as e:
            logger.error("ETL job failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Execution Error", body="Runtime error during ETL execution", exception=e
            )
            return e.exit_code


@dataclass
class ExportSchemaCommand(Command):
    """Command to export the runtime configuration JSON schema."""

    output_filepath: Path

    @staticmethod
    def add_subparser(subparsers: _SubParsersAction) -> None:
        """Register the 'export-schema' subcommand and its arguments.

        Args:
            subparsers: The subparsers action from argparse.
        """
        parser = subparsers.add_parser("export-schema", help="Export the runtime configuration JSON schema")
        parser.add_argument(
            "--output-filepath", required=True, type=str, help="Path where the JSON schema file will be saved"
        )
        # Alert filepath not used for schema export, but required by base class
        parser.add_argument("--alert-filepath", required=False, type=str, help="Not used for export-schema command")
        parser.add_argument("--runtime-filepath", required=False, type=str, help="Not used for export-schema command")

    @classmethod
    def from_args(cls, args: Namespace) -> Self:
        """Create ExportSchemaCommand from parsed arguments.

        Args:
            args: Parsed CLI arguments.

        Returns:
            ExportSchemaCommand: An instance of ExportSchemaCommand.
        """
        output_filepath = Path(args.output_filepath)

        # Provide dummy paths for alert and runtime since they're not used
        return cls(
            alert_filepath=Path(""),
            runtime_filepath=Path(""),
            output_filepath=output_filepath,
        )

    def _execute(self) -> ExitCode:
        """Export the runtime configuration JSON schema to a file."""
        logger.info("Exporting runtime configuration schema to: %s", self.output_filepath)

        try:
            schema = RuntimeController.export_schema()

            # Ensure parent directory exists
            self.output_filepath.parent.mkdir(parents=True, exist_ok=True)

            # Write schema to file with pretty formatting
            with open(self.output_filepath, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=4, ensure_ascii=False)

            logger.info("Runtime configuration schema exported successfully to: %s", self.output_filepath)
            return ExitCode.SUCCESS
        except OSError as e:
            logger.error("Failed to write schema file: %s", e)
            return ExitCode.IO_ERROR
        except Exception as e:  # pylint: disable=broad-except
            logger.error("Unexpected error during schema export: %s", e)
            return ExitCode.UNEXPECTED_ERROR
