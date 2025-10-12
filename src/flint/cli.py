"""Flint CLI command definitions using Click library.

Contains Click command implementations for validate, run, and export-schema operations.
"""

import json
import logging
import os
from pathlib import Path

import click

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
from flint.utils.logger import get_logger, set_logger

logger: logging.Logger = get_logger(__name__)


@click.group(invoke_without_command=True)
@click.version_option(package_name="flint")
@click.option(
    "--log-level",
    default=None,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set the logging level (default: INFO).",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str | None = None) -> None:
    """Flint: Configuration-driven PySpark ETL framework."""
    set_logger(level=log_level)

    # Show help when no command is provided
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command()
@click.option(
    "--alert-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to alert configuration file",
)
@click.option(
    "--runtime-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to runtime configuration file",
)
@click.option(
    "--test-exception",
    type=str,
    default=None,
    help="Test exception message to trigger alert testing",
)
@click.option(
    "--test-env-var",
    multiple=True,
    type=str,
    help="Test env vars (KEY=VALUE)",
)
def validate(
    alert_filepath: Path,
    runtime_filepath: Path,
    test_exception: str | None,
    test_env_var: tuple[str, ...],
) -> None:
    """Validate the ETL pipeline configuration."""
    try:
        logger.info("Running 'validate' command...")

        # Parse test env vars
        test_env_vars = None
        if test_env_var:
            test_env_vars = {}
            for env_var_str in test_env_var:
                key, value = env_var_str.split("=", 1)
                test_env_vars[key] = value

        # Set test env vars if provided
        if test_env_vars:
            for key, value in test_env_vars.items():
                os.environ[key] = value

        try:
            alert = AlertController.from_file(filepath=alert_filepath)
        except FlintIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except FlintAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)

        try:
            _ = RuntimeController.from_file(filepath=runtime_filepath)
            # Not alerting on exceptions as a validate command is often run locally or from CICD
            # and thus an alert would be drowning out real alerts
        except FlintIOError as e:
            logger.error("Cannot access runtime configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except FlintRuntimeConfigurationError as e:
            logger.error("Runtime configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except FlintValidationError as e:
            logger.error("Validation failed: %s", e)
            raise click.exceptions.Exit(e.exit_code)

        # Trigger test exception if specified (either message or env vars)
        if test_exception or test_env_vars:
            try:
                message = test_exception or "Test alert triggered"
                raise FlintAlertTestError(message)
            except FlintAlertTestError as e:
                alert.evaluate_trigger_and_alert(title="Test Alert", body="Test alert", exception=e)
                raise click.exceptions.Exit(e.exit_code)

        logger.info("ETL pipeline validation completed successfully")
        logger.info("Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name)

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e


@cli.command()
@click.option(
    "--alert-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to alert configuration file",
)
@click.option(
    "--runtime-filepath",
    required=True,
    type=click.Path(exists=False, path_type=Path),
    help="Path to runtime configuration file",
)
def run(alert_filepath: Path, runtime_filepath: Path) -> None:
    """Run the ETL pipeline."""
    try:
        logger.info("Running 'run' command...")
        logger.info("Running ETL pipeline with config: %s", runtime_filepath)

        try:
            alert = AlertController.from_file(filepath=alert_filepath)
        except FlintIOError as e:
            logger.error("Cannot access alert configuration file: %s", e)
            raise click.exceptions.Exit(e.exit_code)
        except FlintAlertConfigurationError as e:
            logger.error("Alert configuration is invalid: %s", e)
            raise click.exceptions.Exit(e.exit_code)

        try:
            runtime = RuntimeController.from_file(filepath=runtime_filepath)
            runtime.execute_all()
            logger.info("ETL pipeline completed successfully")
            logger.info(
                "Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name
            )
        except FlintIOError as e:
            logger.error("Cannot access runtime configuration file: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Configuration File Error", body="Failed to read runtime configuration file", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)
        except FlintRuntimeConfigurationError as e:
            logger.error("Runtime configuration is invalid: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Configuration Error", body="Invalid runtime configuration", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)
        except FlintValidationError as e:
            logger.error("Configuration validation failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Validation Error", body="Configuration validation failed", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)
        except FlintJobError as e:
            logger.error("ETL job failed: %s", e)
            alert.evaluate_trigger_and_alert(
                title="ETL Execution Error", body="Runtime error during ETL execution", exception=e
            )
            raise click.exceptions.Exit(e.exit_code)

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e


@cli.command("export-schema")
@click.option(
    "--output-filepath",
    required=True,
    type=click.Path(path_type=Path),
    help="Path where the JSON schema file will be saved",
)
def export_schema(output_filepath: Path) -> None:
    """Export the runtime configuration JSON schema."""
    try:
        logger.info("Running 'export-schema' command...")
        logger.info("Exporting runtime configuration schema to: %s", output_filepath)

        try:
            schema = RuntimeController.export_schema()

            # Ensure parent directory exists
            output_filepath.parent.mkdir(parents=True, exist_ok=True)

            # Write schema to file with pretty formatting
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(schema, f, indent=4, ensure_ascii=False)

            logger.info("Runtime configuration schema exported successfully to: %s", output_filepath)
            logger.info(
                "Command executed successfully with exit code %d (%s).", ExitCode.SUCCESS, ExitCode.SUCCESS.name
            )
        except OSError as e:
            logger.error("Failed to write schema file: %s", e)
            raise click.exceptions.Exit(ExitCode.IO_ERROR) from e

    except click.exceptions.Exit:
        # Re-raise Click's Exit exceptions (these are our controlled exits with proper codes)
        raise
    except KeyboardInterrupt as e:
        logger.warning("Process interrupted by user")
        raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Unexpected exception %s: %s", type(e).__name__, str(e))
        logger.error("Exception details:", exc_info=True)
        raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e
