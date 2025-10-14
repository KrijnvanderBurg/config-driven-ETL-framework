"""Unit tests for the Samara CLI module."""

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from samara.alert import AlertController
from samara.cli import cli
from samara.exceptions import (
    ExitCode,
    FlintAlertConfigurationError,
    FlintIOError,
    FlintJobError,
    FlintRuntimeConfigurationError,
    FlintValidationError,
)
from samara.runtime.controller import RuntimeController


class TestValidateCommand:
    """Test cases for validate command."""

    def test_validate__with_valid_configuration__exits_with_success(self) -> None:
        """Test validate command completes successfully when both configuration files are valid."""
        # Arrange
        runner = CliRunner()
        # Mock controllers to avoid file I/O during validation
        mock_alert = Mock()
        mock_runtime = Mock()

        # Act
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == 0

    def test_validate__when_alert_configuration_fails__exits_with_error(self) -> None:
        """Test validate command returns error when alert configuration fails to load."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock alert configuration failure
        with patch.object(AlertController, "from_file", side_effect=FlintIOError("test")):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.IO_ERROR

    def test_validate__when_alert_configuration_is_invalid__exits_with_configuration_error(self) -> None:
        """Test validate command returns configuration error when alert configuration is malformed."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock invalid alert configuration
        with patch.object(AlertController, "from_file", side_effect=FlintAlertConfigurationError("test")):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.CONFIGURATION_ERROR

    def test_validate__when_runtime_configuration_is_invalid__exits_with_configuration_error(self) -> None:
        """Test validate command returns configuration error when runtime configuration is malformed."""
        # Arrange
        runner = CliRunner()
        mock_alert = Mock()

        # Act
        # Mock invalid runtime configuration
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintRuntimeConfigurationError("test")),
        ):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.CONFIGURATION_ERROR

    def test_validate__when_runtime_io_error_occurs__exits_with_io_error(self) -> None:
        """Test validate command returns IO error when runtime configuration file cannot be accessed."""
        # Arrange
        runner = CliRunner()
        # Mock alert controller to avoid file I/O
        mock_alert = Mock()

        # Act
        # Mock runtime file access failure
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintIOError("test")),
        ):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.IO_ERROR

    def test_validate__when_runtime_configuration_fails__exits_with_error(self) -> None:
        """Test validate command returns error when runtime configuration fails to load."""
        # Arrange
        runner = CliRunner()
        # Mock alert controller to avoid file I/O
        mock_alert = Mock()

        # Act
        # Mock runtime configuration failure
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintValidationError("test")),
        ):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.VALIDATION_ERROR

    def test_validate__when_test_alert_triggered__sets_env_vars_and_exits_with_alert_error(self) -> None:
        """Test validate command triggers test alert when --test-exception or --test-env-var provided."""
        # Arrange
        runner = CliRunner()
        # Mock controllers to avoid file I/O
        mock_alert = Mock()
        mock_runtime = Mock()

        # Act
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
            patch.dict("os.environ", {}, clear=False),
        ):
            result = runner.invoke(
                cli,
                [
                    "validate",
                    "--alert-filepath",
                    "/test/alert.json",
                    "--runtime-filepath",
                    "/test/runtime.json",
                    "--test-exception",
                    "Test exception",
                    "--test-env-var",
                    "TEST_VAR=test_value",
                ],
            )

            # Assert
            assert os.environ["TEST_VAR"] == "test_value"
            assert result.exit_code == ExitCode.ALERT_TEST_ERROR
            mock_alert.evaluate_trigger_and_alert.assert_called_once()

    def test_validate__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test validate command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock unexpected exception to test general error handling
        with patch.object(AlertController, "from_file", side_effect=RuntimeError):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR


class TestRunCommand:
    """Test cases for run command."""

    def test_run__with_valid_configuration__executes_pipeline_and_exits_with_success(self) -> None:
        """Test run command successfully executes ETL pipeline with valid configuration."""
        # Arrange
        runner = CliRunner()
        # Mock controllers to avoid file I/O and actual pipeline execution
        mock_alert = Mock()
        mock_runtime = Mock()

        # Act
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = runner.invoke(
                cli, ["run", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
            )

        # Assert
        assert result.exit_code == 0
        mock_runtime.execute_all.assert_called_once()

    def test_run__when_alert_configuration_fails__exits_with_error(self) -> None:
        """Test run command returns error when alert configuration fails to load."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock alert configuration failure
        with patch.object(AlertController, "from_file", side_effect=FlintIOError("test")):
            result = runner.invoke(
                cli, ["run", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
            )

        # Assert
        assert result.exit_code == ExitCode.IO_ERROR

    def test_run__when_alert_configuration_is_invalid__exits_with_configuration_error(self) -> None:
        """Test run command returns configuration error when alert configuration is malformed."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock invalid alert configuration
        with patch.object(AlertController, "from_file", side_effect=FlintAlertConfigurationError("test")):
            result = runner.invoke(
                cli, ["run", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
            )

        # Assert
        assert result.exit_code == ExitCode.CONFIGURATION_ERROR

    @pytest.mark.parametrize(
        "exception_class,expected_exit_code",
        [
            (FlintIOError, ExitCode.IO_ERROR),
            (FlintRuntimeConfigurationError, ExitCode.CONFIGURATION_ERROR),
            (FlintValidationError, ExitCode.VALIDATION_ERROR),
            (FlintJobError, ExitCode.JOB_ERROR),
        ],
    )
    def test_run__when_runtime_error_occurs__triggers_alert_and_exits_with_correct_code(
        self, exception_class, expected_exit_code
    ) -> None:
        """Test run command triggers alert and returns correct exit code for various runtime errors."""
        # Arrange
        runner = CliRunner()
        # Mock alert controller to test alerting behavior
        mock_alert = Mock()

        # Act
        # Mock runtime errors to test error handling and alerting
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=exception_class("Test error")),
        ):
            result = runner.invoke(
                cli, ["run", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
            )

        # Assert
        assert result.exit_code == expected_exit_code
        mock_alert.evaluate_trigger_and_alert.assert_called_once()

    def test_run__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test run command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock unexpected exception to test general error handling
        with patch.object(AlertController, "from_file", side_effect=RuntimeError):
            result = runner.invoke(
                cli, ["run", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
            )

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR


class TestExportSchemaCommand:
    """Test cases for export-schema command."""

    def test_export_schema__with_valid_output_path__creates_schema_file_and_exits_with_success(self) -> None:
        """Test export-schema command successfully creates schema file with parent directories."""
        # Arrange
        runner = CliRunner()
        mock_schema = {"type": "object", "properties": {}}

        # Act
        # Mock schema generation to avoid dependency on actual schema definition
        with (
            patch.object(RuntimeController, "export_schema", return_value=mock_schema),
            runner.isolated_filesystem(),
        ):
            # Test with nested path to verify directory creation
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "subdir/nested/schema.json"])

            # Assert
            assert result.exit_code == 0
            assert Path("subdir/nested/schema.json").exists()

    def test_export_schema__when_file_write_fails__exits_with_io_error(self) -> None:
        """Test export-schema command returns IO error when file cannot be written."""
        # Arrange
        runner = CliRunner()
        mock_schema = {"type": "object", "properties": {}}

        # Act
        # Mock file write failure to test error handling without requiring actual permission issues
        with (
            patch.object(RuntimeController, "export_schema", return_value=mock_schema),
            patch("builtins.open", side_effect=OSError("Permission denied")),
        ):
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "/invalid/path/schema.json"])

            # Assert
            assert result.exit_code == ExitCode.IO_ERROR

    def test_export_schema__when_unexpected_error_occurs__exits_with_unexpected_error_code(self) -> None:
        """Test export-schema command returns unexpected error code when an unhandled exception occurs."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock unexpected exception to test general error handling
        with patch.object(RuntimeController, "export_schema", side_effect=RuntimeError("Unexpected error")):
            result = runner.invoke(cli, ["export-schema", "--output-filepath", "schema.json"])

        # Assert
        assert result.exit_code == ExitCode.UNEXPECTED_ERROR


class TestCliGroup:
    """Test cases for the CLI group."""

    def test_cli__when_help_flag_provided__displays_all_commands(self) -> None:
        """Test CLI displays help information with all available commands when --help flag is used."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(cli, ["--help"])

        # Assert
        assert result.exit_code == 0
        assert "validate" in result.output
        assert "run" in result.output
        assert "export-schema" in result.output

    def test_cli__when_log_level_specified__accepts_valid_level(self) -> None:
        """Test CLI accepts valid log level option without crashing."""
        # Arrange
        runner = CliRunner()
        # Mock controllers to avoid file I/O
        mock_alert = Mock()
        mock_runtime = Mock()

        # Act
        # Testing one valid level is sufficient - Click validates the choice constraint
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = runner.invoke(
                cli,
                [
                    "--log-level",
                    "DEBUG",
                    "validate",
                    "--alert-filepath",
                    "/test/alert.json",
                    "--runtime-filepath",
                    "/test/runtime.json",
                ],
            )

        # Assert
        assert result.exit_code == 0

    def test_cli__when_invalid_log_level_specified__exits_with_error(self) -> None:
        """Test CLI rejects invalid log level values and displays error."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(
            cli,
            [
                "--log-level",
                "INVALID",
                "validate",
                "--alert-filepath",
                "/test/alert.json",
                "--runtime-filepath",
                "/test/runtime.json",
            ],
        )

        # Assert
        # Click returns exit code 2 for usage errors (invalid arguments)
        assert result.exit_code == ExitCode.USAGE_ERROR

    def test_cli__when_no_command_provided__displays_help_text(self) -> None:
        """Test CLI displays help text when invoked without any command."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(cli, [])

        # Assert
        # Click shows usage information when no command is given
        assert result.exit_code == ExitCode.USAGE_ERROR
        assert "Commands:" in result.output

    def test_cli__when_user_interrupts__exits_gracefully(self) -> None:
        """Test CLI exits gracefully when user sends keyboard interrupt signal."""
        # Arrange
        runner = CliRunner()

        # Act
        # Mock KeyboardInterrupt to simulate Ctrl+C from user
        with patch.object(AlertController, "from_file", side_effect=KeyboardInterrupt):
            result = runner.invoke(
                cli,
                ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"],
            )

        # Assert
        # CLI intercepts KeyboardInterrupt and converts to exit code 98
        assert result.exit_code == ExitCode.KEYBOARD_INTERRUPT
