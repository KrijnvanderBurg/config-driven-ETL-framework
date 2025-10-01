"""Unit tests for the Flint CLI module."""

import os
from argparse import ArgumentParser, Namespace
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from flint.alert import AlertController
from flint.cli import RunCommand, ValidateCommand
from flint.exceptions import (
    ExitCode,
    FlintAlertConfigurationError,
    FlintIOError,
    FlintJobError,
    FlintRuntimeConfigurationError,
    FlintValidationError,
)
from flint.runtime.controller import RuntimeController


class TestCommand:
    """Test cases for the base Command class."""

    def test_execute__with_keyboard_interrupt__returns_keyboard_interrupt_exit_code(self) -> None:
        """Test Command.execute handles KeyboardInterrupt correctly."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with (
            patch.object(AlertController, "from_file", side_effect=KeyboardInterrupt),
        ):
            result = command.execute()

        assert result == ExitCode.KEYBOARD_INTERRUPT

    def test_execute__with_unexpected_exception__returns_unexpected_error_exit_code(self) -> None:
        """Test Command.execute handles unexpected exceptions correctly."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=RuntimeError):
            result = command.execute()

        assert result == ExitCode.UNEXPECTED_ERROR

    def test_execute__with_not_implemented_error__returns_unexpected_error_exit_code(self) -> None:
        """Test Command.execute handles NotImplementedError correctly."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=NotImplementedError):
            result = command.execute()

        assert result == ExitCode.UNEXPECTED_ERROR


class TestValidateCommand:
    """Test cases for ValidateCommand."""

    def test_validate_command_add_subparser__registers_correctly(self) -> None:
        """Test ValidateCommand.add_subparser registers the validate subcommand correctly."""

        parser = ArgumentParser()
        subparsers = parser.add_subparsers()

        ValidateCommand.add_subparser(subparsers)

        # Parse arguments to verify the subcommand was registered correctly
        args = parser.parse_args(
            ["validate", "--alert-filepath", "/test/alert.json", "--runtime-filepath", "/test/runtime.json"]
        )

        assert args.alert_filepath == "/test/alert.json"
        assert args.runtime_filepath == "/test/runtime.json"

    def test_from_args__with_basic_args__creates_command_correctly(self) -> None:
        """Test ValidateCommand.from_args creates command with basic arguments."""
        args = Namespace(
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            test_exception=None,
            test_env_vars=None,
        )

        command = ValidateCommand.from_args(args)

        assert command.alert_filepath == Path("/path/to/alert.json")
        assert command.runtime_filepath == Path("/path/to/runtime.json")
        assert command.test_exception_message is None
        assert command.test_env_vars is None

    def test_from_args__with_test_exception__creates_command_correctly(self) -> None:
        """Test ValidateCommand.from_args with test exception message."""
        args = Namespace(
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            test_exception="Test error message",
            test_env_vars=None,
        )

        command = ValidateCommand.from_args(args)

        assert command.test_exception_message == "Test error message"

    def test_from_args__with_test_env_vars__creates_command_correctly(self) -> None:
        """Test ValidateCommand.from_args with environment variables."""
        args = Namespace(
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            test_exception=None,
            test_env_vars=["ENV1=value1", "ENV2=value2"],
        )

        command = ValidateCommand.from_args(args)

        assert command.test_env_vars == {"ENV1": "value1", "ENV2": "value2"}

    def test_execute__with_successful_validation__returns_success(self) -> None:
        """Test ValidateCommand executes successfully with valid configuration."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()
        mock_runtime = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = command.execute()

        assert result == ExitCode.SUCCESS

    def test_execute__with_alert_io_error__returns_io_error(self) -> None:
        """Test ValidateCommand handles AlertController IO error."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=FlintIOError("test")):
            result = command.execute()

        assert result == ExitCode.IO_ERROR

    def test_execute__with_alert_configuration_error__returns_configuration_error(self) -> None:
        """Test ValidateCommand handles AlertController configuration error."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=FlintAlertConfigurationError("test")):
            result = command.execute()

        assert result == ExitCode.CONFIGURATION_ERROR

    def test_execute__with_runtime_io_error__returns_io_error(self) -> None:
        """Test ValidateCommand handles RuntimeController IO error."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintIOError("test")),
        ):
            result = command.execute()

        assert result == ExitCode.IO_ERROR

    def test_execute__with_runtime_configuration_error__returns_configuration_error(self) -> None:
        """Test ValidateCommand handles RuntimeController configuration error."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintRuntimeConfigurationError("test")),
        ):
            result = command.execute()

        assert result == ExitCode.CONFIGURATION_ERROR

    def test_execute__with_validation_error__returns_validation_error(self) -> None:
        """Test ValidateCommand handles validation error."""
        command = ValidateCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=FlintValidationError("test")),
        ):
            result = command.execute()

        assert result == ExitCode.VALIDATION_ERROR

    def test_execute__with_test_env_vars__sets_environment_variables(self) -> None:
        """Test ValidateCommand sets test environment variables during execution."""
        command = ValidateCommand(
            alert_filepath=Path("/test/alert.json"),
            runtime_filepath=Path("/test/runtime.json"),
            test_env_vars={"TEST_VAR": "test_value", "ANOTHER_VAR": "another_value"},
        )

        mock_alert = Mock()
        mock_runtime = Mock()

        # Mock os.environ to capture what was set
        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
            patch.dict("os.environ") as mock_environ,
        ):
            result = command.execute()

            # Verify environment variables were set (even though it triggers test alert)
            assert mock_environ["TEST_VAR"] == "test_value"
            assert mock_environ["ANOTHER_VAR"] == "another_value"
            assert result == ExitCode.ALERT_TEST_ERROR

    def test_execute__with_test_exception_message__triggers_test_alert(self) -> None:
        """Test ValidateCommand triggers test alert when test_exception_message is provided."""
        command = ValidateCommand(
            alert_filepath=Path("/test/alert.json"),
            runtime_filepath=Path("/test/runtime.json"),
            test_exception_message="Test exception message",
        )

        mock_alert = Mock()
        mock_runtime = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = command.execute()

        # Should trigger alert and return alert test error exit code
        assert result == ExitCode.ALERT_TEST_ERROR
        mock_alert.evaluate_trigger_and_alert.assert_called_once()

    def test_execute__with_test_env_vars_only__triggers_test_alert(self) -> None:
        """Test ValidateCommand triggers test alert when only test_env_vars are provided."""
        command = ValidateCommand(
            alert_filepath=Path("/test/alert.json"),
            runtime_filepath=Path("/test/runtime.json"),
            test_env_vars={"TEST_VAR": "test_value"},
        )

        mock_alert = Mock()
        mock_runtime = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
            patch.dict(os.environ, {}, clear=False),
        ):
            result = command.execute()

        # Should trigger alert and return alert test error exit code
        assert result == ExitCode.ALERT_TEST_ERROR
        mock_alert.evaluate_trigger_and_alert.assert_called_once()


class TestRunCommand:
    """Test cases for RunCommand."""

    def test_run_command_add_subparser__registers_correctly(self) -> None:
        """Test RunCommand.add_subparser registers the run subcommand correctly."""

        parser = ArgumentParser()
        subparsers = parser.add_subparsers()

        RunCommand.add_subparser(subparsers)

        # Parse arguments to verify the subcommand was registered correctly
        args = parser.parse_args(["run", "--config-filepath", "/test/config.json"])

        assert args.config_filepath == "/test/config.json"

    def test_from_args__creates_command_correctly(self) -> None:
        """Test RunCommand.from_args creates command correctly using base Command.from_args."""
        args = Namespace(
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
        )

        command = RunCommand.from_args(args)

        assert command.alert_filepath == Path("/path/to/alert.json")
        assert command.runtime_filepath == Path("/path/to/runtime.json")

    def test_execute__with_successful_run__returns_success(self) -> None:
        """Test RunCommand executes successfully."""
        command = RunCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()
        mock_runtime = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", return_value=mock_runtime),
        ):
            result = command.execute()

        assert result == ExitCode.SUCCESS
        mock_runtime.execute_all.assert_called_once()

    def test_execute__with_alert_io_error__returns_io_error(self) -> None:
        """Test RunCommand handles AlertController IO error."""
        command = RunCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=FlintIOError("test")):
            result = command.execute()

        assert result == ExitCode.IO_ERROR

    def test_execute__with_alert_configuration_error__returns_configuration_error(self) -> None:
        """Test RunCommand handles AlertController configuration error."""
        command = RunCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        with patch.object(AlertController, "from_file", side_effect=FlintAlertConfigurationError("test")):
            result = command.execute()

        assert result == ExitCode.CONFIGURATION_ERROR

    @pytest.mark.parametrize(
        "exception_class,expected_exit_code",
        [
            (FlintIOError, ExitCode.IO_ERROR),
            (FlintRuntimeConfigurationError, ExitCode.CONFIGURATION_ERROR),
            (FlintValidationError, ExitCode.VALIDATION_ERROR),
            (FlintJobError, ExitCode.JOB_ERROR),
        ],
    )
    def test_execute__with_runtime_errors__returns_correct_exit_codes(
        self, exception_class, expected_exit_code
    ) -> None:
        """Test RunCommand handles various runtime errors correctly."""
        command = RunCommand(alert_filepath=Path("/test/alert.json"), runtime_filepath=Path("/test/runtime.json"))

        mock_alert = Mock()

        with (
            patch.object(AlertController, "from_file", return_value=mock_alert),
            patch.object(RuntimeController, "from_file", side_effect=exception_class("Test error")),
        ):
            result = command.execute()

        assert result == expected_exit_code
