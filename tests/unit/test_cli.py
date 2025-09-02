"""
Unit tests for Flint CLI commands.

This module tests the CLI command structure, argument parsing, error handling,
and command execution workflow. Tests are organized to minimize duplication
and maximize maintainability.
"""

from argparse import ArgumentParser, Namespace
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from flint.__main__ import main
from flint.cli import Command, JobCommand, ValidateCommand
from flint.exceptions import ExitCode, FlintConfigurationError, FlintIOError, FlintJobError, FlintValidationError


# Test fixtures to reduce duplication
@pytest.fixture
def mock_config_path() -> Path:
    """Provide a standard config file path for testing."""
    return Path("/tmp/test_config.json")


@pytest.fixture
def mock_alert_manager():
    """Provide a mocked AlertController for testing."""
    with patch("flint.cli.AlertController") as mock_class:
        mock_instance = Mock()
        mock_class.from_file.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_etl():
    """Provide a mocked Etl for testing."""
    with patch("flint.cli.Etl") as mock_class:
        mock_instance = Mock()
        mock_class.from_file.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_namespace() -> Namespace:
    """Provide a sample Namespace for argument testing."""
    return Namespace(config_filepath="/tmp/test.json")


class TestCommandBase:
    """Base test class for common command testing patterns."""

    def _test_command_init(self, command_class: type[Command], config_path: Path) -> None:
        """Helper to test command initialization."""
        cmd = command_class(config_filepath=config_path)
        assert cmd.config_filepath == config_path

    def _test_from_args(self, command_class: type[Command], namespace: Namespace) -> None:
        """Helper to test command creation from args."""
        cmd = command_class.from_args(namespace)
        assert isinstance(cmd, command_class)
        assert cmd.config_filepath == Path(namespace.config_filepath)


class TestCommand:
    """Unit tests for the abstract Command base class."""

    def test_command_is_abstract(self) -> None:
        """Test that Command cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class Command"):
            Command()  # type: ignore

    @pytest.mark.parametrize(
        "exception,expected_exit_code",
        [
            (NotImplementedError("Test not implemented"), ExitCode.UNEXPECTED_ERROR),
            (KeyboardInterrupt("User cancelled"), ExitCode.KEYBOARD_INTERRUPT),
            (ValueError("Unexpected error"), ExitCode.UNEXPECTED_ERROR),
            (RuntimeError("Runtime error"), ExitCode.UNEXPECTED_ERROR),
        ],
    )
    def test_execute_handles_exceptions(self, exception: Exception, expected_exit_code: ExitCode) -> None:
        """Test that execute handles various exceptions correctly."""

        class TestCommand(Command):
            @staticmethod
            def add_subparser(subparsers):
                pass

            @classmethod
            def from_args(cls, args):
                return cls()

            def _execute(self) -> ExitCode:
                raise exception

        cmd = TestCommand()
        result = cmd.execute()
        assert result == expected_exit_code

    def test_execute_returns_success_code(self) -> None:
        """Test that execute returns the success code from _execute."""

        class TestCommand(Command):
            @staticmethod
            def add_subparser(subparsers):
                pass

            @classmethod
            def from_args(cls, args):
                return cls()

            def _execute(self) -> ExitCode:
                return ExitCode.SUCCESS

        cmd = TestCommand()
        result = cmd.execute()
        assert result == ExitCode.SUCCESS


class TestJobCommand(TestCommandBase):
    """Unit tests for JobCommand."""

    def test_init_sets_config_filepath(self, mock_config_path: Path) -> None:
        """Test config_filepath is set on init."""
        self._test_command_init(JobCommand, mock_config_path)

    def test_from_args_sets_config_filepath(self, sample_namespace: Namespace) -> None:
        """Test from_args sets config_filepath."""
        self._test_from_args(JobCommand, sample_namespace)

    def test_add_subparser_registers_run_command(self) -> None:
        """Test that add_subparser properly registers the run command."""
        parser = ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        JobCommand.add_subparser(subparsers)

        # Test that 'run' command is registered
        args = parser.parse_args(["run", "--config-filepath", "/test/config.json"])
        assert args.command == "run"
        assert args.config_filepath == "/test/config.json"

    def test_execute_success(self, mock_config_path: Path, mock_alert_manager: Mock, mock_etl: Mock) -> None:
        """Test successful execution of JobCommand."""
        cmd = JobCommand(config_filepath=mock_config_path)
        result = cmd._execute()

        assert result == ExitCode.SUCCESS
        mock_etl.validate_all.assert_called_once()
        mock_etl.execute_all.assert_called_once()

    @pytest.mark.parametrize(
        "exception_type,expected_exit_code",
        [
            (FlintIOError, ExitCode.IO_ERROR),
            (FlintConfigurationError, ExitCode.CONFIGURATION_ERROR),
            (FlintValidationError, ExitCode.VALIDATION_ERROR),
            (FlintJobError, ExitCode.JOB_ERROR),
        ],
    )
    def test_execute_handles_business_exceptions(
        self,
        mock_config_path: Path,
        mock_alert_manager: Mock,
        mock_etl: Mock,
        exception_type: type[Exception],
        expected_exit_code: ExitCode,
    ) -> None:
        """Test JobCommand handles various business exceptions correctly."""
        if exception_type == FlintIOError:
            # Test AlertController IO error
            with patch("flint.cli.AlertController") as mock_alert_class:
                mock_alert_class.from_file.side_effect = exception_type("Test error")
                cmd = JobCommand(config_filepath=mock_config_path)
                result = cmd._execute()
                assert result == expected_exit_code
        else:
            # Test Job-related errors
            if exception_type == FlintConfigurationError:
                mock_etl.validate_all.side_effect = exception_type("Test error")
            elif exception_type == FlintValidationError:
                mock_etl.validate_all.side_effect = exception_type("Test error")
            elif exception_type == FlintJobError:
                mock_etl.execute_all.side_effect = exception_type("Test error")

            cmd = JobCommand(config_filepath=mock_config_path)
            result = cmd._execute()
            assert result == expected_exit_code

            # Verify alert was processed for non-IO errors
            if exception_type != FlintIOError:
                mock_alert_manager.evaluate_trigger_and_alert.assert_called_once()

    def test_execute_handles_job_io_error_after_alert_manager_success(
        self, mock_config_path: Path, mock_alert_manager: Mock
    ) -> None:
        """Test JobCommand handles Job IO error when AlertController succeeds."""
        # Mock Etl.from_file to raise FlintIOError
        with patch("flint.cli.Etl.from_file") as mock_etl_from_file:
            mock_etl_from_file.side_effect = FlintIOError("Failed to read job config")
            cmd = JobCommand(config_filepath=mock_config_path)
            result = cmd._execute()

            assert result == ExitCode.IO_ERROR
            # AlertController should not process alert for IO errors
            mock_alert_manager.evaluate_trigger_and_alert.assert_not_called()

    def test_execute_handles_configuration_error_with_alert(
        self, mock_config_path: Path, mock_alert_manager: Mock, mock_etl: Mock
    ) -> None:
        """Test JobCommand handles configuration error and processes alert."""
        mock_etl.validate_all.side_effect = FlintConfigurationError("Configuration error")
        cmd = JobCommand(config_filepath=mock_config_path)
        result = cmd._execute()

        assert result == ExitCode.CONFIGURATION_ERROR
        mock_alert_manager.evaluate_trigger_and_alert.assert_called_once_with(
            body="Configuration error occurred",
            title="ETL Pipeline Configuration Error",
            exception=mock_etl.validate_all.side_effect,
        )


class TestValidateCommand(TestCommandBase):
    """Unit tests for ValidateCommand."""

    def test_init_sets_config_filepath(self, mock_config_path: Path) -> None:
        """Test config_filepath is set on init."""
        self._test_command_init(ValidateCommand, mock_config_path)

    def test_from_args_sets_config_filepath(self, sample_namespace: Namespace) -> None:
        """Test from_args sets config_filepath."""
        self._test_from_args(ValidateCommand, sample_namespace)

    def test_add_subparser_registers_validate_command(self) -> None:
        """Test that add_subparser properly registers the validate command."""
        parser = ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        ValidateCommand.add_subparser(subparsers)

        # Test that 'validate' command is registered
        args = parser.parse_args(["validate", "--config-filepath", "/test/config.json"])
        assert args.command == "validate"
        assert args.config_filepath == "/test/config.json"

    def test_execute_success(self, mock_config_path: Path, mock_alert_manager: Mock, mock_etl: Mock) -> None:
        """Test successful execution of ValidateCommand.

        ValidateCommand only validates the ETL configuration, it does not execute.
        """
        cmd = ValidateCommand(config_filepath=mock_config_path)
        result = cmd._execute()

        assert result == ExitCode.SUCCESS
        mock_etl.validate_all.assert_called_once()
        # ValidateCommand should not execute, only validate
        mock_etl.execute_all.assert_not_called()

    @pytest.mark.parametrize(
        "exception_type,expected_exit_code",
        [
            (FlintIOError, ExitCode.IO_ERROR),
            (FlintConfigurationError, ExitCode.CONFIGURATION_ERROR),
            (FlintValidationError, ExitCode.VALIDATION_ERROR),
        ],
    )
    def test_execute_handles_business_exceptions(
        self,
        mock_config_path: Path,
        mock_alert_manager: Mock,
        mock_etl: Mock,
        exception_type: type[Exception],
        expected_exit_code: ExitCode,
    ) -> None:
        """Test ValidateCommand handles various business exceptions correctly.

        Note: ValidateCommand should not encounter FlintJobError since it's validation only.
        """
        if exception_type == FlintIOError:
            # Test AlertController IO error
            with patch("flint.cli.AlertController") as mock_alert_class:
                mock_alert_class.from_file.side_effect = exception_type("Test error")
                cmd = ValidateCommand(config_filepath=mock_config_path)
                result = cmd._execute()
                assert result == expected_exit_code
        else:
            # Test Job-related errors
            if exception_type == FlintConfigurationError:
                mock_etl.validate_all.side_effect = exception_type("Test error")
            elif exception_type == FlintValidationError:
                # In ValidateCommand, validation errors come from validate_all()
                mock_etl.validate_all.side_effect = exception_type("Test error")

            cmd = ValidateCommand(config_filepath=mock_config_path)
            result = cmd._execute()
            assert result == expected_exit_code

            # Verify alert was processed for non-IO errors
            if exception_type != FlintIOError:
                mock_alert_manager.evaluate_trigger_and_alert.assert_called_once()

    def test_execute_handles_job_io_error_after_alert_manager_success(
        self, mock_config_path: Path, mock_alert_manager: Mock
    ) -> None:
        """Test ValidateCommand handles Job IO error when AlertController succeeds."""
        # Mock Etl.from_file to raise FlintIOError
        with patch("flint.cli.Etl.from_file") as mock_etl_from_file:
            mock_etl_from_file.side_effect = FlintIOError("Failed to read job config")
            cmd = ValidateCommand(config_filepath=mock_config_path)
            result = cmd._execute()

            assert result == ExitCode.IO_ERROR
            # AlertController should not process alert for IO errors
            mock_alert_manager.evaluate_trigger_and_alert.assert_not_called()

    def test_execute_handles_configuration_error_with_alert(
        self, mock_config_path: Path, mock_alert_manager: Mock, mock_etl: Mock
    ) -> None:
        """Test ValidateCommand handles configuration error and processes alert."""
        mock_etl.validate_all.side_effect = FlintConfigurationError("Configuration error")
        cmd = ValidateCommand(config_filepath=mock_config_path)
        result = cmd._execute()

        assert result == ExitCode.CONFIGURATION_ERROR
        mock_alert_manager.evaluate_trigger_and_alert.assert_called_once_with(
            body="Configuration error occurred",
            title="ETL Pipeline Configuration Error",
            exception=mock_etl.validate_all.side_effect,
        )


class TestMainFunction:
    """Unit tests for the main CLI entry point function."""

    @pytest.mark.parametrize(
        "command,command_class",
        [
            ("validate", "ValidateCommand"),
            ("run", "JobCommand"),
        ],
    )
    @patch("flint.__main__.ValidateCommand")
    @patch("flint.__main__.JobCommand")
    @patch("flint.__main__.ArgumentParser.parse_args")
    def test_main_command_success(
        self,
        mock_parse_args: Mock,
        mock_job_command_class: Mock,
        mock_validate_command_class: Mock,
        command: str,
        command_class: str,
    ) -> None:
        """Test main function with various commands returns success."""
        mock_args = Mock()
        mock_args.command = command
        mock_parse_args.return_value = mock_args

        mock_command_instance = Mock()
        mock_command_instance.execute.return_value = ExitCode.SUCCESS

        if command_class == "ValidateCommand":
            mock_validate_command_class.from_args.return_value = mock_command_instance
            expected_mock = mock_validate_command_class
        else:
            mock_job_command_class.from_args.return_value = mock_command_instance
            expected_mock = mock_job_command_class

        result = main()

        assert result == ExitCode.SUCCESS
        expected_mock.from_args.assert_called_once_with(mock_args)
        mock_command_instance.execute.assert_called_once()

    @patch("flint.__main__.ArgumentParser.parse_args")
    def test_main_unknown_command_raises_not_implemented(self, mock_parse_args: Mock) -> None:
        """Test main function with unknown command raises NotImplementedError."""
        mock_args = Mock()
        mock_args.command = "unknown-command"
        mock_parse_args.return_value = mock_args

        with pytest.raises(NotImplementedError, match="Unknown command 'unknown-command'"):
            main()

    @pytest.mark.parametrize(
        "argv,expected_exit_code",
        [
            (["flint", "--version"], 0),
            (["flint", "--help"], 0),
            (["flint"], 2),  # No command provided
            (["flint", "validate"], 2),  # Missing required argument
        ],
    )
    def test_main_argument_parsing_edge_cases(self, argv: list[str], expected_exit_code: int) -> None:
        """Test main function handles various argument parsing scenarios."""
        with patch("sys.argv", argv):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == expected_exit_code

    @pytest.mark.parametrize(
        "command,error_code",
        [
            ("validate", ExitCode.CONFIGURATION_ERROR),
            ("run", ExitCode.JOB_ERROR),
        ],
    )
    @patch("flint.__main__.ValidateCommand")
    @patch("flint.__main__.JobCommand")
    @patch("flint.__main__.ArgumentParser.parse_args")
    def test_main_command_error_codes(
        self,
        mock_parse_args: Mock,
        mock_job_command_class: Mock,
        mock_validate_command_class: Mock,
        command: str,
        error_code: ExitCode,
    ) -> None:
        """Test main function propagates error codes correctly."""
        mock_args = Mock()
        mock_args.command = command
        mock_parse_args.return_value = mock_args

        mock_command_instance = Mock()
        mock_command_instance.execute.return_value = error_code

        if command == "validate":
            mock_validate_command_class.from_args.return_value = mock_command_instance
        else:
            mock_job_command_class.from_args.return_value = mock_command_instance

        result = main()
        assert result == error_code
