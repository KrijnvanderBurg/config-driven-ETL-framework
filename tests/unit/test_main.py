"""Unit tests for the Flint main module."""

from argparse import ArgumentParser, Namespace
from unittest.mock import Mock, patch

import pytest

from flint.__main__ import main
from flint.cli import RunCommand, ValidateCommand
from flint.exceptions import ExitCode


class TestMain:
    """Test cases for the main function."""

    def test_main__with_validate_command__creates_and_executes_validate_command(self) -> None:
        """Test main function dispatches to ValidateCommand correctly."""
        mock_args = Namespace(
            command="validate",
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            log_level="INFO",
        )

        with (
            patch.object(ArgumentParser, "parse_args", return_value=mock_args),
            patch.object(ValidateCommand, "from_args") as mock_from_args,
            patch.object(ValidateCommand, "add_subparser"),
            patch.object(RunCommand, "add_subparser"),
        ):
            mock_command = Mock()
            mock_command.execute.return_value = ExitCode.SUCCESS
            mock_from_args.return_value = mock_command

            result = main()

            assert result == ExitCode.SUCCESS
            mock_from_args.assert_called_once_with(mock_args)
            mock_command.execute.assert_called_once()

    def test_main__with_run_command__creates_and_executes_run_command(self) -> None:
        """Test main function dispatches to RunCommand correctly."""
        mock_args = Namespace(
            command="run",
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            log_level="INFO",
        )

        with (
            patch.object(ArgumentParser, "parse_args", return_value=mock_args),
            patch.object(RunCommand, "from_args") as mock_from_args,
            patch.object(ValidateCommand, "add_subparser"),
            patch.object(RunCommand, "add_subparser"),
        ):
            mock_command = Mock()
            mock_command.execute.return_value = ExitCode.SUCCESS
            mock_from_args.return_value = mock_command

            result = main()

            assert result == ExitCode.SUCCESS
            mock_from_args.assert_called_once_with(mock_args)
            mock_command.execute.assert_called_once()

    def test_main__with_unknown_command__raises_value_error(self) -> None:
        """Test main function raises ValueError for unknown commands."""
        mock_args = Namespace(command="unknown-command", log_level="INFO")

        with (
            patch.object(ArgumentParser, "parse_args", return_value=mock_args),
            patch.object(ValidateCommand, "add_subparser"),
            patch.object(RunCommand, "add_subparser"),
        ):
            with pytest.raises(ValueError):
                main()

    def test_main__returns_command_exit_code(self) -> None:
        """Test main function returns the exit code from command execution."""
        mock_args = Namespace(
            command="validate",
            alert_filepath="/path/to/alert.json",
            runtime_filepath="/path/to/runtime.json",
            log_level="INFO",
        )

        with (
            patch.object(ArgumentParser, "parse_args", return_value=mock_args),
            patch.object(ValidateCommand, "from_args") as mock_from_args,
            patch.object(ValidateCommand, "add_subparser"),
            patch.object(RunCommand, "add_subparser"),
        ):
            mock_command = Mock()
            mock_command.execute.return_value = ExitCode.VALIDATION_ERROR
            mock_from_args.return_value = mock_command

            result = main()

            assert result == ExitCode.VALIDATION_ERROR
