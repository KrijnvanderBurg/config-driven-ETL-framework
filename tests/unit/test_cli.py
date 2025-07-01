"""
Unit tests for Flint CLI commands.
"""

from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from flint.cli import RunCommand, ValidateCommand


class TestRunCommand:
    """Unit tests for RunCommand."""

    def test_init_sets_config_filepath(self) -> None:
        """Test config_filepath is set on init."""
        cmd = RunCommand(config_filepath=Path("/tmp/test.json"))
        assert cmd.config_filepath == Path("/tmp/test.json")

    def test_from_args_sets_config_filepath(self) -> None:
        """Test from_args sets config_filepath."""
        args = Namespace(config_filepath=Path("/tmp/args.json"))
        cmd = RunCommand.from_args(args)
        assert isinstance(cmd, RunCommand)
        assert cmd.config_filepath == Path("/tmp/args.json")

    def test_execute_calls_job_methods(self) -> None:
        """Test execute calls Job.validate and Job.execute."""
        args = Namespace(config_filepath=Path("/tmp/job.json"))
        cmd = RunCommand.from_args(args)
        mock_job = MagicMock()
        with patch("flint.cli.Job.from_file", return_value=mock_job) as mock_from_file:
            with patch("pathlib.Path.exists", return_value=True):
                cmd.execute()
                mock_from_file.assert_called_once_with(filepath=cmd.config_filepath)
                mock_job.validate.assert_called_once()
                mock_job.execute.assert_called_once()


class TestValidateCommand:
    """Unit tests for ValidateCommand."""

    def test_init_sets_config_filepath(self) -> None:
        """Test config_filepath is set on init."""
        cmd = ValidateCommand(config_filepath=Path("/tmp/validate.json"))
        assert cmd.config_filepath == Path("/tmp/validate.json")

    def test_from_args_sets_config_filepath(self) -> None:
        """Test from_args sets config_filepath."""
        args = Namespace(config_filepath=Path("/tmp/val_args.json"))
        cmd = ValidateCommand.from_args(args)
        assert isinstance(cmd, ValidateCommand)
        assert cmd.config_filepath == Path("/tmp/val_args.json")

    def test_execute_calls_job_validate(self) -> None:
        """Test execute calls Job.validate only."""
        args = Namespace(config_filepath="/tmp/job.json")
        cmd = ValidateCommand.from_args(args)
        mock_job = MagicMock()
        with patch("flint.cli.Job.from_file", return_value=mock_job) as mock_from_file:
            with patch("pathlib.Path.exists", return_value=True):
                cmd.execute()
                mock_from_file.assert_called_once_with(filepath=cmd.config_filepath)
                mock_job.validate.assert_called_once()
                # ValidateCommand should not call job.execute
                assert not mock_job.execute.called
