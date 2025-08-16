"""
Unit tests for Flint CLI commands.
"""

from argparse import Namespace
from pathlib import Path

from flint.cli import JobCommand, ValidateCommand


class TestRunCommand:
    """Unit tests for RunCommand."""

    def test_init_sets_config_filepath(self) -> None:
        """Test config_filepath is set on init."""
        cmd = JobCommand(config_filepath=Path("/tmp/test.json"))
        assert cmd.config_filepath == Path("/tmp/test.json")

    def test_from_args_sets_config_filepath(self) -> None:
        """Test from_args sets config_filepath."""
        args = Namespace(config_filepath=Path("/tmp/args.json"))
        cmd = JobCommand.from_args(args)
        assert isinstance(cmd, JobCommand)
        assert cmd.config_filepath == Path("/tmp/args.json")


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
