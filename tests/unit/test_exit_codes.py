"""Unit tests for exit code handling.

Tests the exit code enum and exception handling in         cmd = RunCommand(config_filepath=Path("test.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR) commands.
"""

import unittest
from pathlib import Path
from unittest.mock import patch

from flint.cli import RunCommand, ValidateCommand
from flint.exceptions import ConfigurationKeyError, FlintException, ValidationError
from flint.types import ExitCode


class TestExitCodes(unittest.TestCase):
    """Test cases for exit code handling."""

    def test_configuration_key_error(self) -> None:
        """Test that ConfigurationKeyError properly displays missing key and available keys."""
        test_dict = {"name": "test", "value": 123}
        error = ConfigurationKeyError("missing_key", test_dict)

        # Check that the key is preserved
        self.assertEqual(error.key, "missing_key")

        # Check that available keys are preserved
        self.assertListEqual(error.available_keys, ["name", "value"])

        # Check that the error message includes the key and available keys
        self.assertIn("missing_key", str(error))
        self.assertIn("name", str(error))
        self.assertIn("value", str(error))

    def test_exception_chaining(self) -> None:
        """Test that exception chaining works properly."""
        try:
            try:
                raise ValueError("Inner error")
            except ValueError as e:
                raise FlintException("Outer error", ExitCode.CONFIGURATION_ERROR) from e
        except FlintException as e:
            self.assertEqual(str(e), "Outer error")
            self.assertIsNotNone(e.__cause__)
            self.assertEqual(str(e.__cause__), "Inner error")

    @patch("flint.cli.Path.exists")
    def test_configuration_error_handling(self, mock_exists) -> None:
        """Test that configuration errors are handled properly."""
        mock_exists.return_value = False
        cmd = RunCommand(config_filepath=Path("nonexistent.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.CONFIGURATION_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_validation_error_handling(self, mock_exists, mock_job) -> None:
        """Test that validation errors are handled properly."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        mock_job.validate.side_effect = ValidationError("Validation failed")

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_unexpected_error_handling(self, mock_exists, mock_job) -> None:
        """Test that unexpected errors are handled properly."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        mock_job.validate.side_effect = RuntimeError("Something went wrong")

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.RUNTIME_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_successful_execution(self, mock_exists, mock_job) -> None:
        """Test that successful execution returns SUCCESS exit code."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        # No side effects for validate or execute means success

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.SUCCESS)

    @patch("flint.cli.Job")
    def test_validate_command_success(self, mock_exists, mock_job) -> None:
        """Test the ValidateCommand class for successful validation."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job

        cmd = ValidateCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.SUCCESS)

    @patch("flint.cli.Job")
    def test_validate_command_validation_error(self, mock_exists, mock_job) -> None:
        """Test the ValidateCommand class for validation errors."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        mock_job.validate.side_effect = ValidationError("Validation failed")

        cmd = ValidateCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR)


if __name__ == "__main__":
    unittest.main()
