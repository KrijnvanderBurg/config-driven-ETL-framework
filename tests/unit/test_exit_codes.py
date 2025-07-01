"""Unit tests for exit code handling.

Tests the exit code enum and exception handling in         cmd = RunCommand(config_filepath=Path("test.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR) commands.
"""

import unittest
from pathlib import Path
from unittest.mock import patch

from flint.cli import RunCommand, ValidateCommand
from flint.exceptions import (
    ConfigurationError,
    ConfigurationKeyError,
    ExtractError,
    FlintException,
    LoadError,
    TransformError,
    ValidationError,
)
from flint.types import ExitCode


class TestExitCodes(unittest.TestCase):
    """Test cases for exit code handling."""

    def test_exit_code_enum_values(self):
        """Test that the exit code enum values are as expected."""
        self.assertEqual(ExitCode.SUCCESS, 0)
        self.assertEqual(ExitCode.GENERAL_ERROR, 1)
        self.assertEqual(ExitCode.INVALID_ARGUMENTS, 2)
        self.assertEqual(ExitCode.CONFIGURATION_ERROR, 3)
        self.assertEqual(ExitCode.VALIDATION_ERROR, 4)
        self.assertEqual(ExitCode.EXTRACT_ERROR, 10)
        self.assertEqual(ExitCode.TRANSFORM_ERROR, 11)
        self.assertEqual(ExitCode.LOAD_ERROR, 12)

    def test_flint_exception_exit_codes(self):
        """Test that FlintException and derived exceptions have the correct exit codes."""
        self.assertEqual(FlintException("test", ExitCode.GENERAL_ERROR).exit_code, ExitCode.GENERAL_ERROR)
        self.assertEqual(ConfigurationError("test").exit_code, ExitCode.CONFIGURATION_ERROR)
        self.assertEqual(ConfigurationKeyError("key", {"other_key": "value"}).exit_code, ExitCode.CONFIGURATION_ERROR)
        self.assertEqual(ValidationError("test").exit_code, ExitCode.VALIDATION_ERROR)
        self.assertEqual(ExtractError("test").exit_code, ExitCode.EXTRACT_ERROR)
        self.assertEqual(TransformError("test").exit_code, ExitCode.TRANSFORM_ERROR)
        self.assertEqual(LoadError("test").exit_code, ExitCode.LOAD_ERROR)

    def test_configuration_key_error(self):
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

    def test_exception_chaining(self):
        """Test that exception chaining works properly."""
        try:
            try:
                raise ValueError("Inner error")
            except ValueError as e:
                raise FlintException("Outer error", ExitCode.GENERAL_ERROR) from e
        except FlintException as e:
            self.assertEqual(str(e), "Outer error")
            self.assertIsNotNone(e.__cause__)
            self.assertEqual(str(e.__cause__), "Inner error")

    @patch("flint.cli.Path.exists")
    def test_configuration_error_handling(self, mock_exists):
        """Test that configuration errors are handled properly."""
        mock_exists.return_value = False
        cmd = RunCommand(config_filepath=Path("nonexistent.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.CONFIGURATION_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_validation_error_handling(self, mock_exists, mock_job):
        """Test that validation errors are handled properly."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        mock_job.validate.side_effect = ValidationError("Validation failed")

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_unexpected_error_handling(self, mock_exists, mock_job):
        """Test that unexpected errors are handled properly."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        mock_job.validate.side_effect = RuntimeError("Something went wrong")

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.GENERAL_ERROR)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_successful_execution(self, mock_exists, mock_job):
        """Test that successful execution returns SUCCESS exit code."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job
        # No side effects for validate or execute means success

        cmd = RunCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.SUCCESS)

    @patch("flint.cli.Job")
    @patch("flint.cli.Path.exists")
    def test_validate_command(self, mock_exists, mock_job):
        """Test the ValidateCommand class."""
        mock_exists.return_value = True
        mock_job.from_file.return_value = mock_job

        cmd = ValidateCommand(config_filepath=Path("config.json"))
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.SUCCESS)

        # Test validation error
        mock_job.validate.side_effect = ValidationError("Validation failed")
        exit_code = cmd.execute()
        self.assertEqual(exit_code, ExitCode.VALIDATION_ERROR)


if __name__ == "__main__":
    unittest.main()
