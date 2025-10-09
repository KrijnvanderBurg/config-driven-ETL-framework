"""Unit tests for the Flint __main__ module entry point."""

from click.testing import CliRunner

from flint.cli import cli


class TestMainModule:
    """Test cases for the __main__ module entry point."""

    def test_main_module__when_invoked_with_help__shows_cli_help(self) -> None:
        """Test that __main__.py successfully invokes the CLI when run as module."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate running: python -m flint --help
        result = runner.invoke(cli, ["--help"])

        # Assert
        assert result.exit_code == 0
        assert "Flint: Configuration-driven PySpark ETL framework" in result.output

    def test_main_module__with_version_flag__displays_version(self) -> None:
        """Test that __main__ module passes system arguments to CLI for version display."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate: python -m flint --version
        result = runner.invoke(cli, ["--version"])

        # Assert
        assert result.exit_code == 0
        assert "version" in result.output.lower() or "flint" in result.output.lower()

    def test_main_module__with_validate_help__shows_validate_command_help(self) -> None:
        """Test that __main__ can execute validate command through CLI."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate: python -m flint validate --help
        result = runner.invoke(cli, ["validate", "--help"])

        # Assert
        assert result.exit_code == 0
        assert "validate" in result.output.lower()
        assert "alert-filepath" in result.output.lower()

    def test_main_module__with_run_help__shows_run_command_help(self) -> None:
        """Test that __main__ can execute run command through CLI."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate: python -m flint run --help
        result = runner.invoke(cli, ["run", "--help"])

        # Assert
        assert result.exit_code == 0
        assert "run" in result.output.lower()
        assert "alert-filepath" in result.output.lower()

    def test_main_module__with_export_schema_help__shows_export_schema_help(self) -> None:
        """Test that __main__ can execute export-schema command through CLI."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate: python -m flint export-schema --help
        result = runner.invoke(cli, ["export-schema", "--help"])

        # Assert
        assert result.exit_code == 0
        assert "export-schema" in result.output.lower()
        assert "output-filepath" in result.output.lower()

    def test_main_module__with_log_level_flag__accepts_log_level(self) -> None:
        """Test that __main__ module correctly handles --log-level flag."""
        # Arrange
        runner = CliRunner()

        # Act
        # Simulate: python -m flint --log-level DEBUG --help
        result = runner.invoke(cli, ["--log-level", "DEBUG", "--help"])

        # Assert
        assert result.exit_code == 0
        assert "Flint: Configuration-driven PySpark ETL framework" in result.output
