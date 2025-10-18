"""Unit tests for CLI logging initialization order."""

import sys
from unittest.mock import patch

from click.testing import CliRunner


class TestCliLoggingOrder:
    """Test cases for ensuring logging is set before Pydantic models are initialized."""

    def test_cli__sets_logging_before_importing_runtime_controller__when_log_level_specified(self) -> None:
        """Test that logging level is set before RuntimeController is imported."""
        # Arrange
        runner = CliRunner()
        
        # Track the order of calls
        call_order = []
        
        def mock_set_logger(level=None):
            call_order.append(('set_logger', level))
        
        original_import = __import__
        def mock_import(name, *args, **kwargs):
            if name == 'samara.runtime.controller':
                call_order.append(('import_runtime_controller', None))
            return original_import(name, *args, **kwargs)
        
        # Act
        with patch('samara.cli.set_logger', side_effect=mock_set_logger):
            with patch('builtins.__import__', side_effect=mock_import):
                # Mock the RuntimeController to avoid actual execution
                with patch('samara.runtime.controller.RuntimeController') as mock_runtime:
                    mock_runtime.from_file.return_value = None
                    with patch('samara.alert.AlertController') as mock_alert:
                        mock_alert.from_file.return_value = None
                        
                        result = runner.invoke(
                            __import__('samara.cli').cli.cli,
                            ['--log-level', 'WARNING', 'validate', 
                             '--alert-filepath', '/test/alert.json',
                             '--runtime-filepath', '/test/runtime.json']
                        )
        
        # Assert
        # set_logger should be called before import_runtime_controller
        assert len(call_order) >= 2
        set_logger_index = next((i for i, (call, _) in enumerate(call_order) if call == 'set_logger'), -1)
        import_index = next((i for i, (call, _) in enumerate(call_order) if call == 'import_runtime_controller'), -1)
        
        assert set_logger_index >= 0, "set_logger was not called"
        assert import_index >= 0, "RuntimeController was not imported"
        assert set_logger_index < import_index, "set_logger must be called before RuntimeController import"
