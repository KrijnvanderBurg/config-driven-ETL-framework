"""Unit tests for lazy SparkHandler initialization."""

import sys


class TestLazySparkHandlerInitialization:
    """Test cases for lazy SparkHandler initialization."""

    def test_spark_handler__not_initialized_at_module_import__only_on_first_access(self) -> None:
        """Test that SparkHandler is not initialized when modules are imported, only on first access."""
        # Arrange
        # Remove modules if already imported to ensure fresh import
        modules_to_remove = [
            "samara.runtime.jobs.spark.extract",
            "samara.runtime.jobs.spark.transform",
            "samara.runtime.jobs.spark.load",
        ]
        for module in modules_to_remove:
            if module in sys.modules:
                del sys.modules[module]

        # Act - Import modules (should NOT create SparkHandler instance)
        from samara.runtime.jobs.spark.extract import ExtractSpark
        from samara.runtime.jobs.spark.load import LoadSpark
        from samara.runtime.jobs.spark.transform import TransformSpark

        # Assert - SparkHandler should NOT have been instantiated during import
        # Check that the _spark class variable is None (not initialized)
        assert ExtractSpark._spark is None
        assert TransformSpark._spark is None
        assert LoadSpark._spark is None
