from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession

from samara.workflow.jobs.spark.session import SparkHandler


@pytest.fixture(name="spark_handler_obj")
def fixture_spark_handler_obj() -> SparkHandler:
    """Programmatic SparkHandler fixture."""
    # Simple construction using default args; tests should mock internals
    return SparkHandler(app_name="pytest_samara", options={})


@pytest.fixture(name="spark_handler_dict")
def fixture_spark_handler_dict() -> SparkHandler:
    """Construct SparkHandler from a dictionary of parameters.

    Although SparkHandler is not a Pydantic model, it accepts constructor
    kwargs and this fixture demonstrates creating it from a mapping.
    """
    params = {"app_name": "dict_samara", "options": {}}
    return SparkHandler(**params)


@pytest.fixture(name="reset_singleton", autouse=True)
def fixture_reset_singleton() -> None:
    """Reset the singleton instance before each test."""
    SparkHandler._instances.clear()  # type: ignore


class TestSparkHandler:
    """Unit tests for the SparkHandler singleton and session management."""

    @patch.object(SparkSession, "Builder")
    def test_init_default(self, mock_builder: Mock) -> None:
        """Test default initialization of SparkHandler."""
        spark_handler = SparkHandler()

        # Builder should not be called during __init__ (lazy initialization)
        mock_builder.assert_not_called()

        # Access session property to trigger lazy initialization
        _ = spark_handler.session

        mock_builder.assert_called_once()
        mock_builder.return_value.appName.assert_called_once_with(name="samara")
        mock_builder.return_value.appName().config.assert_not_called()

    @patch.object(SparkSession, "Builder")
    def test_init_custom(self, mock_builder: Mock) -> None:
        """Test custom initialization of SparkHandler."""
        spark_handler = SparkHandler(app_name="test_app", options={"spark.executor.memory": "1g"})

        # Builder should not be called during __init__ (lazy initialization)
        mock_builder.assert_not_called()

        # Access session property to trigger lazy initialization
        _ = spark_handler.session

        mock_builder.assert_called_once()
        mock_builder.return_value.appName.assert_called_once_with(name="test_app")
        mock_builder.return_value.appName().config.assert_called_once_with(key="spark.executor.memory", value="1g")

    @patch.object(SparkSession, "Builder")
    def test_session_getter(self, mock_builder: Mock) -> None:
        """Test getting session property."""
        spark_handler = SparkHandler()

        # Mock the builder chain to return a mock session
        mock_session = Mock(spec=SparkSession)
        # Since no options are provided, the chain is: Builder().appName().getOrCreate()
        mock_builder.return_value.appName.return_value.getOrCreate.return_value = mock_session

        # Access session property to trigger lazy initialization
        session = spark_handler.session

        # Session should be created and cached
        assert session == mock_session
        assert spark_handler._session == mock_session

    @patch("pyspark.sql.SparkSession")
    def test_session_deleter(self, mock_session: Mock) -> None:
        """Test session deletion."""
        spark_handler = SparkHandler()

        spark_handler._session = mock_session.return_value  # type: ignore
        del spark_handler.session

        mock_session.return_value.stop.assert_called_once()

    @patch("pyspark.sql.SparkSession")
    def test_add_configs(self, mock_session: Mock) -> None:
        """Test adding configurations."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        configs = {"spark.executor.memory": "2g", "spark.executor.cores": "4"}
        spark_handler.add_configs(configs)

        for key, value in configs.items():
            mock_session.return_value.conf.set.assert_any_call(key=key, value=value)

    @patch("pyspark.sql.SparkSession")
    def test_scoped_configs__restores_previous_values(self, mock_session: Mock) -> None:
        """Test that scoped_configs restores original values after exiting the context."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        mock_session.return_value.conf.get.return_value = "original_value"

        with spark_handler.scoped_configs({"spark.sql.shuffle.partitions": "10"}):
            mock_session.return_value.conf.set.assert_called_with(key="spark.sql.shuffle.partitions", value="10")

        # After exiting, the original value should be restored
        mock_session.return_value.conf.set.assert_called_with(
            key="spark.sql.shuffle.partitions", value="original_value"
        )

    @patch("pyspark.sql.SparkSession")
    def test_scoped_configs__unsets_previously_absent_keys(self, mock_session: Mock) -> None:
        """Test that scoped_configs unsets keys that didn't exist before."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        # Simulate key not existing (conf.get returns None when default=None)
        mock_session.return_value.conf.get.return_value = None

        with spark_handler.scoped_configs({"spark.custom.new.key": "temp_value"}):
            mock_session.return_value.conf.set.assert_called_with(key="spark.custom.new.key", value="temp_value")

        mock_session.return_value.conf.unset.assert_called_with("spark.custom.new.key")

    @patch("pyspark.sql.SparkSession")
    def test_scoped_configs__restores_on_exception(self, mock_session: Mock) -> None:
        """Test that scoped_configs restores configs even when an exception is raised."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        mock_session.return_value.conf.get.return_value = "200"

        with pytest.raises(RuntimeError, match="stage failed"):
            with spark_handler.scoped_configs({"spark.sql.shuffle.partitions": "10"}):
                raise RuntimeError("stage failed")

        # Configs should still be restored despite the exception
        mock_session.return_value.conf.set.assert_called_with(key="spark.sql.shuffle.partitions", value="200")

    @patch("pyspark.sql.SparkSession")
    def test_scoped_configs__with_empty_options__is_noop(self, mock_session: Mock) -> None:
        """Test that scoped_configs with empty options does nothing."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        with spark_handler.scoped_configs({}):
            pass

        mock_session.return_value.conf.get.assert_not_called()
        mock_session.return_value.conf.set.assert_not_called()

    @patch("pyspark.sql.SparkSession")
    def test_scoped_configs__restores_multiple_keys(self, mock_session: Mock) -> None:
        """Test that scoped_configs correctly restores multiple keys."""
        spark_handler = SparkHandler()
        spark_handler._session = mock_session.return_value  # type: ignore

        # First key exists, second key does not
        def get_side_effect(key: str, default: str | None = None) -> str | None:
            if key == "spark.sql.shuffle.partitions":
                return "200"
            return default

        mock_session.return_value.conf.get.side_effect = get_side_effect

        options = {
            "spark.sql.shuffle.partitions": "10",
            "spark.custom.new.key": "temp",
        }

        with spark_handler.scoped_configs(options):
            pass

        # Existing key should be restored
        mock_session.return_value.conf.set.assert_any_call(key="spark.sql.shuffle.partitions", value="200")
        # Non-existing key should be unset
        mock_session.return_value.conf.unset.assert_called_with("spark.custom.new.key")
