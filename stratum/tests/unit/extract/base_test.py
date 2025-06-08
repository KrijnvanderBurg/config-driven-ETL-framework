"""
Extract class tests.

==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import unittest
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.dataframe import DataFrame as DataFramePyspark
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from stratum.exceptions import DictKeyError
from stratum.extract.base import (
    DATA_FORMAT,
    LOCATION,
    METHOD,
    NAME,
    OPTIONS,
    SCHEMA,
    ExtractFormat,
    ExtractMethod,
    ExtractModelFilePyspark,
    ExtractModelPyspark,
    ExtractPyspark,
)
from tests.conftest import enum_tripwire


class TestExtractMethod:
    """
    Test class for ExtractMethod enum.
    """

    def test__enum_tripwire(self) -> None:
        self.__doc__ = enum_tripwire.__doc__

        values = ["batch", "streaming"]
        enum_tripwire(enum_class=ExtractMethod, tested_values=values)


class TestExtractFormat:
    """
    Test class for ExtractMethod enum.
    """

    def test__enum_tripwire(self) -> None:
        self.__doc__ = enum_tripwire.__doc__

        values = ["parquet", "json", "csv"]
        enum_tripwire(enum_class=ExtractFormat, tested_values=values)


class TestExtractModelPyspark: ...


class TestExtractModelPysparkFile:
    """
    Test class for ExtractModel class.
    """

    @pytest.fixture
    def schema(self) -> StructType:
        return StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("job_title", StringType(), True),
            ]
        )

    @pytest.fixture
    def confeti(self, schema: StructType) -> dict[str, Any]:
        # with patch.object(SchemaHandlerPyspark, "schema_factory", return_value=schema):
        return {
            NAME: "test",
            METHOD: ExtractMethod.BATCH.value,
            DATA_FORMAT: ExtractFormat.CSV.value,
            LOCATION: f"path/extract.{ExtractFormat.CSV.value}",
            OPTIONS: {"spark.executor.memory": "2g"},
            SCHEMA: schema.json(),
        }

    def test__from_confeti(self, confeti: dict[str, Any], schema: StructType) -> None:
        """
        Assert that all ExtractModelPyspark attributes are of correct type.
        """
        # Act
        extract_model = ExtractModelFilePyspark.from_confeti(confeti=confeti)

        # Assert
        assert extract_model.name == "test"
        assert extract_model.method == ExtractMethod.BATCH
        assert extract_model.data_format == ExtractFormat.CSV
        assert extract_model.location == f"path/extract.{ExtractFormat.CSV.value}"
        assert extract_model.options == {"spark.executor.memory": "2g"}
        assert extract_model.schema == schema

    @pytest.mark.parametrize("missing_key", [NAME, METHOD, DATA_FORMAT, LOCATION, OPTIONS, SCHEMA])
    def test__from_confeti__missing_keys(self, confeti: dict[str, Any], missing_key: str) -> None:
        """
        Assert that DictKeyError is raised for missing keys.
        """
        del confeti[missing_key]

        with pytest.raises(DictKeyError):
            ExtractModelFilePyspark.from_confeti(confeti=confeti)


class TestExtractPyspark(unittest.TestCase):
    class MockExtractPyspark(ExtractPyspark):
        def _extract_batch(self) -> DataFramePyspark:
            return Mock(spec=DataFramePyspark)

        def _extract_streaming(self) -> DataFramePyspark:
            return Mock(spec=DataFramePyspark)

    @patch("stratum.utils.spark_handler.SparkHandler")
    def test_extract_batch(self, _) -> None:
        # Create a mock DataFrame
        mock_dataframe = Mock(name="DataFramePyspark")

        # Create a mock model
        mock_model = Mock(spec=ExtractModelPyspark)
        mock_model.method = ExtractMethod.BATCH
        mock_model.name = "test_name"
        mock_model.options = {}

        # Instantiate the MockExtractPyspark class with the mock model
        extract_instance = self.MockExtractPyspark(model=mock_model)

        # Use the 'with' statement to patch the _extract_batch method
        with patch.object(self.MockExtractPyspark, "_extract_batch", return_value=mock_dataframe):
            # Call the extract method
            extract_instance.extract()

            # Assert that the DataFrame was registered in the dataframe_registry
            self.assertEqual(extract_instance.data_registry["test_name"], mock_dataframe)

    @patch("stratum.utils.spark_handler.SparkHandler")
    def test_extract_streaming(self, _) -> None:
        # Create a mock DataFrame
        mock_dataframe = Mock(name="DataFramePyspark")

        # Create a mock model
        mock_model = Mock(spec=ExtractModelPyspark)
        mock_model.method = ExtractMethod.STREAMING
        mock_model.name = "test_name"
        mock_model.options = {}

        # Instantiate the MockExtractPyspark class with the mock model
        extract_instance = self.MockExtractPyspark(model=mock_model)

        # Use the 'with' statement to patch the _extract_streaming method
        with patch.object(self.MockExtractPyspark, "_extract_streaming", return_value=mock_dataframe):
            # Call the extract method
            extract_instance.extract()

            # Assert that the DataFrame was registered in the dataframe_registry
            self.assertEqual(extract_instance.data_registry["test_name"], mock_dataframe)

    @patch("stratum.utils.spark_handler.SparkHandler")
    def test_extract_invalid_method(self, _) -> None:
        # Create a mock model with an invalid extraction method
        mock_model = Mock(spec=ExtractModelPyspark)
        mock_model.method = "INVALID_METHOD"  # Invalid method
        mock_model.name = "test_name"
        mock_model.options = {}

        # Instantiate the MockExtractPyspark class with the mock model
        extract_instance = self.MockExtractPyspark(model=mock_model)

        # Assert that calling extract raises a ValueError
        self.assertRaises(ValueError, extract_instance.extract)
