"""


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
from unittest.mock import Mock, patch

from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

from stratum.load.load import LoadMethod, LoadModelPyspark, LoadPyspark


class MockLoadPyspark(LoadPyspark):
    def _load_batch(self) -> None:
        return None

    def _load_streaming(self) -> StreamingQueryPyspark:
        return Mock(spec=StreamingQueryPyspark)


class TestLoadPyspark(unittest.TestCase):
    @patch("stratum.engine.job.load.SparkHandler")
    def test_load_batch(self, _) -> None:
        # Create a mock model
        mock_model = Mock(spec=LoadModelPyspark)
        mock_model.method = LoadMethod.BATCH
        mock_model.name = "test_name"
        mock_model.options = {}
        mock_model.schema_location = "test_schema_location.json"

        # Instantiate the MockLoadPyspark class with the mock model
        load_instance = MockLoadPyspark(model=mock_model)

        # Use the 'with' statement to patch the _load_batch and _load_schema methods
        with (
            patch.object(MockLoadPyspark, "_load_batch") as mock_load_batch,
            patch.object(MockLoadPyspark, "_load_schema") as mock_load_schema,
        ):
            # Call the load method
            load_instance.load()

            # Assert that _load_batch was called once
            mock_load_batch.assert_called_once()

            # Assert that _load_schema was called once
            mock_load_schema.assert_called_once()

    @patch("stratum.engine.job.load.SparkHandler")
    def test_load_streaming(self, _) -> None:
        # Create a mock DataFrame
        mock_streaming_query = Mock(name="StreamingQueryPyspark")

        # Create a mock model
        mock_model = Mock(spec=LoadModelPyspark)
        mock_model.method = LoadMethod.STREAMING
        mock_model.name = "test_name"
        mock_model.options = {}
        mock_model.schema_location = "test_schema_location.json"

        # Instantiate the MockLoadPyspark class with the mock model
        load_instance = MockLoadPyspark(model=mock_model)

        # Use the 'with' statement to patch the _load_streaming method
        with (
            patch.object(MockLoadPyspark, "_load_streaming", return_value=mock_streaming_query),
            patch.object(MockLoadPyspark, "_load_schema") as mock_load_schema,
        ):
            # Call the load method
            load_instance.load()

            # Assert that the DataFrame was registered in the streaming_query_registry
            self.assertEqual(load_instance.data_registry["test_name"], mock_streaming_query)

            # Assert that _load_schema was called once
            mock_load_schema.assert_called_once()

    @patch("stratum.engine.job.load.SparkHandler")
    def test_load_invalid_method(self, _) -> None:
        # Create a mock model with an invalid loadion method
        mock_model = Mock(spec=LoadModelPyspark)
        mock_model.method = "INVALID_METHOD"  # Invalid method
        mock_model.name = "test_name"
        mock_model.options = {}
        mock_model.schema_location = "test_schema_location.json"

        # Instantiate the MockLoadPyspark class with the mock model
        load_instance = MockLoadPyspark(model=mock_model)

        # Assert that calling load raises a ValueError
        self.assertRaises(ValueError, load_instance.load)
