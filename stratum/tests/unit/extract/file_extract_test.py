"""
File extract class tests.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from typing import Any
from unittest.mock import patch

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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
)
from stratum.extract.file_extract import ExtractFilePyspark


class TestExtractFilePyspark:
    """TODO"""

    @pytest.fixture
    def schema(self) -> StructType:
        return StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("job_title", StringType(), True),
            ]
        )

    def test__from_confeti(self, schema: StructType) -> None:
        """
        Assert that all ExtractModelPyspark attributes are of correct type.
        """
        # Arrange

        model: ExtractModelFilePyspark = ExtractModelFilePyspark(
            name="test",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.CSV,
            location=f"path/extract.{ExtractFormat.CSV.value}",
            options={"spark.executor.memory": "2g"},
            schema=schema,
        )

        confeti: dict[str, Any] = {
            NAME: model.name,
            METHOD: model.method.value,
            DATA_FORMAT: model.data_format.value,
            LOCATION: model.location,
            OPTIONS: model.options,
            SCHEMA: schema.json(),
        }

        # Act
        extract_model = ExtractFilePyspark.from_confeti(confeti=confeti)

        # Assert
        assert extract_model.model.__dict__ == model.__dict__

    def test__extract_called__batch(self, schema: StructType) -> None:
        """
        Assert that protected methods are called for the remodeltive method.

        """
        # Arrange
        model = ExtractModelFilePyspark(
            name="test",
            method=ExtractMethod.BATCH,
            data_format=ExtractFormat.JSON,
            location=f"path/extract.{ExtractFormat.JSON.value}",
            options={},
            schema=schema,
        )
        extract = ExtractFilePyspark(model=model)

        with (
            patch.object(extract, "_extract_batch") as extract_batch_mock,
            patch.object(extract, "_extract_streaming") as extract_streaming_mock,
        ):
            # Act
            extract.extract()

            # Assert
            if extract.model.method == ExtractMethod.BATCH:
                extract_batch_mock.assert_called_once()
                extract_streaming_mock.assert_not_called()

    def test__extract_called__streaming(self, schema: StructType) -> None:
        """
        Assert that protected methods are called for the remodeltive method.

        """
        # Arrange
        model = ExtractModelFilePyspark(
            name="test",
            method=ExtractMethod.STREAMING,
            data_format=ExtractFormat.JSON,
            location=f"path/extract.{ExtractFormat.JSON.value}",
            options={},
            schema=schema,
        )
        extract = ExtractFilePyspark(model=model)

        with (
            patch.object(extract, "_extract_batch") as extract_batch_mock,
            patch.object(extract, "_extract_streaming") as extract_streaming_mock,
        ):
            # Act
            extract.extract()

            # Assert
            if extract.model.method == ExtractMethod.BATCH:
                extract_streaming_mock.assert_called_once()
                extract_batch_mock.assert_not_called()

    # YOU WERE HERE

    # @patch("stratum.utils.spark_handler.SparkHandler")
    # def test__extract_called_spark_load__batch(
    #     self,
    #     extract_file_batch_pyspark: ExtractFilePyspark,
    # ) -> None:
    #     """
    #     Test that _extract_batch method calls spark read load.

    #     Args:
    #         spark_session_mock (tuple): Mock builder and session objects.
    #         extract_file_batch_pyspark (ExtractFilePyspark): ExtractFilePyspark fixture.
    #     """
    #     # Arrange
    #     _, mock_session = spark_session_mock
    #     SparkHandler.get_or_create(session=mock_session)

    #     # Act
    #     with patch.object(SparkHandler.session.read, "load") as load_mock:
    #         extract_file_batch_pyspark.extract()

    #         # Assert
    #         load_mock.assert_called_once_with(
    #             path=extract_file_batch_pyspark.location,
    #             format=extract_file_batch_pyspark.data_format.value,
    #             schema=extract_file_batch_pyspark.schema,
    #         )


#     def test__extract_called_spark_load__streaming(
#         self,
#         spark_session_mock: tuple[MagicMock, MagicMock],
#         extract_file_streaming_pyspark: ExtractFilePyspark,
#     ) -> None:
#         """
#         Test that _extract_batch method calls spark read load.

#         Args:
#             spark_session_mock (tuple): Mock builder and session objects.
#             extract_file_streaming_pyspark (ExtractFilePyspark): ExtractFilePyspark fixture.
#         """
#         # Arrange
#         _, mock_session = spark_session_mock
#         SparkHandler.get_or_create(session=mock_session)

#         # Act
#         with patch.object(SparkHandler.session.readStream, "load") as load_mock:
#             extract_file_streaming_pyspark.extract()

#             # Assert
#             load_mock.assert_called_once_with(
#                 path=extract_file_streaming_pyspark.location,
#                 format=extract_file_streaming_pyspark.data_format.value,
#                 schema=extract_file_streaming_pyspark.schema,
#             )

#     # def test__extract_df_equals(spark: SparkSession, df: DataFrame, extract_file_matrix: ExtractFile) -> None:
#     #     """
#     #     Assert that extract method writes the DataFrame without any modifications.

#     #     Args:
#     #         spark (SparkSession): SparkSession fixture.
#     #         df (DataFrame): Test DataFrame fixture.
#     #         extract_file_matrix (ExtractFile): Matrix ExtractFile fixture.

#     #     Returns: None
#     #     """
#     #     # Arrange
#     #     SparkHandler.get_or_create()
#     #     df.write.format(extract_file_matrix.model.data_format.value).save(extract_file_matrix.model.location)

#     #     # Act
#     #     extract_df = extract_file_matrix.extract()

#     #     if extract_file_matrix.model.method == ExtractMethod.STREAMING:
#     #         query = extract_df.writeStream.outputMode("append").format("memory").queryName("memory_table").start()
#     #         query.awaitTermination(timeout=3)
#     #         query.stop()
#     #         extract_df = spark.sql("SELECT * FROM memory_table")

#     #     # Assert
#     #     testing.assertDataFrameEqual(actual=extract_df, expected=df)
