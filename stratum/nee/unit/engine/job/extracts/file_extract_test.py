# """
# File extract class tests.


# ==============================================================================
# Copyright Krijn van der Burg. All rights reserved.

# This software is proprietary and confidential. No reproduction, distribution,
# or transmission is allowed without prior written permission. Unauthorized use,
# disclosure, or distribution is strictly prohibited.

# For inquiries and permission requests, contact Krijn van der Burg at
# krijnvdburg@protonmail.com.
# ==============================================================================
# """

# from unittest.mock import MagicMock, patch

# import pytest
# from pyspark.sql.types import StructType

# from stratum.engine.job.extracts.file_extract import ExtractFilePyspark
# from stratum.models.job.extract_model import ExtractFormat, ExtractMethod, ExtractModelPyspark
# from stratum.utils.spark_handler import SparkHandler

# # Fixtures


# @pytest.fixture(name="extract_file_batch_pyspark")
# def fixture__extract_file__batch__pyspark(
#     schema_pyspark: StructType, options_model_pyspark: OptionsModelPyspark
# ) -> ExtractFilePyspark:
#     """
#     Fixture for creating ExtractModel from valid value.

#     Args:
#         schema_pyspark (StructType): schema pyspark fixture.
#         options_model_pyspark (OptionsModelPyspark): options model pyspark fixture.

#     Returns:
#         ExtractFilePyspark: ExtractFilePyspark fixture.
#     """

#     with (
#         patch.object(ExtractModelPyspark, "schema_factory", return_value=schema_pyspark),
#     ):
#         return ExtractFilePyspark(
#             name="fixture_extract_file_pyspark",
#             method=ExtractMethod.BATCH.value,
#             data_format=ExtractFormat.JSON.value,
#             location=f"path/extract.{ExtractFormat.JSON.value}",
#             options=options_model_pyspark,
#             schema=schema_pyspark.json(),
#         )


# @pytest.fixture(name="extract_file_streaming_pyspark")
# def fixture__extract_file__streaming__pyspark(
#     schema_pyspark: StructType, options_model_pyspark: OptionsModelPyspark
# ) -> ExtractFilePyspark:
#     """
#     Fixture for creating ExtractModel from valid value.

#     Args:
#         schema_pyspark (StructType): schema pyspark fixture.
#         options_model_pyspark (OptionsModelPyspark): options model pyspark fixture.

#     Returns:
#         ExtractFilePyspark: ExtractFilePyspark fixture.
#     """
#     with (
#         patch.object(ExtractModelPyspark, "schema_factory", return_value=schema_pyspark),
#     ):
#         return ExtractFilePyspark(
#             name="fixture_extract_file_pyspark",
#             method=ExtractMethod.STREAMING.value,
#             data_format=ExtractFormat.JSON.value,
#             location=f"path/extract.{ExtractFormat.JSON.value}",
#             options=options_model_pyspark,
#             schema=schema_pyspark.json(),
#         )


# # Tests


# class TestExtractFilePyspark:
#     """TODO"""

#     def test__extract_called__batch(self, extract_file_batch_pyspark: ExtractFilePyspark) -> None:
#         """
#         Assert that protected methods are called for the remodeltive method.

#         Args:
#             extract_file_batch_pyspark (ExtractFilePyspark): ExtractFilePyspark fixture.
#         """
#         with (
#             patch.object(extract_file_batch_pyspark, "_extract_batch") as extract_batch_mock,
#             patch.object(extract_file_batch_pyspark, "_extract_streaming") as extract_streaming_mock,
#         ):
#             # Act
#             extract_file_batch_pyspark.extract()

#             # Assert
#             if extract_file_batch_pyspark.method == ExtractMethod.BATCH:
#                 extract_batch_mock.assert_called_once()
#                 extract_streaming_mock.assert_not_called()

#     def test__extract_called__streaming(self, extract_file_streaming_pyspark: ExtractFilePyspark) -> None:
#         """
#         Assert that protected methods are called for the remodeltive method.

#         Args:
#             extract_file_streaming_pyspark (ExtractFilePyspark): ExtractFilePyspark fixture.
#         """
#         with (
#             patch.object(extract_file_streaming_pyspark, "_extract_batch") as extract_batch_mock,
#             patch.object(extract_file_streaming_pyspark, "_extract_streaming") as extract_streaming_mock,
#         ):
#             # Act
#             extract_file_streaming_pyspark.extract()

#             # Assert
#             if extract_file_streaming_pyspark.method == ExtractMethod.BATCH:
#                 extract_batch_mock.assert_not_called()
#                 extract_streaming_mock.assert_called_once()

#     def test__extract_called_spark_load__batch(
#         self,
#         spark_session_mock: tuple[MagicMock, MagicMock],
#         extract_file_batch_pyspark: ExtractFilePyspark,
#     ) -> None:
#         """
#         Test that _extract_batch method calls spark read load.

#         Args:
#             spark_session_mock (tuple): Mock builder and session objects.
#             extract_file_batch_pyspark (ExtractFilePyspark): ExtractFilePyspark fixture.
#         """
#         # Arrange
#         _, mock_session = spark_session_mock
#         SparkHandler.get_or_create(session=mock_session)

#         # Act
#         with patch.object(SparkHandler.session.read, "load") as load_mock:
#             extract_file_batch_pyspark.extract()

#             # Assert
#             load_mock.assert_called_once_with(
#                 path=extract_file_batch_pyspark.location,
#                 format=extract_file_batch_pyspark.data_format.value,
#                 schema=extract_file_batch_pyspark.schema,
#             )

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
