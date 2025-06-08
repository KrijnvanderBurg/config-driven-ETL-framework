"""
TODO

==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from abc import ABC
from typing import Generic

from pyspark.sql import DataFrame as DataFramePyspark

from stratum.extract.base import ExtractAbstract, ExtractModelFilePyspark, ExtractModelT, ExtractPyspark
from stratum.types import DataFrameT
from stratum.utils.spark_handler import SparkHandler


class ExtractFileAbstract(ExtractAbstract[ExtractModelT, DataFrameT], Generic[ExtractModelT, DataFrameT], ABC):
    """
    Abstract class for file extraction.
    """


class ExtractFilePyspark(ExtractFileAbstract[ExtractModelFilePyspark, DataFramePyspark], ExtractPyspark):
    """
    Concrete class for file extraction using PySpark DataFrame.
    """

    extract_model_concrete = ExtractModelFilePyspark

    def _extract_batch(self) -> DataFramePyspark:
        """
        Read from file in batch mode using PySpark.
        """
        return SparkHandler().session.read.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )

    def _extract_streaming(self) -> DataFramePyspark:
        """
        Read from file in streaming mode using PySpark.
        """
        return SparkHandler().session.readStream.load(
            path=self.model.location,
            format=self.model.data_format.value,
            schema=self.model.schema,
            **self.model.options,
        )
