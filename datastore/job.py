"""
Job

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.extract.base import ExtractSpec
from datastore.extract.factory import ExtractFactory
from datastore.load.base import LoadSpec
from datastore.load.factory import LoadFactory
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


class Job:
    """
    Job class to perform data extraction, transformations and loading (ETL).
    """

    def __init__(self, confeti: dict):
        """
        Initialize Job instance.

        Args:
            config (dict): Confeti dictionary containing 'extract' and 'load' specifications.
        """
        self.extract_spec: ExtractSpec = ExtractSpec.from_confeti(confeti=confeti["extract"])
        self.load_spec: LoadSpec = LoadSpec.from_confeti(confeti=confeti["load"])

    def execute(self) -> DataFrame:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.

        Returns:
            DataFrame: Extracted data.
        """
        df = self._extract()
        self._load(dataframe=df)

        return df

    def _extract(self) -> DataFrame:
        """
        Extract data from specification into a DataFrame.

        Returns:
            DataFrame: Extracted data.
        """
        return ExtractFactory.get(self.extract_spec).extract()

    def _load(self, dataframe: DataFrame) -> StreamingQuery | None:
        """
        Load data to the specification.

        Args:
            data (DataFrame): DataFrame to be loaded.

        Returns:
            DataFrame: The loaded data.
        """
        return LoadFactory.get(self.load_spec, dataframe).load()
