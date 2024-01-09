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
from datastore.transform.base import TransformSpec
from datastore.transform.factory import TransformFactory
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


class Job:
    """
    Job class to perform data extraction, transformations and loading (ETL).
    """

    def __init__(self, extract_spec: ExtractSpec, transform_spec: TransformSpec, load_spec: LoadSpec):
        """
        Initialize Job instance.

        Args:
            config (dict): Confeti dictionary containing 'extract' and 'load' specifications.
        """
        self.extract_spec = extract_spec
        self.transform_spec = transform_spec
        self.load_spec = load_spec

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the job specifications from confeti.

        Returns:
            Job: job instance.
        """
        extract_spec: ExtractSpec = ExtractSpec.from_confeti(confeti=confeti["extract"])
        transform_spec: TransformSpec = TransformSpec.from_confeti(confeti=confeti["transform"])
        load_spec: LoadSpec = LoadSpec.from_confeti(confeti=confeti["load"])

        return cls(extract_spec=extract_spec, transform_spec=transform_spec, load_spec=load_spec)

    def execute(self) -> StreamingQuery | None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.

        Returns:
            DataFrame: Extracted data.
        """
        extract_df = self._extract()
        transform_df = self._transform(dataframe=extract_df)
        load = self._load(dataframe=transform_df)

        return load

    def _extract(self) -> DataFrame:
        """
        Extract data from specification into a DataFrame.

        Returns:
            DataFrame: Extracted data.
        """
        return ExtractFactory.get(self.extract_spec).extract()

    def _transform(self, dataframe: DataFrame) -> DataFrame:
        """
        Transform data from specifiction.

        Args:
            dataframe (DataFrame): Dataframe to be transformed.

        Returns:
            DataFrame: transformed data.
        """

        if not self.transform_spec:
            return dataframe

        for transform in self.transform_spec.transforms:
            f = TransformFactory.get(transform=transform)
            dataframe = dataframe.transform(f)

        return dataframe

    def _load(self, dataframe: DataFrame) -> StreamingQuery | None:
        """
        Load data to the specification.

        Args:
            dataframe (DataFrame): DataFrame to be loaded.

        Returns:
            DataFrame: The loaded data.
        """
        return LoadFactory.get(self.load_spec, dataframe).load()
