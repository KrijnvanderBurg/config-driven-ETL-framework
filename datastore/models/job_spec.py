"""
Job class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC
from pathlib import Path

from datastore.models.job.extract_spec import ExtractSpecAbstract, ExtractSpecPyspark
from datastore.models.job.load_spec import LoadSpecAbstract, LoadSpecPyspark
from datastore.models.job.strategy_spec import StrategyEngine, StrategySpecAbstract, StrategySpecPyspark
from datastore.models.job.transform_spec import TransformSpecAbstract, TransformSpecPyspark
from datastore.utils.file_handler import FileHandler, FileHandlerContext
from datastore.utils.json_handler import JsonHandler


class JobSpecAbstract(ABC):
    """
    Job class to perform data extraction, transformations and loading (ETL).
    """

    strategy_concrete = StrategySpecAbstract
    extract_concrete = ExtractSpecAbstract
    transform_concrete = TransformSpecAbstract
    load_concrete = LoadSpecAbstract

    confeti_schema: dict = {
        "type": "object",
        "properties": {
            "strategy": {"type": "object"},
            "extract": {"type": "object"},
            "transform": {"type": "object"},
            "load": {"type": "object"},
        },
        "required": ["strategy", "extract", "transform", "load"],
    }

    def __init__(
        self,
        strategy: StrategySpecAbstract,
        extract: ExtractSpecAbstract,
        transform: TransformSpecAbstract,
        load: LoadSpecAbstract,
    ) -> None:
        """
        Initialize Job instance.

        Args:
            strategy (StrategySpecAbstract): Strategy specification.
            extract (ExtractSpecAbstract): Extract specification.
            transform (TransformSpecAbstract): Transform specification.
            load (LoadSpecAbstract): Load specification.
        """
        self.strategy = strategy
        self.extract = extract
        self.transform = transform
        self.load = load

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Get the job specifications from confeti.

        Args:
            confeti (dict): dictionary object.

        Returns:
            Job: job instance.
        """
        JsonHandler.validate_json(json=confeti, schema=cls.confeti_schema)

        strategy = cls.strategy_concrete.from_confeti(confeti=confeti["strategy"])
        extract = cls.extract_concrete.from_confeti(confeti=confeti["extract"])
        transform = cls.transform_concrete.from_confeti(confeti=confeti["transform"])
        load = cls.load_concrete.from_confeti(confeti=confeti["load"])

        return cls(strategy=strategy, extract=extract, transform=transform, load=load)


class JobSpecPyspark(JobSpecAbstract):
    """
    Job class to perform data extraction, transformations and loading (ETL) using PySpark.
    """

    strategy_concrete = StrategySpecPyspark
    extract_concrete = ExtractSpecPyspark
    transform_concrete = TransformSpecPyspark
    load_concrete = LoadSpecPyspark


class JobSpecFactory:
    """
    Factory class to create JobSpec instances.
    """

    @classmethod
    def from_file(cls, filepath: str) -> JobSpecAbstract:
        """
        Get the job specifications from confeti.

        Args:
            filepath (str): path to file.

        Returns:
            Job: job instance.
        """
        handler: FileHandler = FileHandlerContext.factory(filepath=filepath)
        file: dict = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_confeti(confeti=file)

        raise NotImplementedError("No handling strategy found.")

    @staticmethod
    def from_confeti(confeti: dict) -> JobSpecAbstract:
        """
        Create a JobSpec instance from a Confeti dictionary.

        Args:
            confeti (dict): The Confeti dictionary.

        Returns:
            JobSpecAbstract: The JobSpec instance created from the Confeti dictionary.

        Raises:
            NotImplementedError: If the strategy value is not recognized or not supported.
        """
        strategy = StrategyEngine(value=confeti["strategy"]["engine"])

        if strategy == StrategyEngine.PYSPARK:
            return JobSpecPyspark.from_confeti(confeti=confeti)

        raise NotImplementedError(f"No strategy handling strategy recognised or supported for value: {strategy}")
