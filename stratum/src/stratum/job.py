"""
Job class.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Final, Generic, Self

from pyspark.sql.streaming.query import StreamingQuery

from stratum.exceptions import DictKeyError
from stratum.extract.base import ExtractAbstract, ExtractModelAbstract
from stratum.extract.factory import ExtractContextAbstract, ExtractContextPyspark
from stratum.load.base import LoadAbstract, LoadModelAbstract
from stratum.load.factory import LoadContextAbstract, LoadContextPyspark
from stratum.transforms.base import FunctionAbstract, TransformAbstract, TransformModelAbstract, TransformPyspark
from stratum.types import DataFrameT, StreamingQueryT
from stratum.utils.file_handler import FileHandlerContext

ENGINE: Final[str] = "engine"
EXTRACTS: Final[str] = "extracts"
TRANSFORMS: Final[str] = "transforms"
LOADS: Final[str] = "loads"


class Engine(Enum):
    """Enumeration for job engines."""

    PYSPARK = "pyspark"


class JobAbstract(Generic[DataFrameT, StreamingQueryT], ABC):
    """
    Job class to perform data extraction, transformations and loading (ETL).
    """

    extract_concrete: type[ExtractContextAbstract]
    transform_concrete: type[TransformAbstract]
    load_concrete: type[LoadContextAbstract]

    def __init__(
        self,
        engine: Engine,
        extracts: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]],
        transforms: list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]],
        loads: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]],
    ) -> None:
        """
        Initialize Job instance.

        Args:
            engine (OptionsEngine): Engine type.
            extracts (list[ExtractAbstract]): Extract modelifications.
            transforms (list[TransformAbstract]): Transform modelifications.
            loads (list[LoadAbstract]): Load modelifications.
        """
        self.engine = engine
        self.extracts = extracts
        self.transforms = transforms
        self.loads = loads

    @property
    def engine(self) -> Engine:
        return self._engine

    @engine.setter
    def engine(self, value: Engine) -> None:
        self._engine = value

    @property
    def extracts(self) -> list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]:
        return self._extracts

    @extracts.setter
    def extracts(self, value: list[ExtractAbstract[ExtractModelAbstract, DataFrameT]]) -> None:
        self._extracts = value

    @property
    def transforms(self) -> list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]]:
        return self._transforms

    @transforms.setter
    def transforms(self, value: list[TransformAbstract[TransformModelAbstract, FunctionAbstract, DataFrameT]]) -> None:
        self._transforms = value

    @property
    def loads(self) -> list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]:
        return self._loads

    @loads.setter
    def loads(self, value: list[LoadAbstract[LoadModelAbstract, DataFrameT, StreamingQueryT]]) -> None:
        self._loads = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Get the job modelifications from confeti.

        Args:
            confeti (dict[str, Any]): dictionary object.

        Returns:
            Job: job instance.
        """
        try:
            engine = Engine(value=confeti[ENGINE])

            extracts: list[ExtractAbstract] = []
            for extract_confeti in confeti[EXTRACTS]:
                extract_strategy = cls.extract_concrete.factory(confeti=extract_confeti)
                extract = extract_strategy.from_confeti(confeti=extract_confeti)
                extracts.append(extract)

            transforms: list[TransformAbstract] = []
            for transform_confeti in confeti[TRANSFORMS]:
                transform = cls.transform_concrete.from_confeti(confeti=transform_confeti)
                transforms.append(transform)

            loads: list[LoadAbstract] = []
            for load_confeti in confeti[LOADS]:
                load_strategy = cls.load_concrete.factory(confeti=load_confeti)
                load = load_strategy.from_confeti(confeti=load_confeti)
                loads.append(load)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        return cls(engine=engine, transforms=transforms, extracts=extracts, loads=loads)

    @abstractmethod
    def execute(self) -> StreamingQuery | None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        raise NotImplementedError

    def _extract(self) -> None:
        """
        Extract data from modelification into a DataFrame.
        """
        for extract in self.extracts:
            extract.extract()

    def _transform(self) -> None:
        """
        Transform data from modelifiction.

        Args:
            df (DataFrame): Dataframe to be transformed.

        Returns:
            DataFrame: transformed data.
        """
        for transform in self.transforms:
            transform.data_registry[transform.model.name] = transform.data_registry[transform.model.upstream_name]
            transform.transform()

    def _load(self) -> None:
        """
        Load data to the modelification.

        Returns:
            DataFrame: The loaded data.
        """
        for load in self.loads:
            load.data_registry[load.model.name] = load.data_registry[load.model.upstream_name]
            load.load()


class JobPyspark(JobAbstract):
    """
    TODO
    """

    extract_concrete = ExtractContextPyspark
    transform_concrete = TransformPyspark
    load_concrete = LoadContextPyspark

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        self._extract()
        self._transform()
        self._load()


class Job:
    """
    Factory class to create JobModel instances.
    """

    @classmethod
    def from_file(cls, filepath: str) -> JobPyspark:
        """
        Get the job modelifications from confeti.

        Args:
            filepath (str): path to file.

        Returns:
            Job: job instance.
        """
        handler = FileHandlerContext.factory(filepath=filepath)
        file: dict[str, Any] = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_confeti(confeti=file)

        raise NotImplementedError("No handling options found.")

    @staticmethod
    def from_confeti(confeti: dict[str, Any]) -> JobPyspark:
        """
        Create a JobModel instance from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            JobModelAbstract: The JobModel instance created from the Confeti dictionary.

        Raises:
            NotImplementedError: If the options value is not recognized or not supported.
        """
        try:
            engine = Engine(value=confeti[ENGINE])
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        if engine == Engine.PYSPARK:
            return JobPyspark.from_confeti(confeti=confeti)

        raise NotImplementedError(f"No engine handling recognised or supported for value: {engine}")
