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

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Final, Generic, Self, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

from stratum.exceptions import DictKeyError
from stratum.types import DataFrameT, RegistrySingleton, StreamingQueryT
from stratum.utils.spark_handler import SparkHandler

NAME: Final[str] = "name"
UPSTREAM_NAME: Final[str] = "upstream_name"
METHOD: Final[str] = "method"
MODE: Final[str] = "mode"
DATA_FORMAT: Final[str] = "data_format"
LOCATION: Final[str] = "location"
SCHEMA_LOCATION: Final[str] = "schema_location"
OPTIONS: Final[str] = "options"


class LoadMethod(Enum):
    """Enumeration for methods of load modes."""

    BATCH = "batch"
    STREAMING = "streaming"


class LoadMode(Enum):
    """Enumeration for methods of load modes.

    pyspark.sql.DataFrameWriter.mode:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html
        * ``append``: Append contents of this :class:`DataFrame` to existing data.
        * ``overwrite``: Overwrite existing data.
        * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
            exists.
        * ``ignore``: Silently ignore this operation if data already exists.

        Options include:
        * ``append``: Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * ``complete``: All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these are some updates
        * ``update``: only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.
    """

    # pyspark batch
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"

    # pyspark streaming
    # APPEND = "append"  # already added above for pyspark batch
    COMPLETE = "complete"
    UPDATE = "update"


class LoadFormat(Enum):
    """Enumeration for methods of input and structures for the load."""

    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"


class LoadModelAbstract(ABC):
    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        location: str,
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            location (str): URI that identifies where to load data in the modelified format.
        """
        self.name = name
        self.upstream_name = upstream_name
        self.method = method
        self.location = location

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def upstream_name(self) -> str:
        return self._upstream_name

    @upstream_name.setter
    def upstream_name(self, value: str) -> None:
        self._upstream_name = value

    @property
    def method(self) -> LoadMethod:
        return self._method

    @method.setter
    def method(self, value: LoadMethod) -> None:
        self._method = value

    @property
    def location(self) -> str:
        return self._location

    @location.setter
    def location(self, value: str) -> None:
        self._location = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class LoadModelPyspark(LoadModelAbstract, ABC):
    """
    Modelification of the sink input.

    Args:
        name (str): ID of the sink modelification.
        method (str): Type of sink load mode.
        mode (str): Type of sink mode.
        data_format (str): Format of the sink input.
        location (str): URI that identifies where to load data in the modelified format.
    """

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        location: str,
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            location (str): URI that identifies where to load data in the modelified format.
            schema_location (str): URI that identifies where to load schema.
            options (dict[str, Any]): Options for the sink input.
        """
        super().__init__(name=name, upstream_name=upstream_name, method=method, location=location)
        self.schema_location = schema_location
        self.options = options

    @property
    def schema_location(self) -> str | None:
        return self._schema_location

    @schema_location.setter
    def schema_location(self, value: str | None) -> None:
        self._schema_location = value

    @property
    def options(self) -> dict[str, str]:
        return self._options

    @options.setter
    def options(self, value: dict[str, str]) -> None:
        self._options = value

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self: ...


class LoadModelFileAbstract(LoadModelAbstract):
    """TODO"""


class LoadModelFilePyspark(LoadModelFileAbstract, LoadModelPyspark):
    """TODO"""

    def __init__(
        self,
        name: str,
        upstream_name: str,
        method: LoadMethod,
        mode: LoadMode,
        data_format: LoadFormat,
        location: str,
        schema_location: str | None,
        options: dict[str, str],
    ) -> None:
        """
        Initialize LoadModelAbstract with the modelified parameters.

        Args:
            name (str): ID of the sink modelification.
            upstream_name (list[str]): ID of the sink modelification.
            method (LoadMethod): Type of sink load mode.
            mode (LoadMode): Type of sink mode.
            data_format (LoadFormat): Format of the sink input.
            location (str): URI that identifies where to load data in the modelified format.
            schema_location (str): URI that identifies where to load schema.
            options (dict[str, Any]): Options for the sink input.
        """
        super().__init__(
            name=name,
            upstream_name=upstream_name,
            method=method,
            location=location,
            schema_location=schema_location,
            options=options,
        )
        self.mode = mode
        self.data_format = data_format

    @property
    def mode(self) -> LoadMode:
        return self._mode

    @mode.setter
    def mode(self, value: LoadMode) -> None:
        self._mode = value

    @property
    def data_format(self) -> LoadFormat:
        return self._data_format

    @data_format.setter
    def data_format(self, value: LoadFormat) -> None:
        self._data_format = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a LoadModelAbstract object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            LoadModelAbstract: The LoadModelAbstract object created from the Confeti dictionary.
        """

        try:
            name = confeti[NAME]
            upstream_name = confeti[UPSTREAM_NAME]
            method = LoadMethod(confeti[METHOD])
            mode = LoadMode(confeti[MODE])
            data_format = LoadFormat(confeti[DATA_FORMAT])
            location = confeti[LOCATION]
            schema_location = confeti.get(SCHEMA_LOCATION, None)
            options = confeti.get(OPTIONS, {})
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        return cls(
            name=name,
            upstream_name=upstream_name,
            method=method,
            mode=mode,
            data_format=data_format,
            location=location,
            schema_location=schema_location,
            options=options,
        )


LoadModelT = TypeVar("LoadModelT", bound=LoadModelAbstract)


class LoadAbstract(Generic[LoadModelT, DataFrameT, StreamingQueryT], ABC):
    """Load abstract class."""

    load_model_concrete: type[LoadModelT]

    def __init__(self, model: LoadModelT) -> None:
        self.model = model
        self.data_registry = RegistrySingleton()

    @property
    def model(self) -> LoadModelT:
        return self._model

    @model.setter
    def model(self, value: LoadModelT) -> None:
        self._model = value

    @property
    def data_registry(self) -> RegistrySingleton:
        return self._data_registry

    @data_registry.setter
    def data_registry(self, value: RegistrySingleton) -> None:
        self._data_registry = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """Create an instance of LoadAbstract from configuration."""
        model: LoadModelT = cls.load_model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)

    @abstractmethod
    def _load_batch(self) -> None:
        """
        Abstract method for batch loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def _load_streaming(self) -> StreamingQueryT:
        """
        Abstract method for streaming loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def _load_schema(self) -> None:
        """
        Abstract method for schema loadion.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self) -> None:
        """
        Main loadion method.
        """
        raise NotImplementedError


class LoadPyspark(LoadAbstract[LoadModelPyspark, DataFramePyspark, StreamingQueryPyspark], ABC):
    """
    Concrete implementation for PySpark DataFrame loadion.
    """

    # load_model_concrete = LoadModelPyspark

    def _load_schema(self) -> None:
        """
        Write schema to file.
        """
        if self.model.schema_location:
            with open(file=self.model.schema_location, mode="w", encoding="utf-8") as file:
                schema_json = self.data_registry[self.model.name].schema.json()
                schema_dict = json.loads(schema_json)
                json.dump(schema_dict, file)

    def load(self) -> None:
        """
        Main loadion method.
        """
        SparkHandler().add_configs(options=self.model.options)

        if self.model.method == LoadMethod.BATCH:
            self._load_batch()
        elif self.model.method == LoadMethod.STREAMING:
            self.data_registry[self.model.name] = self._load_streaming()
        else:
            raise ValueError(f"Loadion method {self.model.method} is not supported for Pyspark.")

        self._load_schema()
