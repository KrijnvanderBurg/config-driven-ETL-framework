from abc import ABC
from typing import Any

from stratum.load.base import DATA_FORMAT, LoadAbstract, LoadFormat
from stratum.load.file_load import LoadFilePyspark


class LoadContextAbstract(ABC):
    """Abstract class representing a strategy context for creating data loads."""

    strategy: dict[LoadFormat, type[LoadAbstract]]

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[LoadAbstract]:
        """
        Get a load instance based on the load modelification via strategy pattern.

        Args:
            confeti (dict[str, Any]): confeti.

        Returns:
            Load: An instance of a data load.

        Raises:
            NotImplementedError: If the modelified load format is not implemented.
        """

        load_strategy = LoadFormat(confeti[DATA_FORMAT])

        if load_strategy in cls.strategy.keys():
            return cls.strategy[load_strategy]

        raise NotImplementedError(f"Load format {load_strategy.value} is not supported. {type(load_strategy)}")


class LoadContextPyspark(LoadContextAbstract):
    """
    _summary_

    Args:
        LoadContextAbstract (_type_): _description_

    Raises:
        NotImplementedError: _description_

    Returns:
        _type_: _description_
    """

    strategy: dict[LoadFormat, type[LoadAbstract]] = {
        LoadFormat.PARQUET: LoadFilePyspark,
        LoadFormat.JSON: LoadFilePyspark,
        LoadFormat.CSV: LoadFilePyspark,
    }
