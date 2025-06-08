from abc import ABC
from typing import Any

from stratum.extract.base import DATA_FORMAT, ExtractAbstract, ExtractFormat
from stratum.extract.file_extract import ExtractFilePyspark


class ExtractContextAbstract(ABC):
    """Extract abstract class."""

    strategy: dict[ExtractFormat, type[ExtractAbstract]]

    @classmethod
    def factory(cls, confeti: dict[str, Any]) -> type[ExtractAbstract]:
        """
        Get an extract instance based on the extract modelification using the strategy pattern.

        Args:
            confeti (dict[str, Any]): confeti.

        Returns:
            DataFramePyspark: An instance of a data extract.

        Raises:
            NotImplementedError: If the modelified extract format is not supported.
        """

        extract_strategy = ExtractFormat(confeti[DATA_FORMAT])

        if extract_strategy not in cls.strategy.keys():
            raise NotImplementedError(f"Extract format {extract_strategy.value} is not supported.")

        return cls.strategy[extract_strategy]


class ExtractContextPyspark(ExtractContextAbstract):
    """
    TODO
    """

    strategy: dict[ExtractFormat, type[ExtractAbstract[Any, Any]]] = {
        ExtractFormat.PARQUET: ExtractFilePyspark,
        ExtractFormat.JSON: ExtractFilePyspark,
        ExtractFormat.CSV: ExtractFilePyspark,
    }
