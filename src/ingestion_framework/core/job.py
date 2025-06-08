"""
Job class.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Self

from ingestion_framework.core.extract import Extract, ExtractContext
from ingestion_framework.core.load import Load, LoadContext
from ingestion_framework.core.transform import Function, Transform
from ingestion_framework.exceptions import DictKeyError
from ingestion_framework.utils.file import FileHandlerContext

ENGINE: Final[str] = "engine"
EXTRACTS: Final[str] = "extracts"
TRANSFORMS: Final[str] = "transforms"
LOADS: Final[str] = "loads"


@dataclass
class Job:
    """
    Job class to perform data extraction, transformations and loading (ETL).

    Args:
        engine (OptionsEngine): Engine type.
        extracts (list[Extract]): Extract modelifications.
        transforms (list[Transform]): Transform modelifications.
        loads (list[Load]): Load modelifications.
    """

    extracts: list[Extract]
    transforms: list[Transform]
    loads: list[Load]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """
        Get the job modelifications from dict_.

        Args:
            filepath (str): path to file.

        Returns:
            Job: job instance.
        """
        handler = FileHandlerContext.from_filepath(filepath=filepath)
        file: dict[str, Any] = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_dict(dict_=file)

        raise NotImplementedError("No handling options found.")

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """
        Get the job modelifications from dict_.

        Args:
            dict_ (dict[str, Any]): dictionary object.

        Returns:
            Job: job instance.
        """
        try:
            extracts: list = []
            for extract_dict in dict_[EXTRACTS]:
                extract_class = ExtractContext.factory(dict_=extract_dict)
                extract = extract_class.from_dict(dict_=extract_dict)
                extracts.append(extract)

            transforms: list = []
            for transform_dict in dict_[TRANSFORMS]:
                transform_class = Transform[Function]
                transform = transform_class.from_dict(dict_=transform_dict)
                transforms.append(transform)

            loads: list = []
            for load_dict in dict_[LOADS]:
                load_class = LoadContext.factory(dict_=load_dict)
                load = load_class.from_dict(dict_=load_dict)
                loads.append(load)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=dict_) from e

        return cls(extracts=extracts, transforms=transforms, loads=loads)

    def execute(self) -> None:
        """
        Extract data into a DataFrame, transform the DataFrame, then load the DataFrame.
        """
        self._extract()
        self._transform()
        self._load()

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
