"""Job implementation for managing ETL processes.

This module provides the Job class, which is the central component of the ingestion framework.
It manages the Extract, Transform, and Load phases of the ETL pipeline, orchestrating
the data flow from source to destination.

Jobs can be created from configuration files or dictionaries, with automatic instantiation
of the appropriate Extract, Transform, and Load components based on the configuration.
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
    """A complete ETL job that orchestrates extract, transform, and load operations.

    The Job class is the main entry point for the ingestion framework. It coordinates
    the execution of extraction, transformation, and loading operations in sequence.
    Jobs can be constructed from configuration files or dictionaries, making it easy
    to define pipelines without code changes.

    Attributes:
        extracts (list[Extract]): Collection of Extract components to obtain data from sources.
        transforms (list[Transform]): Collection of Transform components to process the data.
        loads (list[Load]): Collection of Load components to write data to destinations.

    Example:
        ```python
        from pathlib import Path
        from ingestion_framework.core.job import Job

        # Create from a configuration file
        job = Job.from_file(Path("config.json"))

        # Execute the ETL pipeline
        job.execute()
        ```
    """

    extracts: list[Extract]
    transforms: list[Transform]
    loads: list[Load]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create a Job instance from a configuration file.

        Loads and parses a configuration file to create a Job instance.
        Currently supports JSON configuration files.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured Job instance.

        Raises:
            NotImplementedError: If the file format is not supported.
        """
        handler = FileHandlerContext.from_filepath(filepath=filepath)
        file: dict[str, Any] = handler.read()

        if Path(filepath).suffix == ".json":
            return cls.from_dict(dict_=file)

        raise NotImplementedError("No handling options found.")

    @classmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a Job instance from a dictionary configuration.

        Parses a dictionary containing job configuration to create the extracts,
        transforms, and loads components and assemble them into a Job instance.

        Args:
            dict_: Dictionary containing job configuration data.

        Returns:
            A fully configured Job instance.

        Raises:
            DictKeyError: If a required key is missing from the dictionary.
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
        """Execute the complete ETL pipeline.

        Runs the extract, transform, and load phases in sequence.
        This is the main entry point for running a configured job.
        """
        self._extract()
        self._transform()
        self._load()

    def _extract(self) -> None:
        """Execute the extraction phase of the ETL pipeline.

        Calls the extract method on each configured extract component,
        retrieving data from the specified sources.
        """
        for extract in self.extracts:
            extract.extract()

    def _transform(self) -> None:
        """Execute the transformation phase of the ETL pipeline.

        Copies data from upstream components to the current transform component
        and applies the transformation operations to modify the data.
        """
        for transform in self.transforms:
            transform.data_registry[transform.model.name] = transform.data_registry[transform.model.upstream_name]
            transform.transform()

    def _load(self) -> None:
        """Execute the loading phase of the ETL pipeline.

        Copies data from upstream components to the current load component
        and writes the transformed data to the target destinations.
        """
        for load in self.loads:
            load.data_registry[load.model.name] = load.data_registry[load.model.upstream_name]
            load.load()
