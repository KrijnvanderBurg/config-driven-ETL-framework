"""Runtime configuration module for the Flint ETL framework.

This module provides runtime configuration management using Pydantic models
for type safety and validation. It includes the main Runtime class that holds
the global configuration state for the framework.
"""

from pathlib import Path
from typing import Any, Final, Self

from pydantic import ValidationError

from flint import BaseModel
from flint.exceptions import FlintIOError, FlintRuntimeConfigurationError
from flint.runtime.jobs import JobUnion
from flint.utils.file import FileHandlerContext
from flint.utils.logger import get_logger

logger = get_logger(__name__)

RUNTIME: Final = "runtime"


class RuntimeController(BaseModel):
    """Main runtime configuration class for the Flint ETL framework.

    This class serves as the central configuration holder for the entire
    framework, providing type-safe access to global settings and components.

    Attributes:
        globals: Global configuration including channels and framework settings
    """

    jobs: list[JobUnion]

    @classmethod
    def from_file(cls, filepath: Path) -> Self:
        """Create an RuntimeManager instance from a configuration file.

        Loads and parses a configuration file to create an RuntimeManager instance.

        Args:
            filepath: Path to the configuration file.

        Returns:
            A fully configured RuntimeManager instance.

        Raises:
            FlintIOError: If there are file I/O related issues (file not found, permission denied, etc.)
            FlintRuntimeConfigurationError: If there are configuration parsing or validation issues
        """
        logger.info("Creating RuntimeManager from file: %s", filepath)

        try:
            handler = FileHandlerContext.from_filepath(filepath=filepath)
            dict_: dict[str, Any] = handler.read()
        except (OSError, ValueError) as e:
            logger.error("Failed to read runtime configuration file: %s", e)
            raise FlintIOError(f"Cannot load runtime configuration from '{filepath}': {e}") from e

        try:
            runtime = cls(**dict_[RUNTIME])
            logger.info("Successfully created RuntimeManager from configuration file: %s", filepath)
            return runtime
        except KeyError as e:
            raise FlintRuntimeConfigurationError(f"Missing 'runtime' section in configuration file '{filepath}'") from e
        except ValidationError as e:
            raise FlintRuntimeConfigurationError(f"Invalid runtime configuration in file '{filepath}': {e}") from e

    def execute_all(self) -> None:
        """Execute all jobs in the ETL pipeline.

        Executes each job in the ETL instance by calling their execute method.
        Raises an exception if any job fails during execution.
        """
        logger.info("Executing all %d jobs in ETL pipeline", len(self.jobs))

        for i, job in enumerate(self.jobs):
            logger.info("Executing job %d/%d: %s", i + 1, len(self.jobs), job.name)
            job.execute()

        logger.info("All jobs in ETL pipeline executed successfully")
