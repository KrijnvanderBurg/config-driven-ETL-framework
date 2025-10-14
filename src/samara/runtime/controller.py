"""Runtime configuration module for the Samara ETL framework.

This module provides runtime configuration management using Pydantic models
for type safety and validation. It includes the main Runtime class that holds
the global configuration state for the framework.
"""

from pathlib import Path
from typing import Any, Final, Self

from pydantic import Field, ValidationError
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue

from samara import BaseModel
from samara.exceptions import FlintIOError, FlintRuntimeConfigurationError
from samara.runtime.jobs import JobUnion
from samara.utils.file import FileHandlerContext
from samara.utils.logger import get_logger

logger = get_logger(__name__)

RUNTIME: Final = "runtime"


class PreserveFieldOrderJsonSchema(GenerateJsonSchema):
    """Custom JSON schema generator that preserves field order as defined in code.

    By default, Pydantic sorts JSON schema keys alphabetically. This custom generator
    disables sorting to preserve the order of fields as they appear in the model definition.
    """

    def sort(self, value: JsonSchemaValue, parent_key: str | None = None) -> JsonSchemaValue:
        """No-op sort to preserve field definition order.

        Args:
            value: The JSON schema value to sort (not sorted in this implementation).
            parent_key: Optional parent key context.

        Returns:
            The unmodified value, preserving original field order.
        """
        _ = parent_key
        return value


class RuntimeController(BaseModel):
    """Main runtime configuration class for the Samara ETL framework.

    This class serves as the central configuration holder for the entire
    framework, providing type-safe access to global settings and components.

    Attributes:
        id: Unique identifier for the runtime configuration
        description: Description of the runtime configuration
        enabled: Whether this runtime is enabled
        jobs: List of jobs to execute in the ETL pipeline
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the runtime configuration", min_length=1)
    description: str = Field(..., description="Description of the runtime configuration")
    enabled: bool = Field(..., description="Whether this runtime is enabled")
    jobs: list[JobUnion] = Field(..., description="List of jobs to execute in the ETL pipeline")

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

    @classmethod
    def export_schema(cls) -> dict[str, Any]:
        """Export the JSON schema for the RuntimeController model.

        Returns the complete JSON schema definition for the RuntimeController,
        including all nested models and their validation rules. This schema
        can be used for documentation, validation, or generating configuration
        templates.

        The schema preserves the order of fields as they are defined in the model,
        rather than sorting them alphabetically.

        Returns:
            dict[str, Any]: The JSON schema dictionary conforming to JSON Schema Draft 2020-12.

        Example:
            >>> schema = RuntimeController.export_schema()
            >>> print(schema['properties']['id']['type'])
            'string'
        """
        logger.debug("Exporting RuntimeController JSON schema")
        return cls.model_json_schema(schema_generator=PreserveFieldOrderJsonSchema)

    def execute_all(self) -> None:
        """Execute all jobs in the ETL pipeline.

        Executes each job in the ETL instance by calling their execute method.
        Each job is responsible for clearing its own engine-specific registries
        after execution completes.
        Raises an exception if any job fails during execution.
        """
        if not self.enabled:
            logger.info("Runtime is disabled")
            return

        logger.info("Executing all %d jobs in ETL pipeline", len(self.jobs))

        for i, job in enumerate(self.jobs):
            logger.info("Executing job %d/%d: %s", i + 1, len(self.jobs), job.id_)
            job.execute()

        logger.info("All jobs in ETL pipeline executed successfully")

        logger.info("All jobs in ETL pipeline executed successfully")
