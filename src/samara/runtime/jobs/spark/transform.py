"""PySpark implementation for data transformation operations.

This module provides concrete implementations for transforming data using Apache PySpark.
It includes:

- Abstract base classes defining the transformation interface
- Function-based transformation support with configurable arguments
- Registry mechanisms for dynamically selecting transformation functions
- Configuration-driven transformation functionality

The Transform components represent the middle phase in the ETL pipeline, responsible
for manipulating data between extraction and loading.
"""

import logging
from typing import Any, ClassVar, Self

from pydantic import Field, ValidationInfo, model_validator
from samara.runtime.jobs.models.model_transform import TransformModel
from samara.runtime.jobs.spark.session import SparkHandler
from samara.runtime.jobs.spark.transforms import transform_function_spark_union
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class TransformSpark(TransformModel):
    """
    Concrete implementation for DataFrame transformation.

    This class provides functionality for transforming data.
    """

    spark: ClassVar[SparkHandler] = SparkHandler()
    functions: list[transform_function_spark_union]
    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()
    options: dict[str, Any] = Field(..., description="Transformation options as key-value pairs")

    @model_validator(mode="after")
    def validate_join_other_upstream_ids(self, info: ValidationInfo) -> Self:
        """Validate other_upstream_id references in join functions.

        Ensures that join functions reference valid upstream IDs that exist
        and are defined before the current transform.

        Args:
            info: Validation context information.

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If invalid other_upstream_id references are found.
        """
        # Get validation context containing valid upstream IDs
        if info.context is None:
            # No context provided, skip validation (used in isolated model creation)
            return self
        
        valid_upstream_ids = info.context.get("valid_upstream_ids")
        job_id = info.context.get("job_id", "unknown")
        
        if valid_upstream_ids is None:
            # Context doesn't have valid IDs, skip validation
            return self
        
        # Validate each join function
        for function in self.functions:
            if function.function_type == "join":
                other_upstream_id = function.arguments.other_upstream_id
                
                # Check if other_upstream_id references the transform itself
                if other_upstream_id == self.id_:
                    raise ValueError(
                        f"Join function in transform '{self.id_}' references itself as other_upstream_id "
                        f"in job '{job_id}'. A join cannot reference its own transform id."
                    )
                
                # Check if other_upstream_id exists in valid upstream IDs
                if other_upstream_id not in valid_upstream_ids:
                    raise ValueError(
                        f"Join function in transform '{self.id_}' references other_upstream_id "
                        f"'{other_upstream_id}' in job '{job_id}' which either does not exist or is defined "
                        f"later in the transforms list. other_upstream_id must reference an existing extract or "
                        f"a transform that appears before this one."
                    )
        
        return self

    def transform(self) -> None:
        """
        Apply all transformation functions to the data source.

        This method performs the following steps:
        1. Copies the dataframe from the upstream source to current transform's id
        2. Sequentially applies each transformation function to the dataframe
        3. Each function updates the registry with its results

        Note:
            Functions are applied in the order they were defined in the configuration.
        """
        logger.info("Starting transformation for: %s from upstream: %s", self.id_, self.upstream_id)

        logger.debug("Adding Spark configurations: %s", self.options)
        self.spark.add_configs(options=self.options)

        # Copy the dataframe from upstream to current id
        logger.debug("Copying dataframe from %s to %s", self.upstream_id, self.id_)
        self.data_registry[self.id_] = self.data_registry[self.upstream_id]

        # Apply transformations
        logger.debug("Applying %d transformation functions", len(self.functions))
        for i, function in enumerate(self.functions):
            logger.debug("Applying function %d/%d: %s", i, len(self.functions), function.function_type)

            original_count = self.data_registry[self.id_].count()
            callable_ = function.transform()
            self.data_registry[self.id_] = callable_(df=self.data_registry[self.id_])

            new_count = self.data_registry[self.id_].count()

            logger.info(
                "Function %s applied - rows changed from %d to %d", function.function_type, original_count, new_count
            )

        logger.info("Transformation completed successfully for: %s", self.id_)


TransformSparkUnion = TransformSpark
