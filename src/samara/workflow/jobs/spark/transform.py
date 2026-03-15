"""PySpark transformation implementation - Execute configuration-driven data transformations.

This module provides concrete implementations for applying transformation chains to data
using Apache PySpark as the processing engine. It focuses on configuration-driven
transformations enabling pipeline authors to define complex data manipulation sequences
through structured configuration rather than code.
"""

from typing import Any

from pydantic import Field, PrivateAttr

from samara.telemetry import trace_span
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger
from samara.workflow.jobs.models.model_transform import TransformModel
from samara.workflow.jobs.spark.session import SparkHandler
from samara.workflow.jobs.spark.transforms import TransformFunctionSparkUnion

logger = get_logger(__name__)


class TransformSpark(TransformModel[TransformFunctionSparkUnion]):
    """Apply PySpark-based transformations to dataframes in the pipeline.

    This class executes a sequence of transformation functions configured in the pipeline
    definition. It manages the transformation chain by copying upstream data and applying
    each function sequentially, tracking row counts through each step for debugging and
    monitoring purposes.

    Attributes:
        options: Transformation options passed to Spark as key-value pairs for configuring
            Spark behavior and tuning parameters.

    Example:
        >>> from samara.workflow.jobs.spark.transform import TransformSpark
        >>> transform = TransformSpark(
        ...     id_="customer_transform",
        ...     upstream_id="customer_extract",
        ...     functions=[...],
        ...     options={"spark.sql.adaptive.enabled": "true"}
        ... )
        >>> transform.transform()

        **Configuration in JSON:**
        ```
        {
            "type": "transform",
            "id": "customer_transform",
            "upstream_id": "customer_extract",
            "functions": [
                {
                    "functionType": "select",
                    "params": ["id", "name", "email"]
                },
                {
                    "functionType": "filter",
                    "params": ["age > 18"]
                }
            ],
            "options": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.shuffle.partitions": "200"
            }
        }
        ```

        **Configuration in YAML:**
        ```
        type: transform
        id: customer_transform
        upstream_id: customer_extract
        functions:
          - functionType: select
            params:
              - id
              - name
              - email
          - functionType: filter
            params:
              - "age > 18"
        options:
          spark.sql.adaptive.enabled: "true"
          spark.sql.shuffle.partitions: "200"
        ```

    Note:
        Spark configurations are applied before data transformations. Each transformation
        function modifies the dataframe in place within the registry, so order matters.
    """

    model_config = {"arbitrary_types_allowed": True, "extra": "forbid"}

    _data_registry: DataFrameRegistry = PrivateAttr(default_factory=DataFrameRegistry)
    _spark: SparkHandler = PrivateAttr(default_factory=SparkHandler)

    options: dict[str, Any] = Field(..., description="Transformation options as key-value pairs")

    @trace_span("transform_spark.transform")
    def transform(self) -> None:
        """Execute the complete transformation chain on the upstream dataframe.

        Perform sequential transformation operations on the input data by:
        1. Applying configured Spark options to optimize execution
        2. Copying the dataframe from the upstream stage to this transform's namespace
        3. Applying each transformation function in order, with row count tracking
        4. Logging metrics and results for each transformation step

        Args:
            None

        Note:
            Functions execute sequentially in the order specified in the configuration.
            Each transformation modifies the dataframe registry in place. Row count changes
            are logged to help identify unexpected data loss or multiplication during
            transformation.
        """
        logger.info("Starting transformation for: %s from upstream: %s", self.id_, self.upstream_id)

        logger.debug("Applying scoped Spark configurations: %s", self.options)
        with self._spark.scoped_configs(options=self.options):
            # Copy the dataframe from upstream to current id
            logger.debug("Copying dataframe from %s to %s", self.upstream_id, self.id_)
            self._data_registry[self.id_] = self._data_registry[self.upstream_id]

            # Apply transformations
            logger.debug("Applying %d transformation functions", len(self.functions))
            for i, function in enumerate(self.functions):
                logger.debug("Applying function %d/%d: %s", i, len(self.functions), function.function_type)

                callable_ = function.transform()
                self._data_registry[self.id_] = callable_(df=self._data_registry[self.id_])

                logger.info("Function %s applied successfully", function.function_type)

            logger.info("Transformation completed successfully for: %s", self.id_)


TransformSparkUnion = TransformSpark
