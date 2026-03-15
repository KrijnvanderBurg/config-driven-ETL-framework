"""Base class for Spark transformation functions.

This module provides the base class for all Spark-specific transformation functions,
enabling shared access to the DataFrame registry across transformation operations.
"""

from pydantic import PrivateAttr

from samara.types import DataFrameRegistry
from samara.workflow.jobs.models.model_transform import ArgsT, FunctionModel


class FunctionSpark(FunctionModel[ArgsT]):
    """Extend transformation functions with Spark-specific capabilities.

    This class extends FunctionModel with Spark-specific functionality, including
    access to the shared DataFrame registry for operations that need to reference
    other DataFrames (such as joins across multiple upstream sources). Used with
    multiple inheritance alongside concrete FunctionModel subclasses to provide
    registry access throughout the transformation execution.

    Attributes:
        _data_registry: Shared registry for accessing processed DataFrames by
            their identifier within the pipeline execution context.

    Note:
        The _data_registry is a private attribute initialized per-instance via
        Pydantic's PrivateAttr. Since DataFrameRegistry is a singleton, all
        instances share the same underlying registry, enabling cross-reference
        between DataFrames created by different transformation steps.
    """

    _data_registry: DataFrameRegistry = PrivateAttr(default_factory=DataFrameRegistry)
