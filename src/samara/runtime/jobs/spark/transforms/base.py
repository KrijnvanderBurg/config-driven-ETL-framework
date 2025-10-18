"""Base class for Spark transformation functions.

This module provides the base class for all Spark-specific transformation functions,
adding Spark-specific capabilities like access to the data registry.
"""

from typing import ClassVar

from samara.runtime.jobs.models.model_transform import ArgsT, FunctionModel
from samara.types import DataFrameRegistry


class FunctionSpark(FunctionModel[ArgsT]):
    """Base class for Spark transformation functions.
    
    This class extends FunctionModel with Spark-specific functionality,
    including access to the shared DataFrame registry for operations
    that need to reference other DataFrames (like joins).
    
    This is meant to be used with multiple inheritance alongside the
    specific FunctionModel subclasses.
    
    Attributes:
        data_registry: Shared registry for accessing DataFrames by ID
    """

    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()
