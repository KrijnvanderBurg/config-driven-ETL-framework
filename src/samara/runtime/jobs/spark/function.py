"""Base function class for transform operations."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import ClassVar

from samara.runtime.jobs.models.model_transform import FunctionModel
from samara.types import DataFrameRegistry
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class Function(FunctionModel, ABC):
    """Base class for all transform functions.

    This abstract class defines the interface that all transform functions must implement.
    Each transform function should inherit from this class and implement the run method.
    """

    data_registry: ClassVar[DataFrameRegistry] = DataFrameRegistry()

    @abstractmethod
    def transform(self) -> Callable:
        """Create a callable transformation function based on the model.

        This method should implement the logic to create a function that
        can be called to transform data according to the model configuration.

        Returns:
            A callable function that applies the transformation to data.
        """
