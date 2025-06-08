"""Base model classes for the ingestion framework.

This module provides the foundational Model abstract base class that all
configuration models in the framework inherit from. It defines the common
interface for converting configuration dictionaries into strongly-typed
model instances.

The model classes are used throughout the framework to provide type safety
and validation for configuration data loaded from external sources.
"""

from abc import ABC, abstractmethod
from typing import Any, Self


class Model(ABC):
    """Abstract base class for all configuration models in the framework.

    Defines the common interface that all model classes must implement.
    Model classes are responsible for converting dictionary-based configuration
    into strongly-typed objects that can be used by the framework components.
    """

    @classmethod
    @abstractmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        """Create a model instance from a dictionary.

        Args:
            dict_: Dictionary containing configuration data

        Returns:
            An instance of the model class

        Raises:
            NotImplementedError: When not implemented by subclasses
        """
        raise NotImplementedError
