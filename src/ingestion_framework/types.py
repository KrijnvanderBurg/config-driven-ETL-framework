"""
Core type definitions for the ingestion framework.

This module provides fundamental type definitions and design patterns used throughout
the ingestion framework, including:

- Singleton metaclass for ensuring only one instance of a class exists
- Registry decorators for registering and retrieving classes based on keys
- Type variables and generics for type-safe operations

These types provide the foundation for the ingestion framework's architecture,
enabling features like component registration, singleton services, and type safety.
"""

import threading
from collections.abc import Callable, Iterator
from typing import Any, Generic, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

K = TypeVar("K")
V = TypeVar("V")


class Singleton(type):
    """A metaclass for creating singleton classes.

    This metaclass ensures that only one instance of a class is created. Subsequent instantiations
    of a class using this metaclass will return the same instance.

    Attributes:
        _instances (dict[type, Any]): Dictionary mapping classes to their singleton instances.
        _lock (threading.Lock): Thread lock to ensure thread-safe instance creation.

    Example:
        ```
        class Logger(metaclass=Singleton):
            def __init__(self):
                self.logs = []

        # These will refer to the same instance
        logger1 = Logger()
        logger2 = Logger()
        assert logger1 is logger2
        ```Z
    """

    _instances: dict[Any, Any] = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> Any:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RegistryDecorator(Generic[K, V]):
    """A registry for classes that can be decorated and registered with a key."""

    _registry: dict[K, list[type[V]]] = {}

    @classmethod
    def register(cls, key: K) -> Callable[[type[V]], type[V]]:
        """
        Class decorator to register a class with a specific key in the registry.

        Args:
            key: The key to register the class with

        Returns:
            A decorator function that registers the class and returns it unchanged
        """

        def decorator(registered_class: type[V]) -> type[V]:
            if key not in cls._registry:
                cls._registry[key] = []

            if registered_class not in cls._registry[key]:  # Prevent duplicate registration
                cls._registry[key].append(registered_class)

            return registered_class

        return decorator

    @classmethod
    def get(cls, key: K) -> type[V]:
        """
        Get the first registered class for a key.

        Args:
            key: The key to look up

        Returns:
            The registered class

        Raises:
            KeyError: If no class is registered for the given key
        """
        try:
            return cls._registry[key][0]
        except (KeyError, IndexError) as e:
            raise KeyError(f"No class registered for key: {key}") from e


class RegistryInstance(Generic[K, V], metaclass=Singleton):
    def __init__(self) -> None:
        """Initialize the instance registry."""
        self._items: dict[K, V] = dict()

    # Instance registry methods
    def __setitem__(self, name: K, item: V) -> None:
        """Set an item with a given name. Replaces any existing item."""
        self._items[name] = item

    def __getitem__(self, name: K) -> V:
        """Get an item by its name. Raises KeyError if not found."""
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: K) -> None:
        """Delete an item by its name. Raises KeyError if not found."""
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: K) -> bool:
        """Check if an item exists by its name."""
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items tracked."""
        return len(self._items)

    def __iter__(self) -> Iterator[V]:
        """Iterate over the items."""
        return iter(self._items.values())


class DataFrameRegistry(RegistryInstance[str, DataFrame]):
    """A registry for DataFrame objects."""


class StreamingQueryRegistry(RegistryInstance[str, StreamingQuery]):
    """A registry for StreamingQuery objects."""
