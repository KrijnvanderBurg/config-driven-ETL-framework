"""
Core type definitions for the ingestion framework.

This module provides fundamental type definitions and design patterns used throughout
the ingestion framework, including:

- Singleton metaclass for ensuring only one instance of a class exists
- Registry decorators for registering and retrieving classes based on keys
- Type variables and generics for type-safe operations
- Exit code enum for standardized process exit codes

These types provide the foundation for the ingestion framework's architecture,
enabling features like component registration, singleton services, and type safety.
"""

import threading
from collections.abc import Callable, Iterator
from typing import Any, Generic, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

# Type variables with more specific constraints
K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type


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
        ```
    """

    _instances: dict[Any, Any] = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """
        Create or return a singleton instance of the class.

        Args:
            *args: Positional arguments to pass to the class constructor.
            **kwargs: Keyword arguments to pass to the class constructor.

        Returns:
            The singleton instance of the class.
        """
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RegistryDecorator(Generic[K, V]):
    """A registry for classes that can be decorated and registered with a key.

    This class implements the decorator pattern for registering classes with specific keys,
    enabling dynamic selection of appropriate implementations at runtime.

    Type Parameters:
        K: The type of keys in the registry (typically an enum or string)
        V: The type of values (typically a class type)
        ```
    """

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

            # Prevent duplicate registration
            if registered_class not in cls._registry[key]:
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
            registry = cls._registry
            if not registry.get(key):
                raise KeyError(f"No implementations registered for key: {key}")
            return registry[key][0]
        except (KeyError, IndexError) as e:
            available_keys = list(cls._registry.keys())
            raise KeyError(f"No class registered for key: {key}. Available keys: {available_keys}") from e

    @classmethod
    def get_all(cls, key: K) -> list[type[V]]:
        """
        Get all registered classes for a key.

        Args:
            key: The key to look up

        Returns:
            List of all registered classes for the key

        Raises:
            KeyError: If no class is registered for the given key
        """
        try:
            registry = cls._registry
            if not registry.get(key):
                raise KeyError(f"No implementations registered for key: {key}")
            return registry[key][:]  # Return a copy of the list
        except KeyError as e:
            available_keys = list(cls._registry.keys())
            raise KeyError(f"No class registered for key: {key}. Available keys: {available_keys}") from e


class RegistryInstance(Generic[K, V], metaclass=Singleton):
    """
    A generic registry implementing a singleton pattern that stores items of type V with keys of type K.

    This class provides dictionary-like access to a registry of items, ensuring only one
    registry instance exists through the Singleton metaclass. It can be used to create
    type-specific registries by specifying the key and value types as generic parameters.

    Type Parameters:
        K: The type of the keys used to identify items in the registry.
        V: The type of the values stored in the registry.

    Example:
        ```
        # Define a registry for string-keyed integers
        IntRegistry = RegistryInstance[str, int]
        registry = IntRegistry()

        # Add items
        registry['one'] = 1
        registry['two'] = 2

        # Access items
        value = registry['one']  # Returns 1

        # Check existence
        'one' in registry  # Returns True
        ```
    """

    def __init__(self) -> None:
        """Initialize the instance registry."""
        self._items: dict[K, V] = {}

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
    """A registry for DataFrame objects.

    This singleton registry maintains a collection of named DataFrame objects
    that can be shared between components in the ETL pipeline. It provides
    dictionary-like access with proper error messages when frames are not found.

    Example:
        ```python
        registry = DataFrameRegistry()

        # Store a DataFrame
        registry["customers"] = customers_df

        # Access a DataFrame
        transformed_df = transform(registry["customers"])
        ```
    """

    def __getitem__(self, name: str) -> DataFrame:
        """Get a DataFrame by name with enhanced error messaging.

        Args:
            name: Name of the DataFrame to retrieve

        Returns:
            The requested DataFrame

        Raises:
            KeyError: If the DataFrame is not found, with a list of available frames
        """
        try:
            return super().__getitem__(name)
        except KeyError as e:
            available = list(self._items.keys())
            raise KeyError(f"DataFrame '{name}' not found. Available DataFrames: {available}") from e


class StreamingQueryRegistry(RegistryInstance[str, StreamingQuery]):
    """A registry for StreamingQuery objects.

    This singleton registry maintains a collection of named StreamingQuery objects
    that can be shared between components in the ETL pipeline, enabling management
    and monitoring of streaming jobs.

    Example:
        ```python
        registry = StreamingQueryRegistry()
        registry["customers_stream"] = df.writeStream.start()

        # Check if a stream is active
        query = registry["customers_stream"]
        is_active = query.isActive()
        ```
    """

    def __getitem__(self, name: str) -> StreamingQuery:
        """Get a StreamingQuery by name with enhanced error messaging.

        Args:
            name: Name of the StreamingQuery to retrieve

        Returns:
            The requested StreamingQuery

        Raises:
            KeyError: If the StreamingQuery is not found, with a list of available queries
        """
        try:
            return super().__getitem__(name)
        except KeyError as e:
            available = list(self._items.keys())
            raise KeyError(f"StreamingQuery '{name}' not found. Available queries: {available}") from e
