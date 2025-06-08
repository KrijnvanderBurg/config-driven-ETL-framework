import threading
from collections.abc import Iterator
from typing import Any, TypeVar

from pyspark.sql import DataFrame as DataFramePyspark
from pyspark.sql.streaming.query import StreamingQuery as StreamingQueryPyspark

DataFrameT = TypeVar("DataFrameT", bound=DataFramePyspark)
StreamingQueryT = TypeVar("StreamingQueryT", bound=StreamingQueryPyspark)


class SingletonType(type):
    _instances: dict[type, Any] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> Any:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Registry:
    """A metaclass for tracking and managing any kind of objects."""

    def __init__(self) -> None:
        self._items: dict[str, Any] = dict()

    def __setitem__(self, name: str, item: Any) -> None:
        """Set an item with a given name. Replaces any existing item."""
        self._items[name] = item

    def __getitem__(self, name: str) -> Any:
        """Get an item by its name. Raises KeyError if not found."""
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: str) -> None:
        """Delete an item by its name. Raises KeyError if not found."""
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: str) -> bool:
        """Check if an item exists by its name."""
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items tracked."""
        return len(self._items)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over the items."""
        return iter(self._items.values())


class RegistrySingleton(Registry, metaclass=SingletonType):
    pass
