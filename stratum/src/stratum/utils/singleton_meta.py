import threading
from typing import Generic, TypeVar

_T = TypeVar("_T")


class SingletonType(type, Generic[_T]):
    # WeakValueDictionary is used to store the instances as weak references
    # This allows the instances to be deleted when they are no longer needed through garbage collection
    _instances: dict["SingletonType[_T]", _T] = dict()
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args, **kwargs) -> _T:
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(SingletonType, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class Singleton(metaclass=SingletonType):
    pass
