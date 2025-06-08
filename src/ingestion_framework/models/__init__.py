""" """

from abc import ABC, abstractmethod
from typing import Any, Self


class Model(ABC):
    @classmethod
    @abstractmethod
    def from_dict(cls, dict_: dict[str, Any]) -> Self:
        raise NotImplementedError
