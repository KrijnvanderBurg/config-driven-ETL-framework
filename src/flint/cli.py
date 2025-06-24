"""
"""

from abc import ABC, abstractmethod
from typing import Any


class Command(ABC):
    @abstractmethod
    def execute(self) -> Any:
        pass

class ValidateCommand(Command):
    def execute(self) -> str:
        pass

class RunCommand(Command):
    def execute(self) -> int:
        pass
