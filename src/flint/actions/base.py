from abc import abstractmethod

from pydantic import Field

from flint import BaseModel


class ActionBase(BaseModel):
    """Base class for defining actions in hooks.

    Attributes:
        name (str): The name of the action.
        parameters (dict): A dictionary of parameters for the action.
    """

    name: str = Field(..., description="The name of the action.")
    description: str = Field(..., description="A description of the action.")
    parameters: dict = Field(default_factory=dict, description="A dictionary of parameters for the action.")

    def execute(self) -> None:
        """Execute the action."""
        self._execute()

    @abstractmethod
    def _execute(self) -> None:
        """Execute the action."""
