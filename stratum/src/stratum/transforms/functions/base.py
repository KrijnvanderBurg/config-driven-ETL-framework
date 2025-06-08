"""
Transform functions.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Final, Generic, Self, TypeVar

from stratum.exceptions import DictKeyError

FUNCTION: Final[str] = "function"
ARGUMENTS: Final[str] = "arguments"


class ArgsAbstract(ABC):
    """
    Abstract base class for the arguments of a transformation function.
    """

    @classmethod
    @abstractmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create arguments object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            ArgsAbstract: The arguments object created from the Confeti dictionary.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError


ArgsT = TypeVar("ArgsT", bound=ArgsAbstract)


class FunctionModelAbstract(Generic[ArgsT], ABC):
    """
    Modelification for Transform.

    Args:
        function (str): function name to execute.
        arguments (ArgsT): arguments to pass to the function.
    """

    args_concrete: type[ArgsT]

    def __init__(self, function: str, arguments: ArgsT) -> None:
        """
        Initialize the transformation function.

        Args:
            function (str): The name of the function to execute.
            arguments (ArgsT): The arguments to pass to the function.
        """
        self.function = function
        self.arguments = arguments

    @property
    def function(self) -> str:
        return self._function

    @function.setter
    def function(self, value: str) -> None:
        self._function = value

    @property
    def arguments(self) -> ArgsT:
        return self._arguments

    @arguments.setter
    def arguments(self, value: ArgsT) -> None:
        self._arguments = value

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        Create a transformation function object from a Confeti dictionary.

        Args:
            confeti (dict[str, Any]): The Confeti dictionary.

        Returns:
            FunctionModelAbstract: The transformation function object created from the Confeti dictionary.
        """
        try:
            function_name = confeti[FUNCTION]
            arguments_dict = confeti[ARGUMENTS]
            arguments = cls.args_concrete.from_confeti(confeti=arguments_dict)
        except KeyError as e:
            raise DictKeyError(key=e.args[0], dict_=confeti)

        return cls(function=function_name, arguments=arguments)


class FunctionModelPyspark(FunctionModelAbstract[ArgsAbstract], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """


FunctionModelT = TypeVar("FunctionModelT", bound=FunctionModelAbstract)


class FunctionAbstract(Generic[FunctionModelT], ABC):
    """
    Modelification for Transform.

    Args:
        function (str): function name to execute.
        arguments (AbstractArgs): arguments to pass to the function.
    """

    model_concrete: type[FunctionModelT]

    def __init__(self, model: FunctionModelT) -> None:
        """
        Initialize a CastTransform object.

        Args:
            model (CastModel): The CastModel object containing the casting information.
        """
        self.model = model
        self.callable_ = self.transform()

    @property
    def model(self) -> FunctionModelT:
        return self._model

    @model.setter
    def model(self, value: FunctionModelT) -> None:
        self._model = value

    @property
    def callable_(self) -> Callable:
        return self._callable_

    @callable_.setter
    def callable_(self, value: Callable) -> None:
        self._callable_ = value

    @abstractmethod
    def transform(self) -> Callable:
        """TODO"""

    @classmethod
    def from_confeti(cls, confeti: dict[str, Any]) -> Self:
        """
        TODO

        Args:
            confeti (dict[str, Any]): The dictionary.

        Returns:
            FunctionAbstract: model
        """
        model = cls.model_concrete.from_confeti(confeti=confeti)
        return cls(model=model)


class FunctionPyspark(FunctionAbstract[FunctionModelT], ABC):
    """
    A concrete implementation of transformation functions using PySpark.
    """

    @abstractmethod
    def transform(self) -> Callable:
        """TODO"""
