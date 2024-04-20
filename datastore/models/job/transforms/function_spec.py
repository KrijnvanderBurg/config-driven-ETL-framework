"""
Column transform function.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from abc import ABC, abstractmethod


class FuncAbstract(ABC):
    """
    Specification for Transform.

    Args:
        function (str): function name to execute.
        arguments (AbstractArgs): arguments to pass to the function.
    """

    class ArgsAbstract(ABC):
        """
        Abstract base class for the arguments of a transformation function.
        """

        def __init__(self) -> None:
            """
            Initialize the arguments of the transformation function.
            """

        @classmethod
        @abstractmethod
        def from_confeti(cls, confeti: dict):
            """
            Create arguments object from a Confeti dictionary.

            Args:
                confeti (dict): The Confeti dictionary.

            Returns:
                ArgsAbstract: The arguments object created from the Confeti dictionary.

            Raises:
                NotImplementedError: If the method is not implemented in a subclass.
            """
            raise NotImplementedError

    args_concrete = ArgsAbstract

    def __init__(self, function: str, arguments: ArgsAbstract) -> None:
        """
        Initialize the transformation function.

        Args:
            function (str): The name of the function to execute.
            arguments (ArgsAbstract): The arguments to pass to the function.
        """
        self.function: str = function
        self.arguments: FuncAbstract.ArgsAbstract = arguments

    @classmethod
    def from_confeti(cls, confeti: dict):
        """
        Create a transformation function object from a Confeti dictionary.

        Args:
            confeti (dict): The Confeti dictionary.

        Returns:
            FuncAbstract: The transformation function object created from the Confeti dictionary.
        """
        function_name: str = confeti["function"]
        arguments_dict: dict = confeti["arguments"]

        arguments: FuncAbstract.ArgsAbstract = cls.args_concrete.from_confeti(confeti=arguments_dict)  # type: ignore
        return cls(function=function_name, arguments=arguments)


class FuncPyspark(FuncAbstract):
    """
    A concrete implementation of transformation functions using PySpark.
    """
