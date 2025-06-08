"""
Column transform function.
"""

from collections.abc import Callable
from typing import Final

from ingestion_framework.core.transform import Function, TransformFunctionRegistry
from ingestion_framework.models.transforms.select import SelectFunctionModel

# Import these locally to avoid circular imports
from ingestion_framework.types import DataFrameRegistry

COLUMNS: Final[str] = "columns"


@TransformFunctionRegistry.register("select")
class SelectFunction(Function[SelectFunctionModel]):
    """
    Encapsulates column transformation logic for DataFrames.

    Attributes:
        model (...): The SelectModel object containing the Selecting information.

    Methods:
        from_dict(dict_: dict[str, Any]) -> Self: Create SelectTransform object from json.
        transform() -> Callable: Selects column(s) to new type.
    """

    model_concrete = SelectFunctionModel

    def transform(self) -> Callable:
        """
        Selects columns with aliases.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the dict_ 'select_with_alias' function:

            ```
            {"function": "select_with_alias", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def __f(dataframe_registry: DataFrameRegistry, dataframe_name: str) -> None:
            dataframe_registry[dataframe_name] = dataframe_registry[dataframe_name].select(
                *self.model.arguments.columns
            )

        return __f
