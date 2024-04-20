"""
Column cast class tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema
import pytest
from pyspark.sql import types

from datastore.models.job.transform_spec import TransformSpecPyspark
from datastore.models.job.transforms.function_spec import FuncPyspark
from datastore.models.job.transforms.functions.column_spec import CastPyspark
from datastore.utils.json_handler import JsonHandler

# FuncPyspark fixtures


@pytest.fixture(name="func_pyspark")
def fixture__func__pyspark() -> FuncPyspark:
    """
    Fixture for creating a FuncPyspark instance.

    Returns:
        FuncPyspark: The created FuncPyspark instance.
    """
    # Arrange
    columns: list[CastPyspark.Args.Columns] = [
        CastPyspark.Args.Columns(name="age", data_type=types.StringType()),
    ]
    args: CastPyspark.Args = CastPyspark.Args(columns=columns)
    func: FuncPyspark = FuncPyspark(function="func", arguments=args)

    return func


@pytest.fixture(name="func_confeti_pyspark")
def fixture__func__confeti__pyspark() -> dict:
    """
    Fixture for creating a FuncPyspark instance from confeti.

    Returns:
        dict: The created FuncPyspark instance from confeti.
    """
    return {"function": "func", "arguments": {}}


# FuncPyspark tests


class TestFuncPyspark:
    """
    Test class for TODO
    """

    def test__init(self, func_pyspark: FuncPyspark) -> None:
        """
        Test case for initializing FuncPyspark.

        Args:
            func_pyspark (FuncPyspark): Instance of FuncPyspark.
        """
        # Assert
        assert func_pyspark.function == "func"
        assert isinstance(func_pyspark.arguments, FuncPyspark.ArgsAbstract)

    @pytest.mark.parametrize("missing_property", ["function", "arguments"])
    def test__validate_json__missing_required_properties(
        self,
        missing_property: str,
        cast_confeti_pyspark: dict,
    ) -> None:
        """
        Test case for validating missing required properties individually.

        Args:
            missing_property (str): The property to be removed for testing.
            cast_confeti_pyspark (dict): Configuration of CastPyspark to be tested.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del cast_confeti_pyspark[missing_property]
            JsonHandler.validate_json(cast_confeti_pyspark, TransformSpecPyspark.confeti_schema)
