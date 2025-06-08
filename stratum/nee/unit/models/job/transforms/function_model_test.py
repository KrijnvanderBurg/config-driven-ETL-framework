"""
Column cast class tests.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from typing import Any

import pytest
from pyspark.sql import types

from stratum.models.job.transforms.function_model import ArgsAbstract, FunctionModelPyspark
from stratum.models.job.transforms.functions.cast_model import CastFunctionModelPysparkArgs

# FuncPyspark fixtures


@pytest.fixture(name="func_pyspark")
def fixture__func__pyspark() -> FunctionModelPyspark:
    """
    Fixture for creating a FuncPyspark instance.

    Returns:
        FuncPyspark: The created FuncPyspark instance.
    """
    # Arrange
    args = CastFunctionModelPysparkArgs(column_name="age", data_type=types.StringType())
    func = FunctionModelPyspark(function="func", arguments=args)

    return func


@pytest.fixture(name="func_confeti_pyspark")
def fixture__func__confeti__pyspark() -> dict[str, Any]:
    """
    Fixture for creating a FuncPyspark instance from confeti.

    Returns:
        dict[str, Any]: The created FuncPyspark instance from confeti.
    """
    return {"function": "func", "arguments": {}}


# FuncPyspark tests


class TestFuncPyspark:
    """
    Test class for TODO
    """

    def test__init(self, func_pyspark: FunctionModelPyspark) -> None:
        """
        Test case for initializing FuncPyspark.

        Args:
            func_pyspark (FuncPyspark): Instance of FuncPyspark.
        """
        # Assert
        assert func_pyspark.function == "func"
        assert isinstance(func_pyspark.arguments, ArgsAbstract)

    # @pytest.mark.parametrize("missing_property", ["function", "arguments"])
    # def test__validate_json__missing_required_properties(
    #     self,
    #     missing_property: str,
    #     func_confeti_pyspark: dict,
    # ) -> None:
    #     """
    #     Test case for validating missing required properties individually.

    #     Args:
    #         missing_property (str): The property to be removed for testing.
    #         cast_confeti_pyspark (dict[str, Any]): Configuration of CastPyspark to be tested.
    #     """
    #     with pytest.raises(jsonschema.ValidationError):  # Assert
    #         # Act
    #         del func_confeti_pyspark[missing_property]
    #         JsonHandler.validate_json(func_confeti_pyspark, TransformModelPyspark.confeti_schema)
