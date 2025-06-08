"""
Load class tests.


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

from stratum.models.job.load_model import LoadFormat, LoadMethod, LoadMode, LoadModelPyspark


class TestLoadMethod:
    """
    Test class for LoadMethod enum.
    """

    load_model__method__values = [
        "batch",
        "streaming",
    ]  # Purposely not dynamically get all values as trip wire

    if load_model__method__values.sort() == sorted([enum.value for enum in LoadMethod]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadMethod values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_model__method__values)
    def test__valid_members(self, valid_value: str) -> None:
        """
        Test creating LoadMethod from valid values.

        Args:
            valid_value (str): Valid values for LoadMethod.
        """
        # Act
        load_method = LoadMethod(valid_value)

        # Assert
        assert isinstance(load_method, LoadMethod)
        assert load_method.value == valid_value


class TestLoadMode:
    """
    Test class for LoadMode enum.
    """

    load_model__mode__values = [
        "complete",
        "append",
        "update",
    ]  # Purposely not dynamically get all values as trip wire

    if load_model__mode__values.sort() == sorted([enum.value for enum in LoadMode]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadMode values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_model__mode__values)
    def test__valid_members(self, valid_value: str) -> None:
        """
        Test creating LoadMode from valid values.

        Args:
            valid_value (str): Valid values for LoadMode.
        """
        # Act
        load_mode = LoadMode(valid_value)

        # Assert
        assert isinstance(load_mode, LoadMode)
        assert load_mode.value == valid_value


class TestLoadFormat:
    """
    Test class for LoadFormat enum.
    """

    load_model__format__values = [
        "parquet",
        "json",
        "csv",
    ]  # Purposely not dynamically get all values as trip wire

    if load_model__format__values.sort() == sorted([enum.value for enum in LoadFormat]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadFormat values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_model__format__values)
    def test__valid_members(self, valid_value: str) -> None:
        """
        Test creating LoadFormat from valid values.

        Args:
            valid_value (str): Valid values for LoadFormat.
        """
        # Act
        load_format = LoadFormat(valid_value)

        # Assert
        assert isinstance(load_format, LoadFormat)
        assert load_format.value == valid_value


# LoadModelPyspark fixtures


@pytest.fixture(name="load_model_pyspark")
def fixture__load_model__pyspark() -> LoadModelPyspark:
    """
    Fixture for creating LoadModel from valid value.

    Returns:
        LoadModelPyspark: LoadModelPyspark fixture.
    """
    return LoadModelPyspark(
        name="load_name",
        upstream_name="transform_name",
        method=LoadMethod.BATCH.value,
        mode=LoadMode.COMPLETE.value,
        data_format=LoadFormat.CSV.value,
        location=f"path/load.{LoadFormat.CSV.value}",
        schema_location="path/load_schema.json",
        options={},
    )


@pytest.fixture(name="load_model_confeti_pyspark")
def fixture__load_model__confeti__pyspark(load_model_pyspark: LoadModelPyspark) -> dict[str, Any]:
    """
    Fixture for creating LoadModel from confeti.

    Args:
        load_model_pyspark (LoadModelPyspark): LoadModelPyspark instance fixture.

    Returns:
        dict[str, Any]: LoadModelPyspark confeti fixture.
    """

    return {
        "name": load_model_pyspark.name,
        "upstream_name": load_model_pyspark.upstream_name,
        "method": load_model_pyspark.method.value,
        "mode": load_model_pyspark.mode.value,
        "data_format": load_model_pyspark.data_format.value,
        "location": f"path/load.{load_model_pyspark.data_format.value}",
        "schema_location": "path/load_schema.json",
        "options": {},
    }


# LoadModelPyspark tests


class TestLoadModelPyspark:
    """
    Test class for LoadModel class.
    """

    # def test__from_confeti(
    #     self, load_model_pyspark: LoadModelPyspark, load_model_confeti_pyspark: dict[str, Any]
    # ) -> None:
    #     """
    #     Assert that all LoadModelPyspark attributes are of correct type.

    #     Args:
    #         load_model_pyspark (LoadModelPyspark): LoadModelPyspark instance fixture.
    #         load_model_confeti_pyspark (dict[str, Any]): LoadModelPyspark confeti fixture.
    #     """
    #     # Act
    #     load_model = LoadModelPyspark.from_confeti(confeti=load_model_confeti_pyspark)

    #     # Assert
    #     assert load_model_pyspark.name == load_model.name
    #     assert load_model_pyspark.upstream_name == load_model.upstream_name
    #     assert load_model_pyspark.method == load_model.method
    #     assert load_model_pyspark.mode == load_model.mode
    #     assert load_model_pyspark.data_format == load_model.data_format
    #     assert load_model_pyspark.location == load_model.location
    #     assert load_model_pyspark.schema_location == load_model.schema_location
    #     assert load_model_pyspark.options == load_model.options
