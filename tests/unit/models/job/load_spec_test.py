"""
Load class tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema
import pytest

from datastore.models.job.load_spec import LoadFormat, LoadMethod, LoadOperation, LoadSpecPyspark
from datastore.utils.json_handler import JsonHandler


class TestLoadMethod:
    """
    Test class for LoadMethod enum.
    """

    load_spec__method__values = ["batch", "streaming"]  # Purposely not dynamically get all values as trip wire

    if load_spec__method__values.sort() == sorted([enum.value for enum in LoadMethod]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadMethod values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_spec__method__values)
    def test__valid_members(self, valid_value: str):
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


class TestLoadOperation:
    """
    Test class for LoadOperation enum.
    """

    load_spec__operation__values = [
        "complete",
        "append",
        "update",
    ]  # Purposely not dynamically get all values as trip wire

    if load_spec__operation__values.sort() == sorted([enum.value for enum in LoadOperation]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadOperation values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_spec__operation__values)
    def test__valid_members(self, valid_value: str):
        """
        Test creating LoadOperation from valid values.

        Args:
            valid_value (str): Valid values for LoadOperation.
        """
        # Act
        load_operation = LoadOperation(valid_value)

        # Assert
        assert isinstance(load_operation, LoadOperation)
        assert load_operation.value == valid_value


class TestLoadFormat:
    """
    Test class for LoadFormat enum.
    """

    load_spec__format__values = ["parquet", "json", "csv"]  # Purposely not dynamically get all values as trip wire

    if load_spec__format__values.sort() == sorted([enum.value for enum in LoadFormat]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match LoadFormat values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", load_spec__format__values)
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


# LoadSpecPyspark fixtures


@pytest.fixture(name="load_spec_pyspark")
def fixture__load_spec__pyspark() -> LoadSpecPyspark:
    """
    Fixture for creating LoadSpec from valid value.

    Returns:
        LoadSpecPyspark: LoadSpecPyspark fixture.
    """
    return LoadSpecPyspark(
        name="bronze-test-load-dev",
        method=LoadMethod.BATCH.value,
        operation=LoadOperation.COMPLETE.value,
        data_format=LoadFormat.CSV.value,
        location=f"path/load.{LoadFormat.CSV.value}",
    )


@pytest.fixture(name="load_spec_confeti_pyspark")
def fixture__load_spec__confeti__pyspark(load_spec_pyspark: LoadSpecPyspark) -> dict:
    """
    Fixture for creating LoadSpec from confeti.

    Args:
        load_spec_pyspark (LoadSpecPyspark): LoadSpecPyspark instance fixture.

    Returns:
        dict: LoadSpecPyspark confeti fixture.
    """

    return {
        "name": load_spec_pyspark.name,
        "method": load_spec_pyspark.method.value,
        "operation": load_spec_pyspark.operation.value,
        "data_format": load_spec_pyspark.data_format.value,
        "location": f"path/load.{load_spec_pyspark.data_format.value}",
    }


# LoadSpecPyspark tests


class TestLoadSpecPyspark:
    """
    Test class for LoadSpec class.
    """

    def test__init(self, load_spec_pyspark: LoadSpecPyspark) -> None:
        """
        Assert that all LoadSpecPyspark attributes are of correct type.

        Args:
            load_spec_pyspark (LoadSpecPyspark): LoadSpecPyspark instance fixture.
        """
        # Assert
        assert isinstance(load_spec_pyspark.name, str)
        assert isinstance(load_spec_pyspark.method, LoadMethod)
        assert isinstance(load_spec_pyspark.operation, LoadOperation)
        assert isinstance(load_spec_pyspark.data_format, LoadFormat)
        assert isinstance(load_spec_pyspark.location, str)

    def test__from_confeti(self, load_spec_pyspark: LoadSpecPyspark, load_spec_confeti_pyspark: dict) -> None:
        """
        Assert that all LoadSpecPyspark attributes are of correct type.

        Args:
            load_spec_pyspark (LoadSpecPyspark): LoadSpecPyspark instance fixture.
            load_spec_confeti_pyspark (dict): LoadSpecPyspark confeti fixture.
        """
        # Act
        load_spec = LoadSpecPyspark.from_confeti(confeti=load_spec_confeti_pyspark)

        # Assert
        assert load_spec_pyspark.method == load_spec.method
        assert load_spec_pyspark.operation == load_spec.operation
        assert load_spec_pyspark.data_format == load_spec.data_format
        assert load_spec_pyspark.location == load_spec.location

    @pytest.mark.parametrize("missing_property", ["name", "method", "operation", "data_format", "location"])
    def test__validate_json__missing_required_properties(self, missing_property: str, load_spec_confeti_pyspark: dict):
        """
        Test case for validating that each required property is missing one at a time.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del load_spec_confeti_pyspark[missing_property]
            JsonHandler.validate_json(load_spec_confeti_pyspark, LoadSpecPyspark.confeti_schema)
