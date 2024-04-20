"""
Engine class tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema
import pytest

from datastore.models.job.strategy_spec import StrategyEngine, StrategySpecPyspark
from datastore.utils.json_handler import JsonHandler


class TestEngine:
    """
    Test class for Engine enum.
    """

    strategy_spec__method__values = [
        "pyspark",
    ]  # Purposely not dynamically get all values as trip wire

    if strategy_spec__method__values.sort() == sorted([enum.value for enum in StrategyEngine]):  # type: ignore
        raise pytest.fail(
            "Testing values does not match Engine values."
            "Did you add or remove some? update tests and fixtures accordingly."
        )

    @pytest.mark.parametrize("valid_value", strategy_spec__method__values)
    def test__valid_members(self, valid_value: str):
        """
        Test creating Engine from valid values.

        Args:
            valid_value (str): Valid values for Engine.
        """
        # Act
        strategy_method = StrategyEngine(valid_value)

        # Assert
        assert isinstance(strategy_method, StrategyEngine)
        assert strategy_method.value == valid_value


# StrategySpecPyspark fixtures


@pytest.fixture(name="strategy_spec_pyspark")
def fixture__strategy_spec__pyspark() -> StrategySpecPyspark:
    """
    Matrix fixture for creating StrategySpec from valid value.

    Returns:
        StrategySpecPyspark: StrategySpecPyspark fixture.
    """
    return StrategySpecPyspark(engine=StrategyEngine.PYSPARK.value, options={})


@pytest.fixture(name="strategy_spec_confeti_pyspark")
def fixture__strategy_spec__confeti__pyspark(strategy_spec_pyspark: StrategySpecPyspark) -> dict:
    """
    Fixture for creating ExtractSpec from confeti.

    Args:
        strategy_spec_pyspark (StrategySpecPyspark): StrategySpecPyspark instance fixture.

    Returns:
        dict: ExtractSpecPyspark confeti fixture.
    """

    return {"engine": strategy_spec_pyspark.engine.value, "options": strategy_spec_pyspark.options}


# StrategySpecPyspark tests


class TestStrategySpecPyspark:
    """
    Test class for StrategySpec class.
    """

    def test__init(self, strategy_spec_pyspark: StrategySpecPyspark) -> None:
        """
        Assert that all StrategySpecPyspark attributes are of correct type.

        Args:
            strategy_spec_pyspark (StrategySpecPyspark): StrategySpecPyspark instance fixture.
        """
        # Assert
        assert isinstance(strategy_spec_pyspark.engine, StrategyEngine)
        assert isinstance(strategy_spec_pyspark.options, dict)

    def test__from_confeti(
        self, strategy_spec_pyspark: StrategySpecPyspark, strategy_spec_confeti_pyspark: dict
    ) -> None:
        """
        Assert that all StrategySpecPyspark attributes are of correct type.

        Args:
            strategy_spec_pyspark (StrategySpecPyspark): StrategySpecPyspark instance fixture.
            strategy_spec_confeti_pyspark (dict): StrategySpecPyspark confeti fixture.
        """
        # Act
        strategy_spec = StrategySpecPyspark.from_confeti(confeti=strategy_spec_confeti_pyspark)

        # Assert
        assert strategy_spec_pyspark.engine == strategy_spec.engine
        assert strategy_spec_pyspark.options == strategy_spec.options

    @pytest.mark.parametrize("missing_property", ["engine"])
    def test__validate_json__missing_required_properties(
        self, missing_property: str, strategy_spec_confeti_pyspark: dict
    ):
        """
        Test case for validating that each required property is missing one at a time.
        """
        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del strategy_spec_confeti_pyspark[missing_property]
            JsonHandler.validate_json(strategy_spec_confeti_pyspark, StrategySpecPyspark.confeti_schema)
