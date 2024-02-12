"""
ColumnCast class tests.

| ✓ | Tests
|---|------------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test load spec from confeti are of correct type.
| ✓ | Column type was changed to new type.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Callable

import pytest
from datastore.transform.base import TransformFunction
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType

# ============ Fixtures ================


@pytest.fixture(name="transform_cast")
def fixture_transform_cast_confeti() -> TransformFunction:
    """
    Fixture for Transform instance.

    Returns:
        (Transform): Transform with cast function fixture.
    """
    return TransformFunction(function="cast", arguments={"cols": {"age": "LongType"}})


# ============== Tests =================


def test_transform_column_cast_type(transform_cast: TransformFunction):
    """
    Assert that all Transform cast attributes are of correct type.

    Args:
        transform_cast (Transform): Transform with cast function fixture.
    """
    # Assert
    assert isinstance(transform_cast.function, Callable)


def test_transform_column_cast_from_confeti() -> None:
    """
    Assert that Transform from_confeti method of column cast returns valid Transform.
    """
    # Arrange
    confeti = {"function": "cast", "arguments": {"cols": {"age": "LongType"}}}

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)

    # Assert
    assert isinstance(transform.function, Callable)


def test_transform_column_cast(df: DataFrame, transform_cast: TransformFunction):
    """
    Column type was changed to new type.

    Args:
        df (Dataframe): DataFrame fixture.
        transform_cast (TransformFunction): TransformFunction column cast fixture.
    """
    # Arrange
    transformed_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("job_title", StringType(), True),
        ]
    )

    # Act
    transformed_df = df.transform(transform_cast.function)

    # Arrange
    assert transformed_df.schema == transformed_schema
