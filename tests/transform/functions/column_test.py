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

from datastore.transform.base import TransformFunction
from pyspark import testing
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, MapType, StringType, StructField, StructType

# ============ Fixtures ================

# ============== Tests =================


def test_transform_column_cast(spark: SparkSession) -> None:
    """
    Assert that cast transform function changes Column type to new type.

    Args:
        spark (SparkSession): Spark session fixture.
    """
    # Arrange
    data = [("Alice", 27), ("Bob", 32)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)

    confeti = {
        "function": "cast",
        "arguments": {
            "columns": {
                "age": "StringType",
            }
        },
    }

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)
    transformed_df = df.transform(func=transform.function)

    # Assert
    expected_data = [("Alice", "27"), ("Bob", "32")]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", StringType(), True),
        ]
    )
    expexted_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    testing.assertDataFrameEqual(actual=transformed_df, expected=expexted_df)


def test_transform_column_select_with_alias(spark: SparkSession):
    """
    Assert that select_with_alias transform function selects columns with alias and returned in the given order.

    Args:
        spark (SparkSession): Spark session fixture.
    """
    # Arrange
    data = [("Alice", 27), ("Bob", 32)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)

    confeti = {
        "function": "select_with_alias",
        "arguments": {
            "columns": {
                "age": "years_old",
            }
        },
    }

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)
    transformed_df = df.transform(func=transform.function)

    # Assert
    expected_data = [(27,), (32,)]
    expected_schema = StructType(
        [
            StructField("years_old", IntegerType(), True),
        ]
    )
    expexted_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    testing.assertDataFrameEqual(actual=transformed_df, expected=expexted_df)


def test_transform_column_rename(spark: SparkSession) -> None:
    """
    Assert that rename transform function renames column name.

    Args:
        spark (SparkSession): Spark session fixture.
    """
    # Arrange
    data = [("Alice", 27), ("Bob", 32)]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)

    confeti = {
        "function": "rename",
        "arguments": {
            "columns": {
                "name": "full_name",
            }
        },
    }

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)
    transformed_df = df.transform(func=transform.function)

    # Assert
    expected_data = [("Alice", 27), ("Bob", 32)]
    expected_schema = StructType(
        [
            StructField("full_name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    expexted_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    testing.assertDataFrameEqual(actual=transformed_df, expected=expexted_df)


def test_transform_column_explode_array(spark: SparkSession) -> None:
    """
    Assert that explode_array transforms array column to rows.

    Args:
        spark (SparkSession): The Spark session object.
    """
    # Arrange
    confeti = {"function": "explode_array", "arguments": {"columns": ["languages"]}}
    data = [
        ("bert", ["c", "python"]),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("languages", ArrayType(StringType()), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)
    transformed_df = df.transform(transform.function)

    # Assert
    expected_data = [
        ("bert", "c"),
        ("bert", "python"),
    ]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("languages", StringType(), True),
        ]
    )
    expexted_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    testing.assertDataFrameEqual(actual=transformed_df, expected=expexted_df)


def test_transform_column_explode_map(spark: SparkSession) -> None:
    """
    Assert that explode_map transforms map column to rows.

    Args:
        spark (SparkSession): The Spark session object.
    """
    # Arrange
    confeti = {"function": "explode_map", "arguments": {"columns": ["grades"]}}
    data = [
        ("bert", {"python": 9, "java": 8}),
    ]
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("grades", MapType(StringType(), IntegerType()), True),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)

    # Act
    transform = TransformFunction.from_confeti(confeti=confeti)
    transformed_df = df.transform(func=transform.function)

    # Assert
    expected_data = [
        ("bert", {"key": "python", "value": 9}),
        ("bert", {"key": "java", "value": 8}),
    ]
    expected_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField(
                "grades",
                StructType(
                    [
                        StructField("key", StringType(), True),
                        StructField("value", IntegerType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    expexted_df = spark.createDataFrame(data=expected_data, schema=expected_schema)

    testing.assertDataFrameEqual(actual=transformed_df, expected=expexted_df)
