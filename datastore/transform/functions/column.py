"""
Cast transform function.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections import OrderedDict
from collections.abc import Callable

import pyspark.sql.types as spark_types
from pyspark.sql import DataFrame, functions


class ColumnTransformer:
    """
    Encapsulates column transformation logic for PySpark DataFrames.
    """

    @classmethod
    def cast(cls, columns: dict[str, str]) -> Callable:
        """
        Casts column(s) to new type.

        Args:
            columns (dict): Dictionary with column names as keys and target data types as values.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'cast' function:

            ```
            {"function": "cast", "arguments": {"columns": {"age": "StringType"}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: string (nullable = true)
            ```
        """

        def f(df: DataFrame) -> DataFrame:
            for column, cast_type in columns.items():
                target_type = getattr(spark_types, cast_type)()
                df = df.withColumn(column, functions.col(column).cast(target_type))
            return df

        return f

    @classmethod
    def select_with_alias(cls, columns: OrderedDict) -> Callable:
        """
        Selects columns with aliases.

        Args:
            columns (OrderedDict): Ordered mapping of column names to aliases.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'select_with_alias' function:

            ```
            {"function": "select_with_alias", "arguments": {"columns": {"age": "years_old",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- years_old: integer (nullable = true)
            ```
        """

        def f(df: DataFrame) -> DataFrame:
            selected_columns = []
            for column, alias in columns.items():
                selected_columns.append(functions.col(column).alias(alias))
            df = df.select(*selected_columns)
            return df

        return f

    @classmethod
    def rename(cls, columns: dict[str, str]) -> Callable:
        """
        Renames columns.

        Args:
            columns (dict[str, str]): Dictionary mapping old column names to new column names.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            root
            |-- name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```

            Applying the confeti 'rename' function:

            ```
            {"function": "rename", "arguments": {"columns": {"name": "full_name",}}}
            ```

            The resulting DataFrame schema will be:

            ```
            root
            |-- full_name: string (nullable = true)
            |-- age: integer (nullable = true)
            ```
        """

        def f(df: DataFrame) -> DataFrame:
            for old_name, new_name in columns.items():
                df = df.withColumnRenamed(old_name, new_name)
            return df

        return f

    @classmethod
    def explode_array(cls, columns: list[str]) -> Callable:
        """
        Explodes ArrayType columns.

        Args:
            columns (list[str]): List of ArrayType column names to explode.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame schema:

            ```
            +----+-----------+
            |name|  languages|
            +----+-----------+
            |bert|[c, python]|
            +----+-----------+
            ```

            Applying the confeti 'rename' function:

            ```
            {"function": "explode_array", "arguments": {"columns": ["languages",]}}
            ```

            The resulting DataFrame schema will be:

            ```
            +----+---------+
            |name|languages|
            +----+---------+
            |bert|        c|
            |bert|   python|
            +----+---------+
            ```
        """

        def f(df: DataFrame) -> DataFrame:
            for column in columns:
                df = df.withColumn(column, functions.explode_outer(functions.col(column)))
            return df

        return f

    @classmethod
    def explode_map(cls, columns: list[str]) -> Callable:
        """
        Explodes MapType columns.

        Args:
            columns (list[str]): List of MapType column names to explode.

        Returns:
            (Callable): Function for `DataFrame.transform()`.

        Examples:
            Consider the following DataFrame and schema:

            ```
            +----+--------------------+
            |name|              grades|
            +----+--------------------+
            |bert|{python -> 9, jav...|
            +----+--------------------+

            root
            |-- name: string (nullable = true)
            |-- grades: map (nullable = true)
            |    |-- key: string
            |    |-- value: integer (valueContainsNull = true)
            ```

            Applying the confeti 'rename' function:

            ```
            {"function": "explode_map", "arguments": {"columns": ["languages", ]}}
            ```

            The resulting DataFrame and schema will be:

            ```
            +----+-----------+
            |name|     grades|
            +----+-----------+
            |bert|{python, 9}|
            |bert|  {java, 8}|
            +----+-----------+

            root
            |-- name: string (nullable = true)
            |-- grades: struct (nullable = true)
            |    |-- key: string (nullable = false)
            |    |-- value: integer (nullable = true)
            ```
        """

        def f(df: DataFrame) -> DataFrame:
            for column in columns:
                df = df.withColumn(column, functions.explode_outer(functions.map_entries(functions.col(column))))
            return df

        return f
