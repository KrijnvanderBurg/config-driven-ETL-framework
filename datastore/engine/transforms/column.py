# """
# Column transform function.


# Copyright (c) Krijn van der Burg.

# This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
# Attribution-NonCommercial-NoDerivs 4.0 International License.
# See the accompanying LICENSE file for details,
# or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
# """

# from abc import ABC, abstractmethod
# from collections import OrderedDict
# from collections.abc import Callable

# import pyspark.sql.types as spark_types
# from pyspark.sql import DataFrame, functions


# class Column(ABC):

#     def cast(columns: dict[str, str]) -> Callable:
#         """
#         Casts column(s) to new type.

#         Args:
#             columns (dict): Dictionary with column names as keys and target data types as values.

#         Returns:
#             (Callable): Function for `DataFrame.transform()`.

#         Examples:
#             Consider the following DataFrame schema:

#             ```
#             root
#             |-- name: string (nullable = true)
#             |-- age: integer (nullable = true)
#             ```

#             Applying the confeti 'cast' function:

#             ```
#             {"function": "cast", "arguments": {"columns": {"age": "StringType"}}}
#             ```

#             The resulting DataFrame schema will be:

#             ```
#             root
#             |-- name: string (nullable = true)
#             |-- age: string (nullable = true)
#             ```
#         """
#         raise NotImplementedError
