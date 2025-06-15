"""Built-in transform functions for data manipulation.

This module contains concrete implementations of transformation functions
that can be used in the ingestion framework to manipulate data.

Each transform function is designed to perform a specific data operation
and is automatically registered with the TransformFunctionRegistry when imported.
This allows the functions to be referenced by name in configuration files.

Available transforms:
- SelectFunction: Select columns from a DataFrame
- CalculateBirthYearFunction: Calculate birth year from age
"""
