"""
Load class tests.

| ✓ | Tests
|---|-----------------------------------
| ✓ | Implement load ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.load.base import Load, LoadSpec
from pyspark.sql import DataFrame

# =========== Fixtures =============

# ============= Tests ==============


def test_load_abc_implementation(df: DataFrame, load_spec: LoadSpec) -> None:
    """
    Test implementation of abstract Load class.

    Args:
        load_spec (LoadSpec): LoadSpec fixture.
        df (DataFrame): Test dataframe with schema but without rows fixture.
    """
    # Arrange
    Load.__abstractmethods__ = frozenset()

    # Act
    class MockLoad(Load):
        """Mock implementation of Load class for testing purposes."""

        def load(self):
            """Mock implementation of Load class for testing purposes."""

    # Assert
    assert isinstance(MockLoad(spec=load_spec, dataframe=df), Load)
