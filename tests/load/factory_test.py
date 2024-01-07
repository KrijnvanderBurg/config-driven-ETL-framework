"""
LoadFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Create Load class from Factory by spec format.
| ✓ | Raise NotImplementedError in Factory if spec combination is not implemented.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.load.base import Load, LoadSpec
from datastore.load.factory import LoadFactory
from pyspark.sql import DataFrame

# =========================================
# ====== LoadFactory class ==============
# =========================================

# =============== Fixtures ================


@pytest.fixture(name="load_factory")
def fixture_load_factory(load_spec: LoadSpec, df: DataFrame) -> Generator[Load, None, None]:
    """
    Fixture for creating a Load instance from LoadFactory.

    Args:
        load_spec (LoadSpec): Specification for the desired Load instance.
        df (pyspark.sql.DataFrame): Test DataFrame.

    Yields:
        Load: A Load instance created using the specified LoadSpec.

    Raises:
        pytest.fail: If the combination of format and method is not handled by LoadFactory.
    """
    try:
        yield LoadFactory.get(spec=load_spec, dataframe=df)
    except NotImplementedError as e:
        pytest.fail(f"Unsupported combination {load_spec.data_format}-{load_spec.method}-{load_spec.operation}: {e}")


# ================ Tests ==================


def test_load_factory_create_load(load_factory: Load) -> None:
    """
    Test creating a Load instance from the LoadFactory.

    Args:
        load_factory (Load): The Load instance created by the LoadFactory.
    """
    # Assert
    assert isinstance(load_factory, Load)
