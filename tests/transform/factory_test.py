"""
TransformFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Create Transform class from Strategy by spec format.
| ✓ | Raise NotImplementedError in Strategy if spec combination is not implemented.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.transform.strategy import TransformContext, TransformStrategy
from pyspark.sql import DataFrame

# =============== Fixtures ================

# ================ Tests ==================


def test_transform_factory(transform_spec_matrix, df: DataFrame) -> None:
    """
    Assert that TransformFactory returns a TransformSpec instance from valid input.

    Args:
        transform_spec_matrix (TransformSpec): TransformSpec fixture.
        df (DataFrame): DataFrame fixture.

    Raises:
        pytest.fail: If the combination of format and method is unsupported by TransformFactory.
    """
    try:
        # Act
        transform = TransformContext.get(spec=transform_spec_matrix, df=df)

        # Assert
        assert isinstance(transform, TransformStrategy)

    # Assert
    except NotImplementedError:
        pytest.fail(f"Combination {transform.spec} is not supported.")
