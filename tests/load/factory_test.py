"""
LoadFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Matrix test all LoadSpecs in LoadFactory return Load derived class.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.load.base import Load, LoadSpec
from datastore.load.factory import LoadFactory
from pyspark.sql import DataFrame

# =========================================
# ====== LoadFactory class =============
# =========================================

# =============== Fixtures ================

# ================= Tests =================


def test_load_factory_get(df: DataFrame, load_spec_matrix: LoadSpec) -> None:
    """
    Assert that LoadFactory returns a LoadSpec instance from valid input.

    Args:
        df (DataFrame): DataFrame fixture.
        load_spec_matrix (LoadSpec): LoadSpec fixture.

    Raises:
        pytest.fail: If the combination of format and method is unsupported by LoadFactory.
    """
    try:
        # Act
        load = LoadFactory.get(spec=load_spec_matrix, dataframe=df)
        # Assert
        assert isinstance(load, Load)
    # Assert
    except NotImplementedError:
        pytest.fail(f"Combination {load_spec_matrix.data_format}-{load_spec_matrix.method} is unsupported.")
