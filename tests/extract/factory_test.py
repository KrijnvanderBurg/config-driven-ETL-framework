"""
ExtractFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Matrix test all ExtractSpecs in ExtractFactory return Extract derived class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.extract.base import ExtractSpec, ExtractStrategy
from datastore.extract.strategy import ExtractContext

# =========================================
# ====== ExtractFactory class =============
# =========================================

# =============== Fixtures ================

# ================= Tests =================


def test_extract_factory(extract_spec_matrix: ExtractSpec) -> None:
    """
    Assert that ExtractFactory returns a ExtractSpec instance from valid input.

    Args:
        extract_spec_matrix (ExtractSpec): ExtractSpec fixture.

    Raises:
        pytest.fail: If the combination of format and method is unsupported by ExtractFactory.
    """
    try:
        # Act
        extract = ExtractContext.factory(spec=extract_spec_matrix)
        # Assert
        assert isinstance(extract, ExtractStrategy)
    # Assert
    except NotImplementedError:
        pytest.fail(f"Combination {extract_spec_matrix.data_format}-{extract_spec_matrix.method} is not supported.")
