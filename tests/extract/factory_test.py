"""
ExtractFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Create Extract class from Factory by spec format.
| ✓ | Raise NotImplementedError in Factory if spec combination is not implemented.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.extract.base import Extract, ExtractSpec
from datastore.extract.factory import ExtractFactory

# =========================================
# ====== ExtractFactory class ==============
# =========================================

# =============== Fixtures ================


@pytest.fixture(name="extract_factory")
def fixture_extract_factory(extract_spec: ExtractSpec) -> Generator[Extract, None, None]:
    """
    Fixture for creating a Extract instance from ExtractFactory.

    Args:
        extract_spec (ExtractSpec): Specification for the desired Extract instance.

    Yields:
        Extract: A Extract instance created using the specified ExtractSpec.

    Raises:
        pytest.fail: If the combination of format and method is not handled by ExtractFactory.
    """
    try:
        yield ExtractFactory.get(spec=extract_spec)
    except NotImplementedError as e:
        pytest.fail(
            f"A combination of {extract_spec.data_format}-{extract_spec.method} is not handled by ExtractFactory: {e}"
        )


# ============== Tests =================


def test_extract_factory_create_extract(extract_factory: Extract) -> None:
    """
    Test creating a Extract instance from the ExtractFactory.

    Args:
        extract_factory (Extract): Extract intance fixture from ExtractFactory.
    """
    # Assert
    assert isinstance(extract_factory, Extract)
