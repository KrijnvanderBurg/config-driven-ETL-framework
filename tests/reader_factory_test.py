"""
ReaderFactory class tests.

# | ✓ | Tests
# |---|-----------------------------------------------------------
# | ✓ | Create Reader class from Factory by spec format.
# | ✓ | Raise NotImplementedError in Factory if spec combination is not implemented.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.reader import Reader, ReaderSpec
from datastore.reader_factory import ReaderFactory

# =========================================
# ====== ReaderFactory class ==============
# =========================================

# =============== Fixtures ================


@pytest.fixture(name="reader_factory")
def fixture_reader_factory(reader_spec: ReaderSpec) -> Generator[Reader, None, None]:
    """
    Fixture for creating a Reader instance from ReaderFactory.

    Args:
        reader_spec (ReaderSpec): Specification for the desired Reader instance.

    Yields:
        Reader: A Reader instance created using the specified ReaderSpec.

    Raises:
        pytest.fail: If the combination of reader_format and reader_type is not handled by ReaderFactory.
    """
    try:
        yield ReaderFactory.get(spec=reader_spec)
    except NotImplementedError as e:
        pytest.fail(
            f"A combination of {reader_spec.reader_format}-{reader_spec.reader_type} "
            f" is not handled by ReaderFactory: {e}"
        )


# ============== Tests =================


def test_reader_factory_create_reader(reader_factory: Reader) -> None:
    """
    Test creating a Reader instance from the ReaderFactory.

    Args:
        reader_factory (Reader): Reader intance fixture from ReaderFactory.
    """
    # Assert
    assert isinstance(reader_factory, Reader)
