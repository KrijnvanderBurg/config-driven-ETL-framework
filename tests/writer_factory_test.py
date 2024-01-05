"""
WriterFactory class tests.

# | ✓ | Tests
# |---|-----------------------------------------------------------
# | ✓ | Create Writer class from Factory by spec format.
# | ✓ | Raise NotImplementedError in Factory if spec combination is not implemented.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator

import pytest
from datastore.writer import Writer, WriterSpec
from datastore.writer_factory import WriterFactory
from pyspark.sql import DataFrame

# =========================================
# ====== WriterFactory class ==============
# =========================================

# =============== Fixtures ================


@pytest.fixture(name="writer_factory")
def fixture_writer_factory(writer_spec: WriterSpec, df: DataFrame) -> Generator[Writer, None, None]:
    """
    Fixture for creating a Writer instance from WriterFactory.

    Args:
        writer_spec (WriterSpec): Specification for the desired Writer instance.
        df (pyspark.sql.DataFrame): Test DataFrame.

    Yields:
        Writer: A Writer instance created using the specified WriterSpec.

    Raises:
        pytest.fail: If the combination of writer_format and writer_type is not handled by WriterFactory.
    """
    try:
        yield WriterFactory.get(spec=writer_spec, dataframe=df)
    except NotImplementedError as e:
        pytest.fail(
            f"Unsupported combination "
            f"{writer_spec.writer_format}-{writer_spec.writer_type}-{writer_spec.writer_operation}: {e}"
        )


# ================ Tests ==================


def test_writer_factory_create_writer(writer_factory: Writer) -> None:
    """
    Test creating a Writer instance from the WriterFactory.

    Args:
        writer_factory (Writer): The Writer instance created by the WriterFactory.
    """
    # Assert
    assert isinstance(writer_factory, Writer)
