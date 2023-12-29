"""
Reader classes tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.reader import ReaderSpec
from datastore.reader_factory import ReaderFactory
from datastore.readers.parquet_reader import ParquetReader

# ==================================
# ===== ReaderFactory class ========
# ==================================


@pytest.fixture(name="parquet_reader_spec")
def fixture_parquet_reader_spec():
    """Pytest fixture for ReaderSpec with parquet test values."""
    return ReaderSpec(
        spec_id="test_bronze",
        reader_type="batch",
        reader_format="parquet",
        location="/path/to/data/test.parquet",
    )


def test_reader_factory_create_parquet(parquet_reader_spec):
    """Test if ReaderFactory created ParquetReader object from parquet spec."""
    result = ReaderFactory.get(parquet_reader_spec)
    assert isinstance(result, ParquetReader)


def test_reader_factory_create_unsupported_format():
    """Test if NotImplementedError if spec is not supported by factory."""
    spec = ReaderSpec(
        spec_id="test_bronze", reader_type="batch", reader_format="not_supported", location="/path/to/data/test.fail"
    )
    with pytest.raises(NotImplementedError, match="is not supported"):
        ReaderFactory.get(spec)
