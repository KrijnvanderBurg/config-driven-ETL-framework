"""
Reader classes tests.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.reader import Reader, ReaderFormat, ReaderSpec, ReaderType

# ==================================
# ====== ReaderFormat class ========
# ==================================


def test_reader_format_exists_parquet():
    """
    Test if ReaderForm.exists function returns parquet enum.
    Test if Parquet enum returns correct string representation.
    Tripwire test if parquet enum is removed.
    """
    assert ReaderFormat.exists("parquet")
    assert str(ReaderFormat.PARQUET.value) == "parquet"


def test_reader_format_exists_json():
    """
    Test if ReaderForm.exists function returns json enum.
    Test if json enum returns correct string representation.
    Tripwire test if json enum is removed.
    """
    assert ReaderFormat.exists("json")
    assert str(ReaderFormat.JSON.value) == "json"


def test_reader_format_exists_csv():
    """
    Test if ReaderForm.exists function returns csv enum.
    Test if csv enum returns correct string representation.
    Tripwire test if csv enum is removed.
    """
    assert ReaderFormat.exists("csv")
    assert str(ReaderFormat.CSV.value) == "csv"


def test_reader_format_enum_iteration():
    """Test dynamically if all enums exists."""
    for format_enum in ReaderFormat:
        assert ReaderFormat.exists(format_enum.value)


def test_reader_format_invalid_creation():
    """Tripwire test if (invalid) enum is added."""
    with pytest.raises(ValueError, match="not a valid ReaderFormat"):
        ReaderFormat("invalid_format")


def test_reader_format_values():
    """
    Test if all expected enums exist.
    Note: if a new ReaderFormat is added then it has to be added to this test also.
    """
    expected_values = {"parquet", "json", "csv"}
    assert set(ReaderFormat.values()) == expected_values, "Enum values should match the expected set"


# ==================================
# ======= ReaderTypeclass ==========
# ==================================


def test_reader_type_exists_batch():
    """
    Tripwire test if Batch enum exists.
    Test if Batch enum returns correct string representation.
    """
    assert str(ReaderType.BATCH.value) == "batch"


def test_reader_type_exists_streaming():
    """
    Tripwire test if Streaming enum exists.
    Test if Streaming enum returns correct string representation.
    """
    assert str(ReaderType.STREAMING.value) == "streaming"


# ==================================
# ======= ReaderSpec class =========
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


def test_reader_spec_creation(parquet_reader_spec):
    """test if ReaderSpec object is correctly initialised."""
    assert parquet_reader_spec.spec_id == "test_bronze"
    assert parquet_reader_spec.reader_type == ReaderType.BATCH.value
    assert parquet_reader_spec.reader_format == ReaderFormat.PARQUET.value
    assert parquet_reader_spec.location == "/path/to/data/test.parquet"


# def test_reader_spec_default_values():
#    """Test if default ReaderSpec parameter values are set."""
#    spec = ReaderSpec(
#        id="test_bronze", reader_format="parquet", reader_type="batch", location="path/to/data/test.parquet"
#    )
#    # currently no default values exist therefore cannot assert anythign yet
#    # assert spec.attribute_with_defaul_value is None


def test_reader_spec_invalid_type():
    """Test if ValueError is raised if invalid reader_type is set."""
    with pytest.raises(ValueError, match="Invalid reader_type value"):
        ReaderSpec(
            spec_id="test_bronze",
            reader_format="parquet",
            reader_type="invalid_type",
            location="path/to/data/test.parquet",
        )


# ==================================
# ========= Reader class ===========
# ==================================


def test_reader_abc(parquet_reader_spec):
    """Test reader ABC."""
    Reader.__abstractmethods__ = set()

    class MockReader(Reader):
        """Mock implementation of Reader class for testing purposes."""

        def read(self):
            """Mock implementation of Reader class for testing purposes."""

    reader = MockReader(spec=parquet_reader_spec)
    assert reader.read() is None
