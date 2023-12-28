import pytest
from datastore.reader import ReaderFormat, ReaderType, ReaderSpec, Reader, ReaderFactory
from datastore.reader.parquet_reader import ParquetReader

# ==================================
# ====== ReaderFormat class ========
# ==================================

def test_reader_format_exists_parquet():
    # test if exists function works
    # test if enum is correct string representation
    # trip wire test if parquet is deleted as reader option
    assert ReaderFormat.exists("parquet")
    assert str(ReaderFormat.PARQUET) == "parquet"

def test_reader_format_exists_json():
    # test if exists function works
    # test if enum is correct string representation
    # trip wire test if json is deleted as reader option
    assert ReaderFormat.exists("json")
    assert str(ReaderFormat.JSON) == "json"

def test_reader_format_exists_csv():
    # test if exists function works
    # test if enum is correct string representation
    # trip wire test if csv is deleted as reader option
    assert ReaderFormat.exists("csv")
    assert str(ReaderFormat.CSV) == "csv"

def test_reader_format_enum_iteration():
    # dynamic test if all enums are returned true in exists
    for format_enum in ReaderFormat:
        assert ReaderFormat.exists(format_enum.value)
    
def test_reader_format_invalid_creation():
    # trip wire test if (invalid) enum is added
    with pytest.raises(ValueError, match = "Invalid value for ReaderFormat"):
        ReaderFormat("invalid_format")

def test_reader_format_values():
    # test if all expect enums exist
    # if new reader formats are added then they have to be added to this test also
    expected_values = {"parquet", "json", "csv"}
    assert set(ReaderFormat.values()) == expected_values, "Enum values should match the expected set"

# ==================================
# ======= ReaderTypeclass ==========
# ==================================

def test_reader_type_values():
    # trip wire test if all reader type enum exists
    expected_values = {"batch", "streaming"}
    assert set(ReaderType.values()) == expected_values, "Enum values should match the expected set"

def test_reader_type_exists_batch():
    # trip wire test if batch enum exists
    # test if batch enum is correct string representation
    assert str(ReaderType.STREAMING) == "batch"

def test_reader_type_exists_streaming():
    # trip wire test if streaming enum exists
    # test if streaming enum is correct string representation
    assert str(ReaderType.STREAMING) == "streaming"

# ==================================
# ======= ReaderSpec class =========
# ==================================

@pytest.fixture
def parquet_reader_spec():
    return ReaderSpec(
        id = "test_bronze",
        reader_type = "batch",
        reader_format = "parquet",
        df_name = "df_test_bronze",
        location = "/path/to/data/test.parquet",
    )

def test_reader_spec_creation(parquet_reader_spec):
    # test if object was correctly initialised
    assert parquet_reader_spec.id == "test_bronze"
    assert parquet_reader_spec.reader_type == ReaderType.BATCH
    assert parquet_reader_spec.reader_format == ReaderFormat.PARQUET
    assert parquet_reader_spec.df_name == "df_test_bronze"
    assert parquet_reader_spec.location == "/path/to/data/test.parquet"

def test_reader_spec_default_values():
    spec = ReaderSpec(id="test_bronze", reader_type = "batch")
    assert spec.reader_format is None
    assert spec.df_name is None
    assert spec.location is None

def test_reader_spec_invalid_type():
    with pytest.raises(ValueError, match = "Invalid reader_type value"):
        ReaderSpec(id="test_bronze", reader_type = "invalid_type")

# ==================================
# ========= Reader class ===========
# ==================================

class MockReader(Reader):
    def read(self):
        # Mock implementation for testing purposes
        pass

def test_reader_creation():
    spec = ReaderSpec(id = "test_bronze", reader_type = "batch")
    reader = MockReader(spec = spec)
    assert reader._spec == spec

# ==================================
# ===== ReaderFactory class ========
# ==================================

def test_reader_factory_create_parquet(parquet_reader_spec):
    # test if ReaderFactory created ParquetReader object from parquet spec
    result = ReaderFactory(parquet_reader_spec)
    assert isinstance(result, ParquetReader)

def test_reader_factory_create_unsupported_format():
    # test if NotImplementedError if spec is not supported by factory
    spec = ReaderSpec(id="test_bronze", reader_type = "batch", reader_format = "not_supported", location = "/path/to/data/test.fail")
    with pytest.raises(NotImplementedError, match = "is not supported"):
        ReaderFactory(spec)
