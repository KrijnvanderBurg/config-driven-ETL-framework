"""Unit tests for the SelectFunctionModel class."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.column import Column

from ingestion_framework.models.transforms.model_select import DictKeyError, SelectFunctionModel


class TestSelectFunctionModel:
    """Unit tests for the SelectFunctionModel transformation model."""

    @pytest.fixture
    def valid_select_dict(self) -> dict[str, Any]:
        """Return a valid dictionary for creating a SelectFunctionModel."""
        return {"function": "select", "arguments": {"columns": ["column1", "column2"]}}

    @pytest.fixture
    def mock_columns(self) -> list[MagicMock]:
        """Return mock Column objects for testing."""
        return [MagicMock(spec=Column), MagicMock(spec=Column)]

    @pytest.fixture
    def select_args(self, mock_columns) -> SelectFunctionModel.Args:
        """Return SelectFunctionModel.Args instance."""
        return SelectFunctionModel.Args(columns=mock_columns)

    @pytest.fixture
    def select_model_cls(self, select_args) -> SelectFunctionModel:
        """Return initialized SelectFunctionModel instance."""
        return SelectFunctionModel(function="select", arguments=select_args)

    def test_select_function_model_cls_args(self, mock_columns: list[MagicMock], select_args) -> None:
        """Test SelectFunctionModel.Args initialization."""
        assert len(select_args.columns) == 2
        assert select_args.columns[0] is mock_columns[0]
        assert select_args.columns[1] is mock_columns[1]

    def test_select_function_model_cls_initialization(self, select_model_cls: SelectFunctionModel, select_args) -> None:
        """Test that SelectFunctionModel can be initialized with valid parameters."""
        assert select_model_cls.function == "select"
        assert select_model_cls.arguments is select_args
        assert len(select_model_cls.arguments.columns) == 2

    @patch("pyspark.sql.functions.col")
    def test_from_dict_valid(self, mock_col, valid_select_dict) -> None:
        """Test from_dict method with valid dictionary."""
        # Setup
        mock_col.side_effect = MagicMock(spec=Column)

        # Execute
        model = SelectFunctionModel.from_dict(valid_select_dict)

        # Assert
        assert model.function == "select"
        assert len(model.arguments.columns) == 2
        mock_col.assert_any_call("column1")
        mock_col.assert_any_call("column2")

    @patch("pyspark.sql.functions.col")
    def test_from_dict_missing_function(self, _, valid_select_dict) -> None:
        """Test from_dict method with missing function key."""
        # Remove the function key
        invalid_dict = valid_select_dict.copy()
        del invalid_dict["function"]

        # Execute and Assert
        with pytest.raises(DictKeyError):
            SelectFunctionModel.from_dict(invalid_dict)

    @patch("pyspark.sql.functions.col")
    def test_from_dict_missing_arguments(self, mock_col, valid_select_dict) -> None:
        """Test from_dict method with missing arguments key."""
        # Remove the arguments key
        invalid_dict = valid_select_dict.copy()
        del invalid_dict["arguments"]

        # Execute and Assert
        with pytest.raises(DictKeyError):
            SelectFunctionModel.from_dict(invalid_dict)

    @patch("pyspark.sql.functions.col")
    def test_from_dict_missing_columns(self, mock_col, valid_select_dict) -> None:
        """Test from_dict method with missing columns in arguments."""
        # Remove the columns key from arguments
        invalid_dict = valid_select_dict.copy()
        invalid_dict["arguments"] = {}  # Empty arguments dict without columns

        # Execute and Assert
        with pytest.raises(DictKeyError):
            SelectFunctionModel.from_dict(invalid_dict)
