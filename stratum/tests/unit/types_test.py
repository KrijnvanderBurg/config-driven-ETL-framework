import unittest
from unittest.mock import MagicMock

from pyspark.sql import DataFrame as DataFramePyspark

from stratum.types import RegistrySingleton


class TestRegistrySingleton(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = RegistrySingleton()
        self.registry._items.clear()  # type: ignore

        self.mock_dataframe = MagicMock(spec=DataFramePyspark)

    def test_set_and_get_item(self) -> None:
        self.registry["df1"] = self.mock_dataframe
        self.assertEqual(self.registry["df1"], self.mock_dataframe)

    def test_get_nonexistent_item(self) -> None:
        with self.assertRaises(KeyError):
            _ = self.registry["df1"]

    def test_del_item(self) -> None:
        self.registry["df1"] = self.mock_dataframe
        del self.registry["df1"]
        with self.assertRaises(KeyError):
            _ = self.registry["df1"]

    def test_del_nonexistent_item(self) -> None:
        with self.assertRaises(KeyError):
            del self.registry["df1"]

    def test_contains(self) -> None:
        self.registry["df1"] = self.mock_dataframe
        self.assertIn("df1", self.registry)
        self.assertNotIn("df2", self.registry)

    def test_len(self) -> None:
        self.registry["df1"] = self.mock_dataframe
        self.registry["df2"] = self.mock_dataframe
        self.assertEqual(len(self.registry), 2)

    def test_iter(self) -> None:
        self.registry["df1"] = self.mock_dataframe
        self.registry["df2"] = self.mock_dataframe
        items = list(iter(self.registry))
        self.assertIn(self.mock_dataframe, items)
