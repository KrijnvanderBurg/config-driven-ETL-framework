"""Unit tests for types, including Singleton and RegistryDecorator."""

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from flint.types import DataFrameRegistry, RegistryDecorator, RegistryInstance, Singleton, StreamingQueryRegistry


class TestSingleton:
    """Tests for the Singleton metaclass."""

    def test_singleton_creates_one_instance(self) -> None:
        """Test that singleton only creates one instance."""

        class TestClass(metaclass=Singleton):
            def __init__(self) -> None:
                self.value = 0

        instance1 = TestClass()
        instance2 = TestClass()

        assert instance1 is instance2

    def test_singleton_state_shared(self) -> None:
        """Test that state is shared between references."""

        class TestClass(metaclass=Singleton):
            def __init__(self) -> None:
                self.value = 0

        instance1 = TestClass()
        instance1.value = 42
        instance2 = TestClass()

        assert instance2.value == 42

    def test_different_singleton_classes_separate(self) -> None:
        """Test that different singleton classes have separate instances."""

        class TestClass1(metaclass=Singleton):
            def __init__(self) -> None:
                self.value = 1

        class TestClass2(metaclass=Singleton):
            def __init__(self) -> None:
                self.value = 2

        instance1 = TestClass1()
        instance2 = TestClass2()

        assert instance1 is not instance2
        assert instance1.value == 1
        assert instance2.value == 2

    def test_singleton_ignores_later_args(self) -> None:
        """Test that arguments after first instantiation are ignored."""

        class TestClass(metaclass=Singleton):
            def __init__(self, value=0) -> None:
                self.value = value

        instance1 = TestClass(42)
        instance2 = TestClass(100)

        assert instance1 is instance2
        assert instance1.value == 42

    def test_singleton_thread_safe(self) -> None:
        """Test that singleton is thread-safe."""

        class TestClass(metaclass=Singleton):
            def __init__(self) -> None:
                self.value = 0

        instances = []

        def create_instance() -> None:
            instances.append(TestClass())

        threads = [threading.Thread(target=create_instance) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance


class TestRegistryDecorator:
    """Tests for the RegistryDecorator class."""

    def test_register_and_get_class(self) -> None:
        """Test registering and retrieving a class."""

        @RegistryDecorator.register("test_key")
        class TestClass:
            pass

        result = RegistryDecorator.get("test_key")
        assert result is TestClass

    def test_get_nonexistent_key_raises_error(self) -> None:
        """Test that getting a non-registered key raises KeyError."""
        with pytest.raises(KeyError):
            RegistryDecorator.get("nonexistent")

    def test_multiple_classes_same_key(self) -> None:
        """Test registering multiple classes with same key returns first."""

        @RegistryDecorator.register("multi")
        class FirstClass:
            pass

        @RegistryDecorator.register("multi")
        class SecondClass:
            pass

        result = RegistryDecorator.get("multi")
        assert result is FirstClass

    def test_get_all_classes(self) -> None:
        """Test getting all registered classes for a key."""

        @RegistryDecorator.register("all_test")
        class Class1:
            pass

        @RegistryDecorator.register("all_test")
        class Class2:
            pass

        all_classes = RegistryDecorator.get_all("all_test")
        assert len(all_classes) == 2
        assert Class1 in all_classes
        assert Class2 in all_classes

    def test_get_all_nonexistent_key_raises_error(self) -> None:
        """Test that get_all with non-existent key raises KeyError."""
        with pytest.raises(KeyError):
            RegistryDecorator.get_all("nonexistent")

    def test_duplicate_registration_prevention(self) -> None:
        """Test that duplicate registration of same class is prevented."""

        class TestClass:
            pass

        # Register the same class twice with the same key
        RegistryDecorator.register("dup_test")(TestClass)
        RegistryDecorator.register("dup_test")(TestClass)

        # Should only have one entry
        all_classes = RegistryDecorator.get_all("dup_test")
        assert len(all_classes) == 1
        assert all_classes[0] is TestClass


class TestRegistryInstance:
    """Tests for RegistryInstance class."""

    @pytest.fixture
    def registry(self) -> Any:
        """Return a fresh registry instance."""
        registry = RegistryInstance()
        registry._items.clear()  # type: ignore
        return registry

    def test_set_and_get_item(self, registry: Any) -> None:
        """Test setting and getting items."""
        mock_item = MagicMock()
        registry["item1"] = mock_item
        assert registry["item1"] == mock_item

    def test_get_nonexistent_item_raises_error(self, registry: Any) -> None:
        """Test getting nonexistent item raises KeyError."""
        with pytest.raises(KeyError):
            _ = registry["missing"]

    def test_delete_item(self, registry: Any) -> None:
        """Test deleting items."""
        mock_item = MagicMock()
        registry["item1"] = mock_item
        del registry["item1"]
        with pytest.raises(KeyError):
            _ = registry["item1"]

    def test_delete_nonexistent_item_raises_error(self, registry: Any) -> None:
        """Test deleting nonexistent item raises KeyError."""
        with pytest.raises(KeyError):
            del registry["missing"]

    def test_contains_operator(self, registry: Any) -> None:
        """Test 'in' operator."""
        mock_item = MagicMock()
        registry["item1"] = mock_item
        assert "item1" in registry
        assert "missing" not in registry

    def test_len_function(self, registry: Any) -> None:
        """Test len() function."""
        registry["item1"] = MagicMock()
        registry["item2"] = MagicMock()
        assert len(registry) == 2

    def test_iteration(self, registry: Any) -> None:
        """Test iteration over registry."""
        mock_item = MagicMock()
        registry["item1"] = mock_item
        registry["item2"] = mock_item
        items = list(iter(registry))
        assert mock_item in items


class TestDataFrameRegistry:
    """Tests for DataFrameRegistry class."""

    @pytest.fixture
    def df_registry(self):
        """Return a fresh DataFrame registry instance."""
        registry = DataFrameRegistry()
        registry._items.clear()  # type: ignore
        return registry

    def test_set_and_get_dataframe(self, df_registry: DataFrameRegistry) -> None:
        """Test setting and getting DataFrames."""
        mock_df = MagicMock()
        df_registry["customers"] = mock_df
        assert df_registry["customers"] == mock_df

    def test_get_nonexistent_dataframe_enhanced_error(self, df_registry: DataFrameRegistry) -> None:
        """Test getting nonexistent DataFrame shows available DataFrames."""
        df_registry["existing_df"] = MagicMock()

        with pytest.raises(KeyError):
            _ = df_registry["missing_df"]


class TestStreamingQueryRegistry:
    """Tests for StreamingQueryRegistry class."""

    @pytest.fixture
    def sq_registry(self):
        """Return a fresh StreamingQuery registry instance."""
        registry = StreamingQueryRegistry()
        registry._items.clear()  # type: ignore
        return registry

    def test_set_and_get_streaming_query(self, sq_registry: StreamingQueryRegistry) -> None:
        """Test setting and getting StreamingQueries."""
        mock_query = MagicMock()
        sq_registry["customers_stream"] = mock_query
        assert sq_registry["customers_stream"] == mock_query

    def test_get_nonexistent_query_enhanced_error(self, sq_registry: StreamingQueryRegistry) -> None:
        """Test getting nonexistent StreamingQuery shows available queries."""
        sq_registry["existing_stream"] = MagicMock()

        with pytest.raises(KeyError):
            _ = sq_registry["missing_stream"]
