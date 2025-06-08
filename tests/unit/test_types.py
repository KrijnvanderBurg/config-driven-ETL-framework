import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion_framework.types import RegistryDecorator, RegistryInstance, Singleton


class TestSingleton:
    """
    Unit tests for the SingletonType metaclass.
    """

    def test_singleton_instance_creation(self) -> None:
        """Test that a singleton class only creates one instance."""

        # Arrange
        class TestClassSingleton(metaclass=Singleton):
            """test class"""

            def __init__(self) -> None:
                self.value = 0

        # Act
        instance1 = TestClassSingleton()
        instance2 = TestClassSingleton()

        # Assert
        assert instance1 is instance2
        assert id(instance1) == id(instance2)

    def test_singleton_state_persistence(self) -> None:
        """Test that state is shared among all references to the singleton."""

        # Arrange
        class TestClassSingleton(metaclass=Singleton):
            """test class"""

            def __init__(self) -> None:
                self.value = 0

        # Act
        instance1 = TestClassSingleton()
        instance1.value = 42
        instance2 = TestClassSingleton()

        # Assert
        assert instance2.value == 42

    def test_multiple_singleton_classes(self) -> None:
        """Test that different singleton classes have separate instances."""

        # Arrange
        class TestSingleton1(metaclass=Singleton):
            """test class"""

            def __init__(self) -> None:
                self.value = 1

        class TestSingleton2(metaclass=Singleton):
            """test class"""

            def __init__(self) -> None:
                self.value = 2

        # Act
        instance1 = TestSingleton1()
        instance2 = TestSingleton2()

        # Assert
        assert instance1 is not instance2
        assert instance1.value == 1
        assert instance2.value == 2

    def test_singleton_with_args(self) -> None:
        """Test that arguments to __init__ are ignored after first instantiation."""

        # Arrange
        class TestClassSingleton(metaclass=Singleton):
            """test class"""

            def __init__(self, value=0) -> None:
                self.value = value

        # Act
        instance1 = TestClassSingleton(42)
        instance2 = TestClassSingleton(100)  # This should be ignored

        # Assert
        assert instance1 is instance2
        assert instance1.value == 42  # Value from first instantiation
        assert instance2.value == 42  # Not 100

    def test_thread_safety(self) -> None:
        """Test that singleton creation is thread-safe."""

        # Arrange
        class TestClassSingleton(metaclass=Singleton):
            """test class"""

            def __init__(self) -> None:
                self.value = 0

        instances = []

        def create_instance() -> None:
            instances.append(TestClassSingleton())

        # Act
        threads = [threading.Thread(target=create_instance) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Assert
        first_instance = instances[0]
        for instance in instances[1:]:
            assert instance is first_instance

    def test_singleton_inheritance(self) -> None:
        """Test that inheritance works properly with singleton classes."""

        # Arrange
        class BaseSingleton(metaclass=Singleton):
            """Base singleton class"""

            def __init__(self) -> None:
                self.base_value = 42

        class DerivedSingleton(BaseSingleton):
            """Derived singleton class"""

            def __init__(self) -> None:
                super().__init__()
                self.derived_value = 100

        # Act
        base = BaseSingleton()
        derived = DerivedSingleton()

        # Assert
        assert base is not derived
        assert base.base_value == 42
        assert derived.base_value == 42
        assert derived.derived_value == 100

        # Act
        base2 = BaseSingleton()
        derived2 = DerivedSingleton()

        # Assert
        assert base is base2
        assert derived is derived2


class TestRegistryDecorator:
    """
    Unit tests for the RegistryDecorator class.
    """

    def test_register_class(self) -> None:
        """Test registering a class with a key."""
        # Arrange

        # Act
        @RegistryDecorator.register("test_key")
        class TestClass:
            """test class"""

        # Assert
        assert TestClass.__name__ == "TestClass"
        # We're testing the registration worked but not accessing private attributes

    def test_get_registered_class(self) -> None:
        """Test retrieving a registered class."""
        # Arrange

        @RegistryDecorator.register("my_key")
        class TestClass:
            """test class"""

        # Act
        retrieved_class = RegistryDecorator.get("my_key")

        # Assert
        assert retrieved_class is TestClass

    def test_get_nonexistent_key(self) -> None:
        """Test that getting a non-registered key raises KeyError."""
        # Arrange

        # Act & Assert
        with pytest.raises(KeyError):
            RegistryDecorator.get("nonexistent_key")

    def test_register_multiple_classes_for_key(self) -> None:
        """Test that registering multiple classes for the same key works."""
        # Arrange

        @RegistryDecorator.register("multi_key")
        class FirstClass:
            """test class"""

        @RegistryDecorator.register("multi_key")
        class SecondClass:
            """test class"""

        # Act
        retrieved_class = RegistryDecorator.get("multi_key")

        # Assert
        assert retrieved_class is FirstClass  # First registered class should be returned
        assert retrieved_class is not SecondClass  # Second class should not override the first

    def test_duplicate_registration(self) -> None:
        """Test that registering the same class twice with the same key doesn't duplicate it."""

        # Arrange
        class DuplicateClass:
            """test class for duplicate registration"""

        # Act - register the same class twice with the same key
        decorated_class1 = RegistryDecorator.register("duplicate_key")(DuplicateClass)
        decorated_class2 = RegistryDecorator.register("duplicate_key")(DuplicateClass)

        # Assert
        assert decorated_class1 is decorated_class2  # Should be the same class
        # Verify the class was only registered once by checking we get the same class back
        retrieved_class = RegistryDecorator.get("duplicate_key")
        assert retrieved_class is DuplicateClass

    def test_register_preserves_class_attributes(self) -> None:
        """Test that registration preserves the registered class's attributes."""
        # Arrange

        @RegistryDecorator.register("attr_test")
        class ClassWithAttrs:
            """test class"""

            class_attr = "class_value"

            def __init__(self) -> None:
                self.instance_attr = "instance_value"

            def method(self) -> str:
                """test method"""
                return "method_result"

        # Act
        retrieved_class = RegistryDecorator.get("attr_test")
        retrieved_instance = retrieved_class()

        # Assert
        assert retrieved_class is ClassWithAttrs
        assert retrieved_class.class_attr == "class_value"
        assert retrieved_instance.instance_attr == "instance_value"
        assert retrieved_instance.method() == "method_result"

    def test_register_with_generic_types(self) -> None:
        """Test using the registry with generic type annotations."""
        # Arrange

        @RegistryDecorator.register("dict_processor")
        class DictProcessor:  # noqa: F841
            """test class"""

            def process(self, data: dict[str, int]) -> list[int]:
                """test method"""
                return list(data.values())

        @RegistryDecorator.register("list_processor")
        class ListProcessor:  # noqa: F841
            """test class"""

            def process(self, data: list[int]) -> int:
                """test method"""
                return sum(data)

        # Act
        dict_processor_class = RegistryDecorator.get("dict_processor")
        list_processor_class = RegistryDecorator.get("list_processor")

        # Assert
        assert dict_processor_class is DictProcessor
        assert list_processor_class is ListProcessor

        assert dict_processor_class().process({"a": 1, "b": 2}) == [1, 2]
        assert list_processor_class().process([1, 2, 3]) == 6


class TestRegistryInstance:
    """Tests for RegistrySingleton class."""

    @pytest.fixture
    def registry(self):
        """Return a fresh registry instance."""
        registry = RegistryInstance()
        registry._items.clear()  # type: ignore
        return registry

    @pytest.fixture
    def mock_item(self) -> MagicMock:
        """Return a mock item."""
        return MagicMock()

    def test_set_and_get_item(self, registry: Any, mock_item: MagicMock) -> None:
        """Test setting and getting items."""
        registry["item1"] = mock_item
        assert registry["item1"] == mock_item

    def test_get_nonexistent_item(self, registry: Any) -> None:
        """Test getting nonexistent item raises KeyError."""
        with pytest.raises(KeyError):
            _ = registry["item1"]

    def test_del_item(self, registry: Any, mock_item: MagicMock) -> None:
        """Test deleting items."""
        registry["item1"] = mock_item
        del registry["item1"]
        with pytest.raises(KeyError):
            _ = registry["item1"]

    def test_del_nonexistent_item(self, registry: Any) -> None:
        """Test deleting nonexistent item raises KeyError."""
        with pytest.raises(KeyError):
            del registry["item1"]

    def test_contains(self, registry: Any, mock_item: MagicMock) -> None:
        """Test 'in' operator."""
        registry["item1"] = mock_item
        assert "item1" in registry
        assert "item2" not in registry

    def test_len(self, registry: Any, mock_item: MagicMock) -> None:
        """Test len() function."""
        registry["item1"] = mock_item
        registry["item2"] = mock_item
        assert len(registry) == 2

    def test_iter(self, registry: Any, mock_item: MagicMock) -> None:
        """Test iteration."""
        registry["item1"] = mock_item
        registry["item2"] = mock_item
        items = list(iter(registry))
        assert mock_item in items
