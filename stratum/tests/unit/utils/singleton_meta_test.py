import unittest

from stratum.utils.singleton_meta import SingletonType


class TestSingletonMeta(unittest.TestCase):
    class TestImplementationSingleton(metaclass=SingletonType):
        def __init__(self, value: int) -> None:
            self.value = value

    def setUp(self) -> None:
        SingletonType._instances.clear()  # type: ignore

    def test_singleton_instance(self) -> None:
        """
        Test that SingletonMeta returns the same instance.
        """
        instance1 = self.TestImplementationSingleton(1)
        instance2 = self.TestImplementationSingleton(2)  # different value, but should return instance1
        self.assertIs(instance1, instance2, "SingletonMeta should return the same instance")
        self.assertEqual(instance1.value, 1, "The value should be the one set by the first instance")

    def test_singleton_value_persistence(self) -> None:
        """
        Test that the value of the singleton instance persists.
        """
        _ = self.TestImplementationSingleton(1)
        instance2 = self.TestImplementationSingleton(2)
        self.assertEqual(instance2.value, 1, "The value should persist as the first instance's value")

    def test_singleton_reset(self) -> None:
        """
        Test that resetting the singleton instance works.
        """
        instance1 = self.TestImplementationSingleton(1)

        SingletonType._instances.pop(self.TestImplementationSingleton, None)  # type: ignore

        instance2 = self.TestImplementationSingleton(2)
        self.assertIsNot(instance1, instance2, "A new instance should be created after reset")
        self.assertEqual(instance2.value, 2, "The value should be the one set by the new instance")

    def tearDown(self) -> None:
        SingletonType._instances.clear()  # type: ignore
