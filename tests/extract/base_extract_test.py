"""
Extract class tests.

| ✓ | Tests
|---|-----------------------------------
| ✓ | Implement extract ABC in new class.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from datastore.extract.base import Extract

# =========== Fixtures =============

# ============= Tests ==============


def test_extract_abc_implementation(extract_spec) -> None:
    """
    Test implementation of abstract Extract class.

    Args:
        extract_spec (ExtractSpec): ExtractSpec fixture.
    """
    # Arrange
    Extract.__abstractmethods__ = frozenset()

    # Act
    class MockExtract(Extract):
        """Mock implementation of Extract class for testing purposes."""

        def extract(self):
            """Mock implementation of Extract class for testing purposes."""

    # Assert
    assert isinstance(MockExtract(spec=extract_spec), Extract)
