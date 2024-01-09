"""
TransformFactory class tests.

| ✓ | Tests
|---|-----------------------------------------------------------
| ✓ | Create Transform class from Factory by spec format.
| ✓ | Raise NotImplementedError in Factory if spec combination is not implemented.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import pytest
from datastore.transform.factory import TransformFactory

# =============== Fixtures ================

# ================ Tests ==================


@pytest.mark.parametrize(
    "fixture_transform_name",
    [
        "transform_cast",
    ],
)
def test_load_factory_get(fixture_transform_name, request) -> None:
    """
    Assert that TransformFactory returns a TransformSpec instance from valid input.

    Args:
        request (str): pytest request.
        fixture_transform_name (str): Fixture name of transform function.

    Raises:
        pytest.fail: If the combination of format and method is unsupported by TransformFactory.
    """
    # Arrange
    transform = request.getfixturevalue(fixture_transform_name)

    try:
        # Act
        function = TransformFactory.get(transform=transform)

        # Assert
        assert callable(function)

    # Assert
    except NotImplementedError:
        pytest.fail(f"Combination {transform.function}-{transform.arguments} is unsupported.")
