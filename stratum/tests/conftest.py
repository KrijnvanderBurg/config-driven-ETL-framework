"""
Fixtures to reuse.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import sys
from enum import Enum

import pytest

pytest_plugins: list[str] = []

MIN_SIGNED_64_BIT = ~sys.maxsize  # -9223372036854775808
MAX_UNSIGNED_64_BIT = sys.maxsize  # 9223372036854775808


def enum_tripwire(enum_class: type[Enum], tested_values: list[str]) -> None:
    """
    Helper function to perform tripwire test on an enum class.

    This test fails if the values of the enum do not match the expected values.
    """
    if sorted(tested_values) != sorted([enum.value for enum in enum_class.__members__.values()]):
        raise pytest.fail(
            f"Testing values do not match {enum_class.__name__} values. "
            "Did you add or remove some? Update tests and fixtures accordingly."
        )
