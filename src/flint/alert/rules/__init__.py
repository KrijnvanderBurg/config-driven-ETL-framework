"""Alert trigger rules for flexible condition evaluation.

This module contains the rule-based system for alert triggers, providing
a flexible way to define conditions that determine when alerts should fire.

The rule system follows the same pattern as transform functions, with
abstract base classes, registries, and concrete implementations.
"""

from typing import Annotated

from pydantic import Discriminator

from .env_vars_matches import EnvVarsMatchesRule
from .exception_regex import ExceptionRegexRule

__all__ = [
    "EnvVarsMatchesRule",
    "ExceptionRegexRule",
]


AlertRuleUnion = Annotated[
    EnvVarsMatchesRule | ExceptionRegexRule,
    Discriminator("rule"),
]
