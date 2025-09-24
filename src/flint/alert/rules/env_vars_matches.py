"""Environment variable matching rule for alert triggers.

This module implements a rule that matches current environment variables
against configured expected values.
"""

import logging
import os
from typing import Literal

from flint.alert.rules.base import AlertRule
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class EnvVarsMatchesRule(AlertRule):
    """Rule that matches environment variables against expected values.

    This rule evaluates to True if any of the configured environment
    variables match their expected values.
    """

    rule: Literal["env_vars_matches"]
    env_var_name: str
    env_var_values: list[str]

    def evaluate(self, exception: Exception) -> bool:
        """Evaluate if any environment variable matches the configured rules.

        Args:
            exception: The exception (not used in this rule but required by interface)

        Returns:
            True if any specified environment variable matches one of its expected values, False otherwise
        """
        if not self.env_var_name:
            logger.debug("No env_vars_matches configured; skipping environment variable check.")
            return True

        actual_value = os.environ.get(self.env_var_name, None)

        if actual_value in self.env_var_values:
            logger.debug(
                "Environment variable '%s' value '%s' matches expected values: %s",
                self.env_var_name,
                actual_value,
                self.env_var_values,
            )
            return True

        logger.debug("No environment variable conditions are satisfied.")
        return False
