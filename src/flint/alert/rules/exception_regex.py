"""Exception regex rule for alert triggers.

This module implements a rule that matches exception messages against
regular expression patterns.
"""

import logging
import re
from typing import Literal

from flint.alert.rules.base import AlertRule
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class ExceptionRegexRule(AlertRule):
    """Rule that matches exception messages against regular expressions.

    This rule evaluates to True if the exception message matches
    the configured regular expression pattern.
    """

    rule: Literal["exception_regex"]
    pattern: str

    def evaluate(self, exception: Exception) -> bool:
        """Evaluate if the exception message matches the regex pattern.

        Args:
            exception: The exception to check

        Returns:
            True if the message matches the regex, False otherwise
        """
        if not self.pattern:
            logger.debug("No exception_regex pattern configured; skipping regex check.")
            return True

        message = str(exception)

        if re.search(self.pattern, message):
            logger.debug("Exception message matches regex: '%s'", self.pattern)
            return True

        logger.debug("Exception message '%s' does not match regex: '%s'", message, self.pattern)
        return False
