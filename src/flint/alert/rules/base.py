"""Base classes and registry for alert trigger rules.

This module provides the foundation for the rule-based alert trigger system,
including abstract base classes and the registry mechanism for rule discovery.
"""

import logging
from abc import ABC, abstractmethod

from flint import BaseModel
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class AlertRule(BaseModel, ABC):
    """Base model for alert trigger rules.

    This class represents the configuration for a trigger rule,
    including its name and any rule-specific attributes.

    Args:
        rule: Name of the rule type
    """

    @abstractmethod
    def evaluate(self, exception: Exception) -> bool:
        """Evaluate the rule against the given exception and environment.

        This method should implement the logic to evaluate whether
        the rule conditions are met for the given exception.

        Args:
            exception: The exception to evaluate against the rule

        Returns:
            True if the rule conditions are met, False otherwise.
        """
