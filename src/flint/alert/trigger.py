"""Trigger rules for alert channel selection and filtering.

This module defines the trigger rules system that determines which channels
should receive specific alerts based on flexible rule conditions.

The trigger system supports composable rule evaluation with various criteria
to enable sophisticated alert trigger logic using the same pattern as
transform functions.
"""

import logging

from pydantic import Field

from flint import BaseModel
from flint.alert.template import AlertTemplate
from flint.utils.logger import get_logger

from .rules import alert_rule_union

logger: logging.Logger = get_logger(__name__)


class AlertTrigger(BaseModel):
    """Individual trigger rule for alert channel selection.

    This class represents a single trigger rule that defines conditions
    for when alerts should be sent to specific channels using a flexible
    rule-based system similar to transform functions.

    Attributes:
        id: Unique identifier for the trigger rule
        enabled: Whether this rule is currently active
        channel_ids: List of channel identifiers that should receive alerts matching this rule
        template: Template configuration for formatting alert messages
        rules: List of rules that must all evaluate to True for the trigger to fire
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the trigger rule", min_length=1)
    enabled: bool = Field(..., description="Whether this rule is currently active")
    description: str = Field(..., description="Description of the trigger rule")
    channel_ids: list[str] = Field(
        ..., description="List of channel identifiers that should receive alerts matching this rule"
    )
    template: AlertTemplate = Field(..., description="Template configuration for formatting alert messages")
    rules: list[alert_rule_union] = Field(
        ..., description="List of rules that must all evaluate to True for the trigger to fire"
    )

    def should_fire(self, exception: Exception) -> bool:
        """Check if the conditions are met for triggering an alert.

        This method evaluates all rules against the current alert context.
        All rules must evaluate to True for the trigger to be activated (AND logic).

        Args:
            exception: The exception to evaluate against trigger rules

        Returns:
            True if all rules evaluate to True, False otherwise
        """
        if not self.enabled:
            logger.debug("Trigger '%s' is disabled; skipping trigger check.", self.id_)
            return False

        # If no rules are configured, the trigger should fire (default behavior)
        if not self.rules:
            logger.debug("No rules configured for trigger '%s'; trigger will fire.", self.id_)
            return True

        # All rules must evaluate to True (AND logic)
        for rule in self.rules:
            if not rule.evaluate(exception):
                logger.debug(
                    "Rule '%s' for trigger '%s' evaluated to False; trigger will not fire.", rule.rule_type, self.id_
                )
                return False

        logger.debug("All rules for trigger '%s' evaluated to True; trigger will fire.", self.id_)
        return True
