"""Alerting module for Flint."""

import logging

from flint.alert.controller import AlertController
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


__all__ = ["AlertController"]
