"""Alerting module for Samara."""

import logging

from samara.alert.controller import AlertController
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


__all__ = ["AlertController"]
