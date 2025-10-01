"""HTTP action for executing HTTP requests in ETL pipelines.

This module implements the HTTP action that allows ETL jobs to make
HTTP requests as part of their execution flow. It supports custom headers,
different HTTP methods, and configurable timeouts and failure handling.

The HttpAction follows the Flint framework patterns for configuration-driven
initialization and implements the ActionBase interface.
"""

import logging
from typing import Any, Literal

from pydantic import Field
from typing_extensions import override

from flint.actions.base import ActionBase
from flint.utils.http import HttpBase
from flint.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class HttpAction(HttpBase, ActionBase):
    """HTTP action for making HTTP requests in ETL pipelines.

    This class implements HTTP request functionality for ETL jobs,
    allowing pipelines to interact with external HTTP endpoints.
    It inherits HTTP functionality from HttpBase and action functionality
    from ActionBase.

    Attributes:
        action: Always "http" for HTTP actions
        payload: Optional payload data to send in the request
    """

    action: Literal["http"] = Field("http", description="Type identifier for the HTTP action")
    payload: dict[str, Any] = Field(default_factory=dict, description="Optional payload data to send in the request")

    @override
    def _execute(self) -> None:
        """Execute the HTTP request action.

        Makes an HTTP request to the configured endpoint with the
        specified payload and configuration.

        Raises:
            requests.RequestException: If the HTTP request fails after all retries.
        """
        logger.info("Executing HTTP action: %s", self.name)
        self._make_http_request(self.payload)
        logger.info("HTTP action completed: %s", self.name)
