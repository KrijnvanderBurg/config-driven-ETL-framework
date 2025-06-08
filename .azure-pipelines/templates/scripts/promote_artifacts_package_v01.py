"""
Handles promotion of Python packages via the Azure DevOps API.

The DevOpsHandler class requires the Azure DevOps organization name and a personal access token for authentication.
"""

import base64
import logging
from argparse import ArgumentParser

import requests
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DevOpsHandler:
    """
    Class for promoting Python packages using the Azure DevOps API.
    """

    def __init__(self, devops_organisation: str, devops_access_token: str) -> None:
        """
        Initialize DevOpsHandler.

        Args:
            devops_organisation (str): Azure DevOps organization name.
            devops_access_token (str): Azure DevOps personal access token.
        """
        self.devops_organisation = devops_organisation
        self.devops_access_token = devops_access_token

    def promote_package(
        self, artifacts_feed_name: str, artifacts_feed_view: str, package_name: str, package_version: str
    ) -> None:
        """
        Promote a Python package to a given view.

        Args:
            artifacts_feed_name (str): Name of the artifacts feed.
            artifacts_feed_view (str): View to which the package will be promoted.
            package_name (str): Name of the Python package.
            package_version (str): Version of the Python package.
        """
        uri = (
            f"https://pkgs.dev.azure.com/{self.devops_organisation}/_apis/packaging/feeds/"
            f"{artifacts_feed_name}/pypi/packages/{package_name}/versions/{package_version}?api-version=7.1-preview.1"
        )
        encoded_token = base64.b64encode(f"user:{self.devops_access_token}".encode("ascii")).decode("ascii")
        headers = {"Content-Type": "application/json", "Authorization": f"Basic {encoded_token}"}
        body = {"views": {"op": "add", "path": "/views/-", "value": artifacts_feed_view}}

        request = requests.Session()
        request.headers.update(headers)

        retries = Retry(
            total=5,
            backoff_factor=5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=frozenset(["PATCH"]),
        )
        request.mount("https://", HTTPAdapter(max_retries=retries))

        resp = request.patch(uri, json=body)

        try:
            resp = request.patch(uri, json=body)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Error promoting package: %s", e)
            return

        logger.info("Package %s promoted to %s", package_name, artifacts_feed_view)


if __name__ == "__main__":
    parser = ArgumentParser(description="Promote Python packages to a given view using the DevOps API.")
    parser.add_argument("--devops-organisation", required=True, type=str, help="Azure DevOps organization name.")
    parser.add_argument("--devops-access-token", required=True, type=str, help="Azure DevOps personal access token.")
    parser.add_argument("--artifacts-feed-name", required=True, type=str, help="Name of the artifacts feed.")
    parser.add_argument(
        "--artifacts-feed-view", required=True, type=str, help="View to which the package will be promoted."
    )
    parser.add_argument("--package-name", required=True, type=str, help="Name of the Python package.")
    parser.add_argument("--package-version", required=True, type=str, help="Version of the Python package.")
    args = parser.parse_args()

    devops_handler = DevOpsHandler(
        devops_organisation=args.devops_organisation,
        devops_access_token=args.devops_access_token,
    )

    devops_handler.promote_package(
        artifacts_feed_name=args.artifacts_feed_name,
        artifacts_feed_view=args.artifacts_feed_view,
        package_name=args.package_name,
        package_version=args.package_version,
    )
