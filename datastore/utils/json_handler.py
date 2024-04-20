"""
A utility class for handling JSON data and schema validation.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

import jsonschema

from datastore.logger import set_logger

logger = set_logger(__name__)


class JsonHandler:
    """
    A utility class for handling JSON data and schema validation.
    """

    @staticmethod
    def validate_json(json: dict, schema: dict) -> None:
        """
        Validate JSON data against a JSON schema.

        Args:
            json (dict): JSON data.
            schema (dict): JSON schema.

        Raises:
            jsonschema.ValidationError: If JSON data is not valid against the schema.
            jsonschema.SchemaError: If the schema itself is invalid.
        """
        try:
            jsonschema.validate(instance=json, schema=schema)
        except jsonschema.ValidationError as ve:
            logger.error("The provided JSON does not conform to the schema: %s", ve)
            raise ve
        except jsonschema.SchemaError as se:
            logger.error("The provided schema is invalid: %s", se)
            raise se
