"""
Test suite for json_handler.

==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

# import jsonschema
# import pytest

# from stratum.utils.json_handler import JsonHandler


# class TestValidateJSON:
#     """
#     Test suite for JsonHandler.
#     """

#     def test_valid_json(self) -> None:
#         """
#         Test case for validating valid JSON data against a schema.
#         """
#         # Arrange
#         valid_data = {"key": "value"}
#         schema = {
#             "type": "object",
#             "properties": {"key": {"type": "string"}},
#             "required": ["key"],
#         }

#         # Act
#         result = JsonHandler.validate_json(valid_data, schema)

#         # Assert
#         assert result is None

#     def test_empty_json(self) -> None:
#         """
#         Test case for validating empty JSON data.
#         """
#         # Arrange
#         schema = {"type": "object"}

#         # Act & Assert
#         assert JsonHandler.validate_json({}, schema) is None

#     def test_invalid_json(self) -> None:
#         """
#         Test case for validating invalid JSON data missing a required key.
#         """
#         # Arrange
#         invalid_data = {"invalid_key": "test"}  # Missing required key
#         schema = {
#             "type": "object",
#             "properties": {"key": {"type": "string"}},
#             "required": ["key"],
#         }

#         with pytest.raises(jsonschema.ValidationError):  # Assert
#             # Act
#             JsonHandler.validate_json(invalid_data, schema)

#     def test_invalid_schema(self) -> None:
#         """
#         Test case for validating invalid schema.
#         """
#         # Arrange
#         json_data = {"key": "value"}
#         schema = {
#             "type": "object",
#             "properties": {"key": {"type": "int"}},
#         }  # 'key' should be string type

#         # Act & Assert
#         with pytest.raises(jsonschema.SchemaError):  # Assert
#             # Act
#             JsonHandler.validate_json(json_data, schema)

#     def test_invalid_schema_format(self) -> None:
#         """
#         Test case for validating invalid schema format.
#         """
#         # Arrange
#         json_data = {"key": "value"}
#         schema = {"type": "int"}  # Invalid schema format

#         with pytest.raises(jsonschema.SchemaError):  # Assert
#             # Act
#             JsonHandler.validate_json(json_data, schema)
