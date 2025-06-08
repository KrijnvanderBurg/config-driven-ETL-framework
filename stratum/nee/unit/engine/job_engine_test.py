"""
Job class tests.


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

from unittest import mock

# from unittest.mock import patch
from stratum.job import Job, JobPyspark

# def test__job_pypsark__from_confeti__required() -> None:
#     """TODO"""
#     # Arrange
#     confeti = {
#         "extracts": [
#             {
#                 "name": "bronze-test-extract-dev",
#                 "method": "batch",
#                 "data_format": "parquet",
#                 "location": "/input.parquet",
#                 "schema": "",
#             }
#         ]
#     }

#     # Act
#     JobPyspark.from_confeti(confeti=confeti)


# def test__job_pypsark__from_confeti__optional() -> None:
#     """TODO"""
#     # Arrange
#     confeti = {
#         "engine": "pyspark",
#         "extracts": [
#             {
#                 "name": "bronze-test-extract-dev",
#                 "method": "batch",
#                 "data_format": "parquet",
#                 "location": "/input.parquet",
#                 "schema": "",
#                 "options": {
#                     "options": {
#                         "spark.enforceSchema": True,
#                     },
#                 },
#             }
#         ],
#         "loads": [
#             {
#                 "name": "silver-test-load-dev",
#                 "method": "batch",
#                 "data_format": "parquet",
#                 "mode": "complete",
#                 "location": "/output.parquet",
#                 "options": {
#                     "options": {
#                         # "spark.executor.memory": "4g",
#                         # "spark.executor.cores": "2",
#                     },
#                 },
#             }
#         ],
#     }

#     # Act
#     JobPyspark.from_confeti(confeti=confeti)


class TestJobFactory:
    """
    Test job_model factory class.
    """

    def test_from_confeti_pyspark(
        self,
    ) -> None:
        """
        Test if job_model factory returns pyspark object if given engine pyspark.
        """
        # Arrange
        confeti = {"engine": "pyspark"}
        with mock.patch.object(JobPyspark, "from_confeti") as mock_from_confeti:
            # Act
            Job.from_confeti(confeti=confeti)

            # Assert
            mock_from_confeti.assert_called_once()
