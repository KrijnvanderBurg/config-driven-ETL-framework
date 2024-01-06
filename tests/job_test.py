"""
Job class tests.

# | ✓ | Tests
# |---|-----------------------------------
# | ✓ | Test all attributes are of correct type.
# | ✓ | Test execute sub methods are called.

Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from collections.abc import Generator
from unittest import mock

import pytest
from datastore.extract.base import ExtractSpec
from datastore.job import Job
from datastore.load.base import LoadSpec

# ==================================
# =========== Job Class ============
# ==================================

# ========== Fixtures ==============


@pytest.fixture(name="job")
def fixture_job(confeti: dict) -> Generator[Job, None, None]:
    """
    Fixture for creating job instance from confeti dict.

    Args:
        confeti (dict): valid confeti.

    Yields:
        (Job): Job instance fixture.

    """
    yield Job(confeti=confeti)


# ============ Tests ===============


def test_job_types(job: Job) -> None:
    """
    Assert that all Job attributes are of correct type.

    Args:
        job (Job): Job fixture.
    """
    # Assert
    assert isinstance(job.extract_spec, ExtractSpec)
    assert isinstance(job.load_spec, LoadSpec)


def test_job_execute(job: Job) -> None:
    """
    Assert that extract and load methods are called.

    Args:
        job (Job): Job fixture.
    """
    with mock.patch.object(job, "_extract") as extract_batch_mock, mock.patch.object(
        job, "_load"
    ) as extract_streaming_mock:
        # Act
        job.execute()

    extract_batch_mock.assert_called_once()
    extract_streaming_mock.assert_called_once()
