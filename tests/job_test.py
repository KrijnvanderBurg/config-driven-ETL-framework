"""
Job class tests.

| ✓ | Tests
|---|-----------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test execute sub methods are called.

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
        confeti (dict): valid confeti fixture.

    Yields:
        (Job): Job instance fixture.

    """
    yield Job(confeti=confeti)


# ============ Tests ===============


def test_job_types(confeti_return: dict) -> None:
    """
    Assert that all Job attributes are of correct type.

    Args:
        confeti_return (Dict): A single confeti fixture.
    """
    # Act
    job = Job(confeti=confeti_return)

    # Assert
    assert isinstance(job.extract_spec, ExtractSpec)
    assert isinstance(job.load_spec, LoadSpec)


def test_job_execute(confeti_return: dict) -> None:
    """
    Assert that extract and load methods are called.

    Args:
        confeti_return (Dict): A single confeti fixture.
    """
    # Act
    job = Job(confeti=confeti_return)

    with mock.patch.object(job, "_extract") as extract_batch_mock, mock.patch.object(
        job, "_load"
    ) as extract_streaming_mock:
        # Act
        job.execute()

    extract_batch_mock.assert_called_once()
    extract_streaming_mock.assert_called_once()
