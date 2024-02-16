"""
Job class tests.

| ✓ | Tests
|---|-----------------------------------
| ✓ | Test all attributes are of correct type.
| ✓ | Test execute sub methods are called.
| ✓ | Create instance from confeti.
| ✓ | Test all protected methods _extract, _transform, _load are called
| ✓ | Test if TransformSpec of none does not call job.transform


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
from datastore.transform.base import TransformSpec
from pyspark.sql import DataFrame

# ========== Fixtures ==============


@pytest.fixture(name="job")
def fixture_job(extract_spec: ExtractSpec, transform_spec: TransformSpec, load_spec: LoadSpec) -> Job:
    """
    Fixture for creating job instance from confeti dict.

    Args:
        extract_spec (dict): ExtractSpec fixture.
        transform_spec (dict): TransformSpec fixture.
        load_spec (dict): LoadSpec fixture.

    Returns:
        (Job): Job instance fixture.
    """
    return Job(extract_spec=extract_spec, transform_spec=transform_spec, load_spec=load_spec)


@pytest.fixture(name="job_matrix")
def fixture_job_matrix(
    extract_spec_matrix: ExtractSpec, transform_spec_matrix: TransformSpec, load_spec_matrix: LoadSpec
) -> Generator[Job, None, None]:
    """
    Matrix fixture for creating job instance from confeti dict.

    Args:
        extract_spec_matrix (dict): ExtractSpec fixture.
        transform_spec_matrix (dict): TransformSpec fixture.
        load_spec_matrix (dict): LoadSpec fixture.

    Yields:
        (Job): Job instance fixture.
    """
    yield Job(extract_spec=extract_spec_matrix, transform_spec=transform_spec_matrix, load_spec=load_spec_matrix)


@pytest.fixture(name="job_confeti")
def fixture_job_confeti(extract_spec_confeti: dict, transform_spec_confeti: dict, load_spec_confeti: dict) -> dict:
    """
    Fixture for valid confeti file.

    Args:
        extract_spec_confeti (dict): ExtractSpec confeti fixture.
        transform_spec_confeti (dict): ExtractSpec confeti fixture.
        load_spec_confeti (dict): LoadSpec confeti fixture.

    Returns:
        (dict): valid confeti.
    """
    confeti = {}
    confeti["extract"] = extract_spec_confeti
    confeti["transform"] = transform_spec_confeti
    confeti["load"] = load_spec_confeti

    return confeti


# ============ Tests ===============


def test_job_attrb_types(job: Job) -> None:
    """
    Assert that all Job attributes are of correct type.

    Args:
        job (Job): Job fixture.
    """
    # Assert
    assert isinstance(job.extract_spec, ExtractSpec)
    assert isinstance(job.transform_spec, TransformSpec)
    assert isinstance(job.load_spec, LoadSpec)


def test_job_execute_call(job: Job) -> None:
    """
    Assert that extract and load methods are called.

    Args:
        job (Job): Job matrix fixture.
    """
    # Arrange
    with (
        mock.patch.object(job, "_extract") as extract_mock,
        mock.patch.object(job, "_transform") as transform_mock,
        mock.patch.object(job, "_load") as load_mock,
    ):
        # Act
        job.execute()

    # Assert
    extract_mock.assert_called_once()
    transform_mock.assert_called_once()
    load_mock.assert_called_once()


def test_job_from_confeti(job_confeti: dict) -> None:
    """
    Assert that Job from_confeti method returns valid Job.

    Args:
        job_confeti (dict): Job confeti fixture.
    """
    # Act
    job = Job.from_confeti(confeti=job_confeti)

    # Assert
    assert isinstance(job, Job)
    assert isinstance(job.extract_spec, ExtractSpec)
    assert isinstance(job.transform_spec, TransformSpec)
    assert isinstance(job.load_spec, LoadSpec)


def test_job(job: Job, df: DataFrame) -> None:
    """
    Assert that job calls all extract, transform, and load factory methods.

    Args:
        job (Job): Job matrix fixture.
        df (DataFrame): Test DataFrame fixture.
    """
    # Arrange
    df.write.format(job.extract_spec.data_format.value).save(job.extract_spec.location)

    with (
        mock.patch("datastore.extract.file.ExtractFile.extract") as extract_mock,
        # mock.patch.object(DataFrame, "transform") as transform_mock, # TODO doesnt work
        mock.patch("datastore.load.file.LoadFile.load") as load_mock,
    ):
        # Act
        job.execute()

        # Assert
        extract_mock.assert_called_once()
        # transform_mock.assert_called() # TODO doesnt work
        load_mock.assert_called_once()


def test_job_without_transform(job: Job, df: DataFrame) -> None:
    """
    Assert that job with no transform functions does not call transform, returning the df.

    Args:
        job (Job): Job matrix fixture.
        df (DataFrame): Test DataFrame fixture.
    """
    # Arrange
    job.transform_spec = None
    df.write.format(job.extract_spec.data_format.value).save(job.extract_spec.location)

    with (mock.patch("pyspark.sql.DataFrame.transform") as transform_mock,):
        # Act
        job.execute()

        # Assert
        transform_mock.assert_not_called()
