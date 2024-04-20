"""
Job class tests.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

from unittest.mock import patch

import jsonschema
import pytest

from datastore.models.job.extract_spec import ExtractSpecPyspark
from datastore.models.job.load_spec import LoadSpecPyspark
from datastore.models.job.strategy_spec import StrategySpecPyspark
from datastore.models.job.transform_spec import TransformSpecPyspark
from datastore.models.job_spec import JobSpecFactory, JobSpecPyspark
from datastore.utils.json_handler import JsonHandler


@pytest.fixture(name="job_spec_pyspark")
def fixture__job_spec__pyspark(
    strategy_spec_pyspark: StrategySpecPyspark,
    extract_spec_pyspark: ExtractSpecPyspark,
    transform_spec_pyspark: TransformSpecPyspark,
    load_spec_pyspark: LoadSpecPyspark,
) -> JobSpecPyspark:
    """
    Fixture for creating job instance from confeti dict.

    Args:
        strategy_spec_pyspark (StrategySpecPyspark): StrategySpecPyspark fixture.
        extract_spec_pyspark (ExtractSpecPyspark): ExtractSpec fixture.
        transform_spec_pyspark (TransformSpecPyspark): TransformSpec fixture.
        load_spec_pyspark (LoadSpecPyspark): LoadSpec fixture.

    Returns:
        (Job): Job instance fixture.
    """
    return JobSpecPyspark(
        strategy=strategy_spec_pyspark,
        extract=extract_spec_pyspark,
        transform=transform_spec_pyspark,
        load=load_spec_pyspark,
    )


@pytest.fixture(name="job_spec_confeti_pyspark")
def fixture__job_spec__confeti__pyspark(
    strategy_spec_confeti_pyspark: dict,
    extract_spec_confeti_pyspark: dict,
    transform_spec_confeti_pyspark: dict,
    load_spec_confeti_pyspark: dict,
) -> dict:
    """
    Fixture for valid confeti file.

    Args:
        strategy_spec_confeti_pyspark (dict): StrategySpecPyspark confeti fixture.
        extract_spec_confeti_pyspark (dict): ExtractSpecPyspark confeti fixture.
        transform_spec_confeti_pyspark (dict): TransformSpecPyspark confeti fixture.
        load_spec_confeti_pyspark (dict): LoadSpecPyspark confeti fixture.

    Returns:
        (dict): valid JobSpecPyspark confeti.
    """
    confeti = {}
    confeti["strategy"] = strategy_spec_confeti_pyspark
    confeti["extract"] = extract_spec_confeti_pyspark
    confeti["transform"] = transform_spec_confeti_pyspark
    confeti["load"] = load_spec_confeti_pyspark

    return confeti


class TestJobSpecPyspark:
    """
    Test class for LoadSpec class.
    """

    def test__init(self, job_spec_pyspark: JobSpecPyspark) -> None:
        """
        Assert that all JobSpecPyspark attributes are of correct type.

        Args:
            job_spec_pyspark (StrategySpecPyspark): StrategySpecPyspark instance fixture.
        """

        # Assert
        assert isinstance(job_spec_pyspark.strategy, StrategySpecPyspark)
        assert isinstance(job_spec_pyspark.extract, ExtractSpecPyspark)
        assert isinstance(job_spec_pyspark.transform, TransformSpecPyspark)
        assert isinstance(job_spec_pyspark.load, LoadSpecPyspark)

    def test__from_confeti(self, job_spec_confeti_pyspark: dict) -> None:
        """
        Assert that all JobSpecPyspark attributes are of correct type.

        Args:
            job_spec_confeti_pyspark (dict): JobSpecPyspark confeti fixture.
        """

        # Act
        job_spec_pyspark = JobSpecPyspark.from_confeti(confeti=job_spec_confeti_pyspark)

        # Assert
        assert isinstance(job_spec_pyspark.strategy, StrategySpecPyspark)
        assert isinstance(job_spec_pyspark.extract, ExtractSpecPyspark)
        assert isinstance(job_spec_pyspark.transform, TransformSpecPyspark)
        assert isinstance(job_spec_pyspark.load, LoadSpecPyspark)

    @pytest.mark.parametrize("missing_property", ["strategy", "extract", "transform", "load"])
    def test__validate_json__missing_required_properties(
        self, missing_property: str, job_spec_confeti_pyspark: dict
    ) -> None:
        """
        Test case for validating that each required property is missing one at a time.

        Args:
            missing_property (str): Name of the missing property.
            job_spec_confeti_pyspark (dict): JobSpecPyspark confeti fixture.
        """

        with pytest.raises(jsonschema.ValidationError):  # Assert
            # Act
            del job_spec_confeti_pyspark[missing_property]
            JsonHandler.validate_json(job_spec_confeti_pyspark, JobSpecPyspark.confeti_schema)


class TestJobSpecFactory:
    """
    Test job_spec factory class.
    """

    def test_from_confeti_pyspark(
        self,
    ) -> None:
        """
        Test if job_spec factory returns pyspark object if given engine pyspark.
        """
        # Arrange
        confeti = {"strategy": {"engine": "pyspark"}}
        with patch.object(JobSpecPyspark, "from_confeti") as mock_from_confeti:
            # Act
            JobSpecFactory.from_confeti(confeti=confeti)

            # Assert
            mock_from_confeti.assert_called_once()
