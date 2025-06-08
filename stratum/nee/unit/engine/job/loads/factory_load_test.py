"""


==============================================================================
Copyright Krijn van der Burg. All rights reserved.

This software is proprietary and confidential. No reproduction, distribution,
or transmission is allowed without prior written permission. Unauthorized use,
disclosure, or distribution is strictly prohibited.

For inquiries and permission requests, contact Krijn van der Burg at
krijnvdburg@protonmail.com.
==============================================================================
"""

import pytest

from stratum.engine.job.loads.factory_load import LoadContextPyspark
from stratum.engine.job.loads.file_load import LoadFilePyspark


class TestLoadContextPyspark:
    """
    TODO
    """

    @pytest.mark.parametrize(
        "input_format, expected_class",
        [
            ("parquet", LoadFilePyspark),
            ("json", LoadFilePyspark),
            ("csv", LoadFilePyspark),
        ],
    )
    def test___factory(self, input_format: str, expected_class: LoadFilePyspark) -> None:
        """
        Assert that LoadFactory returns the expected class instance based on input format.
        """
        confeti = {"data_format": input_format}
        load = LoadContextPyspark.factory(confeti=confeti)
        assert load == expected_class
