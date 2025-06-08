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

from stratum.extract.factory import ExtractContextPyspark
from stratum.extract.file_extract import ExtractFilePyspark


class TestExtractContextPyspark:
    """
    TODO
    """

    @pytest.mark.parametrize(
        "input_format, expected_class",
        [
            ("parquet", ExtractFilePyspark),
            ("json", ExtractFilePyspark),
            ("csv", ExtractFilePyspark),
        ],
    )
    def test___factory(self, input_format: str, expected_class: ExtractFilePyspark) -> None:
        """
        Assert that ExtractFactory returns the expected class instance based on input format.
        """
        confeti = {"data_format": input_format}
        extract = ExtractContextPyspark.factory(confeti=confeti)
        assert extract == expected_class
