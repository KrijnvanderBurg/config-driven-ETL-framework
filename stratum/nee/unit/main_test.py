# """
# TODO


# ==============================================================================
# Copyright Krijn van der Burg. All rights reserved.

# This software is proprietary and confidential. No reproduction, distribution,
# or transmission is allowed without prior written permission. Unauthorized use,
# disclosure, or distribution is strictly prohibited.

# For inquiries and permission requests, contact Krijn van der Burg at
# krijnvdburg@protonmail.com.
# ==============================================================================
# """

# import argparse
# from pathlib import Path
# from unittest import mock
# from unittest.mock import mock_open

# from stratum.__main__ import main
# from stratum.engine.job_engine import JobPyspark


# def test__main__pyspark(tmp_path: Path, extract_model_confeti_pyspark, load_model_confeti_pyspark) -> None:
#     """TODO"""

#     confeti_filepath: Path = Path(tmp_path, "confeti").with_suffix(".json")

#     # Arrange
#     args = argparse.Namespace(filepath=confeti_filepath)

# NOTE. YOU ONLY HAVE TO TEST IF THE FUNCTION IS CALLED WHEN PATH IS EMPTY AND NOT EMPTY.
# NOTE DONT TEST IF YOU GET THE RIGHT ENGINE OR NOT, THATS ANOTHER TEST.

#     with (mock.patch.object(argparse.ArgumentParser, "parse_args", return_value=args),):
#         # Act
#         main()

#         # Assert
#         execute_mock.assert_called_once()
