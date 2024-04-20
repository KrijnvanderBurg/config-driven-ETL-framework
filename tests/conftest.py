"""
Fixtures to reuse.


Copyright (c) Krijn van der Burg.

This work is licensed under the Creative Commons BY-NC-ND 4.0 DEED
Attribution-NonCommercial-NoDerivs 4.0 International License.
See the accompanying LICENSE file for details,
or visit https://creativecommons.org/licenses/by-nc-nd/4.0/ to view a copy.
"""

pytest_plugins = [
    "tests.unit.models.job_spec_test",
    "tests.unit.models.job.strategy_spec_test",
    "tests.unit.models.job.extract_spec_test",
    "tests.unit.models.job.transform_spec_test",
    "tests.unit.models.job.load_spec_test",
    "tests.unit.models.job.transforms.functions.column_spec_test",
]
