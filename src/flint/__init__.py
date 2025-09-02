"""PySpark Ingestion Framework.

A scalable, modular framework for building data ingestion pipelines using Apache PySpark.
This framework provides tools and components for extracting data from various sources,
transforming it using customizable operations, and loading it into target systems.

The framework is built with configurability in mind, allowing pipeline definitions
through configuration files rather than code changes. It leverages PySpark for
distributed data processing and follows standard ETL (Extract, Transform, Load) patterns.

Example:
    Basic usage of the framework:

    ```python
    from pathlib import Path
    from flint.etl.core.job import Job

    # Create a job from configuration
    job = Job.from_file(filepath=Path("config.json"))

    # Execute the pipeline
    job.execute()
    ```
"""

__author__ = "Krijn van der Burg"
__copyright__ = "Krijn van der Burg"
__credits__ = [""]
__license__ = "Creative Commons BY-NC-ND 4.0 DEED Attribution-NonCommercial-NoDerivs 4.0 International License"
__maintainer__ = "Krijn van der Burg"
__email__ = ""
__status__ = "Prototype"
