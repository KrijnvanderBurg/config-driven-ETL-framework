"""Job union types for the Flint ETL framework.

This module provides the discriminated union of all job types.
It's separate from the base models to avoid circular import issues.
"""

from flint.runtime.jobs.spark.job import JobSpark

# For now, just use JobSpark directly since it's the only engine
# When more engines are added, this will become a discriminated union:
# Job = Annotated[Union[JobSpark, JobPandas, JobDask], Discriminator("engine")]
Job = JobSpark

__all__ = ["Job"]
