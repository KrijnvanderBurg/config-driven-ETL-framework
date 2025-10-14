"""Job union types for the Samara ETL framework.

This module provides the discriminated union of all job types.
It's separate from the base models to avoid circular import issues.
"""

from samara.runtime.jobs.spark.job import JobSpark

# For now, just use JobSpark directly since it's the only engine
# When more engines are added, this will become a discriminated union:
# JobUnion = Annotated[JobSpark | JobPolars, Discriminator("engine")]
JobUnion = JobSpark

__all__ = ["JobUnion", "JobSpark"]
