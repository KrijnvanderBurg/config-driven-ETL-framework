# Samara Code Review: Comprehensive Analysis

## Summary

Samara is a well-structured, config-driven PySpark ETL framework with clean separation
between configuration models and engine implementations. The Pydantic discriminated union
pattern for transform dispatch is elegant, the telemetry integration is thorough, and the
project has solid CI/CD and testing infrastructure. That said, there are significant issues
ranging from performance-killing Spark anti-patterns to architectural coupling that will
limit the project's scalability. Below is every issue I found, grouped by severity.

---


---

## High: Architecture & Design

### 8. Singleton registries create tight coupling and testing friction
**File:** `src/samara/types.py`

Every pipeline component (extract, transform, load, job) accesses `DataFrameRegistry()`
and `StreamingQueryRegistry()` as singletons. This means:
- No two pipelines can run concurrently in the same process
- Every test must explicitly clear registries to avoid cross-test contamination
- Components cannot be tested in isolation without the global state
- Tests run with `-n 1` (serial) likely because of this shared state

**Fix:** Use dependency injection. Pass the registry into components via constructor or
a pipeline context object. The singletons can remain as a default, but components should
not hardcode their dependency on the global instance.


---

## Medium: Code Quality

### 14. Duplicated error handling boilerplate in CLI
**File:** `src/samara/cli.py`

All three commands (`validate`, `run`, `export_schema`) repeat the same outer try/except:

```python
except click.exceptions.Exit:
    raise
except KeyboardInterrupt as e:
    logger.warning("Process interrupted by user")
    raise click.exceptions.Exit(ExitCode.KEYBOARD_INTERRUPT) from e
except Exception as e:
    logger.error(...)
    raise click.exceptions.Exit(ExitCode.UNEXPECTED_ERROR) from e
```

This is 10 lines duplicated 3 times. Extract into a decorator or context manager.



## Low: Suggestions & Enhancements

### 20. No DAG-based execution for transforms
Transforms execute sequentially based on config ordering. Users must manually ensure
correct topological order. For complex pipelines with joins and branches, this is error-prone.
Consider supporting automatic topological sort based on `upstream_id` references.

### 21. No data quality validation step in the pipeline
For a data pipeline framework, there's no built-in support for data quality checks
(row count thresholds, null percentage checks, schema drift detection, custom assertions).
This is typically essential for production pipelines.

### 22. No checkpointing or resumability
If a job fails in the load phase, all extracts and transforms must be re-executed.
For long-running pipelines, adding optional DataFrame checkpointing between stages
would save significant time on retries.

### 24. Consider `StrEnum` for `ExtractMethod`, `LoadMethod`, `JobEngine`
These enums use `Enum` with string values. Python 3.11+ `StrEnum` would allow direct
string comparison and serialization without `.value` access, reducing boilerplate.
Since the project targets Python 3.13+, this is available.
