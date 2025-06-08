# PySpark Ingestion Framework

A scalable, configurable ETL (Extract, Transform, Load) framework built on Apache PySpark for data ingestion and processing pipelines.

## Overview

This framework provides a structured approach to building data ingestion pipelines with PySpark. It separates the ETL process into distinct phases (extract, transform, load) and allows for configuration-driven pipeline execution.

## Features

- **Configuration-driven**: Define pipelines using simple configuration files
- **Modular architecture**: Pluggable components for extract, transform, and load operations
- **Type safety**: Leverages Python type hints throughout the codebase
- **Extensible**: Register custom components through decorator patterns

## Installation

```bash
# Install using pip
pip install -e .

# Or with poetry
poetry install
```

## Usage

Basic usage:

```python
from ingestion_framework.core.job import Job
from pathlib import Path

# Create and run a job from a configuration file
job = Job.from_file(filepath=Path("path/to/config.json"))
job.execute()
```

Run from command line:

```bash
python -m ingestion_framework --filepath path/to/config.json
```

## Documentation

For detailed documentation, see the [docs](./docs) directory.

## License

This project is licensed under the Creative Commons BY-NC-ND 4.0 DEED Attribution-NonCommercial-NoDerivs 4.0 International License.