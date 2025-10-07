# Documentation

This directory contains comprehensive documentation for using Flint, a configuration-driven data processing framework.

## Contents

### [Alerting](alerting/README.md)
Configure notifications and alerts for pipeline failures and exceptions. The alerting system allows you to:
- Define notification channels (email, HTTP webhooks, file logs)
- Create conditional triggers based on exception patterns and environment variables
- Route different errors to appropriate teams
- Format alert messages with custom templates

**Quick Start**: See [alerting/README.md](alerting/README.md) for overview and [alerting/channels.md](alerting/channels.md) for channel configuration.


### [Runtime](runtime/README.md)
...

## Getting Started

Flint enables you to build complete ETL pipelines through JSON configuration files. Instead of writing code, you define:

1. **Data Sources**: Where to extract data from (files, databases, APIs)
2. **Transformations**: How to process and transform the data
3. **Destinations**: Where to load the processed data
4. **Alerts**: How to notify teams when issues occur

### Basic Pipeline Structure

```jsonc
{
    "alert": {
        "channels": [/* notification destinations */],
        "triggers": [/* conditional alert rules */]
    },
    "runtime": {
        "jobs": [
            {
                "name": "my-pipeline",
                "engine": "spark",
                "extracts": [/* data sources */],
                "transforms": [/* processing steps */],
                "loads": [/* output destinations */],
                "hooks": {/* lifecycle events */}
            }
        ]
    }
}
```

### Running a Pipeline

```bash
python -m flint run \
    --alert-filepath="path/to/alerts.jsonc" \
    --runtime-filepath="path/to/pipeline.jsonc"
```


## CLI Commands

### validate
Validates configuration files and optionally tests alert routing:
x
```bash
python -m flint validate \
    --alert-filepath path/to/alerts.jsonc \
    --runtime-filepath path/to/job.jsonc \
    [--test-exception "error message"] \
    [--test-env-var KEY=VALUE]
```

### run
Executes the configured pipeline:

```bash
python -m flint run \
    --alert-filepath path/to/alerts.jsonc \
    --runtime-filepath path/to/job.jsonc
```

### Options
- `--log-level`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `-v, --version`: Show version information


## Documentation Sections

- **[Alerting](alerting/README.md)**: Configure notifications for pipeline failures and exceptions


## Architecture and flow

1. **Parse Configuration** → Validate and convert JSON/YAML configurations into typed models
2. **Initialize Components** → Set up extract, transform, and load objects based on configuration
3. **Execute Pipeline** → Process data through the configured workflow in sequence
4. **Monitor & Log** → Track execution progress and handle errors

#### Sequence Diagram
![Flint Data Flow](docs/sequence_diagram.png)

### Key Components

- **Registry System**: Central repository that manages registered components and data frames
- **Type Models**: Strongly-typed configuration models providing compile-time validation
- **Function Framework**: Plugin system for custom transformations
- **Execution Engine**: Coordinates the pipeline flow and handles dependencies

#### Class Diagram
![Class Diagram](docs/class_diagram.drawio.png)

- **Job**: Orchestrates the entire pipeline execution
- **Extract**: Reads data from various sources into DataFrames
- **Transform**: Applies business transform logic through registered functions
- **Load**: Writes processed data to destination





## 📋 Configuration Reference

### Pipeline Structure

A Flint pipeline is defined by three core components in your configuration file:

```
Configuration
├── Extracts - Read data from source systems (CSV, JSON, Parquet, etc.)
├── Transforms - Apply business logic and data processing
└── Loads - Write results to destination systems
```

Each component has a standardized schema and connects through named references:

<details>
<summary><b>Extract Configuration</b></summary>

```jsonc
{
  "id": "extract-id",                    // Required: Unique identifier
  "method": "batch|stream",                  // Required: Processing method
  "data_format": "csv|json|parquet|...",     // Required: Source format
  "location": "path/to/source",              // Required: Source location
  "schema": "path/to/schema.json",           // Optional: Schema definition
  "options": {                               // Optional: PySpark reader options
    "header": true,
    "delimiter": ",",
    "inferSchema": false
  }
}
```

**Supported Formats:** CSV, JSON, Parquet, Avro, ORC, Text, JDBC, Delta (with appropriate dependencies)
</details>

<details>
<summary><b>Transform Configuration</b></summary>

```jsonc
{
  "id": "transform-id",                  // Required: Unique identifier
  "upstream_id": "previous-step-id",     // Required: Reference previous stage
  "functions": [                             // Required: List of transformations
    {
      "function_yupe": "transform-function-name", // Required: Registered function name
      "arguments": {                         // Required: Function-specific arguments
        "key1": "value1",
        "key2": "value2"
      }
    }
  ]
}
```

**Function Application:** Transformations are applied in sequence, with each function's output feeding into the next.
</details>

<details>
<summary><b>Load Configuration</b></summary>

```jsonc
{
  "id": "load-id",                       // Required: Unique identifier
  "upstream_id": "previous-step-id",     // Required: Reference previous stage
  "method": "batch|stream",                  // Required: Processing method
  "data_format": "csv|json|parquet|...",     // Required: Destination format
  "location": "path/to/destination",         // Required: Output location
  "mode": "overwrite|append|ignore|error",   // Required: Write mode
  "options": {},                             // Optional: PySpark writer options
  "schema_export": "",
}
```

**Modes explained:**
- `overwrite`: Replace existing data
- `append`: Add to existing data
- `ignore`: Ignore operation if data exists
- `error`: Fail if data already exists
</details>

### Environment variables
- Log level can be set by environment variable `FLINT_LOG_LEVEL`. If not present, then it will use `LOG_LEVEL`. If also not present, then it will default to `INFO`. Both env variables can be set to `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.


## Examples

Complete working examples are available in the `examples/` directory at the project root.

## Need Help?

- Check the [Contributing Guide](../../CONTRIBUTING.md) for development guidelines
- Review the [Code of Conduct](../../CODE_OF_CONDUCT.md)
- Visit the [GitHub repository](https://github.com/krijnvanderburg/config-driven-pyspark-framework) for issues and discussions
