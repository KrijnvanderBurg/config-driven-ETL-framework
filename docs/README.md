# User Documentation

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

## Documentation Sections

- **[Alerting](alerting/README.md)**: Configure notifications for pipeline failures and exceptions

## Examples

Complete working examples are available in the `examples/` directory at the project root.

## Need Help?

- Check the [Contributing Guide](../../CONTRIBUTING.md) for development guidelines
- Review the [Code of Conduct](../../CODE_OF_CONDUCT.md)
- Visit the [GitHub repository](https://github.com/krijnvanderburg/config-driven-pyspark-framework) for issues and discussions
