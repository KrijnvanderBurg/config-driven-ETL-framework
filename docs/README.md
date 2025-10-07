# Flint Documentation
This directory contains comprehensive documentation for Flint, a configuration-driven data processing framework. Flint's architecture consists of two integrated systems that work together through configuration:

## [Getting started](./getting_started.md)
Install Flint, run example pipelines, and learn how to create your own data pipelines using configuration files. Includes detailed examples of extract, transform, and load configurations with explanations of key fields.

## [CLI arguments and exit codes](./cli.md)
Reference for Flint's command-line interface with commands for validating configurations (`validate`) and executing pipelines (`run`). Includes supported options, environment variables, and a complete list of exit codes for troubleshooting.

## [Runtime System](runtime/README.md)
Details about configuring and using the data processing pipeline components.
- **Extracts**: Read data from sources (files, databases)
- **Transforms**: Process data through function chains
- **Loads**: Write data to destinations
- **Hooks**: Execute actions during pipeline lifecycle

## [Alerting System](alerting/README.md)
Configure notifications and alerts for pipeline failures and exceptions:
- Define notification channels (email, HTTP webhooks, file logs)
- Create conditional triggers based on exception patterns and environment variables
- Route different errors to appropriate teams
- Format alert messages with custom templates
- **Channels**: Where alerts are sent (email, HTTP, files)
- **Triggers**: When alerts are sent (rule-based conditions)
- **Templates**: How alerts are formatted
