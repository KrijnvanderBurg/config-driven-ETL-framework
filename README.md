<p align="center">
  <img src="docs/logo2.svg"/>
</p>

<h1 align="center">Samara</h1>

<p align="center">
  <b>An extensible framework for configuration-driven data pipelines</b>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/stargazers">⭐ Star this repo</a> •
  <a href="./docs/README.md">📚 Documentation</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/issues">🐛 Report Issues</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/discussions">💬 Join Discussions</a>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/releases">📥 Releases</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/blob/main/CHANGELOG.md">📝 Changelog (TBD)</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-ETL-framework/blob/main/CONTRIBUTING.md">🤝 Contributing</a>
</p>

<p align="center">
  <b>Built by Krijn van der Burg for the Data Engineering community</b>
</p>

---

Samara transforms data engineering by shifting from custom code to declarative configuration for complete ETL pipeline workflows. The framework handles all execution details while you focus on what your data should do, not how to implement it. This configuration-driven approach standardizes pipeline patterns across teams, reduces complexity for ETL jobs, improves maintainability, and makes data workflows accessible to users with limited programming experience.

The processing engine is abstracted away through configuration, making it easy to switch engines or run the same pipeline in different environments. The current version supports Apache Spark, with Polars support in development.

## ⚡ Quick Start

### Installation
```bash
# Clone the repository
git clone https://github.com/krijnvanderburg/config-driven-ETL-framework.git
cd config-driven-ETL-framework

# Install dependencies
poetry install
```

### Run an example pipeline
```bash
python -m samara run \
  --alert-filepath="examples/join_select/alert.jsonc" \
  --runtime-filepath="examples/join_select/job.jsonc"
```

## 📚 Documentation
Samara's documentation guides you through installation, configuration, and development:

- **[Getting Started](./docs/getting_started.md)** - Installation and basic concepts
- **[Example Pipelines](./examples/)** - Ready-to-run examples demonstrating key features
- **[CLI Reference](./docs/cli.md)** - Command-line interface options and examples
- **[Configuration Reference](./docs/README.md)** - Complete syntax guide for all configuration options
  - **[Runtime System](./docs/runtime/README.md)** - ETL pipeline configuration (extracts, transforms, loads)
  - **[Alert System](./docs/alert/README.md)** - Error handling and notification configuration
- **[Architecture](./docs/architecture.md)** - Design principles and framework structure
- **[Custom Extensions](./docs/architecture.md#extending-with-custom-transforms)** - Building your own transforms

For complete documentation covering all aspects of Samara, visit the documentation home page.

## 🔍 Example: Customer Order Analysis
Running this command executes a complete pipeline that showcases Samara's key capabilities:

- **Multi-format extraction**: Seamlessly reads from both CSV and JSON sources
  - Source options like delimiters and headers are configurable through the configuration file
  - Schema validation ensures data type safety and consistency across all sources

- **Flexible transformation chain**: Performed in order as given
  - First a `join` to combine both datasets on `customer_id`
  - Then applies a `select` transform to project only needed columns
  - Each transform function can be easily customized through its arguments

- **Configurable loading**: Writes results as CSV with customizable settings
  - Easily change to Parquet, Delta, or other formats by modifying `data_format`
  - Output mode (overwrite/append) controlled by a simple parameter
  - Output to multiple formats or locations by creating another load entry

#### Configuration: [`examples/join_select/job.jsonc`](./examples/join_select/job.jsonc)
```jsonc
{
    "runtime": {
        "id": "customer-orders-pipeline",
        "description": "ETL pipeline for processing customer orders data",
        "enabled": true,
        "jobs": [
            {
                "id": "silver",
                "description": "Combine customer and order source data into a single dataset",
                "enabled": true,
                "engine_type": "spark", // Specifies the processing engine to use
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file", // Read from file system
                        "data_format": "csv", // CSV input format
                        "location": "examples/join_select/customers/", // Source directory
                        "method": "batch", // Process all files at once
                        "options": {
                            "delimiter": ",", // CSV delimiter character
                            "header": true, // First row contains column names
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/join_select/customers_schema.json" // Path to schema definition
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "data_format": "json", // JSON input format
                        "location": "examples/join_select/orders/",
                        "method": "batch",
                        "options": {
                            "multiLine": true, // Each JSON object may span multiple lines
                            "inferSchema": false // Use provided schema instead of inferring
                        },
                        "schema": "examples/join_select/orders_schema.json"
                    }
                ],
                "transforms": [
                    {
                        "id": "transform-join-orders",
                        "upstream_id": "extract-customers", // First input dataset from extract stage
                        "options": {},
                        "functions": [
                            {
                                "function_type": "join", // Join customers with orders
                                "arguments": { 
                                    "other_upstream_id": "extract-orders", // Second dataset to join
                                    "on": ["customer_id"], // Join key
                                    "how": "inner" // Join type (inner, left, right, full)
                                }
                            },
                            {
                                "function_type": "select", // Select only specific columns
                                "arguments": {
                                    "columns": ["name", "email", "signup_date", "order_id", "order_date", "amount"]
                                }
                            }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-customer-orders",
                        "upstream_id": "transform-join-orders", // Input dataset for this load
                        "load_type": "file", // Write to file system
                        "data_format": "csv", // Output as CSV
                        "location": "examples/join_select/output", // Output directory
                        "method": "batch", // Write all data at once
                        "mode": "overwrite", // Replace existing files if any
                        "options": {
                            "header": true // Include header row with column names
                        },
                        "schema_export": "" // No schema export
                    }
                ],
                "hooks": {
                    "onStart": [], // Actions to execute before pipeline starts
                    "onFailure": [], // Actions to execute if pipeline fails
                    "onSuccess": [], // Actions to execute if pipeline succeeds
                    "onFinally": [] // Actions to execute after pipeline completes (success or failure)
                }
            }
        ]
    }
}
```

## 🚀 Getting Help
- [**Documentation**](./docs/README.md): Refer to the Configuration Reference section for detailed syntax
- [**Examples**](./examples/): Explore working samples in the examples directory
- [**Community**](https://github.com/krijnvanderburg/config-driven-ETL-framework/issues): Ask questions and report issues on GitHub Issues
- [**Source Code**](./src/samara/): Browse the implementation in the src/samara directory

## 🤝 Contributing
Contributions are welcome! Feel free to submit a pull request and message Krijn van der Burg on [linkedin](https://linkedin.com/in/krijnvanderburg/).

## 📄 License
This project is licensed under the Creative Commons Attribution 4.0 International License (CC-BY-4.0) - see the LICENSE file for details.
