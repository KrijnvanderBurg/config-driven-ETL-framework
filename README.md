<p align="center">
  <img src="docs/logo.svg" alt="Flint Logo" width="250"/>
</p>

<h1 align="center">Flint</h1>

<p align="center">
  <b>A lightweight, extensible framework for PySpark ETL pipelines</b>
</p>

<p align="center">
  <a href="https://pypi.org/project/flint/"><img src="https://img.shields.io/badge/python-3.11-informational" alt="Python Versions"></a>
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/LICENSE"><img src="https://img.shields.io/github/license/krijnvanderburg/config-driven-pyspark-framework?style=flat-square" alt="License"></a>
  <a href="https://spark.apache.org/docs/latest/"><img src="https://img.shields.io/badge/spark-3.5.0+-lightgrey" alt="Apache Spark"></a>
</p>

<p align="center">
  <b>Built by Krijn van der Burg for the data engineering community</b>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/stargazers">⭐ Star this repo</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/issues">🐛 Report Issues</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/discussions">💬 Join Discussions</a>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/releases">📥 Releases (TBD)</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/CHANGELOG.md">📝 Changelog (TBD)</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/CONTRIBUTING.md">🤝 Contributing</a>
</p>

---

Flint changes Data Engineering by shifting from custom pipeline code to declarative configuration for complete ETL pipelines workflows. The framework handles execution details while you focus on what your data should do, not how to implement it. 

This configuration-driven approach standardizes pipeline patterns across teams, reduces code and complexity for straightforward ETL jobs, improves maintainability, and makes complex data workflows accessible to users with limited programming experience.

## ⚡ Quick Start

### Installation

```bash
git clone https://github.com/krijnvanderburg/config-driven-pyspark-framework.git
cd config-driven-pyspark-framework
poetry install

python -m flint run \
  --alert-filepath="examples/join_select/job.jsonc" \
  --runtime-filepath="examples/join_select/jobjson"
```
## 🔍 Example: Customer Order Analysis
Running this command executes a complete pipeline that showcases Flint's key capabilities:

- **Multi-format extraction**: Seamlessly reads from both CSV and JSON sources
  - Source options like delimiters and headers are configurable through the configuration file
  - Schema validation ensures data type safety and consistency across all sources

- **Flexible transformation chain**: Combines domain-specific and generic transforms
  - First a join to combine both datasets on `customer_id`
  - Then applies the generic `select` transform to project only needed columns
  - Each transform function can be easily customized through its arguments

- **Configurable loading**: Writes results as CSV with customizable settings
  - Easily change to Parquet, Delta, or other formats by modifying `data_format`
  - Output mode (overwrite/append) controlled by a simple parameter
  - Output to multiple formats or locations by creating another load entry.

#### Configuration: examples/join_select/job.json

```jsonc
{
    "runtime": {
        "id": "customer-orders-pipeline",
        "description": "ETL pipeline for processing customer orders data",
        "enabled": true,
        "jobs": [
            {
                "id": "bronze",
                "description": "",
                "enabled": true,
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file",
                        "data_format": "csv",
                        "location": "examples/join_select/customers/",
                        "method": "batch",
                        "options": {
                            "delimiter": ",",
                            "header": true,
                            "inferSchema": false
                        },
                        "schema": "examples/join_select/customers_schema.json"
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "data_format": "json",
                        "location": "examples/join_select/orders/",
                        "method": "batch",
                        "options": {
                            "multiLine": true,
                            "inferSchema": false
                        },
                        "schema": "examples/join_select/orders_schema.json"
                    }
                ],
                "transforms": [
                    {
                        "id": "transform-join-orders",
                        "upstream_id": "extract-customers",
                        "options": {},
                        "functions": [
                            {"function_type": "join", "arguments": { "other_upstream_id": "extract-orders", "on": ["customer_id"], "how": "inner"}},
                            {"function_type": "select", "arguments": {"columns": ["name", "email", "signup_date", "order_id", "order_date", "amount"]}}
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-customer-orders",
                        "upstream_id": "transform-join-orders",
                        "load_type": "file",
                        "data_format": "csv",
                        "location": "examples/join_select/output",
                        "method": "batch",
                        "mode": "overwrite",
                        "options": {
                            "header": true
                        },
                        "schema_export": ""
                    }
                ],
                "hooks": {
                    "onStart": [],
                    "onFailure": [],
                    "onSuccess": [],
                    "onFinally": []
                }
            }
        ]
    }
}

```

## Components

Flint's architecture consists of two integrated systems that work together through configuration:

### Runtime System
- **Extracts**: Read from sources (files, databases)
- **Transforms**: Process data through function chains
- **Loads**: Write to destinations
- **Hooks**: Execute actions during pipeline lifecycle

[Runtime System Documentation](docs/runtime/README.md)

### Alert System
- **Channels**: Where alerts are sent (email, HTTP, files)
- **Triggers**: When alerts are sent (rule-based conditions)
- **Templates**: How alerts are formatted

[Alert System Documentation](docs/alert/README.md)

All components are configured through JSON files, following standardized schemas detailed in the [complete documentation](docs/README.md).


## 🚀 Getting Help

- **Examples**: Explore working samples in the [examples/](examples/) directory
- **Documentation**: Refer to the [Configuration Reference](#-configuration-reference) section for detailed syntax
- **Community**: Ask questions and report issues on [GitHub Issues](https://github.com/krijnvanderburg/config-driven-pyspark-framework/issues)
- **Source Code**: Browse the implementation in the [src/flint](src/flint/) directory

## 🤝 Contributing

Contributions are welcome! Feel free to submit a pull request and message me.

## 📄 License

This project is licensed under the Creative Commons Attribution 4.0 International License (CC-BY-4.0) - see the [LICENSE](LICENSE) file for details.
