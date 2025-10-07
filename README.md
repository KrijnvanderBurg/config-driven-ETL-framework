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

## 🔍 Overview

**Flint is an intuitive barebones application framework for Apache Spark** that eliminates complex application setups and provides a configuration-driven approach. This is not a library you install and use, take the source code and extend the application with your implementations.

The core philosophy is simple: provide an barebones structure that lets teams easily create and share their own transformations. No complex abstractions—just a logical framework that makes PySpark development straightforward and maintainable.

Flint was designed to be **minimal yet powerful** - providing structural foundations while enabling your team to extend it with business-specific transforms.

### Build pipelines that are:

✅ **Maintainable** - Clear separation of application code and business logics.  
✅ **Standardized** - Consistent pipelines and patterns across your organization.  
✅ **Version-controlled** - More easily track changes by inspecting one config file.

Flint lets you focus on data transformations while handling the application structure complexities.


## ⚡ Quick Start

### Installation

```bash
git clone https://github.com/krijnvanderburg/config-driven-pyspark-framework.git
cd config-driven-pyspark-framework
code .                                   # shortcut for vscode
git submodule update --init --recursive  # recursive because of nested submodules
poetry install                           # install dependencies
```

## 🔍 Example: Customer Order Analysis

The included example demonstrates a real-world ETL pipeline:

- 📄 **Config**: [`examples/join_select/job.json`](./examples/join_select/job.jsonc)
- 🏃 **Execution**: 
  ```bash
  python -m flint run \
    --alert-filepath="examples/join_select/job.jsonc" 
    --runtime-filepath="examples/join_select/jobjson"
  ```
- 📂 **Output**: `examples/join_select/output/`

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

### Built-in Transformations

Flint includes ready-to-use generic transformations to jumpstart your development. These transformations can be configured directly through your JSON configuration files without writing any additional code.

#### Core Transformations

| Transform | Description | Example Usage |
|-----------|-------------|---------------|
| `select` | Select specific columns from a DataFrame | `{"function": "select", "arguments": {"columns": ["id", "name", "email"]}}` |
| `cast` | Convert columns to specified data types | `{"function": "cast", "arguments": {"columns": {"amount": "double", "date": "timestamp"}}}` |
| `drop` | Remove specified columns from a DataFrame | `{"function": "drop", "arguments": {"columns": ["temp_col", "unused_field"]}}` |
| `dropduplicates` | Remove duplicate rows based on specified columns | `{"function": "dropduplicates", "arguments": {"columns": ["id"]}}` |
| `filter` | Apply conditions to filter rows in a DataFrame | `{"function": "filter", "arguments": {"condition": "amount > 100"}}` |
| `join` | Combine DataFrames using specified join conditions | `{"function": "join", "arguments": {"other_df": "orders_df", "on": ["customer_id"], "how": "inner"}}` |
| `withcolumn` | Add or replace columns with computed values | `{"function": "withcolumn", "arguments": {"column_name": "full_name", "expression": "concat(first_name, ' ', last_name)"}}` |

> **💡 Tip:** You can create your own custom transformations for specific business needs following the pattern shown in the [Extending with Custom Transforms](#-extending-with-custom-transforms) section.


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

### Design Principles

- **Separation of Concerns**: Each component has a single, well-defined responsibility
- **Dependency Injection**: Components receive their dependencies rather than creating them
- **Plugin Architecture**: Extensions are registered with the framework without modifying core code
- **Configuration as Code**: All pipeline behavior is defined declaratively in configuration files

## 🧩 Extending with new generic or Custom Transforms

Flint's power comes from its extensibility. Create custom transformations to encapsulate your business logic. Let's look at a real example from Flint's codebase - the `select` transform:

### Step 1: Define the configuration model

```python
# src/flint/runtime/jobs/models/transforms/model_select.py

class SelectArgs(ArgsModel):
    """Arguments for column selection transform operations."""

    columns: list[str] = Field(..., description="List of column names to select from the DataFrame", min_length=1)


class SelectFunctionModel:
    """Configuration model for column selection transform operations."""

    function_type: Literal["select"] = "select"
    arguments: SelectArgs = Field(..., description="Container for the column selection parameters")
```

### Step 2: Create the transform function

```python
# src/flint/runtime/jobs/spark/transforms/select.py

@TransformFunctionRegistry.register("select")
class SelectFunction(SelectFunctionModel, Function):
    """Selects specified columns from a DataFrame."""
    model_cls = SelectFunctionModel

    def transform(self) -> Callable:
        """Returns a function that projects columns from a DataFrame."""
        def __f(df: DataFrame) -> DataFrame:
            return df.select(*self.model.arguments.columns)

        return __f
```

### Step 3: Use in your pipeline configuration

```jsonc
{
  "extracts": [
    // ...
  ],
  "transforms": [
    {
      "id": "transform-user-data",
      "upstream_id": "extract-users",
      "functions": [
        { "function_type": "select", "arguments": { "columns": ["user_id", "email", "signup_date"] } }
      ]
    }
  ],
  "loads": [
    // ...
  ]
}
```

> 🔍 **Best Practice**: Create transforms that are generic enough to be reusable but specific enough to encapsulate meaningful business logic.

### Building a Transform Library

As your team develops more custom transforms, you create a powerful library of reusable components:

1. **Domain-Specific Transforms**: Create transforms that encapsulate your business rules
2. **Industry-Specific Logic**: Build transforms tailored to your industry's specific needs
3. **Data Quality Rules**: Implement your organization's data quality standards

The registration system makes it easy to discover and use all available transforms in your configurations without modifying the core framework code.


## 🚀 Getting Help

- **Examples**: Explore working samples in the [examples/](examples/) directory
- **Documentation**: Refer to the [Configuration Reference](#-configuration-reference) section for detailed syntax
- **Community**: Ask questions and report issues on [GitHub Issues](https://github.com/krijnvanderburg/config-driven-pyspark-framework/issues)
- **Source Code**: Browse the implementation in the [src/flint](src/flint/) directory

## 🤝 Contributing

Contributions are welcome! Feel free to submit a pull request and message me.

## 📄 License

This project is licensed under the Creative Commons Attribution 4.0 International License (CC-BY-4.0) - see the [LICENSE](LICENSE) file for details.
