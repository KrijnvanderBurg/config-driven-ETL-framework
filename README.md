<p align="center">
  <img src="docs/logo.svg" alt="Flint Logo" width="250"/>
</p>

<h1 align="center">Flint</h1>

<p align="center">
  <b>Build PySpark ETL pipelines with the ultimate extensible framework</b>
</p>

<p align="center">
  <a href="https://pypi.org/project/flint/"><img src="https://img.shields.io/badge/python-3.11-informational" alt="Python Versions"></a>
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/LICENSE"><img src="https://img.shields.io/github/license/krijnvanderburg/config-driven-pyspark-framework?style=flat-square" alt="License"></a>
</p>

---

## 🔍 Overview

**Flint is a barebones, logical framework for Apache Spark** that eliminates repetitive code through a declarative, configuration-driven approach.

The core philosophy is simple: provide a clean, intuitive structure that lets teams easily create and share their own transformations. No complex abstractions—just a logical framework that makes PySpark development straightforward and maintainable.

Flint was designed to be **minimal yet powerful** - giving you the structural foundations while letting your team extend it with your own business-specific transforms.

Data teams waste countless hours writing and maintaining boilerplate Spark code. Flint solves this by letting you define complete ETL workflows with simple JSON/YAML files.

### Build pipelines that are:

✅ **Version-controlled** - More easily track changes by inspecting one config file  
✅ **Maintainable** - Clear separation of business logic and implementation details  
✅ **Standardized** - Consistent patterns across your organization

No more writing repetitive, error-prone Spark code. Flint lets you focus on data transformations while handling the application structure complexities.


## ⚡ Quick Start

### Installation

```bash
git clone https://github.com/krijnvanderburg/config-driven-pyspark-framework.git
cd config-driven-pyspark-framework
poetry install
```

### Running the Example Pipeline

Try the included example that joins customer and order data:

```bash
python -m flint --config-filepath examples/job.json
```

This example uses sample data provided in the `examples/customer_orders/` directory.

### Built-in Transformations

Flint comes with several example transformations, from generic transform functionality to an example source specific business logic:

| Transform | Description |
|-----------|-------------|
| `select` | Generic Select specific columns from a DataFrame. |
| `calculate_birth_year` | Calculate birth year based on age. |
| `customer_orders_bronze` | Example join customer and order data with filtering. |

## 📋 Configuration Reference

### Full Schema Structure

A Flint pipeline consists of three main components working together:

```
Configuration
├── Extracts - Read data from source systems
├── Transforms - Apply business logic
└── Loads - Write data to destination systems
```

Each component is configured through a specific schema:

<details>
<summary><b>Extract Configuration</b></summary>

```json
{
  "name": "extract-name",                    // Required: Unique identifier
  "method": "batch|stream",                  // Required: Processing method
  "data_format": "csv|json|parquet|...",     // Required: Source format
  "location": "path/to/source",              // Required: Source location
  "schema": "path/to/schema.json",           // Optional: Schema definition
  "options": {                               // Optional: Format-specific options
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

```json
{
  "name": "transform-name",                  // Required: Unique identifier
  "upstream_name": "previous-step-name",     // Required: Input data source
  "functions": [                             // Required: List of transformations
    {
      "function": "transform-function-name", // Required: Registered function name
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

```json
{
  "name": "load-name",                       // Required: Unique identifier
  "upstream_name": "previous-step-name",     // Required: Input data source
  "method": "batch|stream",                  // Required: Processing method
  "data_format": "csv|json|parquet|...",     // Required: Destination format
  "location": "path/to/destination",         // Required: Output location
  "mode": "overwrite|append|ignore|error",   // Required: Write mode
  "options": {                               // Optional: Format-specific options
    "compression": "snappy",
    "partitionBy": ["column1", "column2"]
  }
}
```

**Modes explained:**
- `overwrite`: Replace existing data
- `append`: Add to existing data
- `ignore`: Ignore operation if data exists
- `error`: Fail if data already exists
</details>



### Data Flow

1. **Parse Configuration** → Validate and convert JSON/YAML into typed models
2. **Initialize Components** → Set up extract, transform, and load objects
3. **Execute Pipeline** → Process data through the configured workflow
4. **Monitor & Log** → Track execution and handle errors

![Flint Data Flow](docs/sequence_diagram.png)

### Key Components

- **Registry System**: Central repository that manages registered components and data frames
- **Type Models**: Strongly-typed configuration models providing compile-time validation
- **Function Framework**: Plugin system for custom transformations
- **Execution Engine**: Coordinates the pipeline flow and handles dependencies

<details>
<summary><b>Class Structure</b></summary>

![Class Diagram](docs/class_diagram.drawio.png)

- **Job**: Orchestrates the entire pipeline execution
- **Extract**: Reads data from various sources into DataFrames
- **Transform**: Applies business logic through registered functions
- **Load**: Writes processed data to destination systems
</details>

### Design Principles

- **Separation of Concerns**: Each component has a single, well-defined responsibility
- **Dependency Injection**: Components receive their dependencies rather than creating them
- **Plugin Architecture**: Extensions are registered with the framework without modifying core code
- **Configuration as Code**: All pipeline behavior is defined declaratively in configuration files

## 🧩 Extending with Custom Transforms

Flint's power comes from its extensibility. Create custom transformations to encapsulate your business logic:

### Step 1: Define the configuration model

```python
# src/flint/models/transforms/model_encryption.py
from pydantic import BaseModel, Field
from typing import List

class EncryptColumnsModel(BaseModel):
    """Configuration model for column encryption transform."""
    columns: List[str] = Field(
        description="Columns to encrypt",
        min_items=1
    )
    key_name: str = Field(
        description="Encryption key name to use"
    )
```

### Step 2: Create the transform function

```python
# src/flint/core/transforms/encryption.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.model_encryption import EncryptColumnsModel

@TransformFunctionRegistry.register("encrypt_columns")
class EncryptColumnsFunction(Function[EncryptColumnsModel]):
    """Encrypts specified columns in a DataFrame."""
    model_cls = EncryptColumnsModel
    
    def transform(self):
        def __f(df: DataFrame) -> DataFrame:
            # Get parameters from the model
            columns = self.model.columns
            key_name = self.model.key_name
            
            # Apply encryption to each column
            result_df = df
            for column in columns:
                result_df = result_df.withColumn(
                    column,
                    F.expr(f"aes_encrypt({column}, '{key_name}')")
                )
            return result_df
        return __f
```

## 📚 Examples

The [examples/](examples/) directory contains sample configurations and data files:

- `examples/job.json` - Basic example joining customer and order data
- `examples/customer_orders/` - Sample data files and schemas

## 🚀 Getting Help

- Check out the [examples/](examples/) directory for working samples
- Read the [Configuration Reference](#-configuration-reference) for detailed syntax
- Visit our [GitHub Issues](https://github.com/krijnvanderburg/config-driven-pyspark-framework/issues) page for support

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the CC-BY-4.0 License - see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <b>Built with ❤️ by the data engineering community</b>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/stargazers">⭐ Star us on GitHub</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/issues">🐛 Report Issues</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/discussions">💬 Join Discussions</a>
</p>

<p align="center">
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/releases">📥 Releases</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/CHANGELOG.md">📝 Changelog</a> •
  <a href="https://github.com/krijnvanderburg/config-driven-pyspark-framework/blob/main/CONTRIBUTING.md">🤝 Contributing</a>
</p>