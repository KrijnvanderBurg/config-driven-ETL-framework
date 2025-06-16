# Flint - A Config Driven Pyspark Framework

## Overview

Flint is a config-driven ETL (Extract, Transform, Load) framework built on Apache Spark. It enables data engineers to define complex data pipelines through declarative configuration files rather than writing extensive code, reducing development time and promoting standardization across teams.

The framework follows a "configuration as code" philosophy, allowing for version-controlled, easily reviewable data pipelines that separate the business logic from implementation details.

## Key Features

- **Declarative Configuration**: Define entire ETL pipelines using JSON or YAML configuration files
- **Modular Architecture**: Cleanly separated extract, transform, and load components
- **Extensible Transform System**: Easily add custom transformations through a plugin registry
- **Batch & Streaming Support**: Process data in batch or streaming mode using the same framework

### Benefits for Teams

- **Reduced Development Time**: Create new data pipelines with minimal code
- **Standardization**: Enforce consistent approaches to data processing across teams
- **Maintainability**: Declarative configs make pipelines easier to understand and modify
- **Reusability**: Common transformations can be shared across multiple pipelines
- **Type Safety**: Strongly-typed models ensure configuration correctness
- **Separation of Concerns**: Data engineers focus solely on data logic, not implementation details


## ⚡ Quick Start

### Installation

```bash
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

Each pipeline is defined through a configuration file with three main sections: extracts, transforms, and loads, each may consist of multiple elements.

### Extract Configuration

```json
{
  "name": "extract-name",
  "method": "batch|stream",
  "data_format": "csv|json|parquet|...",
  "location": "path/to/source",
  "schema": "path/to/schema.json",
  "options": {
    "header": true,
    "delimiter": ",",
    "inferSchema": false
    // Other format-specific options
  }
}
```

### Transform Configuration

```json
{
  "name": "transform-name",
  "upstream_name": "previous-step-name",
  "functions": [
    {
      "function": "transform-function-name",
      "arguments": {
        // Function-specific arguments
      }
    }
  ]
}
```

### Load Configuration

```json
{
  "name": "load-name",
  "upstream_name": "previous-step-name",
  "method": "batch|stream",
  "data_format": "csv|json|parquet|...",
  "location": "path/to/destination",
  "mode": "overwrite|append|ignore|error",
  "options": {
    // Format-specific options
  }
}
```

## 🏗️ Architecture

Flint is built with a registry-based architecture that dynamically matches different data formats and operations to their implementations. This allows for extension without modifying existing code.

### How It Works

The framework parses configuration files into strongly-typed models that define pipeline behavior. Each job follows this flow:

1. **Configuration Parsing**: JSON/YAML files are parsed into typed models
2. **Extract Phase**: Data is read from source systems into DataFrames
3. **Transform Phase**: Business logic is applied through registered transform functions
4. **Load Phase**: Processed data is written to destination systems
5. **Execution**: The job orchestrates the flow between these components

DataFrames flow through the pipeline via a singleton registry that maintains references by name, enabling multi-step transformations. This design separates configuration from implementation, making pipelines flexible and maintainable while leveraging Spark's distributed processing capabilities.

### Sequence Diagram

![sequence diagram](docs/sequence_diagram.png)

### Class Diagram

![class diagram](docs/class_diagram.drawio.png)

## 🧩 Extending the Framework

### Creating a Custom Transform

Flint is designed to be extended with custom transformations:

1. Create a model in `src/flint/models/transforms/`
2. Create a transformer class in `src/flint/core/transforms/` and register it:

```python
from pyspark.sql import DataFrame
from flint.core.transform import Function, TransformFunctionRegistry
from flint.models.transforms.your_model import YourFunctionModel

@TransformFunctionRegistry.register("your_transform_name")
class YourTransformFunction(Function[YourFunctionModel]):
    model_cls = YourFunctionModel

    def transform(self):
        def __f(df: DataFrame) -> DataFrame:
            # Your transformation logic here
            return transformed_df
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

