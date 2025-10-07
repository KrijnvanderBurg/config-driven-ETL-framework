# Runtime

The runtime system orchestrates ETL pipelines through configuration. Define your data sources, transformations, and destinations in JSON—no code required.

## How It Works

1. Extract data from sources
2. Transform data through function chains, referencing upstream components by ID
3. Load processed data to destinations

## Configuration Structure
```bash
python -m flint run \
    --alert-filepath="examples/join_select/alert.jsonc" \
    --runtime-filepath="examples/join_select/job.jsonc"
```

The structure is as follows, all of the following fields are required

```jsonc
{
    "runtime": {
        "id": "unique-pipeline-id",
        "description": "Pipeline description",
        "enabled": true,                    // Disable entire runtime
        "jobs": [
            /* Job definitions */
        ]
    }
}
```


## Configuration Reference

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
  "id": "extract-id",                        // Required: Unique identifier
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
  "id": "transform-id",                      // Required: Unique identifier
  "upstream_id": "previous-step-id",         // Required: Reference previous stage
  "functions": [                             // Required: List of transformations
    {
      "function_type": "transform-function-name", // Required: Registered function name
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
  "id": "load-id",                           // Required: Unique identifier
  "upstream_id": "previous-step-id",         // Required: Reference previous stage
  "method": "batch|stream",                  // Required: Processing method
  "data_format": "csv|json|parquet|...",     // Required: Destination format
  "location": "path/to/destination",         // Required: Output location
  "mode": "overwrite|append|ignore|error|...",   // Required: Write mode
  "options": {},                             // Optional: PySpark writer options
  "schema_export": ""                        // Optional: Path to export schema
}
```

## Jobs

Each job defines a complete ETL workflow. Jobs execute sequentially.

```jsonc
{
    "id": "job-id",
    "description": "Job description",
    "enabled": true,
    "engine_type": "spark",
    "extracts": [/* data sources */],
    "transforms": [/* processing steps */],
    "loads": [/* destinations */],
    "hooks": {
        "onStart": [/* Actions on start */],
        "onFailure": [/* Actions on error */],
        "onSuccess": [/* Actions on success */],
        "onFinally": [/* Actions always */]
    }
}
```

### Extracts

Read data into the registry, identified by `id`.

```jsonc
{
    "id": "extract-customers",
    "extract_type": "file",
    "method": "batch",                      // batch | streaming
    "data_format": "csv",
    "location": "path/to/data.csv",
    "schema": "path/to/schema.json",        // JSON file or string
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

### Transforms

Apply functions to data from upstream components.

```jsonc
{
    "id": "transform-clean",
    "upstream_id": "extract-customers",     // Reference extract or transform ID
    "options": {},
    "functions": [
        {"function_type": "filter", "arguments": {"condition": "age > 18"}},
        {"function_type": "select", "arguments": {"columns": ["name", "email"]}},
        {"function_type": "cast", "arguments": {"columns": [{"column_name": "age", "cast_type": "IntegerType"}]}}
    ]
}
```

### Loads

Write data from upstream components to destinations.

```jsonc
{
    "id": "load-output",
    "upstream_id": "transform-clean",
    "load_type": "file",
    "method": "batch",
    "data_format": "parquet",
    "location": "output/processed/",
    "schema_export": "output/schema.json",
    "mode": "overwrite",
    "options": {
        // Spark options
    }
}
```

## Engine Support

Currently supports Spark (`"engine_type": "spark"`). Framework designed for multi-engine support.

See [Spark configuration](./spark.md) for engine-specific options.

## Complete Example

The hooks are still in development and will be documented in the future, but they are already required fields which may be kept empty.

```jsonc
{
    "runtime": {
        "id": "customer-analytics",
        "description": "Process customer orders with analytics",
        "enabled": true,
        "jobs": [
            {
                "id": "bronze-to-silver",
                "description": "Clean and join raw data",
                "enabled": true,
                "engine_type": "spark",
                "extracts": [
                    {
                        "id": "extract-customers",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "csv",
                        "location": "data/customers.csv",
                        "schema": "schemas/customers.json",
                        "options": {
                            "header": true,
                            "inferSchema": false
                        }
                    },
                    {
                        "id": "extract-orders",
                        "extract_type": "file",
                        "method": "batch",
                        "data_format": "json",
                        "location": "data/orders/",
                        "schema": "schemas/orders.json",
                        "options": {}
                    }
                ],
                "transforms": [
                    {
                        "id": "clean-customers",
                        "upstream_id": "extract-customers",
                        "options": {},
                        "functions": [
                            {"function_type": "dropduplicates", "arguments": {"columns": ["customer_id"]}},
                            {"function_type": "filter", "arguments": {"condition": "email IS NOT NULL"}}
                        ]
                    },
                    {
                        "id": "join-orders",
                        "upstream_id": "clean-customers",
                        "options": {},
                        "functions": [
                            {
                                "function_type": "join",
                                "arguments": {"right_id": "extract-orders", "on": ["customer_id"], "how": "inner"}
                            },
                            {
                                "function_type": "select",
                                "arguments": {"columns": ["customer_id", "name", "email", "order_id", "amount"]}
                            }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-parquet",
                        "upstream_id": "join-orders",
                        "load_type": "file",
                        "method": "batch",
                        "data_format": "parquet",
                        "location": "output/customer_orders/",
                        "schema_export": "output/schema.json",
                        "mode": "overwrite",
                        "options": {
                            "compression": "snappy"
                        }
                    }
                ],
                "hooks": {
                    "onStart": [],
                    "onSuccess": [],
                    "onFailure": [],
                    "onFinally": []
                }
            }
        ]
    }
}
```

