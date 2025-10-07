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

## Jobs

Each job defines a complete ETL workflow. Jobs execute sequentially.

```jsonc
{
    "id": "job-id",
    "description": "Job description",
    "enabled": true,
    "engine_type": "spark",
    "extracts": [/* Read data */],
    "transforms": [/* Process data */],
    "loads": [/* Write data */],
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

Functions: `select`, `filter`, `cast`, `drop`, `dropduplicates`, `join`, `withcolumn`

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
    "mode": "overwrite",                    // overwrite | append | ignore | error
    "options": {
        "compression": "snappy"
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

