# Spark Workflow Implementation

Configuration reference for Spark engine.

## Extract

### File Extract

```jsonc
{
    "id": "extract-data",
    "extract_type": "file",
    "method": "batch",                      // batch | streaming
    "data_format": "csv",
    "location": "path/to/data.csv",
    "schema": "schemas/data.json",          // JSON file path, JSON string, or empty ""
    "options": {
        // CSV
        "header": true,
        "delimiter": ",",
        "inferSchema": false,
        "dateFormat": "yyyy-MM-dd",
        // JSON
        "multiLine": true,
        // Parquet
        "mergeSchema": false
    }
}
```

## Transform

### Select

```jsonc
{"function_type": "select", "arguments": {"columns": ["id", "name", "email"]}}
```

### Filter

```jsonc
{"function_type": "filter", "arguments": {"condition": "age >= 18 AND status = 'active'"}}
```

Condition uses Spark SQL syntax: `AND`, `OR`, `NOT`, `=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `LIKE`, `IS NULL`

### Cast

```jsonc
{
    "function_type": "cast",
    "arguments": {
        "columns": [
            {"column_name": "age", "cast_type": "IntegerType"},
            {"column_name": "signup_date", "cast_type": "DateType"}
        ]
    }
}
```

### Drop

```jsonc
{"function_type": "drop", "arguments": {"columns": ["temp_col", "internal_id"]}}
```

### Drop Duplicates

```jsonc
{"function_type": "dropduplicates", "arguments": {"columns": ["customer_id"]}}
```

Empty `columns: []` deduplicates on all columns.

### Join

```jsonc
{
    "function_type": "join", "arguments": { "other_upstream_id": "extract-orders", "on": ["customer_id"], "how": "inner"}
}
```

### With Column

```jsonc
{
    "function_type": "withcolumn",
    "arguments": {
        "columns": [
            {"column_name": "full_name", "expression": "concat(first_name, ' ', last_name)"},
            {"column_name": "age_group", "expression": "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END"}
        ]
    }
}
```

Expression uses Spark SQL syntax with all built-in functions.

### Distinct

```jsonc
{"function_type": "distinct", "arguments": {}}
```

Removes all duplicate rows from the DataFrame.

### Drop NA

```jsonc
{
    "function_type": "dropna",
    "arguments": {
        "how": "any",                       // "any" = drop if any null, "all" = drop only if all null
        "thresh": null,                     // Minimum non-null values to keep row (overrides "how" when set)
        "subset": ["email", "phone"]        // Columns to check (null = all columns)
    }
}
```

### Order By

```jsonc
{
    "function_type": "orderby",
    "arguments": {
        "columns": [
            {"column_name": "signup_date", "ascending": false},
            {"column_name": "name", "ascending": true}
        ]
    }
}
```

### Group By

```jsonc
{
    "function_type": "groupby",
    "arguments": {
        "group_columns": ["department"],
        "aggregations": [
            {"function": "sum", "input_column": "salary", "output_column": "total_salary"},
            {"function": "count", "input_column": null, "output_column": "employee_count"},
            {"function": "avg", "input_column": "age", "output_column": "avg_age"}
        ]
    }
}
```

Supported functions: `sum`, `avg`, `mean`, `min`, `max`, `count`, `first`, `last`, `stddev`, `variance`. For `count`, `input_column` must be `null`.

### Aggregate

```jsonc
{
    "function_type": "aggregate",
    "arguments": {
        "group_by_columns": ["category"],   // null for global aggregation
        "aggregate_columns": [
            {"column_name": "price", "function": "avg", "alias": "avg_price"},
            {"column_name": "quantity", "function": "sum", "alias": "total_quantity"}
        ]
    }
}
```

### Pivot

```jsonc
{
    "function_type": "pivot",
    "arguments": {
        "group_by": ["region"],
        "pivot_column": "quarter",
        "values_column": "revenue",
        "agg_func": "sum"                   // sum | avg | max | min | count | first
    }
}
```

## Load

### File Load

```jsonc
{
    "id": "load-output",
    "upstream_id": "transform-data",
    "load_type": "file",
    "method": "batch",                      // batch | streaming
    "data_format": "parquet",
    "location": "output/processed/",
    "schema_export": "output/schema.json", // Empty "" to skip schema export
    "mode": "overwrite",                    // overwrite | append | ignore | error
    "options": {
        // CSV
        "header": true,
        "delimiter": ",",
        // Parquet
        "compression": "snappy",
        "partitionBy": ["year", "month"],
        // JSON
        "compression": "gzip",
        // Streaming only
        "checkpointLocation": "/tmp/checkpoint/",
        "trigger": "processingTime='10 seconds'"
    }
}
```

## Common Options by Format

**CSV**
```jsonc
"options": {
    "header": true,
    "delimiter": ",",
    "inferSchema": false,
    "dateFormat": "yyyy-MM-dd"
}
```

**JSON**
```jsonc
"options": {
    "multiLine": true,
    "dateFormat": "yyyy-MM-dd'T'HH:mm:ss"
}
```

**Parquet**
```jsonc
"options": {
    "mergeSchema": false,
    "pathGlobFilter": "*.parquet"
}
```

For complete options, see [Spark Data Sources documentation](https://spark.apache.org/docs/latest/sql-data-sources.html).


## Complete Example

```jsonc
{
    "workflow": {
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
                        "location": "examples/customer_orders/customers.csv",
                        "method": "batch",
                        "options": {
                            "delimiter": ",",
                            "header": true,
                            "inferSchema": false
                        },
                        "schema": "examples/customer_orders/customers_schema.json"
                    }
                ],
                "transforms": [
                    {
                        "id": "transform-join-orders",
                        "upstream_id": "extract-customers",
                        "options": {},
                        "functions": [
                            { "function_type": "drop", "arguments": {"columns": ["temp_col"]} },
                            { "function_type": "select", "arguments": {"columns": ["name", "email", "signup_date", "order_id", "order_date", "amount"]} }
                        ]
                    }
                ],
                "loads": [
                    {
                        "id": "load-customer-orders",
                        "upstream_id": "transform-join-orders",
                        "load_type": "file",
                        "data_format": "csv",
                        "location": "examples/customer_orders/output",
                        "method": "batch",
                        "mode": "overwrite",
                        "options": {
                            "header": true
                        }
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
