# Transform Functions

Transform functions process data within the pipeline. Each function is applied sequentially to the DataFrame from the upstream component.


### Building a Transform Library

As your team develops more custom transforms, you create a powerful library of reusable components:

1. **Domain-Specific Transforms**: Create transforms that encapsulate your business rules
2. **Industry-Specific Logic**: Build transforms tailored to your industry's specific needs
3. **Data Quality Rules**: Implement your organization's data quality standards

The registration system makes it easy to discover and use all available transforms in your configurations without modifying the core framework code.


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

## Available Functions

### select
Select specific columns from the DataFrame.

```jsonc
{
    "function_type": "select",
    "arguments": {
        "columns": ["id", "name", "email"]
    }
}
```

### filter
Filter rows based on conditions using Spark SQL syntax.

```jsonc
{
    "function_type": "filter",
    "arguments": {
        "condition": "age >= 18 AND status = 'active'"
    }
}
```

### cast
Convert column data types.

```jsonc
{
    "function_type": "cast",
    "arguments": {
        "columns": [
            {"column_name": "age", "cast_type": "IntegerType"},
            {"column_name": "date", "cast_type": "DateType"}
        ]
    }
}
```

Supported types: `IntegerType`, `LongType`, `FloatType`, `DoubleType`, `StringType`, `DateType`, `TimestampType`, `BooleanType`

### drop
Remove columns from the DataFrame.

```jsonc
{
    "function_type": "drop",
    "arguments": {
        "columns": ["temp_col", "unused_field"]
    }
}
```

### dropduplicates
Remove duplicate rows.

```jsonc
{
    "function_type": "dropduplicates",
    "arguments": {
        "columns": ["customer_id"]  // Empty array deduplicates on all columns
    }
}
```

### join
Join with another DataFrame.

```jsonc
{
    "function_type": "join",
    "arguments": {
        "right_id": "other-extract-id",  // ID of DataFrame to join with
        "on": ["customer_id"],           // Join columns
        "how": "inner"                   // inner | left | right | full | cross
    }
}
```

### withcolumn
Add or modify columns using Spark SQL expressions.

```jsonc
{
    "function_type": "withcolumn",
    "arguments": {
        "columns": [
            {
                "column_name": "full_name",
                "expression": "concat(first_name, ' ', last_name)"
            },
            {
                "column_name": "age_group",
                "expression": "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END"
            }
        ]
    }
}
```

### select_with_alias
Select columns with aliases (renaming).

```jsonc
{
    "function_type": "select_with_alias",
    "arguments": {
        "columns": {
            "old_name": "new_name",
            "customer_id": "id"
        }
    }
}
```

## Chaining Transforms

Functions are applied in sequence, with each function's output becoming the input for the next:

```jsonc
{
    "id": "clean-data",
    "upstream_id": "raw-data",
    "functions": [
        {"function_type": "dropduplicates", "arguments": {"columns": ["id"]}},
        {"function_type": "filter", "arguments": {"condition": "status != 'deleted'"}},
        {"function_type": "select", "arguments": {"columns": ["id", "name", "email", "status"]}},
        {"function_type": "cast", "arguments": {"columns": [
            {"column_name": "id", "cast_type": "LongType"}
        ]}}
    ]
}
```

## Custom Functions

To add custom transform functions, implement the Function interface and register with the transform registry. See the [Contributing Guide](../../CONTRIBUTING.md) for details.


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