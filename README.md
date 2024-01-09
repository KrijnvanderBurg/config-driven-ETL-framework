# Datastore ingestion framework
A configuration-driven framework that empowers analytics teams to effortlessly ingest data, thus minimizing reliance on a central Data Platform team, ultimately enabling organizational efficiency and scalability.

## CONFETI
The DataStore Ingestion Framework is configured using CONFETI files: **Conf**igurable **E**TL **t**ask **i**ngestion. CONFETI is a `json` that contains key-value pairs, with each root key representing an ETL stage and its corresponding value providing essential details for the execution of that stage.

### Extract
| Name | Description | Value |
|------|-------------|-------|
| spec_id | Unique ID of extract process. | string |
| method | Method type to extract source. | "batch" \| "streaming" |
| data_format | Data format type of the source. | "parquet" \| "json" \| "csv" |
| location | URI of the source. | string |
| options | Dictionary of options go supply to spark. | dict |
| schema | Json format of Spark StructType schema. | string |
| schema_filepath | Filepath to json schema file. | string |

### Transform
| Name | Description | Value |
|------|-------------|-------|
| spec_id | Unique ID of transform process. | string |
| transforms | List of transform function objects. | Transform |

| Name | Description | Value |
|------|-------------|-------|
| function | Name of transform function. | string |
| arguments | Arguments to pass to function. | dict |

### Load
| Name | Description | Value |
|------|-------------|-------|
| spec_id | Unique ID of extract process. | string |
| method | Method type to extract source. | "batch" \| "streaming" |
| operation | Operation type for method to extract source. | "complete" \| "append" \| "update" |
| data_format | Data format type of the source. | "parquet" \| "json" \| "csv" |
| location | URI of the source. | string |
| options | Dictionary of options go supply to spark. | dict |

### Example CONFETI json
This example provides a template for configuring both the extract and load stages in a CONFETI JSON file. Customize these configurations based on your specific data processing requirements.
```json
{
    "extract": {
        "spec_id": "bronze-test-extract-dev",
        "method": "batch",
        "data_format": "parquet",
        "location": "/input.parquet",
        "options": {},
        "schema": "",
        "schema_filepath": "",
    },
    "transform": {
        "spec_id": "bronze-test-transform-dev",
        "transforms": [
            {"function": "cast", "arguments": {"cols": {"age": "LongType"}}},
            // etc.
        ],
    },
    "load": {
        "spec_id": "silver-test-load-dev",
        "method": "batch",
        "data_format": "parquet",
        "operation": "complete",
        "location": "/output.parquet",
        "options": {},
    }
}
```