# PySpark Ingestion Framework - Simple Example

This document provides a simple example of how to use the PySpark Ingestion Framework with the "select" transform.

## Files Included

- `data/people.csv` - Sample data file with people records
- `data/people_schema.json` - Schema definition for the data
- `data/confeti.json` - Configuration file for the ingestion framework

## Data File: people.csv

The sample CSV file contains information about people:

```
name,age,job_title
John Doe,30,Software Engineer
Jane Smith,25,Data Analyst
Michael Johnson,35,Project Manager
```

## Configuration (confeti.json)

The configuration file defines a simple ETL pipeline that:
1. Reads the CSV data file
2. Selects only the "name" and "age" columns
3. Writes the results to output CSV

```json
{
    "extracts": [
        {
            "data_format": "csv",
            "location": "examples/data/people.csv", 
            "method": "batch",
            "name": "extract-people",
            "options": {
                "delimiter": ",",
                "header": true,
                "inferSchema": false
            },
            "schema": "examples/data/people_schema.json"
        }
    ],
    "transforms": [
        {
            "name": "transform-select-columns",
            "upstream_name": "extract-people",
            "functions": [
                { 
                    "function": "select", 
                    "arguments": { 
                        "columns": ["name", "age"] 
                    } 
                }
            ]
        }
    ],
    "loads": [
        {
            "data_format": "csv",
            "location": "examples/data/output",
            "method": "batch",
            "mode": "overwrite", 
            "name": "load-people-filtered",
            "options": {},
            "schema_location": "examples/data/output_schema.json",
            "upstream_name": "transform-select-columns" 
        }
    ]
}
```

## Running the Example

To run this example:

```bash
python -m ingestion_framework --filepath examples/data/confeti.json
```

When executed, it will:
1. Read `examples/data/people.csv`
2. Select only the "name" and "age" columns
3. Write the results to `examples/data/output` folder
4. Save the output schema to `examples/data/output_schema.json`
