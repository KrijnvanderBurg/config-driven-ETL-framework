stratum - ETL job, in geographical terms a stratum is multiple geographical layers. aka data layers, aka etl.
mesa - a table mountain ,a platform, aka a data platform.
tephra
barchan
arete
karst
avern
talus

[![Build Status](https://dev.azure.com/leevendaal/CompliancyCarnivalCacophony/_apis/build/status%2Fdataplatform.stratum.python?repoName=dataplatform.stratum.python&branchName=main)](https://dev.azure.com/leevendaal/CompliancyCarnivalCacophony/_build/latest?definitionId=13&repoName=dataplatform.stratum.python&branchName=main)

# stratum ingestion framework
A configuration-driven framework that empowers analytics teams to effortlessly ingest data, thus minimizing reliance on a central Data Platform team, ultimately enabling organizational efficiency and scalability.


## Inner-mechanics

The entry point is `job.py`. An instance of Job can be initialised from a Json schema called [CONFETI](#CONFETI).

### Execution flow
The `Job` class has to be initialised, followed by calling its `.execute()` method. This method internally orchestrates the execution of the three primary ETL stages: Extract, Transform, Load. Each stage calls its remodeltive Strategy which determines how the stage should be executed. Because of the strategy pattern, different implementations can be developed and it only has to be integrated in the Strategy to work.

#### Extract stage
The `Job._extract()` method invokes the data extraction process by utilising `extract.strategy.ExtractFactory.get()`. This strategy pattern dictates the data extraction strategy.
- The attribute method (batch, streaming) dictates whether to employ Spark's read or readStream functions.
- The attribute data format (e.g., file, SQL tables, Kafka topics, etc.) modelifies the corresponding Extract class implementation. For instance, selecting "parquet" results in utilizing the `extract.file` class and invokes its derived `extract()` method.

The extracted data is passed to the Transform stage.

#### Transform stage
The `Job._transform()` method invokes the data transformation process by utilising `transform.strategy.TransformFactory.get()`. This strategy pattern dynamically determines the Transform functions to execute based on the list modelified in the CONFETI file. The key `transform.function` is interpreted as a callable function, similar to a lambda function.

- The strategy identifies the function names and replaces them with its remodeltive Callable function, incorporating the provided arguments.
- Each callable function is performed on the DataFrame using `df.transform(callable)`.

The resulting transformed data is passed to the Load stage.

#### Load stage
The `Job._load()` method invokes the data loading process by utilizing `load.strategy.LoadFactory().get()`. This strategy pattern dynamically determines the load method and format for writing.

This stage is similar to extract in functionality but writes the data instead of reading. 


## CONFETI
The DataStore Ingestion Framework is configured using CONFETI files: **Conf**igurable **E**TL **t**ask **i**ngestion. CONFETI is a `json` that contains key-value pairs, with each root key representing an ETL stage and its corresponding value providing essential details for the execution of that stage.

### Extract
| Name | Description | Value |
|------|-------------|-------|
| name | Unique ID of extract process. | string |
| method | Method type to extract source. | "batch" \| "streaming" |
| data_format | Data format type of the source. | "parquet" \| "json" \| "csv" |
| location | URI of the source. | string |
| options | Dictionary of options go supply to spark. | dict |
| schema | Json format of Spark StructType schema. | string |

### Transform
| Name | Description | Value |
|------|-------------|-------|
| name | Unique ID of transform process. | string |
| transforms | List of transform function objects. | Transform |

| Name | Description | Value |
|------|-------------|-------|
| function | Name of transform function. | string |
| arguments | Arguments to pass to function. | dict |

### Load
| Name | Description | Value |
|------|-------------|-------|
| name | Unique ID of extract process. | string |
| method | Method type to extract source. | "batch" \| "streaming" |
| mode | Mode type for method to extract source. | "complete" \| "append" \| "update" |
| data_format | Data format type of the source. | "parquet" \| "json" \| "csv" |
| location | URI of the source. | string |
| options | Dictionary of options go supply to spark. | dict |

### Example CONFETI json
This example provides a template for configuring both the extract and load stages in a CONFETI JSON file. Customize these configurations based on your modelific data processing requirements.
```json
{
    "strategy": {
        "engine":"pyspark",
        "options": {},
    },
    "extract": {
        "name": "bronze-test-extract-dev",
        "method": "batch",
        "data_format": "parquet",
        "location": "/input.parquet",
        "schema": "",
    },
    "transform": {
        "name": "bronze-test-transform-dev",
        "transforms": [
            {"function": "cast", "arguments": {"columns": {"age": "string"}}},
            // etc.
        ],
    },
    "load": {
        "name": "silver-test-load-dev",
        "method": "batch",
        "data_format": "parquet",
        "mode": "complete",
        "location": "/output.parquet",
    }
}
```