# pyiceberg-lakehouse

A lightweight, fully Python‑based workflow for building an Apache Iceberg table from a pure Parquet dataset using `non‑JVM tools`.

## Overview

This repository demonstrates how to:

- **Ingest and Partition Data:** Use Daft to read a Parquet file and write partitioned Parquet files based on a designated column (e.g. `"group"`).
- **Create an Iceberg Table:** Dynamically infer the table schema from the partitioned data and create an Apache Iceberg table using PyIceberg with a SQLite catalog.
- **Append Data & Track Snapshots:** Append partitioned data to the Iceberg table and view snapshot history.
- **Query the Table:** Use DuckDB’s Iceberg extension to query the Iceberg table directly.

## Getting Started

### Prerequisites

- Python 3.8+
- Install dependencies (e.g., via `pip install -r requirements.txt`):
  - daft
  - pyarrow
  - pyiceberg
  - duckdb

Ensure you have a Parquet file named `large_dataset.parquet` in the repository root.

### Run the Pipeline

Execute the main pipeline to convert your Parquet dataset into an Iceberg table.

```bash
python main.py
```
This script will:

* Read and partition the Parquet file.
* Dynamically infer the schema.
* Create an Iceberg table using a SQLite catalog.
* Append the partitioned data.
* Print the table schema and snapshot history.

### Inspect Table Metadata
To view the table’s metadata location and snapshot history, run:

```
python read_history.py
```

### Query the Iceberg Table
Query the table using DuckDB’s Iceberg extension with the latest snapshot metadata:

```
python query_iceberg_duckdb.py
```

#### Repository Structure
* `main.py`: Converts the Parquet dataset into an Apache Iceberg table.
* `read_history.py`: Loads the table from the SQLite catalog and prints metadata and * snapshot history.
* `query_iceberg_duckdb.py`: Uses DuckDB’s Iceberg extension to query the Iceberg table.

### Conclusion
This basic flow demonstrates how modern data tools can be seamlessly integrated on a single node using Python. By leveraging `Daft`, `PyIceberg` (with a `SQLite` catalog), and `DuckDB` <u>without relying on JVM-based</u> solutions—you gain a powerful foundation for building and experimenting with Lakehouse architectures.