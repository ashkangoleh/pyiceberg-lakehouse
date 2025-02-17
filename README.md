# pyiceberg-lakehouse

A lightweight, fully Python-based workflow for building an Apache Iceberg table from a pure Parquet dataset using `non-JVM tools`.

## Overview

This repository demonstrates how to:

- **Ingest and Partition Data:** Use Daft or Polars to read a Parquet file and write partitioned Parquet files based on a designated column (e.g., `group`).
- **Create an Iceberg Table:** Dynamically infer the table schema from the partitioned data and create an Apache Iceberg table using PyIceberg with either a SQLite or PostgreSQL catalog.
- **Append Data & Track Snapshots:** Append partitioned data to the Iceberg table and view snapshot history.
- **Upsert Data:** Merge new data with the existing table data through an upsert operation.
- **Query the Table:** Use DuckDB’s Iceberg extension to query the Iceberg table directly.
- **Utilize Distributed Processing:** Integrate Daft and Polars with Ray for distributed execution.
- **Use PostgreSQL as a Catalog:** Store Iceberg metadata in a PostgreSQL database instead of SQLite.

## Getting Started

### Prerequisites

- Python 3.11+
- Install dependencies using:
  ```bash
  pip install -r requirements.txt
  ```
  The key dependencies include:
  - getdaft[all]
  - getdaft[ray]
  - pyarrow
  - duckdb
  - pyiceberg
  - pyiceberg[sql-sqlite]
  - pyiceberg[sql-postgres]
  - polars

Ensure you have a Parquet file named `large_dataset.parquet` in the repository root.

### Run the Pipeline

Execute the main pipeline to convert your Parquet dataset into an Iceberg table.

#### Native Execution (Daft or Polars with SQLite Catalog)
```bash
python main-daft.py
```
or
```bash
python main-polars.py
```

#### Distributed Execution (Daft or Polars with Ray)
```bash
python main-daft-ray.py
```
or
```bash
python main-polars-ray.py
```

#### PostgreSQL Catalog Integration
```bash
python main-daft-psql.py
```
or
```bash
python main-polars-psql.py
```

This will:

- Read and partition the Parquet file.
- Dynamically infer the schema.
- Create an Iceberg table using either SQLite or PostgreSQL as a catalog.
- Append the partitioned data.
- Print the table schema and snapshot history.

### Upsert Data

A separate upsert script is provided to merge new data into the existing Iceberg table. This operation reads all partitioned files, merges them with new records based on a key (e.g., `id`), and then overwrites the table with the merged result.

Run the upsert process with:
```bash
python upsert.py
```

A variant that works with Parquet files is also available:
```bash
python upsert_parquet.py
```

### Inspect Table Metadata

To view the table’s metadata location and snapshot history, run:
```bash
python read_history.py
```

### Query the Iceberg Table

Query the table using DuckDB’s Iceberg extension with the latest snapshot metadata:
```bash
python query_iceberg_duckdb.py
```

### Repository Structure

- `main-daft.py`: Converts the Parquet dataset into an Apache Iceberg table using Daft and a SQLite catalog.
- `main-daft-psql.py`: Uses Daft with a PostgreSQL catalog.
- `main-daft-ray.py`: Runs Daft with Ray for distributed processing.
- `main-polars.py`: Uses Polars with a SQLite catalog.
- `main-polars-psql.py`: Uses Polars with a PostgreSQL catalog.
- `main-polars-ray.py`: Runs Polars with Ray for distributed processing.
- `upsert.py`: Handles upserts to the Iceberg table.
- `upsert_parquet.py`: Upserts new data using Parquet files.
- `read_history.py`: Loads the table from the catalog and prints metadata and snapshot history.
- `query_iceberg_duckdb.py`: Uses DuckDB’s Iceberg extension to query the Iceberg table.

### Deployment with Docker

A `docker-compose.yml` file is included to run PostgreSQL and Ray for the distributed processing setup.

- To start the PostgreSQL and Ray cluster:
  ```bash
  docker-compose up -d
  ```

### Conclusion

This repository provides a modern Python-based data lakehouse solution leveraging `Daft` or `Polars`, `PyIceberg` (with SQLite or PostgreSQL), and `DuckDB`—without relying on JVM-based tools. By integrating Ray, it enables distributed processing for large-scale datasets efficiently.

