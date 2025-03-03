import os
import sys
import polars as pl
from pyiceberg.catalog import load_catalog

def read_existing_data_from_table(table):
    """
    Read existing data from the given Iceberg table.
    
    Returns a lazy Polars DataFrame with columns cast to the types used in the
    Iceberg table.
    """
    try:
        # Scan the Iceberg table and convert the result (a PyArrow Table) into a Polars DataFrame.
        arrow_table = table.scan().to_arrow()
        # Convert Arrow Table to Polars DataFrame and then to a LazyFrame.
        lf = pl.from_arrow(arrow_table).lazy()
        # Enforce schema: adjust the casts as needed to match your Iceberg table schema.
        lf = lf.with_columns([
            pl.col("id").cast(pl.Int32),
            pl.col("group").cast(pl.Utf8),
            pl.col("value1").cast(pl.Float32),
            pl.col("value2").cast(pl.Int32)
        ])
        return lf
    except Exception as e:
        print(f"Error reading existing data from table: {e}")
        return None

def read_new_data_lazy(new_file):
    """
    Read new data from a Parquet file and return a lazy DataFrame with columns
    cast to the types used in the Iceberg table.
    """
    if not os.path.exists(new_file):
        print(f"Error: New file {new_file} not found.")
        sys.exit(1)
    lf = pl.scan_parquet(new_file)
    return lf.with_columns([
        pl.col("id").cast(pl.Int32),
        pl.col("group").cast(pl.Utf8),
        pl.col("value1").cast(pl.Float32),
        pl.col("value2").cast(pl.Int32)
    ])

def merge_lazy(existing_lf, new_lf, key):
    """
    Merge two lazy DataFrames by concatenating them and then grouping by the key.
    When duplicate keys exist, the last record for each key is retained.
    """
    if existing_lf is None:
        return new_lf
    merged_lf = pl.concat([existing_lf, new_lf])
    # Group by key and take the last value for each field
    merged_lf = merged_lf.group_by(key).agg(pl.all().last())
    return merged_lf

def upsert_parquet_lazy(new_file, catalog_config, namespace, table_name, key):
    """
    Performs an upsert of new data from a Parquet file into an existing Iceberg table.
    
    This function:
      - Loads the Iceberg table from the catalog.
      - Reads existing data from the table via table.scan() and converts it to a
        Polars LazyFrame with enforced schema.
      - Reads new data from the provided Parquet file with the same enforced schema.
      - Merges the two datasets by grouping on the key and retaining the last record.
      - Converts the merged Polars DataFrame to an Arrow table, casts it to the target
        Iceberg schema, and overwrites the table with the merged result.
      
    Note: This read–modify–write cycle is a common workaround for upserts in Python.
    """
    # Load the catalog and the existing table.
    cat_type = catalog_config.pop("catalog_type", "sqlite")
    catalog = load_catalog(cat_type, **catalog_config)
    table_identifier = f"{namespace}.{table_name}"
    table = catalog.load_table(table_identifier)

    # Read existing data from the table.
    existing_lf = read_existing_data_from_table(table)

    # Read new data from the Parquet file.
    new_lf = read_new_data_lazy(new_file)

    # Merge the existing and new data on the given key.
    merged_lazy = merge_lazy(existing_lf, new_lf, key)
    merged_df = merged_lazy.collect()

    # Convert the merged DataFrame to an Arrow table.
    merged_arrow = merged_df.to_arrow()

    # Cast the Arrow table to match the Iceberg table schema.
    target_schema = table.schema().as_arrow()
    merged_arrow = merged_arrow.cast(target_schema)

    # Overwrite the Iceberg table with the merged data.
    table.overwrite(merged_arrow)
    print(f"Upsert committed. Merged data now contains {merged_arrow.num_rows} rows.")

if __name__ == "__main__":
    new_parquet_file = 'data/new3_large_dataset.parquet'
    catalog_config = {
        "catalog_type": "sqlite",
        "uri": "sqlite:///catalog.db",
        "warehouse": "iceberg_warehouse",
        "init_catalog_tables": "true"
    }
    namespace = "default"
    table_name = "my_iceberg_table"
    key = "id"
    upsert_parquet_lazy(new_parquet_file, catalog_config, namespace, table_name, key)
